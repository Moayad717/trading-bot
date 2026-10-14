"""
Tests for main.py's _fix_missing_tps() — the background loop that finds
entries with no take-profit and gives each one its own, confirmed directly
against Bybit's own order tags.

Background — why this replaced the old reconciler (2026-10-14):

  The previous version computed "naked qty" correctly (position size minus
  real resting coverage) but, for any leftover qty it couldn't match to a
  DB row, fell back to ONE combined "ghost" TP at an averaged price. That
  was wrong twice over, both confirmed live: (1) the coverage filter
  excluded any order with a triggerPrice, which wrongly excludes Bybit's
  own PartialTakeProfit orders too, so a manually-placed TP wasn't counted
  and a duplicate got placed on top of it; (2) even with that fixed,
  collapsing several distinct signals into one bulk order throws away the
  one thing that makes each individually auditable — its own tag. Every
  entry already carries a unique tag (<of_id>_E or _C); this version reads
  it directly off Bybit instead of guessing with an average.

  For each naked qty, it now pulls recent order history, finds every filled
  entry whose tag has no take-profit anywhere (open or in history), looks
  up THAT signal's own recorded take-profit price by its own entry
  order_id, and places one TP at exactly that price and that entry's own
  filled qty. An entry with no tag, or a tag with no matching database
  record, is left alone and logged — never guessed, never folded into an
  average.

  - Coverage-filter fix (2026-09-16, carried over unchanged): excludes only
    genuine stop-losses (Bybit's StopLoss/PartialStopLoss stopOrderType, or
    an orderLinkId ending in "_SL") — not any order with a triggerPrice.
  - Back-off (carried over unchanged): after 3 consecutive failures to
    place a specific signal's TP, it is blacklisted for the rest of the
    process's lifetime and exactly one CRITICAL log line is emitted.
"""
import asyncio
import sqlite3
from unittest.mock import AsyncMock, MagicMock, patch

import main
from tests.conftest import insert_signal


def _set_columns(db_path: str, signal_id: int, **cols) -> None:
    conn = sqlite3.connect(db_path)
    for col, val in cols.items():
        conn.execute(f"UPDATE signals SET {col}=? WHERE id=?", (val, signal_id))
    conn.commit()
    conn.close()


def _make_sleep_stopper(after: int):
    """Async sleep replacement: lets `after` calls through, then raises to
    break out of the loop's `while True` deterministically."""
    calls = {"n": 0}

    async def _sleep(_seconds):
        calls["n"] += 1
        if calls["n"] > after:
            raise asyncio.CancelledError("test: stopping loop after N iterations")

    return _sleep


async def _run_n_cycles(n: int, exchange_mock):
    with patch("main.asyncio.sleep", new=_make_sleep_stopper(n)), \
         patch("main.BybitExchange", return_value=exchange_mock):
        try:
            await main._fix_missing_tps()
        except asyncio.CancelledError:
            pass


def _history_page(orders):
    """One page of order history, no further cursor — matches how
    exchange._client.get_order_history(**params) replies."""
    return {"result": {"list": orders, "nextPageCursor": ""}}


def _make_exchange(position, open_orders, history_orders, place_tp_side_effect, qty_step=None):
    ex = MagicMock()
    ex.get_positions.return_value = [position]
    ex.get_all_open_orders.return_value = open_orders
    ex.get_order_history.return_value = []  # only used by the cancelled-TP check
    ex.place_tp_order.side_effect = place_tp_side_effect
    ex._client.get_order_history.return_value = _history_page(history_orders)
    if qty_step is None:
        ex.round_qty.side_effect = lambda qty, symbol, category="linear": qty
    else:
        from decimal import ROUND_DOWN, Decimal
        def _round(qty, symbol, category="linear", _step=qty_step):
            step_d = Decimal(str(_step))
            return float(Decimal(str(qty)).quantize(step_d, rounding=ROUND_DOWN))
        ex.round_qty.side_effect = _round
    return ex


def _entry_order(order_id, of_id, side, position_idx, qty, updated_ms=1700000000000):
    # Role is always "E" for a plain entry regardless of Buy/Sell — direction
    # lives in the of_id's own L/S suffix, not in the role suffix.
    return {
        "orderId": order_id, "orderLinkId": f"{of_id}_E",
        "symbol": "LINKUSDT", "side": side, "positionIdx": position_idx,
        "qty": str(qty), "orderStatus": "Filled", "reduceOnly": False,
        "updatedTime": str(updated_ms),
    }


def test_tagged_entry_gets_its_own_individual_tp(tmp_db):
    """Single tagged entry explaining a naked gap gets its own TP, at its
    own recorded price — the core case this whole mechanism exists for."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "avgPrice": "11.0", "positionIdx": 1}
    entry = _entry_order("entry-1", "LINK-20261013-1500-0050m-L", "Buy", 1, 2.0)

    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-20261013-1500-0050m-L",
                            quantity=2.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-1", take_profit=11.5)

    exchange = _make_exchange(position, [], [entry],
                               place_tp_side_effect=lambda **kw: {"order_id": "tp-1"})

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    asyncio.run(_run_n_cycles(1, exchange))

    exchange.place_tp_order.assert_called_once()
    kwargs = exchange.place_tp_order.call_args.kwargs
    assert kwargs["qty"] == 2.0
    assert kwargs["price"] == 11.5
    assert kwargs["side"] == "Sell"
    assert kwargs["order_link_id_base"] == "LINK-20261013-1500-0050m-L_TP"

    conn = sqlite3.connect(tmp_db)
    row = conn.execute("SELECT status, tp_order_id FROM signals WHERE id=?", (sig_id,)).fetchone()
    conn.close()
    assert row == ("active", "tp-1")


def test_two_tagged_entries_get_two_separate_tps(tmp_db):
    """The exact guarantee this mechanism replaced the old one for: two
    distinct signals making up one naked gap must each get their OWN TP at
    their OWN price — never a single combined order for the total qty."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "5.0", "avgPrice": "12.0", "positionIdx": 2}
    entry_a = _entry_order("entry-a", "LINK-20261013-1000-0040m-S", "Sell", 2, 2.0)
    entry_b = _entry_order("entry-b", "LINK-20261013-1100-0045m-S", "Sell", 2, 3.0)

    sig_a = insert_signal(tmp_db, action="sell", of_id="LINK-20261013-1000-0040m-S",
                           quantity=2.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_a, order_id="entry-a", take_profit=11.8)
    sig_b = insert_signal(tmp_db, action="sell", of_id="LINK-20261013-1100-0045m-S",
                           quantity=3.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_b, order_id="entry-b", take_profit=11.9)

    placed = []
    def _side_effect(**kw):
        placed.append(kw)
        return {"order_id": f"tp-{len(placed)}"}

    exchange = _make_exchange(position, [], [entry_a, entry_b], place_tp_side_effect=_side_effect)

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    asyncio.run(_run_n_cycles(1, exchange))

    assert exchange.place_tp_order.call_count == 2, (
        "two distinct tagged entries must produce two separate TP orders, "
        "never one combined order for the total naked qty"
    )
    qtys_prices = {(round(c["qty"], 3), c["price"]) for c in placed}
    assert qtys_prices == {(2.0, 11.8), (3.0, 11.9)}


def test_untagged_entry_is_left_alone(tmp_db):
    """An entry with no tag (predates tagging) contributing to a naked gap
    must never get a guessed/averaged TP — left alone, logged, nothing
    placed for it."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "4.0", "avgPrice": "10.0", "positionIdx": 1}
    untagged_entry = {
        "orderId": "old-entry", "orderLinkId": "", "symbol": "LINKUSDT", "side": "Buy",
        "positionIdx": 1, "qty": "4.0", "orderStatus": "Filled", "reduceOnly": False,
        "updatedTime": "1700000000000",
    }
    exchange = _make_exchange(position, [], [untagged_entry],
                               place_tp_side_effect=Exception("should never be called"))

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    asyncio.run(_run_n_cycles(1, exchange))

    exchange.place_tp_order.assert_not_called()


def test_cancelled_tp_warning_logs_once_per_order_id(tmp_db, caplog):
    """The 'Fix-TP: Cancelled TP detected' warning used to repeat every
    cycle for as long as the same cancelled order stayed within the
    last-50 order-history window (2026-10-14, reported as log spam). It
    must still fire once, but never again for the same order_id."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "avgPrice": "11.0", "positionIdx": 1}
    covering_tp = {
        "symbol": "LINKUSDT", "side": "Sell", "positionIdx": 1, "qty": "2.0",
        "orderLinkId": "flow1_TP", "reduceOnly": True,
    }
    exchange = _make_exchange(position, [covering_tp], [],
                               place_tp_side_effect=Exception("should never be called"))
    exchange.get_order_history.return_value = [{
        "orderId": "cancelled-tp-1", "symbol": "LINKUSDT", "reduceOnly": True,
        "orderStatus": "Cancelled",
    }]

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    main._cancelled_tp_warned.clear()
    with caplog.at_level("WARNING"):
        asyncio.run(_run_n_cycles(3, exchange))

    warnings = [r for r in caplog.records if "Cancelled TP detected" in r.message]
    assert len(warnings) == 1, "must warn exactly once across 3 cycles, not once per cycle"
    exchange.place_tp_order.assert_not_called()  # fully covered — no placement either


def test_tagged_entry_with_no_db_record_is_left_alone(tmp_db):
    """A tagged entry whose database row no longer exists (or never did) is
    left alone rather than guessed at — only a confirmed, recorded
    take-profit price is ever used."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "1.5", "avgPrice": "10.0", "positionIdx": 1}
    entry = _entry_order("orphan-entry", "LINK-20260101-0000-0010m-L", "Buy", 1, 1.5)

    exchange = _make_exchange(position, [], [entry],
                               place_tp_side_effect=Exception("should never be called"))

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    asyncio.run(_run_n_cycles(1, exchange))

    exchange.place_tp_order.assert_not_called()


def test_entry_already_covered_by_its_own_tp_is_skipped(tmp_db):
    """An entry whose tag already has a take-profit resting (or in history)
    must not get a second one placed for it."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "avgPrice": "11.0", "positionIdx": 1}
    entry = _entry_order("entry-covered", "LINK-20261013-1500-0050m-L", "Buy", 1, 2.0)
    existing_tp = {
        "orderId": "tp-existing", "orderLinkId": "LINK-20261013-1500-0050m-L_TP",
        "symbol": "LINKUSDT", "side": "Sell", "positionIdx": 1, "qty": "2.0", "reduceOnly": True,
    }

    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-20261013-1500-0050m-L",
                            quantity=2.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-covered", take_profit=11.5)

    exchange = _make_exchange(position, [existing_tp], [entry],
                               place_tp_side_effect=Exception("should never be called"))

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    asyncio.run(_run_n_cycles(1, exchange))

    exchange.place_tp_order.assert_not_called()


def test_partial_take_profit_counted_as_coverage(tmp_db):
    """A manually-placed PartialTakeProfit order carries a triggerPrice just
    like a real SL does, but it is NOT a stop-loss — it must count as
    coverage, so no gap is even detected."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.6", "avgPrice": "10.71", "positionIdx": 1}
    manual_partial_tp = {
        "orderId": "manual-tp-1", "orderLinkId": "", "symbol": "LINKUSDT",
        "side": "Sell", "qty": "2.6", "positionIdx": 1, "reduceOnly": True,
        "triggerPrice": "10.904", "stopOrderType": "PartialTakeProfit",
    }
    exchange = _make_exchange(position, [manual_partial_tp], [],
                               place_tp_side_effect=Exception("should not be called"))

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    asyncio.run(_run_n_cycles(1, exchange))

    exchange.place_tp_order.assert_not_called()
    exchange._client.get_order_history.assert_not_called()  # no gap -> no history pull needed


def test_real_stop_loss_stop_order_type_still_excluded_from_coverage(tmp_db):
    """Bybit's own StopLoss/PartialStopLoss stopOrderType must still be
    excluded from coverage even without an "_SL"-suffixed orderLinkId."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "4.4", "avgPrice": "11.28", "positionIdx": 1}
    native_sl = {
        "orderId": "native-sl-1", "orderLinkId": "", "symbol": "LINKUSDT",
        "side": "Sell", "qty": "4.4", "positionIdx": 1, "reduceOnly": False,
        "triggerPrice": "9.500", "stopOrderType": "StopLoss",
    }
    entry = _entry_order("entry-x", "LINK-20261013-1500-0050m-L", "Buy", 1, 4.4)
    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-20261013-1500-0050m-L",
                            quantity=4.4, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-x", take_profit=12.0)

    exchange = _make_exchange(position, [native_sl], [entry],
                               place_tp_side_effect=lambda **kw: {"order_id": "new-tp"})

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    asyncio.run(_run_n_cycles(1, exchange))

    assert exchange.place_tp_order.call_count == 1, (
        "a native Bybit StopLoss order (no '_SL' orderLinkId tag) was wrongly "
        "counted as TP coverage — the position never got its real TP."
    )


def test_qty_rounded_to_step(tmp_db):
    """The qty sent to place_tp_order must be the entry's own real filled
    qty, rounded to the symbol's qtyStep — not a derived/guessed amount."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "1.4", "avgPrice": "11.0", "positionIdx": 2}
    entry = _entry_order("entry-step", "LINK-20261013-0900-0043m-S", "Sell", 2, "1.7406440382941688")

    sig_id = insert_signal(tmp_db, action="sell", of_id="LINK-20261013-0900-0043m-S",
                            quantity=1.7406440382941688, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-step", take_profit=10.88714)

    exchange = _make_exchange(position, [], [entry],
                               place_tp_side_effect=lambda **kw: {"order_id": "tp-ok"},
                               qty_step=0.1)

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    asyncio.run(_run_n_cycles(1, exchange))

    assert exchange.place_tp_order.call_count == 1
    sent_qty = exchange.place_tp_order.call_args.kwargs["qty"]
    assert (sent_qty * 10) == int(sent_qty * 10), (
        f"qty={sent_qty} is not a valid multiple of qtyStep=0.1"
    )


def test_backoff_blacklists_after_3_failures(tmp_db):
    """A signal whose TP placement always fails gets retried exactly 3
    times, then blacklisted with exactly one CRITICAL log line."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "10.0", "avgPrice": "11.0", "positionIdx": 2}
    entry = _entry_order("entry-fail", "LINK-20261013-0800-0060m-S", "Sell", 2, 10.0)

    sig_id = insert_signal(tmp_db, action="sell", of_id="LINK-20261013-0800-0060m-S",
                            quantity=10.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-fail", take_profit=11.5)

    exchange = _make_exchange(position, [], [entry],
                               place_tp_side_effect=RuntimeError("simulated Bybit error"))

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()

    critical_logs = []
    orig_critical = main.logger.critical

    def _capture_critical(msg, *args, **kwargs):
        critical_logs.append(msg % args if args else msg)
        return orig_critical(msg, *args, **kwargs)

    with patch.object(main.logger, "critical", side_effect=_capture_critical):
        asyncio.run(_run_n_cycles(5, exchange))

    assert exchange.place_tp_order.call_count == 3, (
        f"expected exactly 3 placement attempts before backoff kicks in, got "
        f"{exchange.place_tp_order.call_count}"
    )
    assert sig_id in main._tp_giveup, "signal was not blacklisted after 3 failures"
    giveup_logs = [m for m in critical_logs if "GIVING UP" in m and str(sig_id) in m]
    assert len(giveup_logs) == 1, (
        f"expected exactly 1 CRITICAL giveup log, got {len(giveup_logs)} — "
        f"'log once, not every cycle' requirement violated"
    )
