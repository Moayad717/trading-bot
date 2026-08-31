"""
Tests for main.py's _reconcile_positions() — the background loop that keeps
take-profit coverage in sync with what's actually resting on Bybit.

Background on why these specific scenarios exist:

  - Coverage-source fix (2026-08-31): the reconciler used to require a DB
    signal row whose tp_order_id happened to match a currently-open order to
    count that qty as "covered". When several signals' individual TPs get
    consolidated into one bulk order (as happened on the 8003 account), the
    DB view and Bybit's view diverge permanently — DB sees each signal as
    uncovered while Bybit already has the full qty covered, so the old code
    hammered retries every 60s forever trying to "fix" a position that was
    never broken. Confirmed with the client's own numbers: DB reported 6.6
    covered / 99.3 naked while Bybit showed 99.4 fully covered. Fixed by
    reading covered_qty directly from resting orders (symbol+side+positionIdx),
    independent of any DB linkage.

  - Back-off (same date): after 3 consecutive reconciliation-cycle failures
    to place a specific signal's TP, that signal is blacklisted for the rest
    of the process's lifetime and exactly one CRITICAL log line is emitted,
    instead of repeating the same error every 60s indefinitely.

  - SL-exclusion (2026-08-31, point 2 fix follow-up): a conditional stop-loss
    order sits on the exact same symbol+side+positionIdx as its position's
    regular TP (a long's SL is Sell/positionIdx=1, same as its TP) — without
    excluding it, the coverage sum would count the SL as if it were TP
    coverage and never place the real TP the position needs.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import main


def _make_sleep_stopper(after: int):
    """Async sleep replacement: lets `after` calls through, then raises to
    break out of the reconciler's `while True` loop deterministically."""
    calls = {"n": 0}

    async def _sleep(_seconds):
        calls["n"] += 1
        if calls["n"] > after:
            raise asyncio.CancelledError("test: stopping loop after N iterations")

    return _sleep


async def _run_n_cycles(n: int, exchange_mock, signals_by_symbol_action: dict):
    """Drive _reconcile_positions() for exactly n loop iterations against a
    mocked exchange + DB layer, then return cleanly."""
    with patch("main.asyncio.sleep", new=_make_sleep_stopper(n)), \
         patch("main.BybitExchange", return_value=exchange_mock), \
         patch("main.get_active_signals_needing_tp", new=AsyncMock(
             side_effect=lambda symbol, action: signals_by_symbol_action.get((symbol, action), [])
         )), \
         patch("main.set_tp_order_id_sync"):
        try:
            await main._reconcile_positions()
        except asyncio.CancelledError:
            pass


def _make_exchange(position, open_orders, place_tp_side_effect):
    ex = MagicMock()
    ex.get_positions.return_value = [position]
    ex.get_all_open_orders.return_value = open_orders
    ex.place_tp_order.side_effect = place_tp_side_effect
    ex.get_order_history.return_value = []
    return ex


def test_bulk_coverage_recognised():
    """A position fully covered by one bulk resting order, with no DB signal
    individually linked to it, must trigger zero redundant TP placements."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "96.6", "avgPrice": "11.0"}
    bulk_order = {
        "orderId": "bulk-order-id", "symbol": "LINKUSDT", "side": "Buy",
        "qty": "96.6", "positionIdx": 2, "reduceOnly": True,
    }
    orphan_signals = [
        {"id": i, "quantity": 1.3, "take_profit": 9.353, "tp_order_id": f"stale-{i}",
         "order_id": f"entry-{i}", "category": "linear"}
        for i in range(1015, 1027)
    ]
    exchange = _make_exchange(position, [bulk_order],
                               place_tp_side_effect=Exception("should not be called"))

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    asyncio.run(_run_n_cycles(1, exchange, {("LINKUSDT", "sell"): orphan_signals}))

    assert exchange.place_tp_order.call_count == 0, (
        "place_tp_order was called for a position Bybit already shows as fully "
        "covered — the coverage fix regressed, orphaned DB rows are being "
        "treated as naked again."
    )


def test_backoff_blacklists_after_3_failures():
    """A signal whose TP placement always fails gets retried exactly 3 times,
    then blacklisted with exactly one CRITICAL log line — never a 4th attempt
    and never a second CRITICAL for the same signal."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "10.0", "avgPrice": "11.0"}
    failing_signal = [{
        "id": 9999, "quantity": 10.0, "take_profit": 11.5,
        "tp_order_id": None, "order_id": "entry-9999", "category": "linear",
    }]
    # Only the per-signal placement (price=11.5) fails; the ghost-bulk
    # fallback (a different price/call) succeeds harmlessly so it doesn't
    # pollute the per-signal call count this test isolates.
    per_signal_calls = {"n": 0}

    def _side_effect(**kwargs):
        if kwargs.get("price") == 11.5:
            per_signal_calls["n"] += 1
            raise RuntimeError("simulated Bybit error")
        return {"order_id": "ghost-tp-id"}

    exchange = _make_exchange(position, [], place_tp_side_effect=_side_effect)

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()

    critical_logs = []
    orig_critical = main.logger.critical

    def _capture_critical(msg, *args, **kwargs):
        critical_logs.append(msg % args if args else msg)
        return orig_critical(msg, *args, **kwargs)

    with patch.object(main.logger, "critical", side_effect=_capture_critical):
        asyncio.run(_run_n_cycles(5, exchange, {("LINKUSDT", "sell"): failing_signal}))

    assert per_signal_calls["n"] == 3, (
        f"expected exactly 3 placement attempts before backoff kicks in, got "
        f"{per_signal_calls['n']}"
    )
    assert 9999 in main._tp_giveup, "signal was not blacklisted after 3 failures"
    giveup_logs = [m for m in critical_logs if "GIVING UP" in m and "9999" in m]
    assert len(giveup_logs) == 1, (
        f"expected exactly 1 CRITICAL giveup log, got {len(giveup_logs)} — "
        f"'log once, not every cycle' requirement violated"
    )


def test_sl_orders_excluded_from_coverage():
    """A conditional SL order sitting on the same side+positionIdx as its
    position's TP must not be counted as TP coverage."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "4.4", "avgPrice": "11.28"}
    sl_order = {
        "orderId": "sl-order-1", "orderLinkId": "flow1_SL", "symbol": "LINKUSDT",
        "side": "Sell", "qty": "4.4", "positionIdx": 1, "reduceOnly": False,
        "triggerPrice": "9.500",
    }
    signal = [{
        "id": 5001, "quantity": 4.4, "take_profit": 12.0,
        "tp_order_id": None, "order_id": "entry-5001", "category": "linear",
    }]
    exchange = _make_exchange(position, [sl_order],
                               place_tp_side_effect=lambda **kw: {"order_id": "new-tp-1"})

    main._tp_failure_counts.clear()
    main._tp_giveup.clear()
    asyncio.run(_run_n_cycles(1, exchange, {("LINKUSDT", "buy"): signal}))

    assert exchange.place_tp_order.call_count == 1, (
        "expected the reconciler to place a real TP — the SL order at the same "
        "side+positionIdx was incorrectly counted as TP coverage, so the "
        "position never got its actual take-profit."
    )
