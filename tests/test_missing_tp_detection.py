"""
Tests for missing_tp.find_naked_entries — the detection logic behind
main.py's missing-TP notifications (2026-10-19).

Background: this replaced automatic TP placement after two real incidents
on 8003. 10-13: a TP got placed for a signal Mohammad had already closed by
hand with his own partial TP, days earlier — the old check only looked for
a TAGGED closing order, which can't see a manual close. 10-16: the same
shape of mistake recurred for two different signals. Both filled instantly
against unrelated trades, costing real money (confirmed: -5.02 USDT).

Two safety checks added here, on top of everything the old mechanism
already did correctly (tag-based individual TPs, never bulk/averaged):
  1. already_closed_by_someone — a real closing execution for roughly the
     entry's own qty, after its own fill time, already exists — tagged or
     not. If so, the entry is left out entirely.
  2. price_passed — the current mark price has already gone past where
     this TP would sit. Flagged via needs_manual_handling=True rather than
     silently dropped, since it's still a real gap — just not safe to
     propose as a normal limit TP.
  3. tracked_symbols — only settings.FIX_TP_SYMBOLS are scanned at all
     (previously this looked at every symbol with an open position,
     including a user's own unrelated manual trades on other coins).
"""
from decimal import ROUND_DOWN, Decimal
from unittest.mock import MagicMock

from tests.conftest import insert_signal

from missing_tp import find_naked_entries


def _set_columns(db_path: str, signal_id: int, **cols) -> None:
    import sqlite3
    conn = sqlite3.connect(db_path)
    for col, val in cols.items():
        conn.execute(f"UPDATE signals SET {col}=? WHERE id=?", (val, signal_id))
    conn.commit()
    conn.close()


def _history_page(orders):
    return {"result": {"list": orders, "nextPageCursor": ""}}


def _entry_order(order_id, of_id, side, position_idx, qty, updated_ms=1700000000000):
    return {
        "orderId": order_id, "orderLinkId": f"{of_id}_E",
        "symbol": "LINKUSDT", "side": side, "positionIdx": position_idx,
        "qty": str(qty), "orderStatus": "Filled", "reduceOnly": False,
        "updatedTime": str(updated_ms),
    }


def _exec(order_id, order_link_id, side, closed_size, exec_time_ms, symbol="LINKUSDT"):
    return {
        "orderId": order_id, "orderLinkId": order_link_id, "side": side,
        "closedSize": str(closed_size), "execTime": str(exec_time_ms), "symbol": symbol,
    }


def _make_exchange(position, open_orders, history_orders, closing_execs=None, qty_step=None):
    ex = MagicMock()
    ex.get_positions.return_value = [position]
    ex.get_all_open_orders.return_value = open_orders
    ex._client.get_order_history.return_value = _history_page(history_orders)
    ex.get_execution_history.return_value = closing_execs or []
    if qty_step is None:
        ex.round_qty.side_effect = lambda qty, symbol, category="linear": qty
        ex._get_qty_step.return_value = "0.1"
    else:
        def _round(qty, symbol, category="linear", _step=qty_step):
            step_d = Decimal(str(_step))
            return float(Decimal(str(qty)).quantize(step_d, rounding=ROUND_DOWN))
        ex.round_qty.side_effect = _round
        ex._get_qty_step.return_value = str(qty_step)
    return ex


def test_tagged_entry_with_real_gap_is_a_candidate(tmp_db):
    """The core case: a naked gap explained by one tagged entry becomes one
    safe-to-confirm candidate."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "avgPrice": "11.0",
                "positionIdx": 1, "markPrice": "11.2"}
    entry = _entry_order("entry-1", "LINK-20261013-1500-0050m-L", "Buy", 1, 2.0)

    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-20261013-1500-0050m-L",
                            quantity=2.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-1", take_profit=11.5)

    exchange = _make_exchange(position, [], [entry])
    candidates = find_naked_entries(exchange)

    assert len(candidates) == 1
    c = candidates[0]
    assert c["signal_id"] == sig_id
    assert c["qty"] == 2.0
    assert c["price"] == 11.5
    assert c["side"] == "Sell"
    assert c["needs_manual_handling"] is False


def test_two_tagged_entries_get_two_separate_candidates(tmp_db):
    """Never combined/averaged — each entry is its own candidate."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "5.0", "avgPrice": "12.0",
                "positionIdx": 2, "markPrice": "11.9"}
    entry_a = _entry_order("entry-a", "LINK-20261013-1000-0040m-S", "Sell", 2, 2.0)
    entry_b = _entry_order("entry-b", "LINK-20261013-1100-0045m-S", "Sell", 2, 3.0)

    sig_a = insert_signal(tmp_db, action="sell", of_id="LINK-20261013-1000-0040m-S",
                           quantity=2.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_a, order_id="entry-a", take_profit=11.8)
    sig_b = insert_signal(tmp_db, action="sell", of_id="LINK-20261013-1100-0045m-S",
                           quantity=3.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_b, order_id="entry-b", take_profit=11.9)

    exchange = _make_exchange(position, [], [entry_a, entry_b])
    candidates = find_naked_entries(exchange)

    assert len(candidates) == 2
    qtys_prices = {(c["qty"], c["price"]) for c in candidates}
    assert qtys_prices == {(2.0, 11.8), (3.0, 11.9)}


def test_untagged_entry_is_left_alone(tmp_db):
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "4.0", "avgPrice": "10.0",
                "positionIdx": 1, "markPrice": "10.1"}
    untagged_entry = {
        "orderId": "old-entry", "orderLinkId": "", "symbol": "LINKUSDT", "side": "Buy",
        "positionIdx": 1, "qty": "4.0", "orderStatus": "Filled", "reduceOnly": False,
        "updatedTime": "1700000000000",
    }
    exchange = _make_exchange(position, [], [untagged_entry])
    candidates = find_naked_entries(exchange)
    assert candidates == []


def test_tagged_entry_with_no_db_record_is_left_alone(tmp_db):
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "1.5", "avgPrice": "10.0",
                "positionIdx": 1, "markPrice": "10.1"}
    entry = _entry_order("orphan-entry", "LINK-20260101-0000-0010m-L", "Buy", 1, 1.5)
    exchange = _make_exchange(position, [], [entry])
    candidates = find_naked_entries(exchange)
    assert candidates == []


def test_entry_already_covered_by_its_own_tagged_tp_is_skipped(tmp_db):
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "avgPrice": "11.0",
                "positionIdx": 1, "markPrice": "11.1"}
    entry = _entry_order("entry-cov", "LINK-20261013-1500-0050m-L", "Buy", 1, 2.0)
    existing_tp = {
        "symbol": "LINKUSDT", "side": "Sell", "positionIdx": 1, "qty": "2.0",
        "orderLinkId": "LINK-20261013-1500-0050m-L_TP",
    }
    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-20261013-1500-0050m-L",
                            quantity=2.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-cov", take_profit=11.5)

    exchange = _make_exchange(position, [existing_tp], [entry])
    candidates = find_naked_entries(exchange)
    assert candidates == []


def test_partial_take_profit_counted_as_coverage(tmp_db):
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "avgPrice": "11.0",
                "positionIdx": 1, "markPrice": "11.1"}
    entry = _entry_order("entry-ptp", "LINK-20261013-1500-0050m-L", "Buy", 1, 2.0)
    partial_tp = {
        "symbol": "LINKUSDT", "side": "Sell", "positionIdx": 1, "qty": "2.0",
        "orderLinkId": "", "stopOrderType": "PartialTakeProfit", "triggerPrice": "11.5",
    }
    exchange = _make_exchange(position, [partial_tp], [entry])
    candidates = find_naked_entries(exchange)
    assert candidates == []  # fully covered by the partial TP — no gap at all


def test_real_stop_loss_not_counted_as_tp_coverage(tmp_db):
    """A genuine SL sits on the same side/positionIdx as a TP would — it
    must NOT count as TP coverage, or a real gap would be masked."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "avgPrice": "11.0",
                "positionIdx": 1, "markPrice": "11.1"}
    entry = _entry_order("entry-sl", "LINK-20261013-1500-0050m-L", "Buy", 1, 2.0)
    sl_order = {
        "symbol": "LINKUSDT", "side": "Sell", "positionIdx": 1, "qty": "2.0",
        "orderLinkId": "", "stopOrderType": "StopLoss",
    }
    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-20261013-1500-0050m-L",
                            quantity=2.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-sl", take_profit=11.5)

    exchange = _make_exchange(position, [sl_order], [entry])
    candidates = find_naked_entries(exchange)
    assert len(candidates) == 1  # SL doesn't count — still naked, still reported


# ── The two incidents this module exists to prevent ──────────────────────────

def test_entry_already_closed_by_untagged_order_is_never_a_candidate(tmp_db):
    """The exact 10-16 incident shape: an entry whose own TP tag was
    cancelled, but a human already closed the same quantity by hand with an
    untagged order. Must never be proposed again."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "100.0", "avgPrice": "12.5",
                "positionIdx": 2, "markPrice": "12.3"}
    entry = _entry_order("entry-1678", "LINK-20261012-2100-0060m-S", "Sell", 2, 0.7,
                          updated_ms=1760000000000)
    # A human's own untagged manual close, filled AFTER the entry, same qty.
    manual_close = _exec("manual-close-1", "", "Buy", 0.7, 1760500000000)

    sig_id = insert_signal(tmp_db, action="sell", of_id="LINK-20261012-2100-0060m-S",
                            quantity=0.7, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-1678", take_profit=12.78366)

    exchange = _make_exchange(position, [], [entry], closing_execs=[manual_close])
    candidates = find_naked_entries(exchange)

    assert candidates == [], "already closed by hand — must never be re-proposed"


def test_entry_already_closed_by_tagged_partial_close_is_never_a_candidate(tmp_db):
    """Same check, but the closing execution happens to carry a tag — must
    still work (tagged or not is explicitly the point)."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "10.0", "avgPrice": "13.0",
                "positionIdx": 1, "markPrice": "13.2"}
    entry = _entry_order("entry-1604", "LINK-20261010-0126-0043m-L", "Buy", 1, 3.5,
                          updated_ms=1760000000000)
    tagged_close = _exec("tagged-close-1", "LINK-20261010-0126-0043m-L_TP2", "Sell", 3.5, 1760500000000)

    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-20261010-0126-0043m-L",
                            quantity=3.5, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-1604", take_profit=12.953)

    exchange = _make_exchange(position, [], [entry], closing_execs=[tagged_close])
    candidates = find_naked_entries(exchange)

    assert candidates == []


def test_closing_execution_before_entry_fill_does_not_count(tmp_db):
    """An execution from BEFORE this entry even filled can't be proof it
    was closed — must still be reported as a real candidate."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "0.7", "avgPrice": "12.5",
                "positionIdx": 2, "markPrice": "12.3"}
    entry = _entry_order("entry-x", "LINK-20261013-0000-0040m-S", "Sell", 2, 0.7,
                          updated_ms=1760000000000)
    old_exec = _exec("old-exec", "", "Buy", 0.7, 1759000000000)  # before entry_fill

    sig_id = insert_signal(tmp_db, action="sell", of_id="LINK-20261013-0000-0040m-S",
                            quantity=0.7, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-x", take_profit=12.4)

    exchange = _make_exchange(position, [], [entry], closing_execs=[old_exec])
    candidates = find_naked_entries(exchange)

    assert len(candidates) == 1


def test_price_already_passed_for_short_flags_manual_handling(tmp_db):
    """Price has already fallen through the short's TP level — a Buy limit
    there would fill instantly. Still reported (real gap), but flagged."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "0.7", "avgPrice": "12.87",
                "positionIdx": 2, "markPrice": "12.286"}  # below both TP prices
    entry = _entry_order("entry-passed-s", "LINK-20261011-0200-0120m-S", "Sell", 2, 0.7)

    sig_id = insert_signal(tmp_db, action="sell", of_id="LINK-20261011-0200-0120m-S",
                            quantity=0.7, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-passed-s", take_profit=12.81354)

    exchange = _make_exchange(position, [], [entry])
    candidates = find_naked_entries(exchange)

    assert len(candidates) == 1
    assert candidates[0]["needs_manual_handling"] is True


def test_price_already_passed_for_long_flags_manual_handling(tmp_db):
    """Symmetric case: price already rose through a long's TP level."""
    position = {"symbol": "LINKUSDT", "side": "Buy", "size": "2.0", "avgPrice": "11.0",
                "positionIdx": 1, "markPrice": "11.9"}  # above the TP price
    entry = _entry_order("entry-passed-l", "LINK-20261013-1500-0050m-L", "Buy", 1, 2.0)

    sig_id = insert_signal(tmp_db, action="buy", of_id="LINK-20261013-1500-0050m-L",
                            quantity=2.0, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-passed-l", take_profit=11.5)

    exchange = _make_exchange(position, [], [entry])
    candidates = find_naked_entries(exchange)

    assert len(candidates) == 1
    assert candidates[0]["needs_manual_handling"] is True


def test_price_not_yet_passed_does_not_flag_manual_handling(tmp_db):
    """Sanity check for the boundary itself — a TP still ahead of price is
    a completely normal, safe-to-confirm candidate."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "0.7", "avgPrice": "12.87",
                "positionIdx": 2, "markPrice": "12.9"}  # price still above the TP (hasn't fallen yet)
    entry = _entry_order("entry-ok-s", "LINK-20261011-0200-0120m-S", "Sell", 2, 0.7)

    sig_id = insert_signal(tmp_db, action="sell", of_id="LINK-20261011-0200-0120m-S",
                            quantity=0.7, symbol="LINKUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-ok-s", take_profit=12.81354)

    exchange = _make_exchange(position, [], [entry])
    candidates = find_naked_entries(exchange)

    assert len(candidates) == 1
    assert candidates[0]["needs_manual_handling"] is False


def test_untracked_symbol_is_never_even_looked_at(tmp_db, monkeypatch):
    """A user's own manual position on an unrelated coin (e.g. ILVUSDT)
    must never be scanned, let alone acted on — the exact 10-16 finding."""
    position = {"symbol": "ILVUSDT", "side": "Buy", "size": "70.56", "avgPrice": "5.0",
                "positionIdx": 1, "markPrice": "5.1"}
    exchange = _make_exchange(position, [], [])
    candidates = find_naked_entries(exchange)

    assert candidates == []
    exchange._client.get_order_history.assert_not_called()
    exchange.get_execution_history.assert_not_called()


def test_tracked_symbols_is_configurable(monkeypatch, tmp_db):
    from config import settings
    monkeypatch.setattr(settings, "FIX_TP_SYMBOLS", "LINKUSDT,ETHUSDT")

    position = {"symbol": "ETHUSDT", "side": "Buy", "size": "1.0", "avgPrice": "2000.0",
                "positionIdx": 1, "markPrice": "2010.0"}
    entry = _entry_order("entry-eth", "ETH-20261013-1500-0050m-L", "Buy", 1, 1.0)

    sig_id = insert_signal(tmp_db, action="buy", of_id="ETH-20261013-1500-0050m-L",
                            quantity=1.0, symbol="ETHUSDT", category="linear", status="active")
    _set_columns(tmp_db, sig_id, order_id="entry-eth", take_profit=2050.0)

    exchange = _make_exchange(position, [], [entry])
    candidates = find_naked_entries(exchange)

    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "ETHUSDT"


def test_counter_pattern_type_is_preserved_in_candidate(tmp_db):
    """Not asserting the tag here (confirm_missing_tp builds it) — just that
    pattern_type survives through so the confirm step can tag it CTP."""
    position = {"symbol": "LINKUSDT", "side": "Sell", "size": "1.5", "avgPrice": "12.0",
                "positionIdx": 2, "markPrice": "11.9"}
    entry = _entry_order("entry-ctr", "LINK-20261013-1500-0050m-S", "Sell", 2, 1.5)

    sig_id = insert_signal(tmp_db, action="sell", of_id="LINK-20261013-1500-0050m-S",
                            quantity=1.5, symbol="LINKUSDT", category="linear", status="active",
                            pattern_type="COUNTER")
    _set_columns(tmp_db, sig_id, order_id="entry-ctr", take_profit=11.8)

    exchange = _make_exchange(position, [], [entry])
    candidates = find_naked_entries(exchange)

    assert len(candidates) == 1
    assert candidates[0]["pattern_type"] == "COUNTER"
