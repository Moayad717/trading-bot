"""
Tests for order_tracker.py's _handle_fill tag-based fallback (2026-10-14):
a limit entry can fill before the webhook that placed it has finished
writing order_id into its row (see routers/webhook.py's _place_order_sync
docstring for the full race). The row still exists — tagged by its of_id
since before the order was even sent — so a fill event that can't match by
order_id falls back to matching by of_id + role from orderLinkId.
"""
import sqlite3
from unittest.mock import MagicMock

from tests.conftest import insert_signal

from order_tracker import OrderTracker


def _get_row(db_path: str, signal_id: int) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT status, order_id, entry_fill_time, tp_order_id FROM signals WHERE id=?",
        (signal_id,),
    ).fetchone()
    conn.close()
    return dict(row)


def test_fast_fill_with_no_order_id_yet_matches_by_tag(tmp_db):
    """The core race case: the row is still PENDING (order_id not backfilled
    yet) when the fill event arrives — order_id matching finds nothing, so
    the tag fallback must take over and still place the TP."""
    sig_id = insert_signal(tmp_db, action="buy", of_id="flowFill1", status="pending",
                            symbol="LINKUSDT", quantity=2.0)
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE signals SET take_profit=? WHERE id=?", (12.5, sig_id))
    conn.commit()
    conn.close()

    exchange = MagicMock()
    exchange.round_qty.return_value = 2.0
    exchange.place_tp_order.return_value = {"order_id": "tp-fill-1"}
    tracker = OrderTracker(exchange=exchange)

    tracker._handle_fill({
        "orderId": "entry-fast-1", "orderLinkId": "flowFill1_E",
        "symbol": "LINKUSDT", "side": "Buy", "cumExecQty": "2.0",
    })

    row = _get_row(tmp_db, sig_id)
    assert row["status"] == "active"
    assert row["order_id"] == "entry-fast-1"
    assert row["tp_order_id"] == "tp-fill-1"

    exchange.place_tp_order.assert_called_once()
    kwargs = exchange.place_tp_order.call_args.kwargs
    assert kwargs["side"] == "Sell"
    assert kwargs["qty"] == 2.0
    assert kwargs["price"] == 12.5
    assert kwargs["order_link_id_base"] == "flowFill1_TP"


def test_counter_fast_fill_matches_ce_tag_and_places_ctp(tmp_db):
    """A counter entry's tag ends in _CE, not _E — must still match, and
    its TP must be tagged _CTP, not _TP."""
    sig_id = insert_signal(tmp_db, action="sell", of_id="flowFill2", status="pending",
                            symbol="LINKUSDT", quantity=1.5, pattern_type="COUNTER")
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE signals SET take_profit=? WHERE id=?", (11.0, sig_id))
    conn.commit()
    conn.close()

    exchange = MagicMock()
    exchange.round_qty.return_value = 1.5
    exchange.place_tp_order.return_value = {"order_id": "tp-fill-2"}
    tracker = OrderTracker(exchange=exchange)

    tracker._handle_fill({
        "orderId": "entry-fast-2", "orderLinkId": "flowFill2_CE",
        "symbol": "LINKUSDT", "side": "Sell", "cumExecQty": "1.5",
    })

    row = _get_row(tmp_db, sig_id)
    assert row["status"] == "active"
    assert row["order_id"] == "entry-fast-2"

    kwargs = exchange.place_tp_order.call_args.kwargs
    assert kwargs["order_link_id_base"] == "flowFill2_CTP"


def test_normal_order_id_match_takes_priority_over_tag_fallback(tmp_db):
    """When order_id is already backfilled (the common, non-race case), the
    plain order_id match must succeed first — the tag fallback is only
    reached when that lookup finds nothing."""
    sig_id = insert_signal(tmp_db, action="buy", of_id="flowFill3", status="open",
                            symbol="LINKUSDT", quantity=2.0, order_id="entry-normal-3")
    conn = sqlite3.connect(tmp_db)
    conn.execute("UPDATE signals SET take_profit=? WHERE id=?", (13.0, sig_id))
    conn.commit()
    conn.close()

    exchange = MagicMock()
    exchange.round_qty.return_value = 2.0
    exchange.place_tp_order.return_value = {"order_id": "tp-fill-3"}
    tracker = OrderTracker(exchange=exchange)

    tracker._handle_fill({
        "orderId": "entry-normal-3", "orderLinkId": "flowFill3_E",
        "symbol": "LINKUSDT", "side": "Buy", "cumExecQty": "2.0",
    })

    row = _get_row(tmp_db, sig_id)
    assert row["status"] == "active"


def test_unmatched_fill_with_no_recognizable_tag_is_not_treated_as_entry(tmp_db):
    """A fill event whose orderLinkId doesn't look like an entry tag at all
    (e.g. it's actually a TP fill, handled further down in _handle_fill)
    must not be misrouted into the entry fallback."""
    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)

    tracker._handle_fill({
        "orderId": "order-unknown", "orderLinkId": "flowFill4_TP",
        "symbol": "LINKUSDT", "side": "Sell", "cumExecQty": "2.0",
    })

    exchange.place_tp_order.assert_not_called()
