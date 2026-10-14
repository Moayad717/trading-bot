"""
Tests for the read-only TP coverage report (dashboard/routes.py, 2026-10-14).

Background: a pooled Bybit position with many individually-tagged TPs gets
auto-trimmed by Bybit itself (CancelByReduceOnly) whenever total resting
coverage briefly exceeds the position size. Confirmed live for signals
1601/1643/1678 on 8003 — each one's own tagged TP got cancelled this way at
some point, while the position as a whole stayed fully covered by other
signals' orders. The DB kept claiming coverage from a dead order in each
case, with nothing surfacing it automatically. This report finds that drift.
Its one write action (clear_stale_tp) only clears a bookkeeping reference —
it never touches Bybit — and re-verifies against Bybit before doing even
that, matching the project's rule that repairs to position state must be
manual, explicit, and never silent.
"""
import asyncio
import sqlite3
from unittest.mock import MagicMock, patch

from tests.conftest import insert_signal

from dashboard.routes import ClearStaleTpIn, clear_stale_tp, positions_coverage


def _set_columns(db_path: str, signal_id: int, **cols) -> None:
    conn = sqlite3.connect(db_path)
    for col, val in cols.items():
        conn.execute(f"UPDATE signals SET {col}=? WHERE id=?", (val, signal_id))
    conn.commit()
    conn.close()


def _get_tp_order_id(db_path: str, signal_id: int):
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT tp_order_id FROM signals WHERE id=?", (signal_id,)).fetchone()
    conn.close()
    return row[0] if row else None


def _history_response(order_status: str, order_id="tp-stale-1"):
    return {"result": {"list": [{"orderId": order_id, "orderStatus": order_status}]}}


def _mock_exchange(open_order_ids=(), history_status="Cancelled"):
    ex = MagicMock()
    ex.get_all_open_orders.return_value = [{"orderId": oid} for oid in open_order_ids]
    ex._client.get_order_history.return_value = _history_response(history_status)
    return ex


def test_report_empty_when_no_active_signals_have_tp(tmp_db):
    with patch("exchanges.bybit.BybitExchange") as MockEx:
        result = asyncio.run(positions_coverage())
    assert result == {"stale": [], "count": 0}
    MockEx.assert_not_called()  # no signals to check — never even touch Bybit


def test_report_ignores_tp_still_resting(tmp_db):
    sig_id = insert_signal(tmp_db, action="buy", of_id="flowR1", symbol="LINKUSDT",
                            quantity=2.0, status="active", tp_order_id="tp-live-1")

    exchange = _mock_exchange(open_order_ids=["tp-live-1"])
    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(positions_coverage())

    assert result == {"stale": [], "count": 0}


def test_report_flags_cancelled_tp_as_stale(tmp_db):
    sig_id = insert_signal(tmp_db, action="sell", of_id="flowR2", symbol="LINKUSDT",
                            quantity=0.7, status="active", tp_order_id="tp-dead-1")
    _set_columns(tmp_db, sig_id, take_profit=12.783)

    exchange = _mock_exchange(open_order_ids=[], history_status="Cancelled")
    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(positions_coverage())

    assert result["count"] == 1
    row = result["stale"][0]
    assert row["signal_id"] == sig_id
    assert row["of_id"] == "flowR2"
    assert row["tp_order_id"] == "tp-dead-1"
    assert row["tp_order_status"] == "Cancelled"
    assert row["take_profit"] == 12.783


def test_report_flags_rejected_tp_too(tmp_db):
    insert_signal(tmp_db, action="buy", of_id="flowR3", symbol="LINKUSDT",
                  quantity=1.0, status="active", tp_order_id="tp-dead-2")

    exchange = _mock_exchange(open_order_ids=[], history_status="Rejected")
    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(positions_coverage())

    assert result["count"] == 1
    assert result["stale"][0]["tp_order_status"] == "Rejected"


def test_clear_stale_tp_succeeds_when_order_is_cancelled(tmp_db):
    sig_id = insert_signal(tmp_db, action="sell", of_id="flowC1", symbol="LINKUSDT",
                            quantity=0.7, status="active", tp_order_id="tp-dead-3")

    exchange = _mock_exchange(open_order_ids=[], history_status="Cancelled")
    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(clear_stale_tp(
            sig_id, ClearStaleTpIn(tp_order_id="tp-dead-3", symbol="LINKUSDT")
        ))

    assert result == {"cleared": True}
    assert _get_tp_order_id(tmp_db, sig_id) is None


def test_clear_stale_tp_refuses_when_still_resting(tmp_db):
    sig_id = insert_signal(tmp_db, action="sell", of_id="flowC2", symbol="LINKUSDT",
                            quantity=0.7, status="active", tp_order_id="tp-live-2")

    exchange = _mock_exchange(open_order_ids=["tp-live-2"])
    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(clear_stale_tp(
            sig_id, ClearStaleTpIn(tp_order_id="tp-live-2", symbol="LINKUSDT")
        ))

    assert result["cleared"] is False
    assert "still resting" in result["reason"]
    assert _get_tp_order_id(tmp_db, sig_id) == "tp-live-2"


def test_clear_stale_tp_refuses_when_actually_filled(tmp_db):
    """A filled order is a completion to reconcile, not a stale reference —
    clearing it would hide a real trade instead of fixing bookkeeping."""
    sig_id = insert_signal(tmp_db, action="buy", of_id="flowC3", symbol="LINKUSDT",
                            quantity=1.0, status="active", tp_order_id="tp-filled-1")

    exchange = _mock_exchange(open_order_ids=[], history_status="Filled")
    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(clear_stale_tp(
            sig_id, ClearStaleTpIn(tp_order_id="tp-filled-1", symbol="LINKUSDT")
        ))

    assert result["cleared"] is False
    assert "filled" in result["reason"].lower()
    assert _get_tp_order_id(tmp_db, sig_id) == "tp-filled-1"


def test_clear_stale_tp_refuses_on_tp_order_id_mismatch(tmp_db):
    """The signal's real tp_order_id changed since the report was read (a
    legitimate update raced this call) — must not clear the new value."""
    sig_id = insert_signal(tmp_db, action="buy", of_id="flowC4", symbol="LINKUSDT",
                            quantity=1.0, status="active", tp_order_id="tp-new-1")

    exchange = _mock_exchange(open_order_ids=[], history_status="Cancelled")
    with patch("exchanges.bybit.BybitExchange", return_value=exchange):
        result = asyncio.run(clear_stale_tp(
            sig_id, ClearStaleTpIn(tp_order_id="tp-old-stale", symbol="LINKUSDT")
        ))

    assert result["cleared"] is False
    assert _get_tp_order_id(tmp_db, sig_id) == "tp-new-1"
