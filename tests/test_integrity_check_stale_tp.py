"""
Test for scripts/integrity_check.py's check_stale_tp_coverage (2026-10-14) —
the 5th check wired into the existing periodic Telegram alert, so a stale
tp_order_id (confirmed live: signals 1601/1643/1678/1332 on 8003, all had
their own tagged TP auto-cancelled by Bybit while the pooled position stayed
covered by other orders) gets reported automatically instead of only being
found by someone manually hitting /dashboard/positions/coverage.
"""
import sqlite3
from unittest.mock import MagicMock, patch

from tests.conftest import insert_signal

from scripts.integrity_check import check_stale_tp_coverage


def _set_columns(db_path: str, signal_id: int, **cols) -> None:
    conn = sqlite3.connect(db_path)
    for col, val in cols.items():
        conn.execute(f"UPDATE signals SET {col}=? WHERE id=?", (val, signal_id))
    conn.commit()
    conn.close()


def _mock_exchange(open_order_ids=(), history_status="Cancelled"):
    ex = MagicMock()
    ex.get_all_open_orders.return_value = [{"orderId": oid} for oid in open_order_ids]
    ex._client.get_order_history.return_value = {
        "result": {"list": [{"orderId": "whatever", "orderStatus": history_status}]}
    }
    return ex


def test_no_findings_when_no_active_signals_have_tp(tmp_db):
    with patch("scripts.integrity_check.BybitExchange") as MockEx:
        findings = check_stale_tp_coverage()
    assert findings == []
    MockEx.assert_not_called()


def test_no_findings_when_tp_still_resting(tmp_db):
    insert_signal(tmp_db, action="buy", of_id="flowI1", symbol="LINKUSDT",
                  quantity=2.0, status="active", tp_order_id="tp-live-1")

    exchange = _mock_exchange(open_order_ids=["tp-live-1"])
    with patch("scripts.integrity_check.BybitExchange", return_value=exchange):
        findings = check_stale_tp_coverage()

    assert findings == []


def test_flags_cancelled_tp(tmp_db):
    sig_id = insert_signal(tmp_db, action="sell", of_id="flowI2", symbol="LINKUSDT",
                            quantity=0.7, status="active", tp_order_id="tp-dead-1")

    exchange = _mock_exchange(open_order_ids=[], history_status="Cancelled")
    with patch("scripts.integrity_check.BybitExchange", return_value=exchange):
        findings = check_stale_tp_coverage()

    assert len(findings) == 1
    assert f"sig_id={sig_id}" in findings[0]
    assert "flowI2" in findings[0]
    assert "tp-dead-1" in findings[0]
    assert "Cancelled" in findings[0]


def test_does_not_flag_signals_without_tp_order_id(tmp_db):
    """Only the specific thing this check is about — a saved reference
    that turned out dead. A signal with no tp_order_id at all is a
    different, already-covered case (the 30s fix / 5-min reconciler)."""
    insert_signal(tmp_db, action="buy", of_id="flowI3", symbol="LINKUSDT",
                  quantity=1.0, status="active", tp_order_id=None)

    with patch("scripts.integrity_check.BybitExchange") as MockEx:
        findings = check_stale_tp_coverage()

    assert findings == []
    MockEx.assert_not_called()


def test_does_not_flag_completed_signals(tmp_db):
    """A completed signal's tp_order_id being gone from open orders is
    expected (it filled) — only 'active' status is in scope here."""
    insert_signal(tmp_db, action="buy", of_id="flowI4", symbol="LINKUSDT",
                  quantity=1.0, status="completed", tp_order_id="tp-done-1")

    with patch("scripts.integrity_check.BybitExchange") as MockEx:
        findings = check_stale_tp_coverage()

    assert findings == []
    MockEx.assert_not_called()
