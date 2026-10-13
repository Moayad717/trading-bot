"""
Tests for main.py's reconcile_db_with_executions() Pass 2 — the 5-minute
reconciler's fallback for signals with no tp_order_id.

Background (2026-10-14): Pass 2 used to be fuzzy matching — "a closing
execution of about this size, some time after entry, close enough." That
already caused real false completions before (85 rows on 8003, 182 on
8005). Every TP this bot places carries a tag built from its own of_id
(build_order_link_id), so Pass 2 now matches on that tag instead of
guessing from quantity and timing.
"""
import asyncio
import sqlite3
from unittest.mock import MagicMock, patch

import db
import main
from tests.conftest import insert_signal


def _set_columns(db_path: str, signal_id: int, **cols) -> None:
    conn = sqlite3.connect(db_path)
    for col, val in cols.items():
        conn.execute(f"UPDATE signals SET {col}=? WHERE id=?", (val, signal_id))
    conn.commit()
    conn.close()


def _get_row(db_path: str, signal_id: int) -> tuple:
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT status, tp_order_id FROM signals WHERE id=?", (signal_id,)
    ).fetchone()
    conn.close()
    return row


async def _run_reconcile(exchange_mock) -> int:
    """Start the real write-queue worker for this test's event loop —
    complete_signal_async goes through it, same as live traffic.

    db._write_queue is a module-level asyncio.Queue, reused across every
    asyncio.run() call in the process; its internal primitives latch onto
    whichever event loop first touches them, so reusing it across tests
    (each with its own fresh loop) hangs forever on the second test. Give
    each call its own queue, bound to the loop it's actually running in.
    """
    db._write_queue = asyncio.Queue()
    worker = db.start_db_write_worker()
    try:
        with patch("main.BybitExchange", return_value=exchange_mock):
            return await main.reconcile_db_with_executions()
    finally:
        worker.cancel()


def _execution(order_id, order_link_id, symbol="LINKUSDT", closed_size="2.0"):
    return {
        "orderId": order_id, "orderLinkId": order_link_id, "symbol": symbol,
        "execTime": "1700000000000", "closedSize": closed_size, "side": "Sell",
    }


def test_tag_match_fixes_signal_with_no_tp_order_id(tmp_db):
    sig_id = insert_signal(tmp_db, action="buy", of_id="flowR1", symbol="LINKUSDT",
                            quantity=2.0, status="active", tp_order_id=None)
    _set_columns(tmp_db, sig_id, entry_fill_time="2026-10-14T10:00:00")

    exchange = MagicMock()
    exchange.get_execution_history.return_value = [_execution("exec-1", "flowR1_TP")]

    fixed = asyncio.run(_run_reconcile(exchange))

    assert fixed == 1
    status, tp_order_id = _get_row(tmp_db, sig_id)
    assert status == "completed"


def test_counter_signal_matches_ctp_tag_not_tp(tmp_db):
    """A COUNTER signal's closing tag is CTP, not TP — an execution tagged
    plain TP for the same of_id must not satisfy it (that TP belongs to the
    original, a separate signal sharing the same of_id)."""
    sig_id = insert_signal(tmp_db, action="sell", of_id="flowR2", pattern_type="COUNTER",
                            symbol="LINKUSDT", quantity=1.5, status="active", tp_order_id=None)
    _set_columns(tmp_db, sig_id, entry_fill_time="2026-10-14T10:00:00")

    exchange = MagicMock()
    exchange.get_execution_history.return_value = [_execution("exec-2", "flowR2_TP")]

    fixed = asyncio.run(_run_reconcile(exchange))

    assert fixed == 0
    status, tp_order_id = _get_row(tmp_db, sig_id)
    assert status == "active"
    assert tp_order_id is None

    exchange.get_execution_history.return_value = [_execution("exec-3", "flowR2_CTP")]
    fixed = asyncio.run(_run_reconcile(exchange))
    assert fixed == 1
    status, _ = _get_row(tmp_db, sig_id)
    assert status == "completed"


def test_signal_without_of_id_is_left_alone(tmp_db):
    """Pre-dates tagging — no tag to match on, so Pass 2 must not guess
    from symbol/size alone like the old fuzzy matcher did."""
    sig_id = insert_signal(tmp_db, action="buy", of_id=None, symbol="LINKUSDT",
                            quantity=2.0, status="active", tp_order_id=None)
    _set_columns(tmp_db, sig_id, entry_fill_time="2026-10-14T10:00:00")

    exchange = MagicMock()
    exchange.get_execution_history.return_value = [_execution("exec-4", "someOtherFlow_TP")]

    fixed = asyncio.run(_run_reconcile(exchange))

    assert fixed == 0
    status, _ = _get_row(tmp_db, sig_id)
    assert status == "active"


def test_execution_already_used_by_pass1_is_not_reused_in_pass2(tmp_db):
    """Pass 1 and Pass 2 share used_order_ids — an execution that already
    completed one signal by exact tp_order_id match must not also be
    claimed by an unrelated tag-matched signal."""
    sig_with_tp = insert_signal(tmp_db, action="buy", of_id="flowR5a", symbol="LINKUSDT",
                                 quantity=2.0, status="active", tp_order_id="exec-5")
    _set_columns(tmp_db, sig_with_tp, entry_fill_time="2026-10-14T10:00:00")

    sig_no_tp = insert_signal(tmp_db, action="buy", of_id="flowR5b", symbol="LINKUSDT",
                               quantity=2.0, status="active", tp_order_id=None)
    _set_columns(tmp_db, sig_no_tp, entry_fill_time="2026-10-14T10:00:00")

    exchange = MagicMock()
    # Tagged for flowR5a (Pass 1 match by orderId) but orderLinkId happens to
    # also start with flowR5b's expected tag prefix — contrived, but proves
    # Pass 2 checks used_order_ids rather than just the tag prefix.
    exchange.get_execution_history.return_value = [_execution("exec-5", "flowR5b_TP")]

    fixed = asyncio.run(_run_reconcile(exchange))

    assert fixed == 1  # only Pass 1's exact match, not also claimed by Pass 2
    status_a, _ = _get_row(tmp_db, sig_with_tp)
    status_b, _ = _get_row(tmp_db, sig_no_tp)
    assert status_a == "completed"
    assert status_b == "active"
