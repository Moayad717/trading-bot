"""
Tests for db.py's two new functions supporting the placement-race fix
(2026-10-14): a limit entry can fill in under a millisecond, faster than
the REST round-trip that places it and learns its order_id, so the row is
now written (tagged with its of_id) BEFORE placement is attempted — see
routers/webhook.py's _place_order_sync and order_tracker.py's _handle_fill.

  mark_entry_filled_by_tag_sync — matches a fill to that pre-written row by
    of_id + role instead of order_id, for the case where the fill event
    arrives before order_id has been backfilled.
  mark_signal_failed_sync — records a rejected placement on the row that
    now already exists, instead of it vanishing with just a log line.
"""
import sqlite3

from tests.conftest import insert_signal

from db import mark_entry_filled_by_tag_sync, mark_signal_failed_sync


def _get_row(db_path: str, signal_id: int) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT status, order_id, entry_fill_time, error_message FROM signals WHERE id=?",
        (signal_id,),
    ).fetchone()
    conn.close()
    return dict(row)


def test_matches_pending_entry_by_of_id(tmp_db):
    sig_id = insert_signal(tmp_db, action="buy", of_id="flowT1", status="pending")

    matched = mark_entry_filled_by_tag_sync("flowT1", False, "order-1", "2026-10-14T10:00:00")

    assert matched is True
    row = _get_row(tmp_db, sig_id)
    assert row["status"] == "active"
    assert row["order_id"] == "order-1"
    assert row["entry_fill_time"] == "2026-10-14T10:00:00"


def test_matches_open_entry_by_of_id_too(tmp_db):
    """The normal placement flow may have already advanced the row to OPEN
    (order_id backfilled from the REST reply) by the time the fill event is
    processed — still a valid match, just with order_id already set."""
    sig_id = insert_signal(tmp_db, action="buy", of_id="flowT2", status="open",
                            order_id="order-2")

    matched = mark_entry_filled_by_tag_sync("flowT2", False, "order-2", "2026-10-14T10:01:00")

    assert matched is True
    row = _get_row(tmp_db, sig_id)
    assert row["status"] == "active"


def test_does_not_match_already_active_row(tmp_db):
    """Guards against double-processing — a row already marked active
    (e.g. matched moments earlier by order_id) must not match again."""
    insert_signal(tmp_db, action="buy", of_id="flowT3", status="active")

    matched = mark_entry_filled_by_tag_sync("flowT3", False, "order-3", "2026-10-14T10:02:00")

    assert matched is False


def test_counter_guard_matches_only_counter_row(tmp_db):
    """An original and its counter share the same of_id — is_counter=True
    must only ever touch the COUNTER row, never the original's."""
    original_id = insert_signal(tmp_db, action="buy", of_id="flowT4", status="pending",
                                 pattern_type=None)
    counter_id = insert_signal(tmp_db, action="sell", of_id="flowT4", status="pending",
                                pattern_type="COUNTER")

    matched = mark_entry_filled_by_tag_sync("flowT4", True, "order-ce-4", "2026-10-14T10:03:00")

    assert matched is True
    assert _get_row(tmp_db, counter_id)["status"] == "active"
    assert _get_row(tmp_db, original_id)["status"] == "pending"


def test_original_guard_matches_only_original_row(tmp_db):
    original_id = insert_signal(tmp_db, action="buy", of_id="flowT5", status="pending",
                                 pattern_type=None)
    counter_id = insert_signal(tmp_db, action="sell", of_id="flowT5", status="pending",
                                pattern_type="COUNTER")

    matched = mark_entry_filled_by_tag_sync("flowT5", False, "order-e-5", "2026-10-14T10:04:00")

    assert matched is True
    assert _get_row(tmp_db, original_id)["status"] == "active"
    assert _get_row(tmp_db, counter_id)["status"] == "pending"


def test_no_match_for_unknown_of_id(tmp_db):
    matched = mark_entry_filled_by_tag_sync("flow-does-not-exist", False, "order-6",
                                             "2026-10-14T10:05:00")
    assert matched is False


def test_mark_signal_failed_sets_status_and_error(tmp_db):
    sig_id = insert_signal(tmp_db, action="buy", of_id="flowT7", status="pending")

    mark_signal_failed_sync(sig_id, "Bybit error: insufficient balance")

    row = _get_row(tmp_db, sig_id)
    assert row["status"] == "failed"
    assert row["error_message"] == "Bybit error: insufficient balance"
