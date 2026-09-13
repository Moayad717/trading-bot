"""
Tests for the per-timeframe stop-loss on/off switch (client-requested,
2026-09-13): some timeframes perform better with a stop-loss, others
without, so it needs to be a per-timeframe setting rather than all-or-
nothing.

Two layers tested here:
  1. db.py's settings CRUD + the default-on lookup rule.
  2. order_tracker.py's _maybe_place_close_original actually consulting
     that setting before placing a conditional SL, and correctly falling
     through the existing sl_placed=0 "no SL for this pairing" path when
     disabled — deliberately reusing Case A's semantics rather than
     inventing a new state, so nothing downstream needs to change.
"""
from unittest.mock import MagicMock

from tests.conftest import insert_signal

import db
from order_tracker import OrderTracker


# ── db.py layer ──────────────────────────────────────────────────────────

def test_unconfigured_interval_defaults_to_enabled(tmp_db):
    assert db.is_sl_enabled_for_interval_sync("5") is True


def test_none_interval_defaults_to_enabled(tmp_db):
    """Legacy payloads with no interval at all must fail toward protection."""
    assert db.is_sl_enabled_for_interval_sync(None) is True


def test_set_and_read_disabled(tmp_db):
    db.set_sl_timeframe_setting_sync("5", False)
    assert db.is_sl_enabled_for_interval_sync("5") is False


def test_set_and_read_enabled_explicitly(tmp_db):
    db.set_sl_timeframe_setting_sync("5", False)
    db.set_sl_timeframe_setting_sync("5", True)
    assert db.is_sl_enabled_for_interval_sync("5") is True


def test_upsert_updates_existing_row_not_duplicates(tmp_db):
    db.set_sl_timeframe_setting_sync("15", False)
    db.set_sl_timeframe_setting_sync("15", True)
    rows = db.get_sl_timeframe_settings_sync()
    matching = [r for r in rows if r["interval"] == "15"]
    assert len(matching) == 1
    assert matching[0]["sl_enabled"] == 1


def test_delete_reverts_to_default_enabled(tmp_db):
    db.set_sl_timeframe_setting_sync("60", False)
    assert db.is_sl_enabled_for_interval_sync("60") is False

    deleted = db.delete_sl_timeframe_setting_sync("60")
    assert deleted is True
    assert db.is_sl_enabled_for_interval_sync("60") is True


def test_delete_unknown_interval_returns_false(tmp_db):
    assert db.delete_sl_timeframe_setting_sync("does-not-exist") is False


def test_list_settings_returns_all_configured(tmp_db):
    db.set_sl_timeframe_setting_sync("1", False)
    db.set_sl_timeframe_setting_sync("240", True)
    rows = db.get_sl_timeframe_settings_sync()
    by_interval = {r["interval"]: r["sl_enabled"] for r in rows}
    assert by_interval == {"1": 0, "240": 1}


# ── order_tracker.py integration ────────────────────────────────────────

def _set_columns(db_path: str, signal_id: int, **cols) -> None:
    """conftest's insert_signal helper doesn't cover every column (order_id,
    interval) — set them directly for tests that need them."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    for col, val in cols.items():
        conn.execute(f"UPDATE signals SET {col}=? WHERE id=?", (val, signal_id))
    conn.commit()
    conn.close()


def test_sl_skipped_for_disabled_timeframe(tmp_db):
    """End-to-end through the real WS fill-handling entry point
    (_maybe_place_close_original), not just the lookup function in
    isolation — proves the wiring, not just the logic."""
    db.set_sl_timeframe_setting_sync("5", False)
    orig_id = insert_signal(tmp_db, action="buy", of_id="flowA", quantity=2.0,
                             symbol="LINKUSDT", category="linear")
    _set_columns(tmp_db, orig_id, interval="5")

    ctr_id = insert_signal(tmp_db, action="sell", of_id="flowA", pattern_type="COUNTER",
                            quantity=2.0, symbol="LINKUSDT")
    # take_profit isn't in insert_signal's fixed column list — set directly,
    # or the function would return early for an unrelated reason (no trigger
    # price resolvable at all) and this test would pass without actually
    # exercising the interval check it's meant to verify.
    _set_columns(tmp_db, ctr_id, order_id="ctr-entry-order", take_profit=12.0)

    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._maybe_place_close_original({"orderId": "ctr-entry-order", "symbol": "LINKUSDT"})

    exchange.place_conditional_sl.assert_not_called()


# ── db.py layer: cutoff ("margin") ─────────────────────────────────────────

def test_interval_to_minutes_bare_digits():
    assert db.interval_to_minutes("5") == 5
    assert db.interval_to_minutes("120") == 120


def test_interval_to_minutes_days_weeks():
    assert db.interval_to_minutes("1D") == 1440
    assert db.interval_to_minutes("3D") == 3 * 1440
    assert db.interval_to_minutes("1W") == 10080


def test_interval_to_minutes_hour_convenience_alias():
    """Not a real Pine unit, but accepted since the client thinks in hours."""
    assert db.interval_to_minutes("2H") == 120


def test_interval_to_minutes_unparseable_returns_none():
    assert db.interval_to_minutes("not-a-timeframe") is None
    assert db.interval_to_minutes(None) is None
    assert db.interval_to_minutes("") is None


def test_cutoff_unset_by_default(tmp_db):
    assert db.get_sl_cutoff_sync() is None


def test_cutoff_set_and_read(tmp_db):
    db.set_sl_cutoff_sync("120")
    assert db.get_sl_cutoff_sync() == "120"


def test_cutoff_overwrite_not_duplicate(tmp_db):
    db.set_sl_cutoff_sync("120")
    db.set_sl_cutoff_sync("1D")
    assert db.get_sl_cutoff_sync() == "1D"


def test_cutoff_clear(tmp_db):
    db.set_sl_cutoff_sync("120")
    db.set_sl_cutoff_sync(None)
    assert db.get_sl_cutoff_sync() is None


def test_cutoff_disables_sl_at_and_above_threshold(tmp_db):
    db.set_sl_cutoff_sync("120")  # 2 hours
    assert db.is_sl_enabled_for_interval_sync("120") is False   # exactly 2h -> off
    assert db.is_sl_enabled_for_interval_sync("240") is False   # 4h -> off
    assert db.is_sl_enabled_for_interval_sync("1D") is False    # 1 day -> off
    assert db.is_sl_enabled_for_interval_sync("60") is True     # 1h -> still on
    assert db.is_sl_enabled_for_interval_sync("5") is True


def test_cutoff_does_not_override_explicit_per_timeframe_entry(tmp_db):
    """An exact per-timeframe switch is a deliberate override and must win
    over the blanket cutoff rule, in both directions."""
    db.set_sl_cutoff_sync("120")
    db.set_sl_timeframe_setting_sync("240", True)   # explicitly force SL back on above cutoff
    db.set_sl_timeframe_setting_sync("5", False)    # explicitly force SL off below cutoff
    assert db.is_sl_enabled_for_interval_sync("240") is True
    assert db.is_sl_enabled_for_interval_sync("5") is False


def test_no_cutoff_means_only_explicit_switches_apply(tmp_db):
    assert db.is_sl_enabled_for_interval_sync("240") is True
    assert db.is_sl_enabled_for_interval_sync("1D") is True


def test_sl_placed_for_enabled_timeframe(tmp_db):
    """Control case, same end-to-end path: an enabled (default) timeframe
    must still get its SL — confirms the new check doesn't accidentally
    block the normal path for everything else."""
    orig_id = insert_signal(tmp_db, action="buy", of_id="flowB", quantity=2.0,
                             symbol="LINKUSDT", category="linear")
    _set_columns(tmp_db, orig_id, interval="60")  # never configured -> default enabled

    ctr_id = insert_signal(tmp_db, action="sell", of_id="flowB", pattern_type="COUNTER",
                            quantity=2.0, symbol="LINKUSDT")
    _set_columns(tmp_db, ctr_id, order_id="ctr-entry-order-2", take_profit=12.0)

    exchange = MagicMock()
    exchange.round_qty.side_effect = lambda qty, symbol, category="linear": qty
    exchange.place_conditional_sl.return_value = {"order_id": "new-sl-order"}
    tracker = OrderTracker(exchange=exchange)
    tracker._maybe_place_close_original({"orderId": "ctr-entry-order-2", "symbol": "LINKUSDT"})

    exchange.place_conditional_sl.assert_called_once()


def test_cutoff_skips_sl_end_to_end(tmp_db):
    """Same real entry point as the per-timeframe tests above, but driven by
    the cutoff instead of an exact-match row — proves the cutoff is actually
    wired into order placement, not just the lookup function."""
    db.set_sl_cutoff_sync("120")  # 2 hours and above -> no SL

    orig_id = insert_signal(tmp_db, action="buy", of_id="flowC", quantity=2.0,
                             symbol="LINKUSDT", category="linear")
    _set_columns(tmp_db, orig_id, interval="240")  # 4h, at/above the 2h cutoff

    ctr_id = insert_signal(tmp_db, action="sell", of_id="flowC", pattern_type="COUNTER",
                            quantity=2.0, symbol="LINKUSDT")
    _set_columns(tmp_db, ctr_id, order_id="ctr-entry-order-3", take_profit=12.0)

    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._maybe_place_close_original({"orderId": "ctr-entry-order-3", "symbol": "LINKUSDT"})

    exchange.place_conditional_sl.assert_not_called()
