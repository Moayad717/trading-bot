"""
Tests for stop-loss settings and behavior.

History: 2026-09-13 added a per-timeframe SL on/off switch (some timeframes
protected, others not) plus a cutoff and a master kill switch. On
2026-09-16 this was reversed entirely: stop-loss is permanently removed
from the system, not just for now — see order_tracker.py's
_maybe_place_close_original docstring for the full data behind that
decision.

The settings CRUD (db.py) is still real, live code reachable via the
dashboard API, so it's still tested here — but order_tracker.py's
_maybe_place_close_original no longer consults ANY of it. The tests below
are split accordingly:
  1. db.py's settings CRUD + the default-on lookup rule (still-live,
     dashboard-facing infrastructure, disconnected from order placement).
  2. order_tracker.py's _maybe_place_close_original never placing an SL,
     under every combination of settings — proving the removal is
     unconditional, not merely "off by default".
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


# ── order_tracker.py: SL is permanently, unconditionally disabled ─────────
# (removed 2026-09-16). The settings above are still real, live
# CRUD behind the dashboard, but _maybe_place_close_original no longer
# consults any of them — these tests prove that directly, not just that the
# lookup function itself returns the right answer in isolation.

def _set_columns(db_path: str, signal_id: int, **cols) -> None:
    """conftest's insert_signal helper doesn't cover every column (order_id,
    interval, close_original_json, take_profit) — set them directly for
    tests that need them."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    for col, val in cols.items():
        conn.execute(f"UPDATE signals SET {col}=? WHERE id=?", (val, signal_id))
    conn.commit()
    conn.close()


def test_sl_never_placed_even_with_most_permissive_settings(tmp_db):
    """Explicit per-timeframe 'enabled' + master switch ON — the most
    SL-permissive configuration possible — must still never place an SL.
    The removal is hardcoded, not gated by these settings."""
    db.set_sl_timeframe_setting_sync("60", True)
    db.set_sl_master_switch_sync(True)

    orig_id = insert_signal(tmp_db, action="buy", of_id="flowA", quantity=2.0,
                             symbol="LINKUSDT", category="linear")
    _set_columns(tmp_db, orig_id, interval="60")

    ctr_id = insert_signal(tmp_db, action="sell", of_id="flowA", pattern_type="COUNTER",
                            quantity=2.0, symbol="LINKUSDT")
    _set_columns(tmp_db, ctr_id, order_id="ctr-entry-order", take_profit=12.0)

    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._maybe_place_close_original({"orderId": "ctr-entry-order", "symbol": "LINKUSDT"})

    exchange.place_conditional_sl.assert_not_called()


def test_sl_placed_marked_false_for_every_counter_fill(tmp_db):
    """sl_placed must still be recorded False (never left NULL) so the
    legacy completion-inference path never wrongly links an original's
    completion to its counter's TP fill — see
    _maybe_complete_original_after_counter_tp's Case A semantics."""
    insert_signal(tmp_db, action="buy", of_id="flowB", quantity=2.0, symbol="LINKUSDT")
    ctr_id = insert_signal(tmp_db, action="sell", of_id="flowB", pattern_type="COUNTER",
                            quantity=2.0, symbol="LINKUSDT")
    _set_columns(tmp_db, ctr_id, order_id="ctr-entry-order-flag", take_profit=12.0)

    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._maybe_place_close_original({"orderId": "ctr-entry-order-flag", "symbol": "LINKUSDT"})

    import sqlite3
    conn = sqlite3.connect(tmp_db)
    row = conn.execute("SELECT sl_placed FROM signals WHERE id=?", (ctr_id,)).fetchone()
    conn.close()
    assert row[0] == 0


def test_close_original_block_ignored_even_when_present(tmp_db):
    """A counter carrying a real close_original_json block (as Pine still
    sends whenever its 'Counter also stops the original' input is left on)
    must be fully ignored — never parsed for a trigger price, never used to
    query the original's live position size."""
    insert_signal(tmp_db, action="buy", of_id="flowC", quantity=2.0, symbol="LINKUSDT")
    ctr_id = insert_signal(tmp_db, action="sell", of_id="flowC", pattern_type="COUNTER",
                            quantity=2.0, symbol="LINKUSDT")
    _set_columns(
        tmp_db, ctr_id, order_id="ctr-entry-order-block", take_profit=12.0,
        close_original_json='{"mode":"partial_position_sl","trigger_price":"11.9",'
                             '"order_type":"market","place_on":"entry_fill"}',
    )

    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._maybe_place_close_original({"orderId": "ctr-entry-order-block", "symbol": "LINKUSDT"})

    exchange.place_conditional_sl.assert_not_called()
    exchange.get_position_size.assert_not_called()


def test_non_counter_entry_fill_is_a_no_op(tmp_db):
    """A plain (non-COUNTER) entry fill must not touch anything — this
    function only ever concerned itself with counters."""
    orig_id = insert_signal(tmp_db, action="buy", of_id="flowD", quantity=2.0, symbol="LINKUSDT")
    _set_columns(tmp_db, orig_id, order_id="entry-order-std")

    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._maybe_place_close_original({"orderId": "entry-order-std", "symbol": "LINKUSDT"})

    exchange.place_conditional_sl.assert_not_called()


def test_armed_counter_from_before_the_change_still_gets_no_sl(tmp_db):
    """A counter that armed before 2026-09-16 (its
    alert already carried close_original) but is only filling now must
    still get no SL — this is the exact same code path as a fresh signal,
    so 'already armed' isn't a special case that needs separate handling."""
    db.set_sl_timeframe_setting_sync("15", True)  # simulates an old, still-enabled config
    insert_signal(tmp_db, action="buy", of_id="flowE", quantity=2.0, symbol="LINKUSDT")
    ctr_id = insert_signal(tmp_db, action="sell", of_id="flowE", pattern_type="COUNTER",
                            quantity=2.0, symbol="LINKUSDT")
    _set_columns(
        tmp_db, ctr_id, order_id="ctr-entry-order-armed", take_profit=12.0,
        close_original_json='{"trigger_price":"11.9"}',
    )

    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._maybe_place_close_original({"orderId": "ctr-entry-order-armed", "symbol": "LINKUSDT"})

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
    """Not a real Pine unit, but accepted as a convenience for hours."""
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


# ── db.py layer: master kill switch ─────────────────────────────────────────

def test_master_switch_enabled_by_default(tmp_db):
    assert db.get_sl_master_switch_sync() is True


def test_master_switch_set_and_read(tmp_db):
    db.set_sl_master_switch_sync(False)
    assert db.get_sl_master_switch_sync() is False


def test_master_switch_overwrite_not_duplicate(tmp_db):
    db.set_sl_master_switch_sync(False)
    db.set_sl_master_switch_sync(True)
    assert db.get_sl_master_switch_sync() is True


def test_master_switch_off_beats_everything(tmp_db):
    """'Remove every SL' means every SL: beats an explicit per-timeframe
    'on', the cutoff, and even the no-interval fail-safe."""
    db.set_sl_timeframe_setting_sync("5", True)  # explicit ON
    db.set_sl_cutoff_sync("1")                   # cutoff would also allow "5" through
    db.set_sl_master_switch_sync(False)

    assert db.is_sl_enabled_for_interval_sync("5") is False
    assert db.is_sl_enabled_for_interval_sync("1D") is False
    assert db.is_sl_enabled_for_interval_sync(None) is False  # even the missing-interval fail-safe


def test_master_switch_on_restores_normal_rules(tmp_db):
    db.set_sl_master_switch_sync(False)
    db.set_sl_master_switch_sync(True)
    assert db.is_sl_enabled_for_interval_sync("5") is True
    assert db.is_sl_enabled_for_interval_sync(None) is True



# Note: no end-to-end test exercises these settings actually blocking SL via
# _maybe_place_close_original anymore — that function no longer consults them
# at all (see the "order_tracker.py" section above, which proves the removal
# is unconditional). These settings remain real, tested CRUD behind the
# dashboard, but are disconnected from order placement by design.
