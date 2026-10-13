"""
Tests for routers/webhook.py's _place_order_sync changes made for the
placement-race fix (2026-10-14):

  1. The signal row is now inserted BEFORE exchange.place_order() is
     called, not after — so a fill event that arrives faster than the
     REST round-trip still has a row (tagged by of_id) to match against.
  2. A placement failure now records the rejection via
     mark_signal_failed_sync instead of just a log line.
  3. The COUNTER guard falls back to asking Bybit directly
     (_original_active_on_bybit) when the local DB doesn't yet show the
     original as active — covering the same race from the counter side.
"""
import sqlite3
from unittest.mock import MagicMock

from models.signal import Action, OrderType, SignalCreate
from routers.webhook import _place_order_sync


def _get_row(db_path: str, of_id: str) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT status, order_id, error_message FROM signals WHERE of_id=?", (of_id,)
    ).fetchone()
    conn.close()
    return dict(row)


def test_row_exists_pending_before_place_order_is_called(tmp_db):
    """The core ordering guarantee: by the time exchange.place_order() is
    invoked, the row must already be in the DB (status=pending, of_id
    set) — captured from inside place_order's own side effect, which is
    the earliest possible observation point."""
    captured = {}

    def _place_order_side_effect(signal_create):
        conn = sqlite3.connect(tmp_db)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, of_id FROM signals WHERE of_id=?", ("flowOrder1",)
        ).fetchone()
        conn.close()
        captured["row_at_placement_time"] = dict(row) if row else None
        return {"order_id": "entry-order-1"}

    exchange = MagicMock()
    exchange.name = "bybit"
    exchange.place_order.side_effect = _place_order_side_effect

    signal_create = SignalCreate(
        action=Action.BUY, symbol="LINKUSDT", quantity=2.0,
        order_type=OrderType.LIMIT, take_profit=12.0, of_id="flowOrder1",
    )

    _place_order_sync(signal_create, exchange)

    assert captured["row_at_placement_time"] is not None
    assert captured["row_at_placement_time"]["status"] == "pending"
    assert captured["row_at_placement_time"]["of_id"] == "flowOrder1"

    # Placement succeeded — row must have advanced past pending afterward.
    row = _get_row(tmp_db, "flowOrder1")
    assert row["status"] == "open"
    assert row["order_id"] == "entry-order-1"


def test_placement_failure_is_recorded_not_silently_dropped(tmp_db):
    """A rejected order used to vanish after one log line — the row
    already exists (inserted first), so the failure must be recorded onto
    it via mark_signal_failed_sync."""
    exchange = MagicMock()
    exchange.name = "bybit"
    exchange.place_order.side_effect = RuntimeError("Bybit: insufficient balance")

    signal_create = SignalCreate(
        action=Action.BUY, symbol="LINKUSDT", quantity=2.0,
        order_type=OrderType.LIMIT, take_profit=12.0, of_id="flowOrder2",
    )

    _place_order_sync(signal_create, exchange)

    row = _get_row(tmp_db, "flowOrder2")
    assert row["status"] == "failed"
    assert "insufficient balance" in row["error_message"]


def test_counter_guard_falls_back_to_bybit_when_db_lags(tmp_db):
    """The DB has no active original for this of_id yet (placement race on
    the original's own side) — the guard must check Bybit directly before
    rejecting, and let the counter through if Bybit confirms the original
    really did fill."""
    exchange = MagicMock()
    exchange.name = "bybit"
    exchange.place_order.return_value = {"order_id": "ce-order-1"}

    # _original_active_on_bybit: no resting open order for the entry tag...
    exchange._client.get_open_orders.return_value = {"result": {"list": []}}

    def _get_order_history_side_effect(**kwargs):
        link = kwargs.get("orderLinkId", "")
        if link.endswith("_E"):
            return {"result": {"list": [{"orderStatus": "Filled"}]}}
        return {"result": {"list": []}}  # no TP yet for "_TP"

    exchange._client.get_order_history.side_effect = _get_order_history_side_effect

    signal_create = SignalCreate(
        action=Action.SELL, symbol="LINKUSDT", quantity=1.5,
        order_type=OrderType.LIMIT, take_profit=11.0, of_id="flowFallback1",
        pattern_type="COUNTER",
    )

    _place_order_sync(signal_create, exchange)

    exchange.place_order.assert_called_once()


def test_counter_guard_still_rejects_when_bybit_has_no_record_either(tmp_db):
    """Sanity check for the fallback itself: if Bybit has no record of the
    original's entry at all, the counter must still be rejected, same as
    before this change."""
    exchange = MagicMock()
    exchange.name = "bybit"
    exchange._client.get_open_orders.return_value = {"result": {"list": []}}
    exchange._client.get_order_history.return_value = {"result": {"list": []}}

    signal_create = SignalCreate(
        action=Action.SELL, symbol="LINKUSDT", quantity=1.5,
        order_type=OrderType.LIMIT, take_profit=11.0, of_id="flowFallback2",
        pattern_type="COUNTER",
    )

    _place_order_sync(signal_create, exchange)

    exchange.place_order.assert_not_called()
