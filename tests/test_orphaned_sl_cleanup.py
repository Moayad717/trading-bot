"""
Tests for _cancel_orphaned_sl_if_any (order_tracker.py), added 2026-09-12.

Background: an original's conditional SL (see test_sl_completion.py) is only
needed while its counter is still open. Two Pine-alert-driven paths already
existed to cancel it (cancel_close_original, exit_position's
STD_TP_COUNTER_STILL_OPEN) — but neither covers the ordinary, most common
case: the original simply hits its OWN take-profit first. Confirmed live:
zero cleanup ran for that path at all, so every original completing this way
left its conditional SL resting forever, silently consuming one of Bybit's
hard cap of 10 conditional orders per symbol. Once all 10 were used up by
orphans, brand-new originals could not get ANY protection placed — 5 real,
live positions were sitting unprotected because of exactly this before the
fix.
"""
from unittest.mock import MagicMock

from tests.conftest import insert_signal

import db
from order_tracker import OrderTracker


def test_tp_fill_cancels_the_signals_own_orphaned_sl(tmp_db):
    """The everyday case: an original's own TP fills while it still has a
    resting conditional SL from being paired with a counter — that SL must
    be cancelled, not left resting forever."""
    orig_id = insert_signal(
        tmp_db, action="sell", of_id="flow1", tp_order_id="tp-order-1",
        sl_order_id="sl-order-orphan",
    )

    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._cancel_orphaned_sl_if_any("tp-order-1")

    exchange.cancel_order.assert_called_once_with("sl-order-orphan", "LINKUSDT")


def test_no_cancel_when_signal_has_no_sl_order_id(tmp_db):
    """A plain signal with no conditional SL at all (never paired with a
    counter) must not trigger any cancel call — nothing to clean up."""
    insert_signal(tmp_db, action="sell", of_id="flow2", tp_order_id="tp-order-2",
                  sl_order_id=None)

    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._cancel_orphaned_sl_if_any("tp-order-2")

    exchange.cancel_order.assert_not_called()


def test_no_cancel_when_tp_order_id_unknown(tmp_db):
    """A tp_order_id that doesn't match any signal (shouldn't normally
    happen, but must fail safe) triggers no cancel call."""
    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._cancel_orphaned_sl_if_any("tp-order-does-not-exist")

    exchange.cancel_order.assert_not_called()


def test_cancel_failure_is_logged_not_raised(tmp_db):
    """If the cancel call itself fails (e.g. Bybit already cleared the
    order for some other reason), it must not raise — this runs inside the
    live WebSocket fill handler, where an unhandled exception could disrupt
    processing of other, unrelated fill events."""
    insert_signal(tmp_db, action="sell", of_id="flow3", tp_order_id="tp-order-3",
                  sl_order_id="sl-order-gone")

    exchange = MagicMock()
    exchange.cancel_order.side_effect = RuntimeError("order not found")
    tracker = OrderTracker(exchange=exchange)

    tracker._cancel_orphaned_sl_if_any("tp-order-3")  # must not raise

    exchange.cancel_order.assert_called_once_with("sl-order-gone", "LINKUSDT")


def test_full_handle_fill_flow_cancels_orphaned_sl_on_tp_completion(tmp_db):
    """End-to-end through _handle_fill: a signal's TP fill event correctly
    triggers both completion AND the orphaned-SL cancellation, matching what
    actually happens on the live WebSocket stream."""
    insert_signal(
        tmp_db, action="buy", status="active", of_id="flow4",
        tp_order_id="tp-order-4", sl_order_id="sl-order-4",
    )

    exchange = MagicMock()
    tracker = OrderTracker(exchange=exchange)
    tracker._handle_fill({"orderId": "tp-order-4", "symbol": "LINKUSDT"})

    exchange.cancel_order.assert_called_once_with("sl-order-4", "LINKUSDT")
