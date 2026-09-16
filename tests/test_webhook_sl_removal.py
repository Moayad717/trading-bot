"""
Tests for the two routers/webhook.py changes made alongside the permanent
SL removal (2026-09-16):

  1. _handle_cancel_close_original_sync is now a real no-op — the
     "cancel_close_original" action used to cancel a resting conditional SL
     order, but there's never an SL left to cancel anymore.
  2. The market-order take-profit path in _place_order_sync now tags its TP
     order the same way the limit-entry path already does
     (order_tracker.py's _maybe_place_tp) — <of_id>_TP or <of_id>_CTP.
     Not currently reachable by real TradingView traffic (every recent
     signal is order_type=limit), but every order the bot places should
     still carry the current tag, defensively included.
"""
from unittest.mock import MagicMock

from tests.conftest import insert_signal

from exchanges.bybit import build_order_link_id
from models.signal import Action, OrderType, SignalCreate
from routers.webhook import _handle_cancel_close_original_sync, _place_order_sync


def test_cancel_close_original_is_a_noop():
    exchange = MagicMock()
    _handle_cancel_close_original_sync({"id": "flowX", "action": "cancel_close_original"}, exchange)

    exchange.cancel_order.assert_not_called()
    exchange.cancel_partial_sl.assert_not_called()


def test_cancel_close_original_noop_even_without_id():
    """Missing 'id' used to short-circuit with a warning; still must never
    touch the exchange."""
    exchange = MagicMock()
    _handle_cancel_close_original_sync({}, exchange)

    exchange.cancel_order.assert_not_called()
    exchange.cancel_partial_sl.assert_not_called()


def test_market_order_std_tp_is_tagged(tmp_db):
    signal_create = SignalCreate(
        action=Action.BUY, symbol="LINKUSDT", quantity=2.0,
        order_type=OrderType.MARKET, take_profit=12.0, of_id="flowM1",
    )
    exchange = MagicMock()
    exchange.name = "bybit"
    exchange.place_order.return_value = {"order_id": "entry-market-1"}
    exchange.place_tp_order.return_value = {"order_id": "tp-market-1"}

    _place_order_sync(signal_create, exchange)

    exchange.place_tp_order.assert_called_once()
    kwargs = exchange.place_tp_order.call_args.kwargs
    assert kwargs["order_link_id_base"] == build_order_link_id("flowM1", "TP")


def test_market_order_counter_tp_tagged_as_ctp(tmp_db):
    # COUNTER guard requires an active, non-COUNTER original for this of_id.
    insert_signal(tmp_db, action="buy", of_id="flowM2", status="active", pattern_type=None)

    signal_create = SignalCreate(
        action=Action.SELL, symbol="LINKUSDT", quantity=2.0,
        order_type=OrderType.MARKET, take_profit=12.0, of_id="flowM2",
        pattern_type="COUNTER",
    )
    exchange = MagicMock()
    exchange.name = "bybit"
    exchange.place_order.return_value = {"order_id": "entry-market-2"}
    exchange.place_tp_order.return_value = {"order_id": "tp-market-2"}

    _place_order_sync(signal_create, exchange)

    exchange.place_tp_order.assert_called_once()
    kwargs = exchange.place_tp_order.call_args.kwargs
    assert kwargs["order_link_id_base"] == build_order_link_id("flowM2", "CTP")


def test_market_order_tp_without_of_id_stays_untagged(tmp_db):
    """No of_id means nothing to tag with — link_id_base is None, same
    fallback _submit_with_link_id_retry already handles for the limit path."""
    signal_create = SignalCreate(
        action=Action.BUY, symbol="LINKUSDT", quantity=2.0,
        order_type=OrderType.MARKET, take_profit=12.0,
    )
    exchange = MagicMock()
    exchange.name = "bybit"
    exchange.place_order.return_value = {"order_id": "entry-market-3"}
    exchange.place_tp_order.return_value = {"order_id": "tp-market-3"}

    _place_order_sync(signal_create, exchange)

    kwargs = exchange.place_tp_order.call_args.kwargs
    assert kwargs["order_link_id_base"] is None
