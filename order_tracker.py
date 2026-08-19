from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from pybit.unified_trading import WebSocket

from config import now_local, settings
from db import (
    get_signal_by_order_id_sync,
    get_original_signal_by_of_id_sync,
    link_auto_tp_sync,
    mark_entry_filled_sync,
    mark_tp_completed_sync,
    set_close_original_order_id_sync,
    set_tp_order_id_sync,
    update_order_status_sync,
)
from models.signal import SignalStatus

if TYPE_CHECKING:
    from exchanges.bybit import BybitExchange

logger = logging.getLogger(__name__)


def _now_local_iso() -> str:
    return now_local().isoformat()


class OrderTracker:
    """
    Subscribes to Bybit's private order stream via WebSocket.

    Entry fill  → status ACTIVE, entry_fill_time set, TP placed, tp_order_id stored.
                  For COUNTER entries: also sets a Partial stop-loss on the original
                  position at close_original.trigger_price (dynamic SL), using
                  set_trading_stop instead of a reduce-only limit order.
    TP fill     → status COMPLETED, completion_time set.
    New reduce-only order → backup tp_order_id link via link_auto_tp_sync.
    Cancelled / Rejected / Expired → status FAILED.

    The dynamic-SL fill on the original position is NOT tracked here via WebSocket
    because set_trading_stop does not return a pre-known order_id.  Instead, the
    exit_position/STD_DYNAMIC_SL Pine alert handler in webhook.py marks the
    original signal COMPLETED when Pine confirms the counter's TP hit.

    Runs in its own thread managed by pybit — safe to start/stop from asyncio lifespan.
    """

    def __init__(self, exchange: Optional["BybitExchange"] = None) -> None:
        self._ws: WebSocket | None = None
        self._exchange = exchange

    def start(self) -> None:
        if not settings.BYBIT_API_KEY or not settings.BYBIT_API_SECRET:
            logger.warning("Order tracker disabled — no API keys configured")
            return
        try:
            self._ws = WebSocket(
                testnet=settings.TESTNET,
                channel_type="private",
                api_key=settings.BYBIT_API_KEY,
                api_secret=settings.BYBIT_API_SECRET,
            )
            self._ws.order_stream(callback=self._on_order)
            logger.info("Order tracker connected (testnet=%s)", settings.TESTNET)
        except Exception as exc:
            logger.error("Order tracker failed to start: %s", exc)

    def stop(self) -> None:
        if self._ws:
            try:
                self._ws.exit()
            except Exception:
                pass
            self._ws = None
        logger.info("Order tracker stopped")

    def _on_order(self, message: Dict[str, Any]) -> None:
        for order in message.get("data", []):
            order_id     = order.get("orderId", "")
            bybit_status = order.get("orderStatus", "")

            if not order_id or not bybit_status:
                continue

            if bybit_status == "Filled":
                self._handle_fill(order)

            elif bybit_status == "New" and order.get("reduceOnly"):
                # Auto-TP limit order created by Bybit after entry fills
                self._handle_auto_tp_created(order)

            elif bybit_status in ("Cancelled", "Rejected", "Expired", "Deactivated"):
                updated = update_order_status_sync(order_id, SignalStatus.FAILED)
                if updated:
                    logger.info("Order %s: order_id=%s", bybit_status.lower(), order_id)

    def _handle_fill(self, order: Dict[str, Any]) -> None:
        order_id  = order.get("orderId", "")
        fill_time = _now_local_iso()

        # ── 1. Entry fill ──────────────────────────────────────────────────────
        was_entry = mark_entry_filled_sync(order_id, fill_time)
        if was_entry:
            logger.info(
                "Entry filled: order_id=%s symbol=%s",
                order_id, order.get("symbol"),
            )
            self._maybe_place_tp(order)
            # For COUNTER entries: set the Partial SL on the original position
            self._maybe_place_close_original(order)
            return

        # ── 2. Regular TP fill ─────────────────────────────────────────────────
        was_tp = mark_tp_completed_sync(order_id, fill_time)
        if was_tp:
            logger.info(
                "TP filled → position completed: tp_order_id=%s symbol=%s",
                order_id, order.get("symbol"),
            )

    def _maybe_place_tp(self, order: Dict[str, Any]) -> None:
        if self._exchange is None:
            return

        order_id = order.get("orderId", "")
        info     = get_signal_by_order_id_sync(order_id)
        if not info or info.get("take_profit") is None:
            return

        original_side = order.get("side", "")
        tp_side       = "Sell" if original_side == "Buy" else "Buy"
        position_idx  = 1 if original_side == "Buy" else 2

        try:
            filled_qty = round(float(order.get("cumExecQty") or 0), 1)
        except (TypeError, ValueError):
            logger.warning("Invalid cumExecQty in fill event for order_id=%s", order_id)
            return
        if filled_qty <= 0:
            return

        try:
            result = self._exchange.place_tp_order(
                symbol=info["symbol"],
                side=tp_side,
                qty=filled_qty,
                price=float(info["take_profit"]),
                position_idx=position_idx,
                category=info.get("category", "linear"),
            )
            tp_order_id = result.get("order_id", "")
            if tp_order_id:
                set_tp_order_id_sync(order_id, tp_order_id)
            logger.info(
                "TP placed: entry_id=%s tp_id=%s symbol=%s side=%s qty=%s price=%s",
                order_id, tp_order_id, info["symbol"], tp_side, filled_qty, info["take_profit"],
            )
        except Exception as exc:
            logger.error("Failed to place TP for order_id=%s: %s", order_id, exc)

    def _maybe_place_close_original(self, order: Dict[str, Any]) -> None:
        """When a COUNTER entry fills, set a Partial stop-loss on the original position.

        The SL trigger price comes from close_original_json stored with the signal
        (populated from the Pine payload's close_original block).  Falls back to
        counter's take_profit if the block is absent (older Pine versions).

        Uses Bybit's set_trading_stop (tpslMode=Partial, slOrderType=Market) instead
        of a reduce-only limit order — a limit on the active side fills immediately
        for short positions and is therefore not suitable as a stop-loss.
        """
        if self._exchange is None:
            return

        order_id = order.get("orderId", "")
        counter  = get_signal_by_order_id_sync(order_id)
        if counter is None:
            return

        if (counter.get("pattern_type") or "").upper() != "COUNTER":
            return

        of_id = counter.get("of_id")
        if not of_id:
            logger.warning(
                "COUNTER fill has no of_id: order_id=%s — cannot set close_original SL",
                order_id,
            )
            return

        # Resolve SL trigger price from payload block or fall back to counter's TP
        counter_tp        = counter.get("take_profit")
        sl_trigger_price: Optional[float] = None

        co_json = counter.get("close_original_json")
        if co_json:
            try:
                co = json.loads(co_json)
                raw_tp = co.get("trigger_price")
                if raw_tp is not None:
                    sl_trigger_price = float(raw_tp)
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                logger.warning(
                    "close_original_json parse failed for order_id=%s: %s — falling back to take_profit",
                    order_id, exc,
                )

        if sl_trigger_price is None:
            if counter_tp is None:
                logger.error(
                    "COUNTER fill has no close_original_json and no take_profit: "
                    "order_id=%s — cannot set SL on original position",
                    order_id,
                )
                return
            sl_trigger_price = float(counter_tp)
            logger.warning(
                "COUNTER fill: no close_original_json, falling back to take_profit=%.5f "
                "as SL trigger: order_id=%s",
                sl_trigger_price, order_id,
            )

        original = get_original_signal_by_of_id_sync(of_id)
        if not original:
            logger.warning(
                "COUNTER fill: no active original signal for of_id=%s order_id=%s",
                of_id, order_id,
            )
            return

        # Hedge mode: Long original → positionIdx=1; Short original → positionIdx=2
        orig_action  = original["action"]  # "buy" or "sell"
        position_idx = 1 if orig_action == "buy" else 2

        try:
            sl_size = round(float(original["quantity"]), 1)
        except (TypeError, ValueError):
            logger.error(
                "Invalid original quantity for signal id=%s — cannot set SL",
                original["id"],
            )
            return
        if sl_size <= 0:
            return

        try:
            self._exchange.place_partial_sl(
                symbol=original["symbol"],
                position_idx=position_idx,
                sl_trigger_price=sl_trigger_price,
                sl_size=sl_size,
                category=original.get("category", "linear"),
            )
            logger.info(
                "Partial SL set on original: signal_id=%s symbol=%s "
                "positionIdx=%s sl_trigger=%.5f sl_size=%s",
                original["id"], original["symbol"],
                position_idx, sl_trigger_price, sl_size,
            )
        except Exception as exc:
            logger.error(
                "Failed to set Partial SL on original signal_id=%s of_id=%s: %s",
                original["id"], of_id, exc,
            )

    def _handle_auto_tp_created(self, order: Dict[str, Any]) -> None:
        symbol      = order.get("symbol", "")
        tp_order_id = order.get("orderId", "")
        side        = order.get("side", "")
        qty_str     = order.get("qty", "0")

        if not (symbol and tp_order_id and side):
            return

        try:
            qty = round(float(qty_str), 3)
        except (TypeError, ValueError):
            return

        action = "buy" if side == "Sell" else "sell"
        logger.info(
            "Auto-TP created: symbol=%s side=%s qty=%s tp_order_id=%s action=%s",
            symbol, side, qty, tp_order_id, action,
        )
        linked = link_auto_tp_sync(symbol, action, qty, tp_order_id)
        logger.info(
            "Auto-TP link result: linked=%s symbol=%s action=%s qty=%s",
            linked, symbol, action, qty,
        )
