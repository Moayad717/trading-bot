from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from pybit.unified_trading import WebSocket

from config import now_local, settings
from db import (
    get_tp_info_sync,
    link_auto_tp_sync,
    mark_entry_filled_sync,
    mark_tp_completed_sync,
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
    - Entry fill  → status ACTIVE, entry_fill_time set, reduce-only TP placed, tp_order_id stored.
    - TP fill     → status COMPLETED, completion_time set.
    - New reduce-only order → backup tp_order_id link via link_auto_tp_sync.
    - Cancelled / Rejected / Expired → status FAILED.
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

        was_entry = mark_entry_filled_sync(order_id, fill_time)
        if was_entry:
            logger.info(
                "Entry filled: order_id=%s symbol=%s",
                order_id, order.get("symbol"),
            )
            self._maybe_place_tp(order)
            return

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
        info     = get_tp_info_sync(order_id)
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

