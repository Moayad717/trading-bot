from __future__ import annotations

import logging
from typing import Any, Dict

from pybit.unified_trading import WebSocket

from config import settings
from db import update_order_status_sync
from models.signal import SignalStatus

logger = logging.getLogger(__name__)


class OrderTracker:
    """
    Subscribes to Bybit's private order stream via WebSocket.
    Updates OPEN signals to FILLED (or FAILED) as Bybit sends execution events.
    Runs in its own thread managed by pybit — safe to start/stop from asyncio lifespan.
    """

    def __init__(self) -> None:
        self._ws: WebSocket | None = None

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
            order_id = order.get("orderId", "")
            bybit_status = order.get("orderStatus", "")

            if not order_id or not bybit_status:
                continue

            if bybit_status == "Filled":
                updated = update_order_status_sync(order_id, SignalStatus.FILLED)
                if updated:
                    logger.info("Order filled: order_id=%s symbol=%s", order_id, order.get("symbol"))

            elif bybit_status == "Cancelled":
                updated = update_order_status_sync(order_id, SignalStatus.FAILED)
                if updated:
                    logger.info("Order cancelled: order_id=%s", order_id)
