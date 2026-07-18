from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from pybit.unified_trading import WebSocket

from config import now_local, settings
from db import (
    bulk_complete_signals_sync,
    get_active_signals_for_symbol_sync,
    get_tp_info_sync,
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
    - Entry fill  → status ACTIVE, entry_fill_time set, TP order placed, tp_order_id stored.
    - TP fill     → status COMPLETED, completion_time set.
    - Cancelled / Rejected / Expired → status FAILED.
    Runs in its own thread managed by pybit — safe to start/stop from asyncio lifespan.
    """

    def __init__(self, exchange: Optional[BybitExchange] = None) -> None:
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

            elif bybit_status in ("Cancelled", "Rejected", "Expired", "Deactivated"):
                updated = update_order_status_sync(order_id, SignalStatus.FAILED)
                if updated:
                    logger.info("Order %s: order_id=%s", bybit_status.lower(), order_id)

    def _handle_fill(self, order: Dict[str, Any]) -> None:
        order_id    = order.get("orderId", "")
        fill_time   = _now_local_iso()
        reduce_only = order.get("reduceOnly", False)

        # Try entry fill first (matches rows with status='open')
        was_entry = mark_entry_filled_sync(order_id, fill_time)
        if was_entry:
            logger.info(
                "Entry filled: order_id=%s symbol=%s",
                order_id, order.get("symbol"),
            )
            self._maybe_place_tp(order)
            return

        # TP fill — may be signal-linked or a bulk watchdog TP with no matching tp_order_id
        was_signal_tp = mark_tp_completed_sync(order_id, fill_time)
        if was_signal_tp:
            logger.info(
                "TP filled → position completed: tp_order_id=%s symbol=%s",
                order_id, order.get("symbol"),
            )

        # After any reduce-only fill, reconcile DB against Bybit position size.
        # This catches bulk watchdog TPs that cover multiple signals at once.
        if reduce_only:
            symbol  = order.get("symbol", "")
            tp_side = order.get("side", "")  # TP order side — opposite of position side
            if symbol and tp_side:
                position_side = "Sell" if tp_side == "Buy" else "Buy"
                self._reconcile_tp_fill(symbol, position_side, fill_time)

    def _reconcile_tp_fill(self, symbol: str, position_side: str, fill_time: str) -> None:
        """Compare Bybit's live position size against active DB signals and mark the
        excess as completed FIFO (oldest entry_fill_time first).

        Called after every reduce-only fill so bulk watchdog TPs (which may cover
        several individual signals at once) are fully reflected in the dashboard.
        """
        if self._exchange is None:
            return

        action = "buy" if position_side == "Buy" else "sell"

        try:
            bybit_size = self._exchange.get_position_size(symbol, position_side)
        except Exception as exc:
            logger.error(
                "TP reconciliation: failed to get position size symbol=%s: %s", symbol, exc
            )
            return

        active = get_active_signals_for_symbol_sync(symbol, action)
        if not active:
            return

        db_qty   = sum(float(s["quantity"]) for s in active)
        closed   = round(db_qty - bybit_size, 8)

        if closed <= 0:
            return

        logger.info(
            "TP reconciliation: symbol=%s bybit_size=%s db_active=%s to_close=%s",
            symbol, bybit_size, db_qty, closed,
        )

        # Walk FIFO (oldest first), accumulate until we've covered closed qty
        to_complete: list[int] = []
        accumulated = 0.0
        for sig in active:
            if accumulated >= closed:
                break
            to_complete.append(sig["id"])
            accumulated += float(sig["quantity"])

        if to_complete:
            n = bulk_complete_signals_sync(to_complete, fill_time)
            logger.info(
                "TP reconciliation: marked %d signal(s) completed symbol=%s", n, symbol
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
                "TP order placed: entry_order_id=%s tp_order_id=%s symbol=%s side=%s qty=%s price=%s",
                order_id, tp_order_id, info["symbol"], tp_side, filled_qty, info["take_profit"],
            )
        except Exception as exc:
            logger.error("Failed to place TP for order_id=%s: %s", order_id, exc)
