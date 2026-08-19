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
    mark_close_original_filled_sync,
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
                  For COUNTER entries: also places a reduce-only limit (close_original)
                  on the original position at the counter's TP price (dynamic SL),
                  and stores its order_id as close_original_order_id.

    close_original fill  → original signal COMPLETED (dynamic SL hit).
    TP fill              → signal COMPLETED.

    New reduce-only order → backup tp_order_id link via link_auto_tp_sync.
    Cancelled / Rejected / Expired → status FAILED.

    No exit_position alerts are expected from the current Pine build; the exchange
    manages both the counter TP and the original close_original limit autonomously.
    cancel_close_original is handled in webhook.py (webhook thread, not here).

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
            # For COUNTER entries: place a reduce-only limit on the original position
            self._maybe_place_close_original(order)
            return

        # ── 2. close_original fill (dynamic SL on the original position) ────────
        # This fires when the counter's TP price is reached and the reduce-only
        # limit placed on the original fills simultaneously with the counter's TP.
        was_close_original = mark_close_original_filled_sync(order_id, fill_time)
        if was_close_original:
            logger.info(
                "close_original filled → original position closed: order_id=%s symbol=%s",
                order_id, order.get("symbol"),
            )
            return

        # ── 3. Regular TP fill ─────────────────────────────────────────────────
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
        """When a COUNTER entry fills, place a reduce-only limit on the original position.

        The Pine payload's close_original block specifies:
          "order_type": "limit"
          "price":      "<counter's TP price>"  ← the dynamic SL level
          "side":       "sell" (close long) or "buy" (close short)
          "reduce_only": true
          "place_on":   "entry_fill"

        This order rests on the exchange alongside the counter's own TP.  When price
        reaches the counter's TP level, both orders fill simultaneously — the exchange
        handles it without a second Pine alert.

        Falls back to counter's take_profit when the close_original block is absent
        (older Pine versions).
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
                "COUNTER fill has no of_id: order_id=%s — cannot place close_original",
                order_id,
            )
            return

        # Resolve the limit price from the close_original block or fall back to take_profit
        sl_limit_price: Optional[float] = None

        co_json = counter.get("close_original_json")
        if co_json:
            try:
                co = json.loads(co_json)
                raw = co.get("price")          # Pine sends "price" for a limit order
                if raw is not None:
                    sl_limit_price = float(raw)
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                logger.warning(
                    "close_original_json parse failed for order_id=%s: %s "
                    "— falling back to take_profit",
                    order_id, exc,
                )

        if sl_limit_price is None:
            counter_tp = counter.get("take_profit")
            if counter_tp is None:
                logger.error(
                    "COUNTER fill has no close_original_json.price and no take_profit: "
                    "order_id=%s — cannot place close_original",
                    order_id,
                )
                return
            sl_limit_price = float(counter_tp)
            logger.warning(
                "COUNTER fill: no close_original_json, falling back to take_profit=%.5f "
                "as close_original price: order_id=%s",
                sl_limit_price, order_id,
            )

        original = get_original_signal_by_of_id_sync(of_id)
        if not original:
            logger.warning(
                "COUNTER fill: no active original signal for of_id=%s order_id=%s",
                of_id, order_id,
            )
            return

        if original.get("close_original_order_id"):
            logger.info(
                "close_original already placed for original signal id=%s — skipping",
                original["id"],
            )
            return

        # Derive close side and hedge-mode positionIdx from original's action.
        # Pine's close_original.side agrees with this derivation, but we rely on
        # the DB record rather than the payload to be resilient against format changes.
        orig_action  = original["action"]      # "buy" (long) or "sell" (short)
        close_side   = "Sell" if orig_action == "buy" else "Buy"
        position_idx = 1 if orig_action == "buy" else 2

        try:
            # Use the counter's actual filled qty for an exact match
            filled_qty = round(float(order.get("cumExecQty") or original["quantity"]), 1)
        except (TypeError, ValueError):
            filled_qty = round(float(original["quantity"]), 1)
        if filled_qty <= 0:
            return

        try:
            result = self._exchange.place_tp_order(
                symbol=original["symbol"],
                side=close_side,
                qty=filled_qty,
                price=sl_limit_price,
                position_idx=position_idx,
                category=original.get("category", "linear"),
            )
            close_order_id = result.get("order_id", "")
            if close_order_id:
                set_close_original_order_id_sync(original["id"], close_order_id)
                logger.info(
                    "close_original placed: original_signal_id=%s close_order_id=%s "
                    "symbol=%s side=%s qty=%s price=%.5f",
                    original["id"], close_order_id,
                    original["symbol"], close_side, filled_qty, sl_limit_price,
                )
            else:
                logger.error(
                    "close_original order returned no orderId: "
                    "original_signal_id=%s symbol=%s",
                    original["id"], original["symbol"],
                )
        except Exception as exc:
            logger.error(
                "Failed to place close_original for original_signal_id=%s of_id=%s: %s",
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
