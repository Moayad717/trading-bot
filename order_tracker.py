from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from pybit.unified_trading import WebSocket

from config import now_local, settings
from db import (
    complete_signal_by_id_sync,
    get_original_signal_by_of_id_sync,
    get_signal_by_order_id_sync,
    get_signal_by_tp_order_id_sync,
    link_auto_tp_sync,
    mark_entry_filled_sync,
    mark_tp_completed_sync,
    set_sl_placed_sync,
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
                  For COUNTER entries: also calls set_trading_stop to attach a Partial
                  stop-loss to the original position at the counter's TP price.
                  set_trading_stop returns no order_id — SL detection happens indirectly.

    COUNTER TP fill → counter COMPLETED + original marked COMPLETED (partial SL fired).
    Regular TP fill → signal COMPLETED.

    New reduce-only order → backup tp_order_id link via link_auto_tp_sync.
    Cancelled / Rejected / Expired → status FAILED.

    Pine never fires exit_position; cancel_close_original is handled in webhook.py.

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
            # For COUNTER entries: attach a partial-position SL to the original
            self._maybe_place_close_original(order)
            return

        # ── 2. TP fill ────────────────────────────────────────────────────────
        was_tp = mark_tp_completed_sync(order_id, fill_time)
        if was_tp:
            logger.info(
                "TP filled → position completed: tp_order_id=%s symbol=%s",
                order_id, order.get("symbol"),
            )
            # When a COUNTER's TP fills, the partial SL on the original fired at
            # the same price — mark the original COMPLETED too.
            self._maybe_complete_original_after_counter_tp(order_id, fill_time)

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
            raw_filled = float(order.get("cumExecQty") or 0)
        except (TypeError, ValueError):
            logger.warning("Invalid cumExecQty in fill event for order_id=%s", order_id)
            return
        category    = info.get("category", "linear")
        filled_qty  = self._exchange.round_qty(raw_filled, info["symbol"], category)
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
        """When a COUNTER entry fills, attach a Partial stop-loss to the original position.

        Pine's close_original block:
          "mode":          "partial_position_sl"
          "trigger_price": "<counter's TP price>"  ← the dynamic SL trigger
          "order_type":    "market"
          "place_on":      "entry_fill"

        A reduce-only LIMIT is explicitly forbidden by Pine: it sits on the active
        side of the book and would fill immediately (below market for longs, above
        for shorts), closing the original at counter entry rather than at the
        trigger. set_trading_stop with tpslMode=Partial is the correct call.

        Falls back to counter's take_profit as the trigger when the block is absent.
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

        # Resolve the SL trigger price from close_original block or fall back to take_profit
        sl_trigger: Optional[float] = None

        co_json = counter.get("close_original_json")
        if co_json:
            try:
                co = json.loads(co_json)
                raw = co.get("trigger_price")    # Pine sends "trigger_price" for set_trading_stop
                if raw is not None:
                    sl_trigger = float(raw)
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                logger.warning(
                    "close_original_json parse failed for order_id=%s: %s "
                    "— falling back to take_profit",
                    order_id, exc,
                )

        if sl_trigger is None:
            counter_tp = counter.get("take_profit")
            if counter_tp is None:
                logger.error(
                    "COUNTER fill has no close_original_json.trigger_price and no take_profit: "
                    "order_id=%s — cannot place close_original",
                    order_id,
                )
                return
            sl_trigger = float(counter_tp)
            logger.warning(
                "COUNTER fill: no close_original_json, falling back to take_profit=%.5f "
                "as SL trigger: order_id=%s",
                sl_trigger, order_id,
            )

        original = get_original_signal_by_of_id_sync(of_id)
        if not original:
            logger.warning(
                "COUNTER fill: no active original signal for of_id=%s order_id=%s",
                of_id, order_id,
            )
            return

        # Derive hedge-mode positionIdx from the original's action.
        orig_action  = original["action"]      # "buy" (long) or "sell" (short)
        position_idx = 1 if orig_action == "buy" else 2

        try:
            # Use the original's quantity rounded to its symbol's qtyStep.
            # The SL covers the original position's size, not the counter's fill qty.
            raw_sl_size = float(original["quantity"])
            sl_size = self._exchange.round_qty(
                raw_sl_size, original["symbol"], original.get("category", "linear")
            )
        except (TypeError, ValueError):
            logger.error(
                "COUNTER fill: invalid original quantity for of_id=%s — cannot size SL",
                of_id,
            )
            return
        if sl_size <= 0:
            return

        self._place_partial_sl_with_retry(
            counter=counter,
            original=original,
            position_idx=position_idx,
            sl_trigger=sl_trigger,
            sl_size=sl_size,
            of_id=of_id,
        )

    def _place_partial_sl_with_retry(
        self,
        counter: dict,
        original: dict,
        position_idx: int,
        sl_trigger: float,
        sl_size: float,
        of_id: str,
    ) -> None:
        """Place the partial SL for the original position, retrying once on transient errors.

        Case A — Bybit returns "zero position": the original closed before the counter
        filled. Per spec 6.5 this is legitimate. Log INFO and mark sl_placed=0 so the
        original is NOT marked COMPLETED via the counter's TP fill path.

        Case B — any other error: retry once after 2 s. If the retry also fails, log
        CRITICAL so the operator knows a counter is live with no stop on its original.
        Mark sl_placed=0 in both failure outcomes.
        """
        for attempt in range(1, 3):
            try:
                self._exchange.place_partial_sl(
                    symbol=original["symbol"],
                    position_idx=position_idx,
                    sl_trigger_price=sl_trigger,
                    sl_size=sl_size,
                    category=original.get("category", "linear"),
                )
                # ── Success ──────────────────────────────────────────────────
                set_sl_placed_sync(counter["id"], placed=True)
                logger.info(
                    "close_original partial SL set: original_signal_id=%s "
                    "symbol=%s position_idx=%s sl_trigger=%.5f sl_size=%s",
                    original["id"], original["symbol"], position_idx, sl_trigger, sl_size,
                )
                return
            except RuntimeError as exc:
                if "zero position" in str(exc).lower():
                    # ── Case A ───────────────────────────────────────────────
                    # Original is already flat. Counter runs alone to its TP.
                    set_sl_placed_sync(counter["id"], placed=False)
                    logger.info(
                        "close_original: original position already closed before counter "
                        "entry filled (original_signal_id=%s of_id=%s) — "
                        "counter runs unpaired, no SL needed (spec 6.5)",
                        original["id"], of_id,
                    )
                    return
                # ── Case B, attempt 1 ────────────────────────────────────────
                if attempt == 1:
                    logger.warning(
                        "close_original partial SL failed (attempt 1/2): "
                        "original_signal_id=%s of_id=%s: %s — retrying in 2s",
                        original["id"], of_id, exc,
                    )
                    time.sleep(2)
                else:
                    # ── Case B, attempt 2 — give up ──────────────────────────
                    set_sl_placed_sync(counter["id"], placed=False)
                    logger.critical(
                        "CRITICAL: close_original partial SL failed after retry — "
                        "counter is LIVE with NO stop on original. "
                        "MANUAL INTERVENTION REQUIRED. "
                        "original_signal_id=%s of_id=%s symbol=%s: %s",
                        original["id"], of_id, original["symbol"], exc,
                    )
            except Exception as exc:
                # Network / unexpected error — same retry path as Case B
                if attempt == 1:
                    logger.warning(
                        "close_original partial SL unexpected error (attempt 1/2): "
                        "original_signal_id=%s of_id=%s: %s — retrying in 2s",
                        original["id"], of_id, exc,
                    )
                    time.sleep(2)
                else:
                    set_sl_placed_sync(counter["id"], placed=False)
                    logger.critical(
                        "CRITICAL: close_original partial SL failed after retry — "
                        "counter is LIVE with NO stop on original. "
                        "MANUAL INTERVENTION REQUIRED. "
                        "original_signal_id=%s of_id=%s symbol=%s: %s",
                        original["id"], of_id, original["symbol"], exc,
                    )

    def _maybe_complete_original_after_counter_tp(
        self, tp_order_id: str, fill_time: str
    ) -> None:
        """When a COUNTER's TP fills, the partial SL on the original fired at the same
        price — mark the original COMPLETED in the DB.

        set_trading_stop produces no separate order_id so there is no WS fill event
        to catch for the SL itself. The counter's TP fill is the reliable signal.
        """
        counter = get_signal_by_tp_order_id_sync(tp_order_id)
        if counter is None:
            return
        if (counter.get("pattern_type") or "").upper() != "COUNTER":
            return
        of_id = counter.get("of_id")
        if not of_id:
            return
        # sl_placed values:
        #   1    → SL was placed successfully; its fill caused the counter's TP to hit.
        #          Mark the original COMPLETED.
        #   0    → SL was never placed (Case A: original closed early, or Case B: API failed).
        #          The original's closure is unrelated to this counter. Do NOT mark COMPLETED.
        #   NULL → Legacy counter created before this column existed. Preserve old behavior
        #          and mark COMPLETED so existing in-flight pairs are not broken.
        sl_placed = counter.get("sl_placed")
        if sl_placed == 0:
            logger.info(
                "COUNTER TP filled but sl_placed=0 — SL was never placed for of_id=%s "
                "(original closed independently). Original status unchanged.",
                of_id,
            )
            return

        original = get_original_signal_by_of_id_sync(of_id)
        if not original:
            logger.info(
                "COUNTER TP filled but original already gone for of_id=%s — nothing to close",
                of_id,
            )
            return
        completed = complete_signal_by_id_sync(original["id"], fill_time)
        logger.info(
            "COUNTER TP filled → original partial SL fired: "
            "original_signal_id=%s of_id=%s db_updated=%s",
            original["id"], of_id, completed,
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
