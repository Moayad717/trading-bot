from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from pybit.unified_trading import WebSocket

from config import now_local, settings
from db import (
    complete_signal_by_id_sync,
    complete_signal_by_sl_fill_sync,
    get_original_signal_by_of_id_sync,
    get_signal_by_order_id_sync,
    get_signal_by_sl_order_id_sync,
    get_signal_by_tp_order_id_sync,
    link_auto_tp_sync,
    mark_entry_filled_sync,
    mark_tp_completed_sync,
    set_sl_placed_sync,
    set_tp_order_id_sync,
    update_order_status_sync,
)
from exchanges.bybit import build_order_link_id
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
                  For COUNTER entries: no stop-loss is placed on the original —
                  stop-loss is permanently disabled system-wide (client decision
                  2026-09-16), see _maybe_place_close_original's docstring for the
                  full rationale and data. Every entry (original and counter) now
                  runs independently to its own take-profit only.

    SL fill (real order) → LEGACY ONLY. Rows with a real sl_order_id predate the
                  2026-09-16 removal; if one of those pre-existing conditional
                  orders still fills, the original completes directly from its
                  own fill event, same as before. No new rows ever get an
                  sl_order_id, so this branch naturally goes quiet over time.
    Regular TP fill → signal COMPLETED.

    New reduce-only order → backup tp_order_id link via link_auto_tp_sync.
    Cancelled / Rejected / Expired → status FAILED.

    Pine never fires exit_position; cancel_close_original is a no-op — see
    _handle_cancel_close_original_sync in webhook.py.

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
            # If the signal that just completed is itself an ORIGINAL carrying
            # its own conditional SL (placed to protect it while a counter was
            # open), that SL is now orphaned — the position it protects is
            # closed. Cancel it, or it rests forever. Proven gap (2026-09-12):
            # there was no cleanup at all for the ordinary "original closes via
            # its own TP" path — only the Pine-alert-driven cancel_close_original
            # and exit_position paths cancelled it, and neither fires here. Every
            # original completing this way (the everyday case) leaked one of
            # Bybit's 10-per-symbol conditional-order slots permanently, until
            # all 10 were exhausted and NEW originals could get no protection
            # placed at all — confirmed live: 5 real positions sitting
            # unprotected because of exactly this.
            self._cancel_orphaned_sl_if_any(order_id)
            # LEGACY ONLY: rows whose original still uses the old position-level
            # set_trading_stop (sl_order_id NULL) have no fill event of their own
            # for the SL, so completion is still inferred from the counter's TP
            # fill for those. New rows (sl_order_id set) are completed directly
            # by branch 3 below when their own SL order actually fills — this
            # call is a no-op for them (see the sl_order_id check inside it).
            self._maybe_complete_original_after_counter_tp(order_id, fill_time)
            return

        # ── 3. SL fill (real conditional order) ─────────────────────────────────
        # The original completes from its OWN order's fill — not inferred from
        # the counter's TP. Fixes the proven bug where set_trading_stop's single
        # stop slot per position side got silently overwritten by a second
        # concurrent counter, leaving the first original's DB row marked
        # completed with no matching exchange execution ever having happened.
        sl_signal = get_signal_by_sl_order_id_sync(order_id)
        if sl_signal and sl_signal.get("status") == "active":
            completed = complete_signal_by_sl_fill_sync(order_id, fill_time)
            if completed:
                logger.info(
                    "SL filled → original completed from its own fill: "
                    "signal_id=%s order_id=%s symbol=%s",
                    sl_signal["id"], order_id, order.get("symbol"),
                )

    def _cancel_orphaned_sl_if_any(self, tp_order_id: str) -> None:
        """When a signal's own TP fills, if that same signal also carries a
        still-resting conditional SL (sl_order_id) — meaning it was itself an
        original being protected while paired with a counter — that SL no
        longer protects anything and must be cancelled. Left alone, it rests
        forever (conditional orders don't self-expire — see
        BYBIT_QUIRKS.md #2), permanently consuming one of Bybit's 10-per-symbol
        conditional-order slots. Idempotent and safe: if the order is already
        gone (e.g. Bybit auto-cleared it for some other reason), the cancel
        call just fails harmlessly and is logged, not raised.
        """
        if self._exchange is None:
            return
        sig = get_signal_by_tp_order_id_sync(tp_order_id)
        if not sig or not sig.get("sl_order_id"):
            return
        try:
            self._exchange.cancel_order(sig["sl_order_id"], sig["symbol"])
            logger.info(
                "Cancelled orphaned conditional SL order_id=%s for signal_id=%s "
                "(closed via its own TP, sl_order_id was still resting)",
                sig["sl_order_id"], sig["id"],
            )
        except Exception as exc:
            logger.warning(
                "Failed to cancel orphaned SL order_id=%s for signal_id=%s "
                "(may already be gone): %s",
                sig["sl_order_id"], sig["id"], exc,
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
            raw_filled = float(order.get("cumExecQty") or 0)
        except (TypeError, ValueError):
            logger.warning("Invalid cumExecQty in fill event for order_id=%s", order_id)
            return
        category    = info.get("category", "linear")
        filled_qty  = self._exchange.round_qty(raw_filled, info["symbol"], category)
        if filled_qty <= 0:
            return

        link_id_base = None
        if info.get("of_id"):
            role = "CTP" if (info.get("pattern_type") or "").upper() == "COUNTER" else "TP"
            link_id_base = build_order_link_id(info["of_id"], role)

        try:
            result = self._exchange.place_tp_order(
                symbol=info["symbol"],
                side=tp_side,
                qty=filled_qty,
                price=float(info["take_profit"]),
                position_idx=position_idx,
                category=info.get("category", "linear"),
                order_link_id_base=link_id_base,
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
        """PERMANENTLY DISABLED — client decision 2026-09-16: stop-loss is
        removed from the system entirely, "not now and not later". A COUNTER
        fill no longer attaches any stop to its original; the original runs
        to its own take-profit only. This is unconditional and hardcoded —
        it does NOT consult any dashboard setting (per-timeframe switch,
        cutoff, or the master kill switch in db.py), specifically so a
        setting can never be flipped, misconfigured, or defaulted back into
        placing an SL again. Any close_original block on the alert is
        ignored outright, including for counters that armed before this
        change and are only filling now — same code path, same outcome
        (client spec point 5).

        Data behind the decision (Bybit executions, both bots, 2026-09-06 to
        2026-09-13, each entry judged against its own price, never the
        position average): every TP/CTP closed within +0.500%-+0.508% of its
        own entry, zero exceptions (48 closes on 8003, 171 on 8005). Every
        SL closed at a loss, -0.99% to -1.90% against its own entry, and was
        the only source of losing closes in either bot (6 on 8003, 11 on
        8005). SL fills also sometimes left the original's TP resting,
        double-closing the same entry (3 flows on 8003, 11 on 8005) — that
        class of bug disappears entirely once SL is gone, since every entry
        then has exactly one exit. See BYBIT_QUIRKS.md.

        sl_placed is still recorded as False for every counter fill (not
        left NULL) — this preserves the existing, already-correct Case A
        semantics in _maybe_complete_original_after_counter_tp: "no SL
        exists for this pairing, the original and its counter are two
        independent trades, never infer one's completion from the other's
        fill." Removing this call would silently resurrect the legacy
        completion-inference bug for every new signal.
        """
        order_id = order.get("orderId", "")
        counter  = get_signal_by_order_id_sync(order_id)
        if counter is None:
            return
        if (counter.get("pattern_type") or "").upper() != "COUNTER":
            return

        set_sl_placed_sync(counter["id"], placed=False)
        logger.info(
            "close_original: SL permanently disabled (client decision 2026-09-16) — "
            "of_id=%s counter_signal_id=%s runs independently from its original, "
            "take-profit only, no stop-loss.",
            counter.get("of_id"), counter["id"],
        )

    def _maybe_complete_original_after_counter_tp(
        self, tp_order_id: str, fill_time: str
    ) -> None:
        """LEGACY FALLBACK ONLY. When a COUNTER's TP fills, infer that the partial
        SL on the original (an old position-level set_trading_stop field with no
        order_id of its own) fired at the same price, and mark the original
        COMPLETED.

        This inference is WRONG whenever a second counter on the same side filled
        before the first original closed: set_trading_stop has exactly one stop
        slot per position side, so the second call silently overwrote the first
        counter's stop, yet this function would still mark the first original
        COMPLETED purely because ITS counter's TP happened to fill — with no
        exchange execution ever having closed it. Confirmed live with real data:
        five counters filled within 3 seconds on 2026-08-28, all five originals
        got marked completed this way, and the exchange shows only one small
        unrelated sell that entire day.

        New rows are placed via place_conditional_sl and get a real order_id
        stored in sl_order_id — those complete directly from their OWN fill
        event in _handle_fill's branch 3, never through this inference. This
        function now only fires for rows that predate that change.
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

        # New-style row: a real conditional SL order is tracking this original.
        # Do NOT infer completion here — wait for that order's own fill event.
        # Marking it completed now would be exactly the proven bug this whole
        # mechanism replaces (see docstring above).
        if original.get("sl_order_id"):
            logger.info(
                "COUNTER TP filled but original_signal_id=%s has a real conditional "
                "SL order (sl_order_id=%s) — completion will come from that order's "
                "own fill, not this inference. of_id=%s",
                original["id"], original["sl_order_id"], of_id,
            )
            return

        completed = complete_signal_by_id_sync(original["id"], fill_time)
        logger.info(
            "COUNTER TP filled → original partial SL fired (legacy inference path): "
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
