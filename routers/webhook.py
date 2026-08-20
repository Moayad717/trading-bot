from __future__ import annotations

import asyncio
import csv as _csv
import logging
import os as _os
import time
from datetime import datetime as _dt, timezone as _tz
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request, status

from config import now_local, settings
from config import now_local as _now_local
from db import (
    complete_signal_by_id_sync,
    get_counter_signal_by_of_id_sync,
    get_original_signal_by_of_id_sync,
    insert_signal_sync,
    mark_market_order_filled_sync,
    set_tp_order_id_sync,
    update_signal_status_sync,
)
from exchanges.bybit import BybitExchange
from models.signal import Action, OrderType, Signal, SignalStatus
from sources import tradingview as tv_parser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhook", tags=["webhook"])

# exchange registry
_EXCHANGES = {
    "bybit": BybitExchange,
}


def _get_exchange():
    cls = _EXCHANGES.get(settings.active_exchange)
    if cls is None:
        raise RuntimeError(f"Unknown exchange: {settings.active_exchange}")
    return cls()


# ── Deduplication ────────────────────────────────────────────────────────────
_recent_signals: dict[str, float] = {}
_DEDUP_WINDOW = 60.0  # seconds

# ── Net-delta cap ─────────────────────────────────────────────────────────────
_DELTA_LOG = "net_delta.log"
_DELTA_HEADER = [
    "utc", "action", "interval", "qty",
    "committed_net", "new_net", "cap_coin", "equity",
    "mark_price", "net_leverage", "decision",
]

_delta_snapshot: dict = {}
_delta_snapshot_ts: float = 0.0


def _write_delta_log(*fields: Any) -> None:
    write_header = not _os.path.exists(_DELTA_LOG) or _os.path.getsize(_DELTA_LOG) == 0
    try:
        with open(_DELTA_LOG, "a", newline="") as f:
            w = _csv.writer(f)
            if write_header:
                w.writerow(_DELTA_HEADER)
            w.writerow(fields)
    except Exception as exc:
        logger.warning("Net delta log write failed: %s", exc)


def _check_net_delta(signal_create: Any, exchange: Any) -> str:
    """Evaluate net-delta imbalance for this incoming order.

    Returns one of: ALLOW | REFUSE | SHADOW_WOULD_REFUSE | ERROR_FAILOPEN.
    Only REFUSE causes the order to be rejected; all others allow it through.
    Fail-open on any API or computation error.
    """
    global _delta_snapshot, _delta_snapshot_ts

    try:
        now = time.time()
        if now - _delta_snapshot_ts >= settings.NET_DELTA_CACHE_SEC:
            _delta_snapshot = {
                "positions":   exchange.get_positions(category="linear", settle_coin="USDT"),
                "open_orders": exchange.get_all_open_orders(category="linear", settle_coin="USDT"),
                "equity":      exchange.get_equity(),
            }
            _delta_snapshot_ts = now

        positions   = _delta_snapshot["positions"]
        open_orders = _delta_snapshot["open_orders"]
        equity      = float(_delta_snapshot["equity"])
        mark_price  = float(signal_create.trigger_price or 0)

        if equity <= 0 or mark_price <= 0:
            logger.warning(
                "Net delta cap: invalid equity=%s or mark_price=%s — fail open",
                equity, mark_price,
            )
            return "ERROR_FAILOPEN"

        # Signed delta from filled positions
        pos_net = sum(
            float(p.get("size", 0)) * (1 if p.get("side") == "Buy" else -1)
            for p in positions
            if float(p.get("size", 0)) > 0
        )

        # Signed delta from non-reduceOnly pending orders
        ord_net = sum(
            float(o.get("qty", 0)) * (1 if o.get("side") == "Buy" else -1)
            for o in open_orders
            if not o.get("reduceOnly")
        )

        committed_net = pos_net + ord_net
        signed_qty    = signal_create.quantity if signal_create.action == Action.BUY else -signal_create.quantity
        new_net       = committed_net + signed_qty
        cap_coin      = settings.NET_DELTA_CAP_PCT * equity / mark_price
        net_leverage  = abs(new_net) * mark_price / equity

        # Block only when new order makes imbalance worse AND exceeds cap
        would_refuse = abs(new_net) > cap_coin and abs(new_net) > abs(committed_net)

        if would_refuse and settings.NET_DELTA_CAP_SHADOW:
            decision = "SHADOW_WOULD_REFUSE"
        elif would_refuse:
            decision = "REFUSE"
        else:
            decision = "ALLOW"

        _write_delta_log(
            _dt.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            signal_create.action.value,
            signal_create.interval or "",
            signal_create.quantity,
            round(committed_net, 4),
            round(new_net, 4),
            round(cap_coin, 4),
            round(equity, 2),
            mark_price,
            round(net_leverage, 4),
            decision,
        )
        logger.info(
            "Net delta: committed=%.3f new_net=%.3f cap=%.3f equity=%.2f"
            " net_leverage=%.4f decision=%s",
            committed_net, new_net, cap_coin, equity, net_leverage, decision,
        )
        return decision

    except Exception as exc:
        logger.warning("Net delta cap check failed — fail open: %s", exc)
        return "ERROR_FAILOPEN"


def _cancel_resting_if_over_cap(signal_create: Any, exchange: Any, placed_order_id: str) -> None:
    """After placing an entry order, fetch a fresh snapshot and cancel same-side
    resting non-reduceOnly orders if net delta still exceeds the cap.

    In shadow mode logs the would-be cancellations without acting.
    Fail-open: any error is logged as a warning and the function returns quietly.
    """
    global _delta_snapshot_ts
    _delta_snapshot_ts = 0  # force fresh fetch on next _check_net_delta call

    try:
        positions   = exchange.get_positions(category="linear", settle_coin="USDT")
        open_orders = exchange.get_all_open_orders(category="linear", settle_coin="USDT")
        equity      = float(exchange.get_equity())
        mark_price  = float(signal_create.trigger_price or 0)

        if equity <= 0 or mark_price <= 0:
            logger.warning(
                "NET_DELTA post-place: invalid equity=%s mark_price=%s — skipping cancel check",
                equity, mark_price,
            )
            return

        pos_net = sum(
            float(p.get("size", 0)) * (1 if p.get("side") == "Buy" else -1)
            for p in positions
            if float(p.get("size", 0)) > 0
        )
        ord_net = sum(
            float(o.get("qty", 0)) * (1 if o.get("side") == "Buy" else -1)
            for o in open_orders
            if not o.get("reduceOnly")
        )
        committed_net = pos_net + ord_net
        cap_coin      = settings.NET_DELTA_CAP_PCT * equity / mark_price

        if abs(committed_net) <= cap_coin:
            return  # within cap — nothing to do

        side = "Buy" if signal_create.action == Action.BUY else "Sell"
        to_cancel = [
            o for o in open_orders
            if not o.get("reduceOnly")
            and o.get("side") == side
            and o.get("orderId") != placed_order_id
        ]

        if not to_cancel:
            return

        if settings.NET_DELTA_CAP_SHADOW:
            logger.info(
                "NET_DELTA (shadow): WOULD cancel %d resting %s orders, "
                "net_delta=%.3f exceeded cap=%.3f after fill",
                len(to_cancel), side, committed_net, cap_coin,
            )
            return

        cancelled = 0
        for o in to_cancel:
            try:
                exchange.cancel_order(o["orderId"], o["symbol"])
                cancelled += 1
            except Exception as exc:
                logger.warning(
                    "NET_DELTA: failed to cancel order_id=%s symbol=%s: %s",
                    o.get("orderId"), o.get("symbol"), exc,
                )

        logger.info(
            "NET_DELTA: cancelled %d resting %s orders, "
            "net_delta=%.3f exceeded cap=%.3f after fill",
            cancelled, side, committed_net, cap_coin,
        )

    except Exception as exc:
        logger.warning("NET_DELTA post-place cancel check failed — fail open: %s", exc)


def _is_duplicate(key: str) -> bool:
    """Return True if key fired within the dedup window; register it and return False otherwise.
    Evicts expired entries on every call to keep the dict bounded.
    """
    now = time.time()
    expired = [k for k, ts in _recent_signals.items() if now - ts >= _DEDUP_WINDOW]
    for k in expired:
        del _recent_signals[k]
    if now - _recent_signals.get(key, 0.0) < _DEDUP_WINDOW:
        return True
    _recent_signals[key] = now
    return False


def _verify_secret(request: Request) -> None:
    if not settings.WEBHOOK_SECRET:
        return  # no secret set, skip check
    token = request.headers.get("X-Webhook-Secret") or request.query_params.get("secret")
    if token != settings.WEBHOOK_SECRET:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid webhook secret")


def _handle_exit_position_sync(payload: dict, exchange: Any) -> None:
    """Handle exit_position alerts from Pine.

    NOTE: The current Pine build (RIOT_Counter2) no longer fires exit_position
    alerts.  Counter TP and original close_original both rest on the exchange as
    limit orders and fill autonomously — Pine treats the counter TP hit as
    bookkeeping-only and emits no alert.  This handler is kept for forward
    compatibility with future Pine versions or manual operator calls.

    STD_DYNAMIC_SL — counter's TP hit; the close_original reduce-only limit on
        the original fills at the same price via the WS.  As a safety net:
          1. Cancel the original's regular TP order (Bybit auto-cancels reduceOnly
             on a closed position, but being explicit avoids brief races).
          2. Mark the original COMPLETED in the DB (WS should already handle this
             via mark_close_original_filled_sync, so this is belt-and-suspenders).

    STD_TP_COUNTER_STILL_OPEN — original reached its own TP naturally while a
        counter was still alive (no longer fired by current Pine; kept for compat).
    """
    try:
        of_id  = payload.get("id", "")
        reason = payload.get("reason", "")
        if not of_id:
            logger.warning("exit_position alert missing 'id' field — ignoring")
            return

        original = get_original_signal_by_of_id_sync(of_id)
        counter  = get_counter_signal_by_of_id_sync(of_id)

        if reason == "STD_DYNAMIC_SL":
            # 1. Cancel original's TP (position closing via SL, TP is now orphaned)
            if original and original.get("tp_order_id"):
                try:
                    exchange.cancel_order(original["tp_order_id"], original["symbol"])
                    logger.info(
                        "exit_position/STD_DYNAMIC_SL: cancelled original TP order_id=%s symbol=%s",
                        original["tp_order_id"], original["symbol"],
                    )
                except Exception as exc:
                    logger.warning(
                        "exit_position/STD_DYNAMIC_SL: cancel original TP failed "
                        "(may already be gone): %s", exc,
                    )

            # 2. Mark original as completed — Pine confirms position is now closed
            if original:
                closed = complete_signal_by_id_sync(
                    original["id"], _now_local().isoformat()
                )
                logger.info(
                    "exit_position/STD_DYNAMIC_SL: original signal id=%s marked completed "
                    "(db_updated=%s) symbol=%s",
                    original["id"], closed, original["symbol"],
                )
            else:
                logger.warning(
                    "exit_position/STD_DYNAMIC_SL: no active original signal for of_id=%s "
                    "— cannot mark completed",
                    of_id,
                )

        elif reason == "STD_TP_COUNTER_STILL_OPEN":
            # 1. Remove the Partial SL from original's position (it's already closed by TP)
            if original:
                try:
                    orig_action  = original["action"]
                    position_idx = 1 if orig_action == "buy" else 2
                    exchange.cancel_partial_sl(
                        symbol=original["symbol"],
                        position_idx=position_idx,
                        category=original.get("category", "linear"),
                    )
                    logger.info(
                        "exit_position/STD_TP_COUNTER_STILL_OPEN: removed Partial SL "
                        "from original signal_id=%s symbol=%s",
                        original["id"], original["symbol"],
                    )
                except Exception as exc:
                    logger.warning(
                        "exit_position/STD_TP_COUNTER_STILL_OPEN: cancel_partial_sl failed "
                        "(position already closed, SL auto-removed): %s", exc,
                    )

            # 2. Cancel counter TP + 3. Market-close counter
            if counter:
                ctr_action   = counter["action"]
                close_side   = "Sell" if ctr_action == "buy" else "Buy"
                position_idx = 1 if ctr_action == "buy" else 2

                if counter.get("tp_order_id"):
                    try:
                        exchange.cancel_order(counter["tp_order_id"], counter["symbol"])
                        logger.info(
                            "exit_position/STD_TP_COUNTER_STILL_OPEN: cancelled counter TP "
                            "order_id=%s symbol=%s",
                            counter["tp_order_id"], counter["symbol"],
                        )
                    except Exception as exc:
                        logger.warning(
                            "exit_position/STD_TP_COUNTER_STILL_OPEN: cancel counter TP failed: %s",
                            exc,
                        )

                try:
                    qty = round(float(counter.get("quantity") or 0), 1)
                    if qty > 0:
                        exchange.place_market_close_order(
                            symbol=counter["symbol"],
                            side=close_side,
                            qty=qty,
                            position_idx=position_idx,
                            category=counter.get("category", "linear"),
                        )
                        logger.info(
                            "exit_position/STD_TP_COUNTER_STILL_OPEN: market-closed counter "
                            "symbol=%s side=%s qty=%s",
                            counter["symbol"], close_side, qty,
                        )
                except Exception as exc:
                    logger.error(
                        "exit_position/STD_TP_COUNTER_STILL_OPEN: market close counter failed: %s",
                        exc,
                    )

        else:
            logger.warning(
                "exit_position: unrecognised reason=%r of_id=%s — no action taken",
                reason, of_id,
            )

    except Exception as exc:
        logger.error("Unexpected error in _handle_exit_position_sync: %s", exc)


def _handle_cancel_close_original_sync(payload: dict, exchange: Any) -> None:
    """Handle cancel_close_original alerts from Pine.

    Fired when the ORIGINAL reaches its own TP while a Partial stop-loss set via
    set_trading_stop is still attached to that position.  The original's position
    is now closing on its TP, so the orphaned SL must be removed to avoid a
    spurious partial-close later.

    set_trading_stop has no separate order_id; cancellation is done by calling
    set_trading_stop again with slSize=0 (cancel_partial_sl).  Bybit may have
    already removed it when the position closed — that's fine, the call is
    idempotent and any error is logged without raising.
    """
    try:
        of_id = payload.get("id", "")
        if not of_id:
            logger.warning("cancel_close_original alert missing 'id' field — ignoring")
            return

        original = get_original_signal_by_of_id_sync(of_id)
        if not original:
            logger.warning(
                "cancel_close_original: no active original signal for of_id=%s", of_id
            )
            return

        orig_action  = original["action"]
        position_idx = 1 if orig_action == "buy" else 2

        try:
            exchange.cancel_partial_sl(
                symbol=original["symbol"],
                position_idx=position_idx,
                category=original.get("category", "linear"),
            )
            logger.info(
                "cancel_close_original: Partial SL removed from original "
                "signal_id=%s symbol=%s position_idx=%s",
                original["id"], original["symbol"], position_idx,
            )
        except Exception as exc:
            # Position may already be flat — SL auto-removed, no action needed.
            logger.warning(
                "cancel_close_original: cancel_partial_sl failed "
                "(position may already be closed, SL auto-removed): %s", exc,
            )

    except Exception as exc:
        logger.error("Unexpected error in _handle_cancel_close_original_sync: %s", exc)


def _place_order_sync(signal_create: Any, exchange: Any) -> None:
    """Runs in a thread-pool executor after the 200 OK is returned to TradingView.

    All Bybit API calls are synchronous (pybit HTTP) so running this in a thread
    keeps the event loop free to handle the next incoming webhook immediately.
    All errors are logged — never raised (no HTTP context available).
    """
    try:
        # ── COUNTER guard — must run before ANY exchange call ─────────────────
        # If the original signal is not active in our DB, refuse to open the
        # counter position.  An unhedged counter (no original to close) would be
        # an unmanaged directional trade.
        if (signal_create.pattern_type or "").upper() == "COUNTER":
            of_id = signal_create.of_id or ""
            if not of_id:
                logger.error(
                    "COUNTER order REJECTED — payload missing 'id'/of_id field: "
                    "symbol=%s. Refusing to open unhedged counter position.",
                    signal_create.symbol,
                )
                return
            original = get_original_signal_by_of_id_sync(of_id)
            if not original:
                logger.error(
                    "COUNTER order REJECTED — no active original signal found for "
                    "of_id=%s symbol=%s. Refusing to open unhedged counter position.",
                    of_id, signal_create.symbol,
                )
                return
            logger.info(
                "COUNTER guard passed: of_id=%s original_signal_id=%s symbol=%s",
                of_id, original["id"], signal_create.symbol,
            )

        if settings.NET_DELTA_CAP_ENABLED:
            if _check_net_delta(signal_create, exchange) == "REFUSE":
                logger.info(
                    "Net delta cap: order refused symbol=%s action=%s qty=%s",
                    signal_create.symbol, signal_create.action.value, signal_create.quantity,
                )
                return

        try:
            result = exchange.place_order(signal_create)
        except Exception as exc:
            logger.error(
                "Order placement failed: symbol=%s error=%s",
                signal_create.symbol, exc,
            )
            return

        order_id = result.get("order_id", "")

        if settings.NET_DELTA_CAP_ENABLED:
            _cancel_resting_if_over_cap(signal_create, exchange, order_id)

        signal = Signal(
            **signal_create.model_dump(),
            timestamp=now_local(),
            exchange=exchange.name,
            status=SignalStatus.PENDING,
        )
        signal_id: int = 0
        for attempt in range(3):
            try:
                signal_id = insert_signal_sync(signal)
                break
            except Exception as exc:
                if attempt == 2:
                    logger.critical(
                        "CRITICAL: order placed on Bybit but DB insert failed after 3 attempts — "
                        "manual reconciliation required! symbol=%s order_id=%s error=%s",
                        signal_create.symbol, order_id, exc,
                    )
                    return
                logger.warning(
                    "DB insert attempt %d/3 failed (symbol=%s): %s — retrying",
                    attempt + 1, signal_create.symbol, exc,
                )
                time.sleep(0.3 * (attempt + 1))

        if signal_create.order_type == OrderType.MARKET:
            fill_time = now_local().isoformat()
            mark_market_order_filled_sync(signal_id, order_id, fill_time)
            placed_status = SignalStatus.ACTIVE

            if signal_create.take_profit and order_id:
                tp_side      = "Sell" if signal_create.action == Action.BUY else "Buy"
                position_idx = 1 if signal_create.action == Action.BUY else 2
                try:
                    tp_result = exchange.place_tp_order(
                        symbol=signal_create.symbol,
                        side=tp_side,
                        qty=signal_create.quantity,
                        price=signal_create.take_profit,
                        position_idx=position_idx,
                        category=signal_create.category,
                    )
                    tp_oid = tp_result.get("order_id", "")
                    if tp_oid:
                        set_tp_order_id_sync(order_id, tp_oid)
                    logger.info(
                        "Market TP placed: entry_id=%s tp_id=%s symbol=%s",
                        order_id, tp_oid, signal_create.symbol,
                    )
                except Exception as exc:
                    logger.error(
                        "Market TP failed (watchdog will recover): entry_id=%s symbol=%s error=%s",
                        order_id, signal_create.symbol, exc,
                    )
        else:
            update_signal_status_sync(signal_id, SignalStatus.OPEN, order_id)
            placed_status = SignalStatus.OPEN

        logger.info(
            "Order placed: id=%s symbol=%s action=%s status=%s order_id=%s",
            signal_id, signal_create.symbol, signal_create.action.value,
            placed_status.value, order_id,
        )

    except Exception as exc:
        logger.error("Unexpected error in thread-pool order task: %s", exc)


@router.post("/tradingview", status_code=status.HTTP_200_OK)
async def tradingview_webhook(request: Request) -> Dict[str, Any]:
    _verify_secret(request)

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Request body must be valid JSON")

    dry_run: bool = bool(payload.get("dry_run", False))

    # ── Special action types that bypass the normal parse/order path ──────────
    raw_action = str(payload.get("action", "")).strip().lower()

    if raw_action == "exit_position":
        exchange = _get_exchange()
        asyncio.get_event_loop().run_in_executor(
            None, _handle_exit_position_sync, payload, exchange
        )
        return {"status": "received", "action": "exit_position"}

    if raw_action == "cancel_close_original":
        exchange = _get_exchange()
        asyncio.get_event_loop().run_in_executor(
            None, _handle_cancel_close_original_sync, payload, exchange
        )
        return {"status": "received", "action": "cancel_close_original"}

    try:
        signal_create = tv_parser.parse(payload)
    except ValueError as exc:
        logger.warning("Failed to parse TradingView payload: %s | payload=%s", exc, payload)
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))

    if dry_run:
        logger.info("Dry run — parsed OK, no order placed: %s", signal_create.model_dump())
        return {"dry_run": True, "parsed": signal_create.model_dump()}

    dedup_key = (
        f"{signal_create.symbol}-{signal_create.action.value}"
        f"-{payload.get('rule_type', '')}-{signal_create.interval or ''}"
    )
    if _is_duplicate(dedup_key):
        logger.info("Webhook rejected: duplicate signal key=%s", dedup_key)
        return {"status": "rejected", "reason": "duplicate signal within dedup window"}

    exchange = _get_exchange()

    if settings.PAPER_TRADING:
        signal = Signal(
            **signal_create.model_dump(),
            timestamp=now_local(),
            exchange=exchange.name,
            status=SignalStatus.PENDING,
        )
        signal_id = await insert_signal(signal)
        await update_signal_status(signal_id, status=SignalStatus.OPEN)
        logger.info("Paper trade recorded: id=%s symbol=%s action=%s",
                    signal_id, signal.symbol, signal.action)
        return {
            "signal_id": signal_id,
            "status": SignalStatus.OPEN.value,
            "order_id": None,
            "exchange": "paper",
        }

    asyncio.get_event_loop().run_in_executor(None, _place_order_sync, signal_create, exchange)
    return {"status": "received"}
