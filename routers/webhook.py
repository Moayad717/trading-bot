from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request, status

from config import now_local, settings
from db import insert_signal, mark_market_order_filled, set_tp_order_id_sync, update_signal_status
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


_recent_signals: dict[str, float] = {}
_DEDUP_WINDOW = 60.0  # seconds


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


@router.post("/tradingview", status_code=status.HTTP_200_OK)
async def tradingview_webhook(request: Request) -> Dict[str, Any]:
    _verify_secret(request)

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Request body must be valid JSON")

    dry_run: bool = bool(payload.get("dry_run", False))

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

    # Place the order on Bybit first so we know the order_id before writing to DB.
    try:
        result = exchange.place_order(signal_create)
    except Exception as exc:
        error_msg = str(exc)
        logger.error("Order failed: symbol=%s error=%s", signal_create.symbol, error_msg)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={"error": error_msg},
        )

    order_id = result.get("order_id", "")

    # Persist the signal to DB — retry up to 3 times so a transient write-queue
    # stall does not leave a live Bybit order with no dashboard record.
    signal = Signal(
        **signal_create.model_dump(),
        timestamp=now_local(),
        exchange=exchange.name,
        status=SignalStatus.PENDING,
    )
    signal_id: int = 0
    for attempt in range(3):
        try:
            signal_id = await insert_signal(signal)
            break
        except Exception as exc:
            if attempt == 2:
                logger.critical(
                    "CRITICAL: order placed on Bybit but DB insert failed after 3 attempts — "
                    "manual reconciliation required! symbol=%s order_id=%s error=%s",
                    signal_create.symbol, order_id, exc,
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail={"error": "order placed but DB record failed", "order_id": order_id},
                )
            logger.warning(
                "DB insert attempt %d/3 failed (symbol=%s): %s — retrying",
                attempt + 1, signal_create.symbol, exc,
            )
            await asyncio.sleep(0.3 * (attempt + 1))

    if signal_create.order_type == OrderType.MARKET:
        # Market orders fill instantly — mark active then place TP inline.
        # WebSocket fill event fires before the DB record exists, so TP must be
        # placed here rather than in order_tracker.
        fill_time = now_local().isoformat()
        await mark_market_order_filled(signal_id, order_id, fill_time)
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
        # Limit order: sits on the book; WebSocket handles fill → ACTIVE → TP → COMPLETED.
        await update_signal_status(signal_id, status=SignalStatus.OPEN, order_id=order_id)
        placed_status = SignalStatus.OPEN

    logger.info("Order placed: id=%s symbol=%s action=%s status=%s order_id=%s",
                signal_id, signal.symbol, signal.action, placed_status.value, order_id)
    return {
        "signal_id": signal_id,
        "status": placed_status.value,
        "order_id": order_id,
        "exchange": exchange.name,
    }
