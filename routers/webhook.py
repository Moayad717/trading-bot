from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Request, status

from config import now_local, settings
from db import insert_signal, update_signal_status
from exchanges.bybit import BybitExchange
from models.signal import Signal, SignalStatus
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

    try:
        signal_create = tv_parser.parse(payload)
    except ValueError as exc:
        logger.warning("Failed to parse TradingView payload: %s | payload=%s", exc, payload)
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))

    exchange = _get_exchange()
    signal = Signal(
        **signal_create.model_dump(),
        timestamp=now_local(),
        exchange=exchange.name,
        status=SignalStatus.PENDING,
    )
    signal_id = await insert_signal(signal)

    try:
        result = exchange.place_order(signal_create)
        await update_signal_status(
            signal_id,
            status=SignalStatus.FILLED,
            order_id=result.get("order_id"),
        )
        logger.info("Order placed: id=%s symbol=%s action=%s order_id=%s",
                    signal_id, signal.symbol, signal.action, result.get("order_id"))
        return {
            "signal_id": signal_id,
            "status": "filled",
            "order_id": result.get("order_id"),
            "exchange": exchange.name,
        }

    except Exception as exc:
        error_msg = str(exc)
        await update_signal_status(signal_id, status=SignalStatus.FAILED, error_message=error_msg)
        logger.error("Order failed: id=%s symbol=%s error=%s", signal_id, signal.symbol, error_msg)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={"signal_id": signal_id, "error": error_msg},
        )
