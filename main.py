from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from config import settings
from dashboard.routes import router as dashboard_router
from db import get_naked_active_positions, init_db_sync, set_signal_error, set_signal_tp_order_id
from exchanges.bybit import BybitExchange
from order_tracker import OrderTracker
from routers.webhook import router as webhook_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


tracker = OrderTracker(exchange=BybitExchange())


async def _reconcile_naked_positions() -> None:
    """Every 60 s, find active positions with no TP order and retry placing it."""
    exchange = BybitExchange()
    while True:
        await asyncio.sleep(60)
        try:
            naked = await get_naked_active_positions()
            if not naked:
                continue
            logger.info("Reconciliation: found %d naked position(s)", len(naked))
            for pos in naked:
                if not pos["take_profit"] or not pos["order_id"]:
                    continue
                tp_side      = "Sell" if pos["action"] == "buy" else "Buy"
                position_idx = 1     if pos["action"] == "buy" else 2
                try:
                    tp_result = exchange.place_tp_order(
                        symbol=pos["symbol"],
                        side=tp_side,
                        qty=pos["quantity"],
                        price=pos["take_profit"],
                        position_idx=position_idx,
                        category=pos["category"],
                    )
                    tp_oid = tp_result.get("order_id", "")
                    if tp_oid:
                        await set_signal_tp_order_id(pos["id"], tp_oid)
                        logger.info(
                            "Reconciliation: TP placed signal_id=%s order_id=%s tp_id=%s",
                            pos["id"], pos["order_id"], tp_oid,
                        )
                    else:
                        logger.warning(
                            "Reconciliation: TP placed but no order_id returned for signal_id=%s",
                            pos["id"],
                        )
                except Exception as exc:
                    logger.error(
                        "Reconciliation: TP failed signal_id=%s order_id=%s error=%s",
                        pos["id"], pos["order_id"], exc,
                    )
                    await set_signal_error(pos["id"], f"Reconciliation TP failed: {exc}")
        except Exception as exc:
            logger.error("Reconciliation watchdog error: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db_sync()
    mode = "TESTNET" if settings.TESTNET else "LIVE"
    logger.info("Trading bot started — exchange=%s mode=%s", settings.active_exchange, mode)
    tracker.start()
    watchdog = asyncio.create_task(_reconcile_naked_positions())
    yield
    watchdog.cancel()
    tracker.stop()
    logger.info("Trading bot stopped")


app = FastAPI(
    title="Trading Bot API",
    description="TradingView → Bybit (extensible) trading bot",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/dashboard/static", StaticFiles(directory="dashboard/static"), name="dashboard-static")
app.include_router(webhook_router)
app.include_router(dashboard_router)


@app.get("/health", tags=["meta"])
async def health() -> dict:
    return {
        "status": "ok",
        "exchange": settings.active_exchange,
        "testnet": settings.TESTNET,
    }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
