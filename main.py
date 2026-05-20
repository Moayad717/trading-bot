from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from dashboard.routes import router as dashboard_router
from db import init_db_sync
from order_tracker import OrderTracker
from routers.webhook import router as webhook_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


tracker = OrderTracker()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db_sync()
    mode = "TESTNET" if settings.TESTNET else "LIVE"
    logger.info("Trading bot started — exchange=%s mode=%s", settings.active_exchange, mode)
    tracker.start()
    yield
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
