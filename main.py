from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from config import settings
from dashboard.routes import router as dashboard_router
from db import complete_signal_async, get_active_signals_with_tp, init_db_sync, start_db_write_worker
from exchanges.bybit import BybitExchange
from order_tracker import OrderTracker
from routers.webhook import router as webhook_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


tracker = OrderTracker(exchange=BybitExchange())


async def _reconcile_positions() -> None:
    """Every 60 s, reconcile open Bybit positions against active TP orders directly.

    Approach: position-level, not signal-level. Catches naked positions regardless
    of whether they have a DB record (e.g. after a DB write failure).

    Steps:
      1. Fetch all open positions from Bybit.
      2. Fetch all active open orders; sum reduce-only qty per (symbol, side).
      3. For each position: naked_qty = position_size - covered_tp_qty.
         If naked_qty > 0, place a reduce-only limit TP (+0.5% long / -0.5% short).
      4. Log any recently cancelled/rejected reduce-only orders as warnings
         (replacements are handled implicitly by step 3 on the next or same cycle).
    """
    exchange = BybitExchange()
    while True:
        await asyncio.sleep(60)
        try:
            # 1. Open positions (USDT-settled linear perps)
            positions = exchange.get_positions(category="linear", settle_coin="USDT")
            active = [p for p in positions if float(p.get("size", 0)) > 0]
            if not active:
                continue

            # 2. Active open orders (all pages) — build covered-TP map keyed by (symbol, tp_order_side)
            open_orders = exchange.get_all_open_orders(category="linear", settle_coin="USDT")
            covered: dict[tuple[str, str], float] = {}
            for o in open_orders:
                if o.get("reduceOnly"):
                    key = (o["symbol"], o["side"])
                    covered[key] = covered.get(key, 0.0) + float(o.get("qty", 0))

            # 3. Place TP for any uncovered position qty
            for pos in active:
                symbol       = pos["symbol"]
                pos_side     = pos["side"]          # "Buy" (long) or "Sell" (short)
                size         = float(pos["size"])
                avg_price    = float(pos["avgPrice"])
                position_idx = 1 if pos_side == "Buy" else 2
                tp_side      = "Sell" if pos_side == "Buy" else "Buy"

                covered_qty = covered.get((symbol, tp_side), 0.0)
                naked_qty   = round(size - covered_qty, 3)

                if naked_qty <= 0:
                    continue

                tp_price = round(avg_price * (1.005 if pos_side == "Buy" else 0.995), 2)

                if naked_qty * tp_price < 5.0:
                    logger.info(
                        "Reconciliation: skip TP symbol=%s side=%s qty=%s price=%s "
                        "notional=%.4f below minimum 5 USDT",
                        symbol, pos_side, naked_qty, tp_price, naked_qty * tp_price,
                    )
                    continue
                logger.info(
                    "Reconciliation: naked position symbol=%s side=%s size=%s covered=%s "
                    "naked=%s tp_price=%s",
                    symbol, pos_side, size, covered_qty, naked_qty, tp_price,
                )
                try:
                    result = exchange.place_tp_order(
                        symbol=symbol,
                        side=tp_side,
                        qty=naked_qty,
                        price=tp_price,
                        position_idx=position_idx,
                        category="linear",
                    )
                    logger.info(
                        "Reconciliation: TP placed symbol=%s side=%s qty=%s price=%s tp_id=%s",
                        symbol, tp_side, naked_qty, tp_price, result.get("order_id"),
                    )
                except Exception as exc:
                    logger.error(
                        "Reconciliation: failed to place TP symbol=%s side=%s qty=%s error=%s",
                        symbol, tp_side, naked_qty, exc,
                    )

            # 4. Warn on recently cancelled/rejected reduce-only orders
            history = exchange.get_order_history(category="linear", settle_coin="USDT", limit=50)
            for o in history:
                if o.get("reduceOnly") and o.get("orderStatus") in ("Cancelled", "Rejected"):
                    logger.warning(
                        "Reconciliation: %s TP detected symbol=%s order_id=%s "
                        "— position check will replace if position still open",
                        o["orderStatus"], o["symbol"], o.get("orderId"),
                    )

        except Exception as exc:
            logger.error("Reconciliation watchdog error: %s", exc)


async def reconcile_db_with_executions() -> int:
    """Match active DB signals against Bybit execution history and mark completed ones.

    Uses actual Bybit trade records (tp_order_id matched against closedSize > 0
    executions), so it is exact — no size comparisons or FIFO guessing.
    Returns the number of signals fixed.
    """
    exchange = BybitExchange()
    try:
        executions = exchange.get_execution_history("LINKUSDT", category="linear", limit=200)
    except Exception as exc:
        logger.error("Execution reconciliation: failed to fetch executions: %s", exc)
        return 0

    if not executions:
        return 0

    # Build orderId → ISO completion time from execution timestamps (milliseconds)
    closed_map: dict[str, str] = {
        ex["orderId"]: datetime.fromtimestamp(
            int(ex["execTime"]) / 1000, tz=timezone.utc
        ).replace(tzinfo=None).isoformat()
        for ex in executions
    }

    signals = await get_active_signals_with_tp()
    fixed = 0
    for sig in signals:
        tp_oid = sig["tp_order_id"]
        if tp_oid in closed_map:
            await complete_signal_async(sig["id"], closed_map[tp_oid])
            logger.info(
                "Execution reconciliation: fixed signal_id=%s tp_order_id=%s completed_at=%s",
                sig["id"], tp_oid, closed_map[tp_oid],
            )
            fixed += 1

    if fixed:
        logger.info("Execution reconciliation: %d signal(s) marked completed", fixed)
    else:
        logger.debug("Execution reconciliation: nothing to fix")

    return fixed


async def _execution_sync_loop() -> None:
    """Run execution reconciliation every 5 minutes."""
    while True:
        await asyncio.sleep(300)
        try:
            await reconcile_db_with_executions()
        except Exception as exc:
            logger.error("Execution sync loop error: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db_sync()
    mode = "TESTNET" if settings.TESTNET else "LIVE"
    logger.info("Trading bot started — exchange=%s mode=%s", settings.active_exchange, mode)
    tracker.start()
    write_worker = start_db_write_worker()
    await reconcile_db_with_executions()  # fix historical backlog on startup
    watchdog  = asyncio.create_task(_reconcile_positions())
    exec_sync = asyncio.create_task(_execution_sync_loop())
    yield
    exec_sync.cancel()
    watchdog.cancel()
    write_worker.cancel()
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


@app.post("/admin/reconcile", tags=["admin"])
async def trigger_reconciliation() -> dict:
    """Manually trigger an execution-based DB reconciliation. Returns count of signals fixed."""
    fixed = await reconcile_db_with_executions()
    return {"fixed": fixed}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
