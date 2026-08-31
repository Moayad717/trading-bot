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
from db import (
    complete_signal_async,
    get_active_signals_needing_tp,
    get_active_signals_no_tp,
    get_active_signals_with_tp,
    init_db_sync,
    set_tp_order_id_sync,
    start_db_write_worker,
)
from exchanges.bybit import BybitExchange
from order_tracker import OrderTracker
from routers.webhook import router as webhook_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


tracker = OrderTracker(exchange=BybitExchange())

# ── Reconciliation back-off state ──────────────────────────────────────────
# Per-signal TP-placement failure counters. After _TP_MAX_FAILURES consecutive
# reconciliation cycles fail to place a signal's TP, the signal is blacklisted
# (never retried again this process's lifetime) and ONE critical log line is
# emitted, instead of the same error repeating every 60 s forever.
_tp_failure_counts: dict[int, int] = {}
_tp_giveup: set[int] = set()
_TP_MAX_FAILURES = 3


async def _reconcile_positions() -> None:
    """Every 60 s, reconcile open Bybit positions against active TP orders.

    For each naked position qty:
      1. Place individual TP orders for each DB signal that has no TP or a stale one,
         using each signal's own qty and configured take_profit price.
         Updates tp_order_id in DB so WebSocket fill events mark exactly that signal.
      2. Any qty not accounted for by DB signals (ghost positions) gets a single
         fallback bulk TP at avg_price ±0.5%.
    """
    exchange = BybitExchange()
    while True:
        await asyncio.sleep(60)
        try:
            positions = exchange.get_positions(category="linear", settle_coin="USDT")
            active = [p for p in positions if float(p.get("size", 0)) > 0]
            if not active:
                continue

            open_orders = exchange.get_all_open_orders(category="linear", settle_coin="USDT")
            open_order_ids = {o["orderId"] for o in open_orders if "orderId" in o}

            for pos in active:
                symbol       = pos["symbol"]
                pos_side     = pos["side"]
                size         = float(pos["size"])
                avg_price    = float(pos["avgPrice"])
                position_idx = 1 if pos_side == "Buy" else 2
                tp_side      = "Sell" if pos_side == "Buy" else "Buy"
                action       = "buy" if pos_side == "Buy" else "sell"

                # Covered qty is read directly from Bybit's own resting orders for this
                # exact symbol+side+positionIdx — NOT from DB tp_order_id linkage. The DB
                # view and Bybit's view can diverge badly (e.g. when several signals'
                # individual TPs get consolidated into one bulk order): the DB then sees
                # each of those signals as "uncovered" while Bybit already has the full
                # qty covered by that one order, and the old DB-based check would hammer
                # retries forever trying to "fix" a position that was never broken.
                # Scoping by positionIdx (not just symbol+side) avoids counting an
                # unrelated order on the opposite hedge-mode position as coverage.
                signals = await get_active_signals_needing_tp(symbol, action)
                covered_qty = sum(
                    float(o.get("qty", 0))
                    for o in open_orders
                    if o.get("symbol") == symbol
                    and o.get("side") == tp_side
                    and o.get("positionIdx") == position_idx
                )
                naked_qty   = round(size - covered_qty, 3)

                if naked_qty <= 0:
                    continue

                fallback_tp_price = round(avg_price * (1.005 if pos_side == "Buy" else 0.995), 2)

                logger.info(
                    "Reconciliation: naked position symbol=%s side=%s size=%s covered=%s naked=%s",
                    symbol, pos_side, size, covered_qty, naked_qty,
                )

                # Per-signal TP placement — signals with no TP or a cancelled/stale one,
                # excluding any signal that has already exhausted its retry budget.
                signals_needing_tp = [
                    s for s in signals
                    if (not s.get("tp_order_id") or s["tp_order_id"] not in open_order_ids)
                    and s["id"] not in _tp_giveup
                ]

                remaining = naked_qty
                for sig in signals_needing_tp:
                    if remaining <= 0:
                        break

                    sig_qty   = min(round(float(sig["quantity"]), 3), remaining)
                    raw_tp    = sig.get("take_profit")
                    tp_price  = float(raw_tp) if raw_tp is not None else fallback_tp_price
                    entry_oid = sig.get("order_id", "")

                    if sig_qty * tp_price < 5.0:
                        logger.info(
                            "Reconciliation: skip signal TP sig_id=%s symbol=%s qty=%s "
                            "notional=%.4f below 5 USDT",
                            sig["id"], symbol, sig_qty, sig_qty * tp_price,
                        )
                        continue

                    logger.info(
                        "Reconciliation: placing TP for sig_id=%s symbol=%s side=%s qty=%s price=%s",
                        sig["id"], symbol, tp_side, sig_qty, tp_price,
                    )
                    try:
                        result = exchange.place_tp_order(
                            symbol=symbol,
                            side=tp_side,
                            qty=sig_qty,
                            price=tp_price,
                            position_idx=position_idx,
                            category=sig.get("category", "linear"),
                        )
                        tp_order_id = result.get("order_id", "")
                        if tp_order_id and entry_oid:
                            set_tp_order_id_sync(entry_oid, tp_order_id)
                        _tp_failure_counts.pop(sig["id"], None)
                        logger.info(
                            "Reconciliation: TP placed sig_id=%s tp_id=%s symbol=%s",
                            sig["id"], tp_order_id, symbol,
                        )
                        remaining = round(remaining - sig_qty, 8)
                    except Exception as exc:
                        fail_count = _tp_failure_counts.get(sig["id"], 0) + 1
                        _tp_failure_counts[sig["id"]] = fail_count
                        if fail_count >= _TP_MAX_FAILURES:
                            _tp_giveup.add(sig["id"])
                            logger.critical(
                                "Reconciliation: GIVING UP on sig_id=%s symbol=%s after %d "
                                "consecutive failed TP placements — will not retry again this "
                                "run. MANUAL INTERVENTION REQUIRED. Last error: %s",
                                sig["id"], symbol, fail_count, exc,
                            )
                        else:
                            logger.error(
                                "Reconciliation: failed TP for sig_id=%s symbol=%s "
                                "(attempt %d/%d): %s",
                                sig["id"], symbol, fail_count, _TP_MAX_FAILURES, exc,
                            )

                # Ghost bulk TP for qty not covered by any DB signal
                ghost_qty = round(remaining, 3)
                if ghost_qty > 0:
                    ghost_notional = ghost_qty * fallback_tp_price
                    if ghost_notional < 5.0:
                        logger.info(
                            "Reconciliation: skip ghost TP symbol=%s qty=%s notional=%.4f below 5 USDT",
                            symbol, ghost_qty, ghost_notional,
                        )
                        continue
                    logger.info(
                        "Reconciliation: ghost position symbol=%s side=%s qty=%s tp_price=%s",
                        symbol, pos_side, ghost_qty, fallback_tp_price,
                    )
                    try:
                        result = exchange.place_tp_order(
                            symbol=symbol,
                            side=tp_side,
                            qty=ghost_qty,
                            price=fallback_tp_price,
                            position_idx=position_idx,
                            category="linear",
                        )
                        logger.info(
                            "Reconciliation: ghost TP placed symbol=%s side=%s qty=%s tp_id=%s",
                            symbol, tp_side, ghost_qty, result.get("order_id"),
                        )
                    except Exception as exc:
                        logger.error(
                            "Reconciliation: failed ghost TP symbol=%s side=%s qty=%s: %s",
                            symbol, tp_side, ghost_qty, exc,
                        )

            # Only check for cancelled TPs when positions are still open — avoids
            # repeated warnings for stale entries that linger in order history.
            active_symbols = {pos["symbol"] for pos in active}
            if active_symbols:
                history = exchange.get_order_history(category="linear", settle_coin="USDT", limit=50)
                for o in history:
                    if (o.get("reduceOnly")
                            and o.get("orderStatus") in ("Cancelled", "Rejected")
                            and o.get("symbol") in active_symbols):
                        logger.warning(
                            "Reconciliation: %s TP detected symbol=%s order_id=%s",
                            o["orderStatus"], o["symbol"], o.get("orderId"),
                        )

        except Exception as exc:
            logger.error("Reconciliation watchdog error: %s", exc)


async def reconcile_db_with_executions() -> int:
    """Match active DB signals against Bybit execution history and mark completed ones.

    Pass 1 — exact match: tp_order_id → closedSize execution orderId.
    Pass 2 — fuzzy match: signals with NULL tp_order_id matched by
              symbol + action + qty + execTime > entry_fill_time.
    Returns the total number of signals fixed.
    """
    exchange = BybitExchange()
    try:
        executions = exchange.get_execution_history(category="linear", limit=200)
    except Exception as exc:
        logger.error("Execution reconciliation: failed to fetch executions: %s", exc)
        return 0

    if not executions:
        return 0

    def _exec_iso(ex: dict) -> str:
        return (
            datetime.fromtimestamp(int(ex["execTime"]) / 1000, tz=timezone.utc)
            .replace(tzinfo=None)
            .isoformat()
        )

    fixed = 0
    used_order_ids: set[str] = set()

    # Pass 1 — exact tp_order_id match
    closed_map: dict[str, dict] = {ex["orderId"]: ex for ex in executions}
    for sig in await get_active_signals_with_tp():
        tp_oid = sig["tp_order_id"]
        if tp_oid in closed_map:
            ex = closed_map[tp_oid]
            await complete_signal_async(sig["id"], _exec_iso(ex))
            used_order_ids.add(tp_oid)
            logger.info(
                "Execution reconciliation: fixed signal_id=%s tp_order_id=%s",
                sig["id"], tp_oid,
            )
            fixed += 1

    # Pass 2 — fuzzy match for signals with no tp_order_id
    for sig in await get_active_signals_no_tp():
        close_side = "Sell" if sig["action"] == "buy" else "Buy"
        sig_qty    = float(sig["quantity"])

        entry_time_ms = 0
        if sig.get("entry_fill_time"):
            try:
                entry_time_ms = int(
                    datetime.fromisoformat(sig["entry_fill_time"]).timestamp() * 1000
                )
            except (ValueError, OSError):
                pass

        candidates = [
            ex for ex in executions
            if ex.get("symbol") == sig["symbol"]
            and ex["side"] == close_side
            and abs(float(ex["closedSize"]) - sig_qty) < 0.001
            and int(ex["execTime"]) > entry_time_ms
            and ex["orderId"] not in used_order_ids
        ]

        if not candidates:
            continue

        best = min(candidates, key=lambda x: int(x["execTime"]))
        await complete_signal_async(sig["id"], _exec_iso(best))
        used_order_ids.add(best["orderId"])
        logger.info(
            "Execution reconciliation (qty match): fixed signal_id=%s symbol=%s qty=%s",
            sig["id"], sig["symbol"], sig_qty,
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
