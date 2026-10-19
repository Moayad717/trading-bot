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
    get_active_signals_no_tp,
    get_active_signals_with_tp,
    init_db_sync,
    start_db_write_worker,
)
from exchanges.bybit import BybitExchange, build_order_link_id
from missing_tp import find_naked_entries, tracked_symbols
from order_tracker import OrderTracker
from routers.webhook import router as webhook_router
from telegram_alerts import send_telegram

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


tracker = OrderTracker(exchange=BybitExchange())

# Signal ids already Telegram-notified for a missing TP, while the gap
# persists — pruned back down to only currently-pending ids every cycle, so
# a resolved one can notify again if it ever recurs. Prevents re-spamming
# the same finding every 30 s (2026-10-19).
_missing_tp_notified: set[int] = set()

# Order IDs already warned about for "Cancelled/Rejected TP detected" below —
# without this, the same cancelled order repeats the warning every cycle for
# as long as it stays within the last-50 order-history window (2026-10-14,
# reported as log spam). Read-only either way; this only limits how often we
# say it, never what we do.
_cancelled_tp_warned: set[str] = set()


async def _fix_missing_tps() -> None:
    """Every 30 s, find entries that genuinely still need their own TP and
    tell a human about it — never places anything itself (changed
    2026-10-19, after two real incidents where automatic placement here hit
    an entry a human had already closed by hand with an untagged order, and
    placed a redundant TP that filled instantly against unrelated trades,
    costing real money — see missing_tp.py's module docstring for the full
    history of both incidents).

    find_naked_entries (missing_tp.py) does the actual detection — scoped to
    settings.FIX_TP_SYMBOLS only (previously this scanned every symbol with
    an open position on the account, including a user's own unrelated
    manual trades on other coins), and checked against Bybit's real
    execution history, not just tagged orders, so an untagged manual close
    is caught too. Anything where the price has already passed the TP level
    is flagged as needing manual handling rather than offered for a normal
    confirm — a resting limit order there would just fill instantly as a
    market order.

    Each signal is only Telegram-notified ONCE while the gap persists
    (_missing_tp_notified) — not re-spammed every 30 s. The notification
    points at POST /dashboard/missing-tps/{id}/confirm, which re-verifies
    everything fresh at the moment of confirmation rather than trusting
    anything this loop found a moment — or a Telegram message — ago.
    """
    exchange = BybitExchange()
    while True:
        await asyncio.sleep(30)
        try:
            candidates = find_naked_entries(exchange)
            current_ids = {c["signal_id"] for c in candidates}
            _missing_tp_notified.intersection_update(current_ids)

            for c in candidates:
                sig_id = c["signal_id"]
                if sig_id in _missing_tp_notified:
                    continue
                _missing_tp_notified.add(sig_id)

                if c["needs_manual_handling"]:
                    logger.warning(
                        "Fix-TP: sig_id=%s of_id=%s symbol=%s needs a TP but price has "
                        "already passed %s — cannot auto-propose, needs manual handling "
                        "on Bybit directly.",
                        sig_id, c["of_id"], c["symbol"], c["price"],
                    )
                    send_telegram(
                        f"⚠️ {c['symbol']} sig_id={sig_id} of_id={c['of_id']} needs "
                        f"a take-profit but price has already passed {c['price']} "
                        f"(side={c['side']} qty={c['qty']}). A normal limit TP would fill "
                        f"instantly here — this needs manual handling on Bybit, not a "
                        f"confirm."
                    )
                else:
                    logger.info(
                        "Fix-TP: missing TP sig_id=%s of_id=%s symbol=%s side=%s qty=%s "
                        "price=%s — awaiting manual confirmation.",
                        sig_id, c["of_id"], c["symbol"], c["side"], c["qty"], c["price"],
                    )
                    send_telegram(
                        f"\U0001f514 Missing TP — sig_id={sig_id} of_id={c['of_id']}\n"
                        f"{c['symbol']} side={c['side']} qty={c['qty']} price={c['price']}\n"
                        f"Confirm: POST /dashboard/missing-tps/{sig_id}/confirm"
                    )
        except Exception as exc:
            logger.error("Fix-TP loop error: %s", exc)

        try:
            # Only check for cancelled TPs when tracked-symbol positions are
            # still open — avoids repeated warnings for stale entries that
            # linger in order history. Read-only — this only logs a
            # warning, it never places or cancels anything itself. Each
            # order_id warns at most once per process lifetime
            # (_cancelled_tp_warned) — previously this repeated every cycle
            # for as long as the same cancelled order stayed within the
            # last-50 window, reported as log spam on 2026-10-14.
            positions = exchange.get_positions(category="linear", settle_coin="USDT")
            tracked = tracked_symbols()
            active_symbols = {
                p["symbol"] for p in positions
                if float(p.get("size", 0)) > 0 and p.get("symbol") in tracked
            }
            if active_symbols:
                history = exchange.get_order_history(category="linear", settle_coin="USDT", limit=50)
                for o in history:
                    order_id = o.get("orderId")
                    if (o.get("reduceOnly")
                            and o.get("orderStatus") in ("Cancelled", "Rejected")
                            and o.get("symbol") in active_symbols
                            and order_id not in _cancelled_tp_warned):
                        _cancelled_tp_warned.add(order_id)
                        logger.warning(
                            "Fix-TP: %s TP detected symbol=%s order_id=%s",
                            o["orderStatus"], o["symbol"], o.get("orderId"),
                        )
        except Exception as exc:
            logger.error("Fix-TP cancelled-check error: %s", exc)


async def reconcile_db_with_executions() -> int:
    """Match active DB signals against Bybit execution history and mark completed ones.

    Pass 1 — exact match: tp_order_id → closedSize execution orderId.
    Pass 2 — tag match: signals with NULL tp_order_id matched by their own
              of_id-derived order tag (see below), not fuzzy qty/timing.
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

    # Pass 2 — tag match for signals with no tp_order_id (changed 2026-10-14,
    # was fuzzy qty+timing matching: "a closing execution of about this size,
    # some time after entry, close enough" — the exact same shape of mistake
    # already fixed once for TP placement (_fix_missing_tps). It already
    # caused real false completions before (85 rows on 8003, 182 on 8005)
    # and nothing stopped it from happening again. Every TP this bot places
    # carries a tag built from its own of_id — use it, don't guess.
    for sig in await get_active_signals_no_tp():
        of_id = sig.get("of_id")
        if not of_id:
            continue  # no tag to match on — pre-dates tagging, left alone

        role = "CTP" if (sig.get("pattern_type") or "").upper() == "COUNTER" else "TP"
        expected_tag = build_order_link_id(of_id, role)

        match = next(
            (
                ex for ex in executions
                if ex.get("symbol") == sig["symbol"]
                and ex["orderId"] not in used_order_ids
                and str(ex.get("orderLinkId", "")).startswith(expected_tag)
            ),
            None,
        )
        if match is None:
            continue

        await complete_signal_async(sig["id"], _exec_iso(match))
        used_order_ids.add(match["orderId"])
        logger.info(
            "Execution reconciliation (tag match): fixed signal_id=%s of_id=%s "
            "tag=%s",
            sig["id"], of_id, match.get("orderLinkId"),
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
    watchdog  = asyncio.create_task(_fix_missing_tps())
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
