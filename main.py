from __future__ import annotations

import asyncio
import logging
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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
    get_signal_by_order_id_sync,
    init_db_sync,
    mark_entry_filled_sync,
    set_tp_order_id_sync,
    start_db_write_worker,
)
from exchanges.bybit import BybitExchange, build_order_link_id, is_closing_order
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

# Bybit's own stopOrderType values for a real stop-loss — used to exclude SL
# orders from TP coverage. NOT triggerPrice presence: confirmed live
# 2026-09-16 that Bybit's own auto-generated PartialTakeProfit orders also
# carry a triggerPrice, so a triggerPrice-based filter wrongly excludes a
# manually-placed TP from coverage too.
_SL_STOP_ORDER_TYPES = {"StopLoss", "PartialStopLoss"}

# Strips an order's role suffix off its orderLinkId to recover the of_id it
# belongs to — e.g. "LINK-20261013-1500-0050m-S_TP" -> the part before "_TP".
# Covers both the current closing roles (ETP/CTP/CLOSE) and the legacy ones
# (TP/SL) still resting on the exchange from before the rename.
_CLOSING_TAG_RE = re.compile(r"^(.*)_(?:ETP|CTP|CLOSE|TP|SL)\d*$")
# Strips an ENTRY order's role suffix (_E for a plain entry, _CE for a
# counter entry — NOT "_C", that was a real bug here until 2026-10-14: it
# never matched "_CE" at all, since "_CE" doesn't end in "_C", so every
# counter entry was silently invisible to this whole function) to recover
# its of_id.
_ENTRY_TAG_RE = re.compile(r"^(.*)_(?:CE|E)$")

# History pagination depth per gap found — 6 pages of 50 is comfortably more
# than a 30 s cycle could ever need to explain a newly-formed gap; it is not
# meant to reach back for old, pre-tagging exposure (see function docstring).
_HISTORY_PAGES = 6


def _ms_to_local_iso(ms: str | int) -> str:
    return (
        datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
        .astimezone(ZoneInfo(settings.TIMEZONE))
        .replace(tzinfo=None)
        .isoformat()
    )


async def _fix_missing_tps() -> None:
    """Every 30 s, find entries with no take-profit and give each one its
    own — confirmed directly against Bybit's own order tags, never a
    combined/bulk order for leftover qty.

    Replaces the old reconciler (removed 2026-10-14), which fell back to one
    averaged-price "ghost" TP for any qty it couldn't match to a DB row.
    That was wrong on two counts, both hit live: (1) its coverage count
    excluded any order with a triggerPrice, which wrongly excludes Bybit's
    own PartialTakeProfit orders too, so a manually-placed TP wasn't counted
    and a duplicate got placed alongside it; (2) even after that was fixed,
    collapsing several distinct signals into one bulk order throws away the
    one thing that makes each of them individually auditable — its own tag.
    Every entry this system places already carries a unique tag
    (<of_id>_E or _C); the fix is to use it, not guess with an average.

    For each naked position qty:
      1. Compute coverage the same correct way as before — real resting
         closing orders, excluding only genuine stop-losses.
      2. If a gap exists, pull recent order history and find every FILLED
         entry whose tag has no matching take-profit anywhere (open or in
         history) — this is checked directly against Bybit, not the local
         database, since the database is exactly what can fall behind (a
         missed WebSocket event around a connection drop).
      3. For each one, look up its own recorded take-profit price by its own
         entry order_id and place a TP at exactly that price and that
         entry's own filled qty, tagged to match.
      4. An entry with no tag, or a tag with no matching database record
         (pre-dates the tagging system), is left alone and logged — never
         guessed, never folded into an average.
    """
    exchange = BybitExchange()
    while True:
        await asyncio.sleep(30)
        try:
            positions = exchange.get_positions(category="linear", settle_coin="USDT")
            active = [p for p in positions if float(p.get("size", 0)) > 0]
            if not active:
                continue

            open_orders = exchange.get_all_open_orders(category="linear", settle_coin="USDT")

            for pos in active:
                symbol       = pos["symbol"]
                pos_side     = pos["side"]
                size         = float(pos["size"])
                position_idx = 1 if pos_side == "Buy" else 2
                tp_side      = "Sell" if pos_side == "Buy" else "Buy"

                # Every close-type order counts as coverage — TakeProfit,
                # PartialTakeProfit, and plain reduce-only limit TPs alike.
                # Only a genuine stop-loss is excluded.
                covered_qty = sum(
                    float(o.get("qty", 0))
                    for o in open_orders
                    if o.get("symbol") == symbol
                    and o.get("side") == tp_side
                    and o.get("positionIdx") == position_idx
                    and not str(o.get("orderLinkId", "")).endswith("_SL")
                    and o.get("stopOrderType") not in _SL_STOP_ORDER_TYPES
                )
                naked_qty = round(size - covered_qty, 3)
                if naked_qty <= 0:
                    continue

                logger.info(
                    "Fix-TP: naked position symbol=%s side=%s size=%s covered=%s naked=%s",
                    symbol, pos_side, size, covered_qty, naked_qty,
                )

                # A gap exists — find exactly which tagged entries explain it,
                # straight from Bybit's own order history (not the DB).
                history: list[dict] = []
                cursor = ""
                for _ in range(_HISTORY_PAGES):
                    params = {"category": "linear", "symbol": symbol, "limit": 50}
                    if cursor:
                        params["cursor"] = cursor
                    resp = exchange._client.get_order_history(**params)
                    result = resp.get("result", {})
                    history.extend(result.get("list", []))
                    cursor = result.get("nextPageCursor", "")
                    if not cursor:
                        break

                tp_of_ids: set[str] = set()
                for o in open_orders + history:
                    if is_closing_order(o) and o.get("orderLinkId"):
                        m = _CLOSING_TAG_RE.match(o["orderLinkId"])
                        if m:
                            tp_of_ids.add(m.group(1))

                entries = [
                    o for o in history
                    if o.get("side") == pos_side
                    and o.get("positionIdx") == position_idx
                    and o.get("orderStatus") == "Filled"
                    and not o.get("reduceOnly")
                ]

                seen: set[str] = set()
                for o in entries:
                    if naked_qty <= 0:
                        break

                    m = _ENTRY_TAG_RE.match(o.get("orderLinkId") or "")
                    if not m:
                        continue  # untagged / pre-dates tagging — never guessed
                    of_id = m.group(1)
                    if of_id in seen or of_id in tp_of_ids:
                        continue
                    seen.add(of_id)

                    entry_order_id = o.get("orderId", "")
                    row = get_signal_by_order_id_sync(entry_order_id)
                    if row is None or row.get("take_profit") is None:
                        logger.info(
                            "Fix-TP: of_id=%s entry_order_id=%s has no TP and no "
                            "database record with a take_profit price — left alone, "
                            "not guessed.",
                            of_id, entry_order_id,
                        )
                        continue

                    sig_id = row["id"]
                    if sig_id in _tp_giveup:
                        continue

                    tp_price = float(row["take_profit"])
                    category = row.get("category", "linear")
                    fill_qty = exchange.round_qty(float(o.get("qty", 0)), symbol, category)
                    if fill_qty <= 0:
                        continue
                    if fill_qty * tp_price < 5.0:
                        logger.info(
                            "Fix-TP: skip sig_id=%s of_id=%s notional=%.4f below 5 USDT",
                            sig_id, of_id, fill_qty * tp_price,
                        )
                        continue

                    logger.info(
                        "Fix-TP: placing individual TP sig_id=%s of_id=%s symbol=%s "
                        "side=%s qty=%s price=%s",
                        sig_id, of_id, symbol, tp_side, fill_qty, tp_price,
                    )
                    try:
                        result = exchange.place_tp_order(
                            symbol=symbol,
                            side=tp_side,
                            qty=fill_qty,
                            price=tp_price,
                            position_idx=position_idx,
                            category=category,
                            order_link_id_base=build_order_link_id(of_id, "TP"),
                        )
                        tp_order_id = result.get("order_id", "")
                        if not tp_order_id:
                            raise RuntimeError(f"no order_id in place_tp_order result: {result}")

                        # Catch the row up to the same state order_tracker would
                        # have left it in had the fill event not been missed.
                        updated_ms = o.get("updatedTime")
                        if updated_ms:
                            mark_entry_filled_sync(entry_order_id, _ms_to_local_iso(updated_ms))
                        set_tp_order_id_sync(entry_order_id, tp_order_id)

                        _tp_failure_counts.pop(sig_id, None)
                        logger.info(
                            "Fix-TP: TP placed sig_id=%s of_id=%s tp_id=%s symbol=%s",
                            sig_id, of_id, tp_order_id, symbol,
                        )
                        naked_qty = round(naked_qty - fill_qty, 8)
                    except Exception as exc:
                        fail_count = _tp_failure_counts.get(sig_id, 0) + 1
                        _tp_failure_counts[sig_id] = fail_count
                        if fail_count >= _TP_MAX_FAILURES:
                            _tp_giveup.add(sig_id)
                            logger.critical(
                                "Fix-TP: GIVING UP on sig_id=%s of_id=%s symbol=%s after %d "
                                "consecutive failed TP placements — will not retry again "
                                "this run. MANUAL INTERVENTION REQUIRED. Last error: %s",
                                sig_id, of_id, symbol, fail_count, exc,
                            )
                        else:
                            logger.error(
                                "Fix-TP: failed TP for sig_id=%s of_id=%s symbol=%s "
                                "(attempt %d/%d): %s",
                                sig_id, of_id, symbol, fail_count, _TP_MAX_FAILURES, exc,
                            )

            # Only check for cancelled TPs when positions are still open — avoids
            # repeated warnings for stale entries that linger in order history.
            # Read-only — this only logs a warning, it never places or cancels
            # anything itself.
            active_symbols = {pos["symbol"] for pos in active}
            if active_symbols:
                history = exchange.get_order_history(category="linear", settle_coin="USDT", limit=50)
                for o in history:
                    if (o.get("reduceOnly")
                            and o.get("orderStatus") in ("Cancelled", "Rejected")
                            and o.get("symbol") in active_symbols):
                        logger.warning(
                            "Fix-TP: %s TP detected symbol=%s order_id=%s",
                            o["orderStatus"], o["symbol"], o.get("orderId"),
                        )

        except Exception as exc:
            logger.error("Fix-TP loop error: %s", exc)


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
