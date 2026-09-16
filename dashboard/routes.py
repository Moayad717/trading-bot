from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from datetime import datetime, timedelta
from math import ceil
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from db import (
    delete_sl_timeframe_setting_sync,
    get_all_signals,
    get_all_signals_for_summary,
    get_daily_report,
    get_performance_stats,
    get_signal_counts,
    get_signals_before_today,
    get_signals_by_date,
    get_signals_for_stats,
    get_signals_paginated,
    get_signals_today,
    get_sl_cutoff_sync,
    get_sl_master_switch_sync,
    get_sl_timeframe_settings_sync,
    interval_to_minutes,
    set_sl_cutoff_sync,
    set_sl_master_switch_sync,
    set_sl_timeframe_setting_sync,
)
from utils.session import SESSIONS, classify_sessions

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

_DIR  = os.path.dirname(__file__)
_HTML = os.path.join(_DIR, "index.html")
_SW   = os.path.join(_DIR, "sw.js")

RULES      = ["STD", "OB", "REV", "Unknown"]
DIRECTIONS = ["long", "short"]


@router.get("/", include_in_schema=False)
async def dashboard_ui() -> FileResponse:
    return FileResponse(_HTML, media_type="text/html")


@router.get("/sw.js", include_in_schema=False)
async def service_worker() -> FileResponse:
    return FileResponse(_SW, media_type="application/javascript")


@router.get("/stats", summary="Performance statistics")
async def performance_stats() -> Dict[str, Any]:
    return await get_performance_stats()


@router.get("/api-key", summary="API key expiration info")
async def api_key_info() -> Dict[str, Any]:
    from exchanges.bybit import BybitExchange
    loop = asyncio.get_event_loop()
    try:
        info = await loop.run_in_executor(None, lambda: BybitExchange().get_api_key_info())
    except Exception as exc:
        return {"error": str(exc), "days_left": None, "expired_at": None, "permanent": False}
    days_left  = info.get("deadlineDay")
    expired_at = info.get("expiredAt", "")
    permanent  = (days_left == 0 and not expired_at)
    return {
        "days_left":  days_left,
        "expired_at": expired_at,
        "permanent":  permanent,
    }


@router.get("/balance", summary="Account equity / balance")
async def account_balance() -> Dict[str, Any]:
    from exchanges.bybit import BybitExchange

    def _fetch() -> Dict[str, Any]:
        ex = BybitExchange()
        return {"equity": ex.get_equity(), "balance": ex.get_balance()}

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, _fetch)
    except Exception as exc:
        return {"error": str(exc), "equity": None, "unrealised_pnl": None}

    bal = result["balance"]

    def _to_float(v: Any) -> Optional[float]:
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    return {
        "equity":         result["equity"],
        "wallet_balance": _to_float(bal.get("walletBalance")),
        "unrealised_pnl": _to_float(bal.get("unrealisedPnl")),
    }


class SLTimeframeSettingIn(BaseModel):
    interval: str
    sl_enabled: bool


@router.get("/sl-timeframe-settings", summary="Per-timeframe stop-loss on/off switches")
async def list_sl_timeframe_settings() -> Dict[str, Any]:
    loop = asyncio.get_event_loop()
    settings_list = await loop.run_in_executor(None, get_sl_timeframe_settings_sync)
    return {"settings": settings_list}


@router.post("/sl-timeframe-settings", summary="Add or update a timeframe's stop-loss switch")
async def upsert_sl_timeframe_setting(body: SLTimeframeSettingIn) -> Dict[str, Any]:
    interval = body.interval.strip()
    if not interval:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="interval cannot be empty")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: set_sl_timeframe_setting_sync(interval, body.sl_enabled))
    return {"interval": interval, "sl_enabled": body.sl_enabled}


@router.delete("/sl-timeframe-settings/{interval}", summary="Remove a timeframe's switch (reverts to default: enabled)")
async def remove_sl_timeframe_setting(interval: str) -> Dict[str, Any]:
    loop = asyncio.get_event_loop()
    deleted = await loop.run_in_executor(None, lambda: delete_sl_timeframe_setting_sync(interval))
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"no setting found for interval={interval!r}")
    return {"interval": interval, "deleted": True}


class SLCutoffIn(BaseModel):
    interval: str


@router.get("/sl-cutoff", summary="Stop-loss cutoff: disable SL at/above this timeframe")
async def get_sl_cutoff() -> Dict[str, Any]:
    loop = asyncio.get_event_loop()
    interval = await loop.run_in_executor(None, get_sl_cutoff_sync)
    return {"interval": interval}


@router.post("/sl-cutoff", summary="Set the stop-loss cutoff (e.g. \"120\" = disable SL for 2h and above)")
async def upsert_sl_cutoff(body: SLCutoffIn) -> Dict[str, Any]:
    interval = body.interval.strip()
    if not interval:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="interval cannot be empty")
    if interval_to_minutes(interval) is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"could not parse {interval!r} as a timeframe (expected e.g. \"120\", \"1D\", \"1W\")",
        )
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: set_sl_cutoff_sync(interval))
    return {"interval": interval}


@router.delete("/sl-cutoff", summary="Clear the stop-loss cutoff")
async def remove_sl_cutoff() -> Dict[str, Any]:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: set_sl_cutoff_sync(None))
    return {"cleared": True}


class SLMasterSwitchIn(BaseModel):
    sl_enabled: bool


@router.get("/sl-master-switch", summary="Master kill switch: is SL enabled at all")
async def get_sl_master_switch() -> Dict[str, Any]:
    loop = asyncio.get_event_loop()
    enabled = await loop.run_in_executor(None, get_sl_master_switch_sync)
    return {"sl_enabled": enabled}


@router.post("/sl-master-switch", summary="Flip the master kill switch (off = no SL anywhere, TP/limits unaffected)")
async def upsert_sl_master_switch(body: SLMasterSwitchIn) -> Dict[str, Any]:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda: set_sl_master_switch_sync(body.sl_enabled))
    return {"sl_enabled": body.sl_enabled}


@router.get("/signals/counts", summary="Per-status signal counts (all-time and today)")
async def signal_counts() -> Dict[str, Any]:
    return await get_signal_counts()


@router.get("/signals/export", summary="Export all filtered signals as CSV")
async def export_signals(
    status:    str = Query(default="all"),
    symbol:    str = Query(default=""),
    from_date: str = Query(default="", alias="from"),
    to_date:   str = Query(default="", alias="to"),
    direction: str = Query(default="all"),
    rule:      str = Query(default="all"),
    session:   str = Query(default="all"),
    interval:  str = Query(default="all"),
    sort:      str = Query(default="timestamp"),
    sort_dir:  str = Query(default="desc"),
) -> StreamingResponse:
    result = await get_signals_paginated(
        fetch_all=True,
        status=status, symbol=symbol,
        from_date=from_date, to_date=to_date,
        direction=direction, interval=interval,
        sort=sort, sort_dir=sort_dir,
    )
    signals = _apply_derived_filters(result["signals"], rule=rule, session=session)

    import csv, io
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "Date", "Symbol", "Action", "Pattern", "Interval", "Rule", "Session",
        "Entry", "TP", "Status", "Fill Time", "Completion", "Order ID",
    ])
    for s in signals:
        ts   = s.get("timestamp", "")
        rule_val = _extract_rule(s.get("pattern_type"))
        try:
            ts_dt    = datetime.fromisoformat(str(ts).replace(" ", "T"))
            sessions = ", ".join(classify_sessions(ts_dt))
        except Exception:
            sessions = ""
        writer.writerow([
            ts, s.get("symbol", ""), s.get("action", ""),
            s.get("pattern_type", ""), s.get("interval", ""), rule_val, sessions,
            s.get("price") or s.get("trigger_price", ""), s.get("take_profit", ""),
            s.get("status", ""), s.get("entry_fill_time", ""),
            s.get("completion_time", ""), s.get("order_id", ""),
        ])

    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=signals.csv"},
    )


@router.get("/signals", summary="Paginated signals with server-side filtering")
async def all_signals(
    page:      int = Query(default=1,    ge=1),
    limit:     int = Query(default=25,   ge=1, le=200),
    status:    str = Query(default="all"),
    symbol:    str = Query(default=""),
    from_date: str = Query(default="", alias="from"),
    to_date:   str = Query(default="", alias="to"),
    direction: str = Query(default="all"),
    rule:      str = Query(default="all"),
    session:   str = Query(default="all"),
    interval:  str = Query(default="all"),
    sort:      str = Query(default="timestamp"),
    sort_dir:  str = Query(default="desc"),
) -> Dict[str, Any]:
    need_python_filter = rule != "all" or session != "all"

    result = await get_signals_paginated(
        page=page if not need_python_filter else 1,
        limit=limit if not need_python_filter else 10_000,
        fetch_all=need_python_filter,
        status=status, symbol=symbol,
        from_date=from_date, to_date=to_date,
        direction=direction, interval=interval,
        sort=sort, sort_dir=sort_dir,
    )

    signals = result["signals"]
    if need_python_filter:
        signals = _apply_derived_filters(signals, rule=rule, session=session)
        total   = len(signals)
        start   = (page - 1) * limit
        signals = signals[start : start + limit]
    else:
        total = result["total"]

    pages = max(1, ceil(total / limit))
    return {"total": total, "page": page, "pages": pages, "signals": signals}


@router.get("/signals/today", summary="Signals received today")
async def signals_today() -> Dict[str, Any]:
    signals = await get_signals_today()
    return {"total": len(signals), "signals": signals}


@router.get("/signals/history", summary="Signals received before today")
async def signals_history() -> Dict[str, Any]:
    signals = await get_signals_before_today()
    return {"total": len(signals), "signals": signals}


@router.get("/report/daily", summary="Daily report — per-symbol breakdown")
async def daily_report(
    date: Optional[str] = Query(
        default=None,
        description="ISO date (YYYY-MM-DD). Defaults to today.",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )
) -> Dict[str, Any]:
    return await get_daily_report(date)


@router.get("/daily", summary="All signals for a specific date")
async def signals_for_date(
    date: str = Query(
        description="ISO date (YYYY-MM-DD).",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )
) -> Dict[str, Any]:
    signals = await get_signals_by_date(date)
    return {"date": date, "total": len(signals), "signals": signals}


# ── Shared helpers ────────────────────────────────────────────────────────────

def _apply_derived_filters(
    signals: List[dict],
    rule: str = "all",
    session: str = "all",
) -> List[dict]:
    """Post-filter signals by rule (derived from pattern_type) and/or session."""
    if rule == "all" and session == "all":
        return signals
    out = []
    for s in signals:
        if rule != "all" and _extract_rule(s.get("pattern_type")) != rule:
            continue
        if session != "all":
            try:
                ts_dt = datetime.fromisoformat(str(s.get("timestamp", "")).replace(" ", "T"))
                if session not in classify_sessions(ts_dt):
                    continue
            except Exception:
                continue
        out.append(s)
    return out


# ── Stats helpers ─────────────────────────────────────────────────────────────

def _extract_rule(pattern_type: Optional[str]) -> str:
    if not pattern_type:
        return "Unknown"
    for known in ("STD", "OB", "REV"):
        if known in pattern_type.upper().split("_"):
            return known
    return "Unknown"


def _direction(action: str) -> str:
    return "long" if action.lower() == "buy" else "short"


def _parse_date(ts: Any) -> Optional[str]:
    if not ts:
        return None
    try:
        return str(ts)[:10]
    except Exception:
        return None


def _parse_dt(ts: Any) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace(" ", "T"))
    except (ValueError, TypeError):
        return None


def _empty_cell() -> Dict[str, Any]:
    return {
        "unactive_prev":         0,
        "unactive_before":       0,
        "active_prev":           0,
        "active_before":         0,
        "completed_same_prev":   0,
        "completed_carried_prev": 0,
        "completed_same_before": 0,
        "completed_carried_before": 0,
        "total_prev":            0,
        "total_before":          0,
    }


def _compute_stats(
    signals: List[dict],
    prev_day: str,
    day_before: str,
) -> Dict[str, Dict[str, Dict[str, Dict[str, Any]]]]:
    """Return stats[direction][rule][session] = cell_dict."""
    # Use defaultdict(defaultdict(defaultdict(_empty_cell)))
    stats: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]] = {}

    def get_cell(direction: str, rule: str, session: str) -> Dict[str, Any]:
        return (
            stats
            .setdefault(direction, {})
            .setdefault(rule, {})
            .setdefault(session, _empty_cell())
        )

    for sig in signals:
        ts_dt = _parse_dt(sig.get("timestamp"))
        if ts_dt is None:
            continue

        confirmed_date = ts_dt.date().isoformat()
        direction      = _direction(sig.get("action", ""))
        rule           = _extract_rule(sig.get("pattern_type"))
        sessions       = classify_sessions(ts_dt)
        status         = sig.get("status", "")
        completion_str = sig.get("completion_time")
        completion_date = _parse_date(completion_str)

        # Each signal contributes to every session it belongs to + Overall.
        # For "Overall" we count each signal exactly once.
        session_targets = sessions + ["Overall"]

        for session in session_targets:
            cell = get_cell(direction, rule, session)

            # ── Signals confirmed on prev_day ──────────────────────────────
            if confirmed_date == prev_day:
                cell["total_prev"] += 1
                if status == "open":
                    cell["unactive_prev"] += 1
                elif status in ("active", "filled"):
                    cell["active_prev"] += 1
                elif status == "completed":
                    if completion_date == prev_day:
                        cell["completed_same_prev"] += 1
                    else:
                        # Entry filled but TP hit on a later day — shows as
                        # active when looking at prev_day's snapshot.
                        cell["active_prev"] += 1

            # ── Signals confirmed on day_before ────────────────────────────
            elif confirmed_date == day_before:
                cell["total_before"] += 1
                if status == "open":
                    cell["unactive_before"] += 1
                elif status in ("active", "filled"):
                    cell["active_before"] += 1
                elif status == "completed":
                    if completion_date == day_before:
                        cell["completed_same_before"] += 1
                    else:
                        cell["active_before"] += 1

            # ── Carried completions (confirmed on an earlier day) ──────────
            # These signals were fetched because their completion_time falls
            # in the query window; we credit them to the completion date.
            if completion_date == prev_day and confirmed_date < prev_day:
                cell["completed_carried_prev"] += 1

            if completion_date == day_before and confirmed_date < day_before:
                cell["completed_carried_before"] += 1

    # Add same_day_rate to each cell
    for d_cells in stats.values():
        for r_cells in d_cells.values():
            for cell in r_cells.values():
                tp = cell["total_prev"]
                tb = cell["total_before"]
                cell["rate_prev"]   = round(cell["completed_same_prev"]   / tp * 100, 1) if tp else 0.0
                cell["rate_before"] = round(cell["completed_same_before"] / tb * 100, 1) if tb else 0.0

    return stats


def _totals_row(
    stats: Dict[str, Dict[str, Dict[str, Dict[str, Any]]]],
    sessions: List[str],
) -> Dict[str, Dict[str, Any]]:
    """Aggregate all direction+rule combos into a TOTAL row per session."""
    totals: Dict[str, Dict[str, Any]] = {}
    keys = [
        "unactive_prev", "unactive_before",
        "active_prev", "active_before",
        "completed_same_prev", "completed_carried_prev",
        "completed_same_before", "completed_carried_before",
        "total_prev", "total_before",
    ]
    for session in sessions + ["Overall"]:
        t = {k: 0 for k in keys}
        for d_cells in stats.values():
            for r_cells in d_cells.values():
                cell = r_cells.get(session, {})
                for k in keys:
                    t[k] += cell.get(k, 0)
        tp = t["total_prev"]
        tb = t["total_before"]
        t["rate_prev"]   = round(t["completed_same_prev"]   / tp * 100, 1) if tp else 0.0
        t["rate_before"] = round(t["completed_same_before"] / tb * 100, 1) if tb else 0.0
        totals[session] = t
    return totals


@router.get("/stats/table", summary="Statistics table for two consecutive days")
async def stats_table(
    date: Optional[str] = Query(
        default=None,
        description="ISO date for 'previous day' (YYYY-MM-DD). Defaults to yesterday.",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    )
) -> Dict[str, Any]:
    if date:
        prev_day = date
    else:
        prev_day = (datetime.utcnow().date() - timedelta(days=1)).isoformat()

    day_before = (
        datetime.fromisoformat(prev_day).date() - timedelta(days=1)
    ).isoformat()

    signals = await get_signals_for_stats(prev_day, day_before)
    stats   = _compute_stats(signals, prev_day, day_before)
    totals  = _totals_row(stats, SESSIONS)

    # Serialise into rows for easy table rendering
    rows = []
    for direction in DIRECTIONS:
        for rule in RULES:
            row_cells: Dict[str, Any] = {}
            for session in SESSIONS + ["Overall"]:
                row_cells[session] = stats.get(direction, {}).get(rule, {}).get(session, _empty_cell())
            rows.append({"direction": direction, "rule": rule, "cells": row_cells})

    return {
        "prev_day":   prev_day,
        "day_before": day_before,
        "rows":       rows,
        "totals":     totals,
        "sessions":   SESSIONS,
    }


@router.get("/stats/summary", summary="Best direction / rule / session / combination")
async def stats_summary() -> Dict[str, Any]:
    signals = await get_all_signals_for_summary(days=30)

    # Accumulators: count completed and total per dimension
    def _acc():
        return {"completed": 0, "total": 0, "same_day": 0}

    by_direction: Dict[str, Dict[str, int]] = defaultdict(_acc)
    by_rule:      Dict[str, Dict[str, int]] = defaultdict(_acc)
    by_session:   Dict[str, Dict[str, int]] = defaultdict(_acc)
    by_combo:     Dict[str, Dict[str, int]] = defaultdict(_acc)

    for sig in signals:
        ts_dt = _parse_dt(sig.get("timestamp"))
        if ts_dt is None:
            continue
        direction       = _direction(sig.get("action", ""))
        rule            = _extract_rule(sig.get("pattern_type"))
        sessions        = classify_sessions(ts_dt)
        status          = sig.get("status", "")
        confirmed_date  = ts_dt.date().isoformat()
        completion_date = _parse_date(sig.get("completion_time"))
        is_completed    = status == "completed"
        is_same_day     = is_completed and completion_date == confirmed_date

        def _update(d: Dict[str, Any]) -> None:
            d["total"] += 1
            if is_completed:
                d["completed"] += 1
            if is_same_day:
                d["same_day"] += 1

        _update(by_direction[direction])
        _update(by_rule[rule])
        for s in sessions:
            _update(by_session[s])
            combo_key = f"{direction}|{rule}|{s}"
            _update(by_combo[combo_key])

    def _best_by_same_day_rate(d: Dict[str, Dict[str, int]]) -> Optional[str]:
        best, best_rate = None, -1.0
        for key, v in d.items():
            if v["total"] == 0:
                continue
            rate = v["same_day"] / v["total"]
            if rate > best_rate:
                best_rate, best = rate, key
        return best

    def _best_by_completed(d: Dict[str, Dict[str, int]]) -> Optional[str]:
        return max(d, key=lambda k: d[k]["completed"], default=None)

    best_combo_key = _best_by_same_day_rate(by_combo)
    if best_combo_key:
        bc_dir, bc_rule, bc_session = best_combo_key.split("|")
        best_combo = {"direction": bc_dir, "rule": bc_rule, "session": bc_session}
    else:
        best_combo = None

    return {
        "best_direction": _best_by_completed(by_direction),
        "best_rule":      _best_by_same_day_rate(by_rule),
        "best_session":   _best_by_same_day_rate(by_session),
        "best_combination": best_combo,
    }
