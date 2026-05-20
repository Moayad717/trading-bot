from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from fastapi.responses import FileResponse

from db import (
    get_all_signals,
    get_daily_report,
    get_performance_stats,
    get_signals_before_today,
    get_signals_today,
)

router = APIRouter(prefix="/dashboard", tags=["dashboard"])

_HTML = os.path.join(os.path.dirname(__file__), "index.html")


@router.get("/", include_in_schema=False)
async def dashboard_ui() -> FileResponse:
    return FileResponse(_HTML, media_type="text/html")


@router.get("/stats", summary="Performance statistics")
async def performance_stats() -> Dict[str, Any]:
    return await get_performance_stats()


@router.get("/signals", summary="All signals (most recent first)")
async def all_signals() -> Dict[str, Any]:
    signals = await get_all_signals()
    return {"total": len(signals), "signals": signals}


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
