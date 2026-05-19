from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager, contextmanager
from datetime import date, datetime
from typing import AsyncGenerator, Generator, List, Optional

import aiosqlite

from config import settings
from models.signal import Signal, SignalStatus

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS signals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    action        TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    quantity      REAL NOT NULL,
    price         REAL,
    order_type    TEXT NOT NULL DEFAULT 'market',
    category      TEXT NOT NULL DEFAULT 'linear',
    status        TEXT NOT NULL DEFAULT 'pending',
    exchange      TEXT NOT NULL DEFAULT '',
    order_id      TEXT,
    source        TEXT NOT NULL DEFAULT 'unknown',
    error_message TEXT
);
"""


# ---------------------------------------------------------------------------
# Sync helpers (used at startup)
# ---------------------------------------------------------------------------

def init_db_sync() -> None:
    with sqlite3.connect(settings.DB_PATH) as conn:
        conn.execute(_CREATE_TABLE)
        conn.commit()


# ---------------------------------------------------------------------------
# Async connection factory
# ---------------------------------------------------------------------------

@asynccontextmanager
async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    async with aiosqlite.connect(settings.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        yield db


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------

async def insert_signal(signal: Signal) -> int:
    """Persist a new signal and return its generated id."""
    async with get_db() as db:
        cursor = await db.execute(
            """
            INSERT INTO signals
                (timestamp, action, symbol, quantity, price, order_type, category,
                 status, exchange, order_id, source, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                signal.timestamp.isoformat(),
                signal.action.value,
                signal.symbol,
                signal.quantity,
                signal.price,
                signal.order_type.value,
                signal.category,
                signal.status.value,
                signal.exchange,
                signal.order_id,
                signal.source,
                signal.error_message,
            ),
        )
        await db.commit()
        return cursor.lastrowid


async def update_signal_status(
    signal_id: int,
    status: SignalStatus,
    order_id: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    async with get_db() as db:
        await db.execute(
            """
            UPDATE signals
               SET status = ?, order_id = ?, error_message = ?
             WHERE id = ?
            """,
            (status.value, order_id, error_message, signal_id),
        )
        await db.commit()


# ---------------------------------------------------------------------------
# Read operations (dashboard)
# ---------------------------------------------------------------------------

def _row_to_dict(row: aiosqlite.Row) -> dict:
    return dict(row)


async def get_all_signals() -> List[dict]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM signals ORDER BY timestamp DESC"
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_signals_today() -> List[dict]:
    today = date.today().isoformat()
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM signals WHERE date(timestamp) = ? ORDER BY timestamp DESC",
            (today,),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_signals_before_today() -> List[dict]:
    today = date.today().isoformat()
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM signals WHERE date(timestamp) < ? ORDER BY timestamp DESC",
            (today,),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_daily_report(report_date: Optional[str] = None) -> dict:
    """Return per-symbol counts and statuses for a given date (defaults to today)."""
    target = report_date or date.today().isoformat()
    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT symbol,
                   action,
                   status,
                   COUNT(*)   AS count,
                   -- NOTE: notional is 0 for market orders (no price at signal time).
                   -- To fix later: store actual fill price returned by Bybit after order placement.
                   SUM(quantity * COALESCE(price, 0)) AS notional
              FROM signals
             WHERE date(timestamp) = ?
             GROUP BY symbol, action, status
             ORDER BY symbol, action
            """,
            (target,),
        )
        rows = await cursor.fetchall()
        total_cur = await db.execute(
            "SELECT COUNT(*) FROM signals WHERE date(timestamp) = ?", (target,)
        )
        total = (await total_cur.fetchone())[0]

    return {
        "date": target,
        "total_signals": total,
        "breakdown": [_row_to_dict(r) for r in rows],
    }
