from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from typing import AsyncGenerator, List, Optional

import aiosqlite

from config import settings, today_local
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


def init_db_sync() -> None:
    with sqlite3.connect(settings.DB_PATH) as conn:
        conn.execute(_CREATE_TABLE)
        for col, typedef in [("stop_loss", "REAL"), ("take_profit", "REAL"),
                             ("trigger_price", "REAL"), ("pattern_type", "TEXT")]:
            try:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {col} {typedef}")
            except sqlite3.OperationalError:
                pass  # column already exists
        conn.commit()


@asynccontextmanager
async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    async with aiosqlite.connect(settings.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        yield db


async def insert_signal(signal: Signal) -> int:
    async with get_db() as db:
        cursor = await db.execute(
            """
            INSERT INTO signals
                (timestamp, action, symbol, quantity, price, order_type, category,
                 status, exchange, order_id, source, error_message, stop_loss, take_profit,
                 trigger_price, pattern_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                signal.stop_loss,
                signal.take_profit,
                signal.trigger_price,
                signal.pattern_type,
            ),
        )
        await db.commit()
        return cursor.lastrowid or 0


def update_order_status_sync(order_id: str, new_status: SignalStatus) -> bool:
    """Sync update called from the WebSocket callback thread.
    Only transitions OPEN orders — ignores anything already FILLED or FAILED."""
    with sqlite3.connect(settings.DB_PATH) as conn:
        cur = conn.execute(
            "UPDATE signals SET status = ? WHERE order_id = ? AND status = 'open'",
            (new_status.value, order_id),
        )
        conn.commit()
        return cur.rowcount > 0


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
    today = today_local()
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM signals WHERE date(timestamp) = ? ORDER BY timestamp DESC",
            (today,),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_signals_before_today() -> List[dict]:
    today = today_local()
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM signals WHERE date(timestamp) < ? ORDER BY timestamp DESC",
            (today,),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_performance_stats() -> dict:
    today = today_local()
    async with get_db() as db:
        total_row   = await (await db.execute("SELECT COUNT(*) FROM signals")).fetchone()
        filled_row  = await (await db.execute("SELECT COUNT(*) FROM signals WHERE status = 'filled'")).fetchone()
        failed_row  = await (await db.execute("SELECT COUNT(*) FROM signals WHERE status = 'failed'")).fetchone()
        filled_today_row = await (await db.execute(
            "SELECT COUNT(*) FROM signals WHERE status = 'filled' AND date(timestamp) = ?", (today,)
        )).fetchone()
        failed_today_row = await (await db.execute(
            "SELECT COUNT(*) FROM signals WHERE status = 'failed' AND date(timestamp) = ?", (today,)
        )).fetchone()

    total  = total_row[0]  if total_row  else 0
    filled = filled_row[0] if filled_row else 0
    failed = failed_row[0] if failed_row else 0
    filled_today = filled_today_row[0] if filled_today_row else 0
    failed_today = failed_today_row[0] if failed_today_row else 0
    win_rate = round(filled / total * 100, 1) if total > 0 else 0.0

    return {
        "total_signals": total,
        "filled": filled,
        "failed": failed,
        "win_rate": win_rate,
        "total_filled_today": filled_today,
        "total_failed_today": failed_today,
    }


async def get_daily_report(report_date: Optional[str] = None) -> dict:
    target = report_date or today_local()
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
        row = await total_cur.fetchone()
        total = row[0] if row else 0

    return {
        "date": target,
        "total_signals": total,
        "breakdown": [_row_to_dict(r) for r in rows],
    }
