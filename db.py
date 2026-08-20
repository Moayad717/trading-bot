from __future__ import annotations

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, List, Optional

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

# Columns added after initial schema — safe to run on existing DBs.
_MIGRATIONS = [
    ("stop_loss",       "REAL"),
    ("take_profit",     "REAL"),
    ("trigger_price",   "REAL"),
    ("pattern_type",    "TEXT"),
    ("tp_order_id",     "TEXT"),
    ("entry_fill_time",           "DATETIME"),
    ("completion_time",           "DATETIME"),
    ("interval",                  "TEXT"),
    ("of_id",                     "TEXT"),
    ("close_original_order_id",   "TEXT"),
    ("close_original_json",       "TEXT"),
]


def init_db_sync() -> None:
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(_CREATE_TABLE)
        for col, typedef in _MIGRATIONS:
            try:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {col} {typedef}")
            except sqlite3.OperationalError:
                pass  # column already exists
        conn.commit()


@asynccontextmanager
async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    async with aiosqlite.connect(settings.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        yield db


# ── Write queue — serializes all async DB writes through one worker ───────────

_write_queue: asyncio.Queue = asyncio.Queue()


async def _enqueue_write(coro: Any) -> Any:
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()
    await _write_queue.put((coro, fut))
    return await fut


async def _db_write_worker() -> None:
    while True:
        coro, fut = await _write_queue.get()
        try:
            result = await coro
            if not fut.done():
                fut.set_result(result)
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)


def start_db_write_worker() -> asyncio.Task:
    return asyncio.create_task(_db_write_worker())


# ── Async writes (serialized through queue) ───────────────────────────────────

async def _insert_signal_impl(signal: Signal) -> int:
    async with get_db() as db:
        cursor = await db.execute(
            """
            INSERT INTO signals
                (timestamp, action, symbol, quantity, price, order_type, category,
                 status, exchange, order_id, source, error_message, stop_loss, take_profit,
                 trigger_price, pattern_type, interval, of_id, close_original_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                signal.interval,
                signal.of_id,
                signal.close_original_json,
            ),
        )
        await db.commit()
        return cursor.lastrowid or 0


async def insert_signal(signal: Signal) -> int:
    return await _enqueue_write(_insert_signal_impl(signal))


async def _mark_market_order_filled_impl(signal_id: int, order_id: str, fill_time: str) -> None:
    async with get_db() as db:
        await db.execute(
            "UPDATE signals SET status='active', order_id=?, entry_fill_time=? WHERE id=?",
            (order_id, fill_time, signal_id),
        )
        await db.commit()


async def mark_market_order_filled(signal_id: int, order_id: str, fill_time: str) -> None:
    """Set status=active + order_id + entry_fill_time for a market order that filled instantly."""
    await _enqueue_write(_mark_market_order_filled_impl(signal_id, order_id, fill_time))


async def _set_signal_error_impl(signal_id: int, error_message: str) -> None:
    async with get_db() as db:
        await db.execute(
            "UPDATE signals SET error_message=? WHERE id=?",
            (error_message, signal_id),
        )
        await db.commit()


async def set_signal_error(signal_id: int, error_message: str) -> None:
    """Append an error message to a signal without changing its status."""
    await _enqueue_write(_set_signal_error_impl(signal_id, error_message))


async def _set_signal_tp_order_id_impl(signal_id: int, tp_order_id: str) -> None:
    async with get_db() as db:
        await db.execute(
            "UPDATE signals SET tp_order_id=? WHERE id=?",
            (tp_order_id, signal_id),
        )
        await db.commit()


async def set_signal_tp_order_id(signal_id: int, tp_order_id: str) -> None:
    await _enqueue_write(_set_signal_tp_order_id_impl(signal_id, tp_order_id))


async def _update_signal_status_impl(
    signal_id: int,
    status: SignalStatus,
    order_id: Optional[str],
    error_message: Optional[str],
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


async def update_signal_status(
    signal_id: int,
    status: SignalStatus,
    order_id: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    await _enqueue_write(_update_signal_status_impl(signal_id, status, order_id, error_message))


# ── Sync helpers (called from WebSocket thread) ───────────────────────────────

def mark_entry_filled_sync(order_id: str, fill_time: str) -> bool:
    """Transition OPEN → ACTIVE and record fill timestamp. Returns True if updated."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        cur = conn.execute(
            "UPDATE signals SET status='active', entry_fill_time=? WHERE order_id=? AND status='open'",
            (fill_time, order_id),
        )
        conn.commit()
        return cur.rowcount > 0


def set_tp_order_id_sync(order_id: str, tp_order_id: str) -> None:
    """Store the TP order ID so we can match its fill event later."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        conn.execute(
            "UPDATE signals SET tp_order_id=? WHERE order_id=?",
            (tp_order_id, order_id),
        )
        conn.commit()


def mark_tp_completed_sync(tp_order_id: str, completion_time: str) -> bool:
    """Transition ACTIVE → COMPLETED and record completion timestamp. Returns True if updated."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        cur = conn.execute(
            "UPDATE signals SET status='completed', completion_time=? WHERE tp_order_id=? AND status='active'",
            (completion_time, tp_order_id),
        )
        conn.commit()
        return cur.rowcount > 0


def update_order_status_sync(order_id: str, new_status: SignalStatus) -> bool:
    """Generic sync update — used for FAILED transitions (cancelled/rejected/expired)."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        cur = conn.execute(
            "UPDATE signals SET status = ? WHERE order_id = ? AND status IN ('open', 'active')",
            (new_status.value, order_id),
        )
        conn.commit()
        return cur.rowcount > 0


def insert_signal_sync(signal: Signal) -> int:
    """Insert a new signal row from a thread-pool context. Returns the new row id."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        cur = conn.execute(
            """
            INSERT INTO signals
                (timestamp, action, symbol, quantity, price, order_type, category,
                 status, exchange, order_id, source, error_message, stop_loss, take_profit,
                 trigger_price, pattern_type, interval, of_id, close_original_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                signal.interval,
                signal.of_id,
                signal.close_original_json,
            ),
        )
        conn.commit()
        return cur.lastrowid or 0


def mark_market_order_filled_sync(signal_id: int, order_id: str, fill_time: str) -> None:
    """Set status=active, order_id, entry_fill_time by signal id. Thread-pool safe."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(
            "UPDATE signals SET status='active', order_id=?, entry_fill_time=? WHERE id=?",
            (order_id, fill_time, signal_id),
        )
        conn.commit()


def update_signal_status_sync(
    signal_id: int,
    status: SignalStatus,
    order_id: Optional[str] = None,
) -> None:
    """Update status (and optionally order_id) by signal id. Thread-pool safe."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(
            "UPDATE signals SET status=?, order_id=? WHERE id=?",
            (status.value, order_id, signal_id),
        )
        conn.commit()


def get_tp_info_sync(order_id: str) -> Optional[dict]:
    """Return TP placement data for a filled entry order. Called from WebSocket thread."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        cur = conn.execute(
            "SELECT take_profit, symbol, action, category FROM signals WHERE order_id = ?",
            (order_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {"take_profit": row[0], "symbol": row[1], "action": row[2], "category": row[3]}


def get_signal_by_tp_order_id_sync(tp_order_id: str) -> Optional[dict]:
    """Return the signal whose tp_order_id matches. Used by order_tracker to detect
    COUNTER TP fills so the original can be marked COMPLETED (partial SL fired)."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        cur = conn.execute(
            "SELECT id, pattern_type, of_id FROM signals WHERE tp_order_id = ? LIMIT 1",
            (tp_order_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {"id": row[0], "pattern_type": row[1], "of_id": row[2]}


def get_signal_by_order_id_sync(order_id: str) -> Optional[dict]:
    """Return full signal data for a filled entry order. Used by order_tracker."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        cur = conn.execute(
            """SELECT id, take_profit, symbol, action, category,
                      pattern_type, of_id, quantity, close_original_order_id,
                      close_original_json
               FROM signals WHERE order_id = ?""",
            (order_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "id":                      row[0],
        "take_profit":             row[1],
        "symbol":                  row[2],
        "action":                  row[3],
        "category":                row[4],
        "pattern_type":            row[5],
        "of_id":                   row[6],
        "quantity":                row[7],
        "close_original_order_id": row[8],
        "close_original_json":     row[9],
    }


def get_original_signal_by_of_id_sync(of_id: str) -> Optional[dict]:
    """Return the active original (non-COUNTER) signal that owns this Pine ID."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT id, order_id, symbol, action, quantity,
                      take_profit, tp_order_id, category, close_original_order_id
               FROM signals
               WHERE of_id = ?
                 AND (pattern_type IS NULL OR pattern_type != 'COUNTER')
                 AND status = 'active'
               LIMIT 1""",
            (of_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def get_counter_signal_by_of_id_sync(of_id: str) -> Optional[dict]:
    """Return the COUNTER signal linked to this Pine ID."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """SELECT id, order_id, symbol, action, quantity,
                      take_profit, tp_order_id, category
               FROM signals
               WHERE of_id = ? AND pattern_type = 'COUNTER'
                 AND status IN ('open', 'active')
               LIMIT 1""",
            (of_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def set_close_original_order_id_sync(signal_id: int, close_order_id: str) -> None:
    """Store the order_id of the limit close placed on the original position."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        conn.execute(
            "UPDATE signals SET close_original_order_id = ? WHERE id = ?",
            (close_order_id, signal_id),
        )
        conn.commit()


def clear_close_original_order_id_sync(signal_id: int) -> None:
    """Null out close_original_order_id after it has been cancelled."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        conn.execute(
            "UPDATE signals SET close_original_order_id = NULL WHERE id = ?",
            (signal_id,),
        )
        conn.commit()


def mark_close_original_filled_sync(close_original_order_id: str, fill_time: str) -> bool:
    """Transition original signal ACTIVE → COMPLETED when its close_original order fills."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        cur = conn.execute(
            """UPDATE signals
                  SET status='completed', completion_time=?
                WHERE close_original_order_id = ? AND status = 'active'""",
            (fill_time, close_original_order_id),
        )
        conn.commit()
        return cur.rowcount > 0


def complete_signal_by_id_sync(signal_id: int, completion_time: str) -> bool:
    """Transition a signal ACTIVE → COMPLETED by primary key. Used when the
    dynamic SL fires (exit_position/STD_DYNAMIC_SL) so the original signal row
    is marked closed without needing a matching order_id."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        cur = conn.execute(
            "UPDATE signals SET status='completed', completion_time=? WHERE id=? AND status='active'",
            (completion_time, signal_id),
        )
        conn.commit()
        return cur.rowcount > 0


def link_auto_tp_sync(symbol: str, action: str, qty: float, tp_order_id: str) -> bool:
    """Match a Bybit auto-created TP order to the most recently filled active signal.
    Matches by symbol, action, and quantity (within 0.001 tolerance).
    """
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        cur = conn.execute(
            """
            SELECT id FROM signals
             WHERE symbol = ? AND action = ? AND status = 'active'
               AND tp_order_id IS NULL
               AND ABS(quantity - ?) < 0.001
             ORDER BY entry_fill_time DESC
             LIMIT 1
            """,
            (symbol, action, qty),
        )
        row = cur.fetchone()
        if not row:
            return False
        conn.execute(
            "UPDATE signals SET tp_order_id = ? WHERE id = ?",
            (tp_order_id, row[0]),
        )
        conn.commit()
        return True


def get_active_signals_for_symbol_sync(symbol: str, action: str) -> list:
    """Return active signals for a symbol/action sorted by entry_fill_time ASC (FIFO)."""
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            """
            SELECT id, quantity, entry_fill_time
              FROM signals
             WHERE symbol = ? AND action = ? AND status = 'active'
             ORDER BY entry_fill_time ASC
            """,
            (symbol, action),
        )
        return [dict(row) for row in cur.fetchall()]


def bulk_complete_signals_sync(signal_ids: list, completion_time: str) -> int:
    """Mark the given signal IDs as completed. Returns count of rows updated."""
    if not signal_ids:
        return 0
    placeholders = ",".join("?" * len(signal_ids))
    with sqlite3.connect(settings.DB_PATH, timeout=5) as conn:
        cur = conn.execute(
            f"UPDATE signals SET status='completed', completion_time=?"
            f" WHERE id IN ({placeholders}) AND status='active'",
            [completion_time, *signal_ids],
        )
        conn.commit()
        return cur.rowcount


async def _complete_signal_impl(signal_id: int, completion_time: str) -> None:
    async with get_db() as db:
        await db.execute(
            "UPDATE signals SET status='completed', completion_time=? WHERE id=? AND status='active'",
            (completion_time, signal_id),
        )
        await db.commit()


async def complete_signal_async(signal_id: int, completion_time: str) -> None:
    """Mark a single signal as completed with the given timestamp. Goes through the write queue."""
    await _enqueue_write(_complete_signal_impl(signal_id, completion_time))


# ── Async reads (bypass queue — SQLite handles concurrent reads fine) ─────────

def _row_to_dict(row: aiosqlite.Row) -> dict:
    return dict(row)


async def get_active_signals_needing_tp(symbol: str, action: str) -> List[dict]:
    """Return active filled signals for a symbol/action, including their current tp_order_id."""
    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT id, order_id, quantity, take_profit, tp_order_id, category
              FROM signals
             WHERE status = 'active'
               AND symbol = ?
               AND action = ?
               AND entry_fill_time IS NOT NULL
            """,
            (symbol, action),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_active_signals_no_tp() -> List[dict]:
    """Return active filled signals that have no tp_order_id (TP never placed or failed)."""
    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT id, symbol, action, quantity, entry_fill_time
              FROM signals
             WHERE status = 'active'
               AND tp_order_id IS NULL
               AND entry_fill_time IS NOT NULL
            """
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_active_signals_with_tp() -> List[dict]:
    """Return active signals that have a tp_order_id set."""
    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT id, action, tp_order_id, entry_fill_time
              FROM signals
             WHERE status = 'active'
               AND tp_order_id IS NOT NULL
            """
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_naked_active_positions() -> List[dict]:
    """Return active signals that have filled but have no TP order placed yet."""
    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT id, order_id, symbol, action, quantity, take_profit, category
              FROM signals
             WHERE status = 'active'
               AND tp_order_id IS NULL
               AND entry_fill_time IS NOT NULL
            """
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_all_signals() -> List[dict]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM signals ORDER BY timestamp DESC"
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_signal_counts() -> dict:
    """Return per-status counts for all-time and today via COUNT queries — no full scan."""
    today = today_local()
    statuses = ("open", "active", "completed", "failed")
    async with get_db() as db:
        total_row = await (await db.execute("SELECT COUNT(*) FROM signals")).fetchone()
        today_total_row = await (await db.execute(
            "SELECT COUNT(*) FROM signals WHERE date(timestamp) = ?", (today,)
        )).fetchone()

        all_time: dict[str, int] = {}
        today_counts: dict[str, int] = {}
        for st in statuses:
            r = await (await db.execute(
                "SELECT COUNT(*) FROM signals WHERE status = ?", (st,)
            )).fetchone()
            all_time[st] = r[0] if r else 0
            r2 = await (await db.execute(
                "SELECT COUNT(*) FROM signals WHERE status = ? AND date(timestamp) = ?",
                (st, today),
            )).fetchone()
            today_counts[st] = r2[0] if r2 else 0

    return {
        "total":              total_row[0] if total_row else 0,
        "open":               all_time["open"],
        "active":             all_time["active"],
        "completed":          all_time["completed"],
        "failed":             all_time["failed"],
        "today_total":        today_total_row[0] if today_total_row else 0,
        "today_open":         today_counts["open"],
        "today_active":       today_counts["active"],
        "today_completed":    today_counts["completed"],
        "today_failed":       today_counts["failed"],
    }


_SORT_COLS = {"timestamp", "symbol", "action", "price", "take_profit", "status", "quantity"}


async def get_signals_paginated(
    *,
    page: int = 1,
    limit: int = 25,
    status: str = "all",
    symbol: str = "",
    from_date: str = "",
    to_date: str = "",
    direction: str = "all",
    interval: str = "all",
    sort: str = "timestamp",
    sort_dir: str = "desc",
    fetch_all: bool = False,
) -> dict:
    """Return a page of signals with SQL-level filtering and pagination.

    Rule and session filters are derived fields; pass fetch_all=True from the
    route handler to get the full filtered set for Python post-filtering.
    """
    sort_col = sort if sort in _SORT_COLS else "timestamp"
    order = "ASC" if sort_dir.lower() == "asc" else "DESC"

    conditions: list[str] = []
    params: list[Any] = []

    if status != "all":
        conditions.append("status = ?")
        params.append(status)
    if symbol:
        conditions.append("UPPER(symbol) LIKE ?")
        params.append(f"%{symbol.upper()}%")
    if from_date:
        conditions.append("date(timestamp) >= ?")
        params.append(from_date)
    if to_date:
        conditions.append("date(timestamp) <= ?")
        params.append(to_date)
    if direction != "all":
        conditions.append("action = ?")
        params.append(direction)
    if interval != "all":
        conditions.append("interval = ?")
        params.append(interval)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    async with get_db() as db:
        count_row = await (await db.execute(
            f"SELECT COUNT(*) FROM signals {where}", params
        )).fetchone()
        total = count_row[0] if count_row else 0

        if fetch_all:
            cursor = await db.execute(
                f"SELECT * FROM signals {where} ORDER BY {sort_col} {order}",
                params,
            )
        else:
            offset = (page - 1) * limit
            cursor = await db.execute(
                f"SELECT * FROM signals {where} ORDER BY {sort_col} {order} LIMIT ? OFFSET ?",
                params + [limit, offset],
            )
        rows = await cursor.fetchall()

    return {"total": total, "signals": [_row_to_dict(r) for r in rows]}


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


async def get_signals_by_date(target_date: str) -> List[dict]:
    async with get_db() as db:
        cursor = await db.execute(
            "SELECT * FROM signals WHERE date(timestamp) = ? ORDER BY timestamp DESC",
            (target_date,),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_signals_for_stats(prev_day: str, day_before: str) -> List[dict]:
    """Fetch all signals relevant to a two-day stats window.

    Includes signals confirmed on either day AND signals completed on either day
    (even if confirmed earlier — for 'carried' completion counting).
    """
    async with get_db() as db:
        cursor = await db.execute(
            """
            SELECT * FROM signals
            WHERE date(timestamp) IN (?, ?)
               OR (completion_time IS NOT NULL AND date(completion_time) IN (?, ?))
            """,
            (prev_day, day_before, prev_day, day_before),
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]


async def get_performance_stats() -> dict:
    today = today_local()
    # 'active', 'completed', and legacy 'filled' all count as successful entries
    _success = "status IN ('filled', 'active', 'completed')"
    async with get_db() as db:
        total_row        = await (await db.execute("SELECT COUNT(*) FROM signals")).fetchone()
        filled_row       = await (await db.execute(f"SELECT COUNT(*) FROM signals WHERE {_success}")).fetchone()
        failed_row       = await (await db.execute("SELECT COUNT(*) FROM signals WHERE status='failed'")).fetchone()
        filled_today_row = await (await db.execute(
            f"SELECT COUNT(*) FROM signals WHERE {_success} AND date(timestamp) = ?", (today,)
        )).fetchone()
        failed_today_row = await (await db.execute(
            "SELECT COUNT(*) FROM signals WHERE status='failed' AND date(timestamp) = ?", (today,)
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


async def get_all_signals_for_summary(days: int = 30) -> List[dict]:
    """Fetch the last N days of signals for summary/best-combo computation."""
    async with get_db() as db:
        cursor = await db.execute(
            f"""
            SELECT * FROM signals
            WHERE date(timestamp) >= date('now', '-{days} days')
            ORDER BY timestamp DESC
            """
        )
        rows = await cursor.fetchall()
        return [_row_to_dict(r) for r in rows]
