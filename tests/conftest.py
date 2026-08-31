"""
Shared pytest fixtures. These env vars must be set before `config`/`db`/`main`
are imported anywhere (including by other test modules) — pytest imports
conftest.py in a directory before any test module in that directory, so this
runs first as long as nothing outside tests/ imports the app first.

No test in this suite ever makes a real network call — BybitExchange is
always mocked. These dummy credentials exist only so `config.Settings()`
doesn't need a real `.env` to construct successfully.
"""
import os
import sqlite3

os.environ.setdefault("BYBIT_API_KEY", "test-key")
os.environ.setdefault("BYBIT_API_SECRET", "test-secret")
os.environ.setdefault("TESTNET", "true")

import pytest  # noqa: E402

from config import settings  # noqa: E402
import db  # noqa: E402


@pytest.fixture
def tmp_db(tmp_path):
    """Point settings.DB_PATH at a fresh temp SQLite file for the duration of
    one test, with the schema initialised, then restore the original path.

    settings is a module-level singleton (config.py: `settings = Settings()`)
    read at call-time by every db.py function — mutating .DB_PATH after
    import is sufficient to redirect it, no need to reimport anything.
    """
    db_path = str(tmp_path / "test_signals.db")
    original = settings.DB_PATH
    settings.DB_PATH = db_path
    db.init_db_sync()
    yield db_path
    settings.DB_PATH = original
    for ext in ("", "-wal", "-shm"):
        p = db_path + ext
        if os.path.exists(p):
            os.remove(p)


def insert_signal(db_path: str, **kwargs) -> int:
    """Insert a minimal signals row directly (bypassing the app's normal
    insert_signal_sync, which expects a full Signal model) — for tests that
    only need specific columns populated."""
    defaults = dict(
        action="buy", symbol="LINKUSDT", quantity=1.0, status="active",
        order_type="limit", category="linear", exchange="bybit", source="test",
        pattern_type=None, of_id=None, tp_order_id=None, sl_order_id=None, sl_placed=None,
    )
    defaults.update(kwargs)
    conn = sqlite3.connect(db_path)
    conn.execute(
        """INSERT INTO signals (action, symbol, quantity, status, order_type, category,
                                 exchange, source, pattern_type, of_id, tp_order_id,
                                 sl_order_id, sl_placed)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (defaults["action"], defaults["symbol"], defaults["quantity"], defaults["status"],
         defaults["order_type"], defaults["category"], defaults["exchange"], defaults["source"],
         defaults["pattern_type"], defaults["of_id"], defaults["tp_order_id"],
         defaults["sl_order_id"], defaults["sl_placed"]),
    )
    conn.commit()
    row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return row_id


def get_status(db_path: str, row_id: int) -> str | None:
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT status FROM signals WHERE id=?", (row_id,)).fetchone()
    conn.close()
    return row[0] if row else None
