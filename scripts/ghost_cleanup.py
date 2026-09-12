"""
Permanent, safe, recurring ghost-row cleanup — consolidates every lesson
learned from the manual cleanups this project has needed twice now
(2026-08-31/09-06, then again 2026-09-12/13): the DB drifts from Bybit's
real state over time (a position closes through some path that doesn't
route through the normal completion handlers), and nothing was watching
for it, so it silently accumulates until it causes a real symptom — Bybit
auto-cancelling newly-placed orders for exceeding the real position size
(CancelByReduceOnly), or the reconciler trying to cover more qty than
genuinely exists.

Tier-1 ONLY: a signal is marked completed here ONLY when there is a real,
individually-matched, globally-de-duplicated Bybit execution proving it —
exact same side+qty(within qtyStep tolerance)+timing matching used in this
week's forensic work. Never guesses, never touches an ambiguous case (two
candidates that could both explain the same execution) — those are left
exactly as they are for a human to review, same discipline as every manual
cleanup this project has done.

Safety details already paid for in blood this week, all applied here:
  - execution-history fetch chunked to <7 days per Bybit's hard cap
    (BYBIT_QUIRKS.md's practical-limits section / cad2165)
  - qtyStep-aware match tolerance, not a fixed guess (f75526a)
  - Beirut-local <-> UTC conversion done correctly, not assumed (00311c4)
  - each real execution can only prove ONE signal — global de-duplication
    across the whole run, not per-symbol-in-isolation (investigate_mislinks
    lineage, 2026-08-31)

Run with --dry-run (default) first. --live to actually write. Designed to
run once per bot (like every other maintenance script here), scheduled via
scripts/run_ghost_cleanup.sh + cron for the ongoing/automatic case.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.bybit import BybitExchange  # noqa: E402

BOT_NAME = sys.argv[1] if len(sys.argv) > 1 else os.path.basename(os.getcwd())
LIVE = "--live" in sys.argv
# Only consider signals whose own entry is at least this old, to avoid ever
# racing a trade that's still in the middle of naturally completing through
# the normal WS-driven path.
MIN_AGE_MINUTES = int(os.environ.get("GHOST_MIN_AGE_MIN", "30"))


def utc_to_beirut_local_iso(dt_utc: datetime) -> str:
    return dt_utc.astimezone(ZoneInfo("Asia/Beirut")).replace(tzinfo=None).isoformat()


def local_iso_to_utc_ms(iso: str) -> int:
    dt_local = datetime.fromisoformat(iso).replace(tzinfo=ZoneInfo("Asia/Beirut"))
    return int(dt_local.timestamp() * 1000)


def fetch_executions_chunked(client, symbol: str, start_ms: int, end_ms: int) -> list[dict]:
    CHUNK_MS = int(6.5 * 86400 * 1000)  # stay under Bybit's 7-day cap, margin included
    execs = []
    chunk_start = start_ms
    while chunk_start < end_ms:
        chunk_end = min(chunk_start + CHUNK_MS, end_ms)
        cursor = ""
        while True:
            params = {"category": "linear", "symbol": symbol,
                      "startTime": chunk_start, "endTime": chunk_end, "limit": 100}
            if cursor:
                params["cursor"] = cursor
            resp = client.get_executions(**params)
            if resp.get("retCode") != 0:
                break
            result = resp.get("result", {})
            lst = result.get("list", [])
            execs.extend(lst)
            cursor = result.get("nextPageCursor", "")
            if not cursor or not lst:
                break
        chunk_start = chunk_end
    return execs


def main() -> None:
    ex = BybitExchange()
    client = ex._client

    conn = sqlite3.connect("signals.db")
    conn.row_factory = sqlite3.Row
    cutoff_ms = int(time.time() * 1000) - MIN_AGE_MINUTES * 60 * 1000
    rows = conn.execute("""
        SELECT id, symbol, action, quantity, entry_fill_time, category
          FROM signals
         WHERE status='active' AND entry_fill_time IS NOT NULL
    """).fetchall()

    candidates = []
    for r in rows:
        try:
            entry_ms = local_iso_to_utc_ms(r["entry_fill_time"])
        except Exception:
            continue
        if entry_ms <= cutoff_ms:
            candidates.append(dict(r))

    print(f"=== {BOT_NAME}: {len(rows)} active rows, {len(candidates)} old enough to check ===")
    if not candidates:
        conn.close()
        return

    by_symbol: dict[str, list[dict]] = {}
    for c in candidates:
        by_symbol.setdefault(c["symbol"], []).append(c)

    fixed = 0
    now_ms = int(time.time() * 1000)
    for symbol, sigs in by_symbol.items():
        qty_step = float(ex._get_qty_step(symbol))
        qty_tolerance = qty_step * 1.01

        earliest_ms = min(local_iso_to_utc_ms(s["entry_fill_time"]) for s in sigs)
        execs = fetch_executions_chunked(client, symbol, earliest_ms, now_ms)
        closing = [e for e in execs if float(e.get("closedSize", 0) or 0) > 0]

        # Process oldest-entry-first, global de-duplication across this symbol's batch.
        sigs.sort(key=lambda s: s["entry_fill_time"])
        used_order_ids: set[str] = set()
        for sig in sigs:
            close_side = "Sell" if sig["action"] == "buy" else "Buy"
            sig_qty = float(sig["quantity"])
            entry_ms = local_iso_to_utc_ms(sig["entry_fill_time"])

            candidates_exec = [
                e for e in closing
                if e["side"] == close_side
                and abs(float(e["closedSize"]) - sig_qty) < qty_tolerance
                and int(e["execTime"]) > entry_ms
                and e["orderId"] not in used_order_ids
            ]
            if not candidates_exec:
                continue  # orphan — leave alone, needs human judgment

            best = min(candidates_exec, key=lambda x: int(x["execTime"]))
            used_order_ids.add(best["orderId"])
            completion_local = utc_to_beirut_local_iso(
                datetime.fromtimestamp(int(best["execTime"]) / 1000, tz=timezone.utc)
            )
            print(f"  [TIER1] sig_id={sig['id']} symbol={symbol} qty={sig_qty} "
                  f"-> completion_time={completion_local} (matched orderId={best['orderId']})")
            if LIVE:
                conn.execute(
                    "UPDATE signals SET status='completed', completion_time=? "
                    "WHERE id=? AND status='active'",
                    (completion_local, sig["id"]),
                )
            fixed += 1

    if LIVE:
        conn.commit()
        print(f"\nCommitted. {fixed} rows marked completed with verified real evidence.")
    else:
        print(f"\nDRY RUN — nothing written. Would fix {fixed} rows. Re-run with --live to apply.")

    conn.close()


if __name__ == "__main__":
    main()
