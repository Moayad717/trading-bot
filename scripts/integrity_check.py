"""
Real-time integrity check — the thing this whole week's worst bug proved was
missing. Run periodically (see scripts/run_integrity_check.sh + cron) from
inside each bot's own directory, exactly like the other maintenance scripts.

Four checks:

  1. Recently-completed signals with NO matching real Bybit execution. This
     is the exact shape of the proven 2026-08-28 bug: five originals got
     marked 'completed' in the DB (via an inference that assumed a shared
     stop-loss slot had fired) while Bybit's execution history shows nothing
     closed four of the five positions. That bug sat undetected for three
     days because nothing checked "does 'completed' in our DB actually
     correspond to a real closing trade on the exchange?" This check asks
     exactly that question, on every signal completed in the lookback
     window, every time it runs.

  2. A spike in ErrCode 110017 ("orderQty will be truncated to zero" — the
     reduce-only-quota-exhausted rejection, see BYBIT_QUIRKS.md #1) in the
     bot's own recent error log — the exact shape of the reconciler-hammering
     bug (57,000+ rejections/day before the coverage fix).

  3. Any CRITICAL-level log line in the recent error log. The code already
     flags several genuinely dangerous situations this way (SL placement
     failing after retry with a live counter and no stop on the original;
     an order placed on Bybit but the DB write failing after 3 attempts;
     a signal's TP permanently blacklisted after 3 failed placements) —
     those log lines already exist, nothing was reading them in real time.
     This is a deliberate catch-all: rather than trying to anticipate every
     future failure mode individually, alert on anything the code itself
     already considers critical.

  4. A NEW duplicate tp_order_id group — two different signals sharing one
     take-profit order id, the original client-reported mislink bug
     (link_auto_tp_sync's quantity-based guessing). All known-and-already-
     resolved cases from before 2026-08-31 are excluded via
     _KNOWN_RESOLVED_DUPLICATE_TP_ORDER_IDS below (their status/timestamps
     were corrected with real evidence, but the tp_order_id column itself
     was deliberately left alone — see BYBIT_QUIRKS.md and this week's
     commit history) — only a genuinely NEW occurrence fires this check.

Sends one Telegram message per bot per run ONLY when something is actually
wrong — no periodic "all clear" spam from this script (see
run_integrity_check.sh for the separate daily heartbeat).

Never call this script for one-off manual investigation of a SPECIFIC past
incident — it's tuned for cheap, frequent, narrow-window checks. Use the
wider historical scripts from this week's forensic work for that instead.
"""
from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import now_local, settings  # noqa: E402
from exchanges.bybit import BybitExchange  # noqa: E402

BOT_NAME = sys.argv[1] if len(sys.argv) > 1 else os.path.basename(os.getcwd())
LOOKBACK_MINUTES = int(os.environ.get("INTEGRITY_LOOKBACK_MIN", "20"))
ERROR_LOG_PATH = f"/root/.pm2/logs/{BOT_NAME}-error.log"
ERROR_SPIKE_THRESHOLD = 30  # occurrences within the lookback window

# Every duplicate-tp_order_id group found and resolved (status/completion_time
# corrected with real evidence) across live/8003/8005 on 2026-08-31. Global —
# Bybit order ids are unique across accounts, no risk of an id from one bot
# accidentally excluding a real new duplicate on another. The tp_order_id
# column itself was deliberately left as-is (see module docstring), so these
# will keep showing up in a raw duplicate query forever; only something NOT
# in this set represents a genuinely new occurrence.
_KNOWN_RESOLVED_DUPLICATE_TP_ORDER_IDS = {
    "03c065bb-5758-4020-b449-23ecdb44e0b5", "070f0661-cf56-47e0-931a-976de6b70cb1",
    "09c3a7c9-ee1e-43a6-9be7-0f00a75e67d7", "0c943c17-38d0-42ce-89d3-0d8e4fa64254",
    "12344272-8717-40a8-82b3-2ff295e2649c", "334d6f42-0e9b-4400-8e13-942523661c87",
    "34629068-0043-4558-ae54-9afc7088fb98", "389884ce-c2d9-48dd-ab6a-3a81a6c6dd10",
    "5e46e266-204b-410c-8051-12094468596c", "633ccc13-0395-4e13-989a-778fee24cdac",
    "6b51c78a-36c9-402e-b26a-1d7588aad868", "7e0a9653-1caf-4b00-8927-3727c250e782",
    "84c3df38-61b8-4fd8-b450-f058e85c0b35", "8a7f4963-1af3-424d-a4ea-cb90d785a881",
    "bf034881-1cb1-4229-bbc1-0f69db1a3fa1", "d46e6109-a790-405d-bb69-919a70a87acd",
    "429c76a0-b314-43b7-abdf-8987ca9c338b",
    "20bf69cb-e387-4a7c-821b-8ee3e5d68e36", "41158c6d-a138-4a65-8f43-af388448d11e",
    "48e1e0af-4124-46b2-92b2-0dc5628c16c2", "4f8b1fb0-d0f5-4a49-8294-5f65cef8e6ba",
    "50e57434-e990-4445-bc40-645f97b8d962", "72097604-2bbe-442b-8cce-4b3064cebcfc",
    "c9a307cf-f4a6-4602-999f-a8b6685c517d", "d64584ce-346a-4f63-9742-864e7fb9dc5f",
}


def send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set — printing instead of sending:")
        print(text)
        return
    import urllib.request
    import urllib.parse
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10)
    except Exception as exc:
        print(f"Telegram send failed: {exc}")


def check_completed_without_evidence() -> list[str]:
    """Every signal completed in the lookback window must have a real,
    matching Bybit closing execution. No match = alert."""
    findings = []
    cutoff = (now_local() - timedelta(minutes=LOOKBACK_MINUTES)).isoformat()

    conn = sqlite3.connect(settings.DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT id, symbol, action, quantity, entry_fill_time, completion_time
             FROM signals
            WHERE status='completed' AND completion_time >= ?""",
        (cutoff,),
    ).fetchall()
    conn.close()

    if not rows:
        return findings

    ex = BybitExchange()
    client = ex._client

    by_symbol: dict[str, list[dict]] = {}
    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(dict(r))

    for symbol, sigs in by_symbol.items():
        earliest_entry = min(
            (s["entry_fill_time"] for s in sigs if s.get("entry_fill_time")), default=None
        )
        start_ms = int(time.time() * 1000) - (LOOKBACK_MINUTES + 60 * 24 * 3) * 60 * 1000
        if earliest_entry:
            try:
                start_ms = min(start_ms, int(
                    datetime.fromisoformat(earliest_entry).replace(tzinfo=timezone.utc).timestamp() * 1000
                ))
            except Exception:
                pass
        end_ms = int(time.time() * 1000)

        execs = []
        cursor = ""
        while True:
            params = {"category": "linear", "symbol": symbol,
                      "startTime": start_ms, "endTime": end_ms, "limit": 100}
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
        closing = [e for e in execs if float(e.get("closedSize", 0) or 0) > 0]

        used_order_ids: set[str] = set()
        for sig in sigs:
            close_side = "Sell" if sig["action"] == "buy" else "Buy"
            sig_qty = float(sig["quantity"])
            entry_ms = 0
            if sig.get("entry_fill_time"):
                try:
                    entry_ms = int(
                        datetime.fromisoformat(sig["entry_fill_time"])
                        .replace(tzinfo=timezone.utc).timestamp() * 1000
                    )
                except Exception:
                    pass
            candidates = [
                e for e in closing
                if e["side"] == close_side
                and abs(float(e["closedSize"]) - sig_qty) < 0.01
                and int(e["execTime"]) > entry_ms
                and e["orderId"] not in used_order_ids
            ]
            if candidates:
                best = min(candidates, key=lambda x: int(x["execTime"]))
                used_order_ids.add(best["orderId"])
                continue
            findings.append(
                f"sig_id={sig['id']} symbol={symbol} action={sig['action']} "
                f"qty={sig['quantity']} completed_at={sig['completion_time']} — "
                f"NO matching Bybit execution found. Marked completed with no "
                f"proof it actually closed on the exchange."
            )

    return findings


def check_error_spike() -> list[str]:
    """A burst of 110017 rejections means the reconciler is hammering a
    saturated reduce-only quota again — the pre-fix pattern (57k/day)."""
    if not os.path.exists(ERROR_LOG_PATH):
        return []
    cutoff = datetime.now() - timedelta(minutes=LOOKBACK_MINUTES)
    count = 0
    try:
        result = subprocess.run(
            ["tail", "-n", "5000", ERROR_LOG_PATH],
            capture_output=True, text=True, timeout=10,
        )
        for line in result.stdout.splitlines():
            if "110017" not in line:
                continue
            m = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            if not m:
                continue
            try:
                ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            if ts >= cutoff:
                count += 1
    except Exception as exc:
        return [f"error-log check failed: {exc}"]

    if count >= ERROR_SPIKE_THRESHOLD:
        return [
            f"{count} ErrCode 110017 rejections in the last {LOOKBACK_MINUTES} minutes "
            f"— reconciler is hammering a saturated reduce-only quota (the pre-fix "
            f"57k/day pattern)."
        ]
    return []


def check_critical_logs() -> list[str]:
    """Catch-all: the code already flags several genuinely dangerous
    situations with logger.critical(...) — SL failed after retry (counter
    live, original unprotected), DB write failed after a real order was
    already placed on Bybit, a signal's TP permanently blacklisted. Those
    lines already existed; nothing was reading them in real time."""
    if not os.path.exists(ERROR_LOG_PATH):
        return []
    cutoff = datetime.now() - timedelta(minutes=LOOKBACK_MINUTES)
    findings = []
    try:
        result = subprocess.run(
            ["tail", "-n", "5000", ERROR_LOG_PATH],
            capture_output=True, text=True, timeout=10,
        )
        for line in result.stdout.splitlines():
            if "CRITICAL" not in line:
                continue
            m = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            if not m:
                continue
            try:
                ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            if ts >= cutoff:
                findings.append(f"CRITICAL log: {line.strip()[:300]}")
    except Exception as exc:
        return [f"critical-log check failed: {exc}"]
    return findings


def check_new_duplicate_tp_order_id() -> list[str]:
    """Alert only on a duplicate tp_order_id NOT already in the known-resolved
    set — a genuinely new occurrence of the original client-reported mislink
    bug, not the historical cases already corrected."""
    conn = sqlite3.connect(settings.DB_PATH, timeout=5)
    rows = conn.execute("""
        SELECT tp_order_id, COUNT(*) n, group_concat(id) sig_ids
          FROM signals
         WHERE tp_order_id IS NOT NULL AND tp_order_id<>''
         GROUP BY tp_order_id HAVING COUNT(*)>1
    """).fetchall()
    conn.close()

    findings = []
    for tp_order_id, n, sig_ids in rows:
        if tp_order_id in _KNOWN_RESOLVED_DUPLICATE_TP_ORDER_IDS:
            continue
        findings.append(
            f"NEW duplicate tp_order_id={tp_order_id} shared by {n} signals "
            f"(ids={sig_ids}) — the original mislink bug may be recurring."
        )
    return findings


def main() -> None:
    findings: list[str] = []
    try:
        findings.extend(check_completed_without_evidence())
    except Exception as exc:
        findings.append(f"completed-without-evidence check itself failed: {exc}")
    try:
        findings.extend(check_error_spike())
    except Exception as exc:
        findings.append(f"error-spike check itself failed: {exc}")
    try:
        findings.extend(check_critical_logs())
    except Exception as exc:
        findings.append(f"critical-log check itself failed: {exc}")
    try:
        findings.extend(check_new_duplicate_tp_order_id())
    except Exception as exc:
        findings.append(f"duplicate-tp_order_id check itself failed: {exc}")

    if not findings:
        print(f"[{BOT_NAME}] clean — no issues in the last {LOOKBACK_MINUTES}m")
        return

    message = f"🚨 {BOT_NAME}: {len(findings)} issue(s) found\n\n" + "\n\n".join(findings)
    print(message)
    send_telegram(message)


if __name__ == "__main__":
    main()
