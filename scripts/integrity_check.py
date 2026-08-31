"""
Real-time integrity check — the thing this whole week's worst bug proved was
missing. Run periodically (see scripts/run_integrity_check.sh + cron) from
inside each bot's own directory, exactly like the other maintenance scripts.

Two checks:

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

    if not findings:
        print(f"[{BOT_NAME}] clean — no issues in the last {LOOKBACK_MINUTES}m")
        return

    message = f"🚨 {BOT_NAME}: {len(findings)} issue(s) found\n\n" + "\n\n".join(findings)
    print(message)
    send_telegram(message)


if __name__ == "__main__":
    main()
