#!/bin/bash
# Runs ghost_cleanup.py (Tier-1, evidence-only) once per bot, each in its own
# directory — same pattern as every other maintenance script here. One
# bot's failure does not stop the others.
#
# Usage:
#   bash run_ghost_cleanup.sh            # dry run, prints what it would fix
#   bash run_ghost_cleanup.sh --live     # actually writes the fixes
#
# Intended cron use: daily, --live, low-traffic hours. This is deliberately
# separate from the 15-min integrity_check.sh loop — this one only ever
# fixes confidently-provable rows, never just alerts, so it's safe to run
# unattended on a schedule instead of paging a human every time.
set -uo pipefail

MODE="${1:-}"

if [ -f /root/telegram_alert.env ]; then
    set -a
    source /root/telegram_alert.env
    set +a
fi

declare -A BOTS=(
    [trading-bot-live]=trading-bot-live
    [trading-bot-8003]=trading-bot-8003
    [trading-bot-8005]=trading-bot-8005
    [trading-bot-aiko]=trading-bot-aiko
)

TOTAL_FIXED=0
SUMMARY=""
for dir in "${!BOTS[@]}"; do
    name="${BOTS[$dir]}"
    cd "/root/$dir" || continue
    venv_python="python3"
    [ -x "venv/bin/python3" ] && venv_python="venv/bin/python3"
    [ -x ".venv/bin/python3" ] && venv_python=".venv/bin/python3"

    out="$($venv_python scripts/ghost_cleanup.py "$name" $MODE 2>&1)"
    echo "--- $name ---"
    echo "$out"

    fixed_count="$(echo "$out" | grep -oE '[0-9]+ rows (marked completed|Would fix)' | grep -oE '^[0-9]+' | head -1)"
    fixed_count="${fixed_count:-0}"
    if [ "$fixed_count" -gt 0 ]; then
        TOTAL_FIXED=$((TOTAL_FIXED + fixed_count))
        SUMMARY="${SUMMARY}${name}: ${fixed_count} rows\n"
    fi
done

# Only notify when something was actually found/fixed — no daily noise for
# a genuinely clean run (unlike integrity_check.sh's heartbeat, this isn't
# a "prove the cron job is alive" signal, it's a "here's what changed" one).
if [ "$TOTAL_FIXED" -gt 0 ] && [ -n "${TELEGRAM_BOT_TOKEN:-}" ]; then
    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
        --data-urlencode "text=🧹 Ghost cleanup: ${TOTAL_FIXED} stale row(s) fixed with verified real evidence.

$(echo -e "$SUMMARY")" > /dev/null
fi
