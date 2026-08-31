#!/bin/bash
# Runs integrity_check.py once per bot, each in its own directory (so it
# loads that bot's own .env / signals.db / Bybit credentials the same way
# every other maintenance script this week did). One bot's check failing
# does not stop the others.
#
# Usage:
#   bash run_integrity_check.sh            # normal run, alerts only on findings
#   bash run_integrity_check.sh --heartbeat # also sends one "still alive" summary
#
# Cron (see deploy notes): every 15 min for the normal run, once/day with
# --heartbeat so a silently-dead cron job doesn't look identical to "all clean".
set -uo pipefail

if [ -f /root/telegram_alert.env ]; then
    set -a
    source /root/telegram_alert.env
    set +a
else
    echo "WARNING: /root/telegram_alert.env not found — alerts will only print, not send."
fi

declare -A BOTS=(
    [trading-bot-live]=trading-bot-live
    [trading-bot-8003]=trading-bot-8003
    [trading-bot-8005]=trading-bot-8005
    [trading-bot-aiko]=trading-bot-aiko
)

SUMMARY=""
for dir in "${!BOTS[@]}"; do
    name="${BOTS[$dir]}"
    cd "/root/$dir" || continue
    venv_python="python3"
    [ -x "venv/bin/python3" ] && venv_python="venv/bin/python3"
    [ -x ".venv/bin/python3" ] && venv_python=".venv/bin/python3"
    out="$($venv_python "scripts/integrity_check.py" "$name" 2>&1)"
    echo "--- $name ---"
    echo "$out"
    SUMMARY="${SUMMARY}${name}: $(echo "$out" | tail -1)"$'\n'
done

if [ "${1:-}" = "--heartbeat" ]; then
    if [ -n "${TELEGRAM_BOT_TOKEN:-}" ]; then
        curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
            --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
            --data-urlencode "text=✅ Daily integrity check heartbeat — all 4 bots checked.

${SUMMARY}" > /dev/null
    fi
fi
