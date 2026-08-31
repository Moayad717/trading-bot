#!/bin/bash
# Deploy the latest main branch to all 4 bots and verify each one comes back
# healthy. Replaces the manual "git fetch + reset --hard + pm2 restart per
# bot, by hand, four times" dance that's been done by hand all week.
#
# NEVER git pull — always fetch + reset --hard, matching this project's
# standing rule (uncommitted server-side edits must never silently merge).
#
# Usage: ssh onto the server, then run this script (or `bash deploy_all.sh`).
set -uo pipefail

declare -A BOTS=(
    [trading-bot-live]=8001
    [trading-bot-8003]=8003
    [trading-bot-8005]=8005
    [trading-bot-aiko]=8006
)

echo "=== Pulling latest code (fetch + reset --hard, never pull) ==="
for bot in "${!BOTS[@]}"; do
    echo "--- $bot ---"
    if ! (cd "/root/$bot" && git fetch origin && git reset --hard origin/main); then
        echo "ABORT: git update failed for $bot — no bots restarted, nothing else touched."
        exit 1
    fi
done

echo
echo "=== Restarting all bots ==="
if ! pm2 restart "${!BOTS[@]}" --update-env; then
    echo "ABORT: pm2 restart failed — check 'pm2 list' and logs manually."
    exit 1
fi

echo
echo "=== Waiting 6s for startup ==="
sleep 6

echo
echo "=== Health check ==="
FAILED=0
for bot in "${!BOTS[@]}"; do
    port="${BOTS[$bot]}"
    resp="$(curl -s -m 5 "http://localhost:${port}/health" || true)"
    if echo "$resp" | grep -q '"status":"ok"' || echo "$resp" | grep -q '"status": "ok"'; then
        echo "  $bot (:$port) -> OK"
    else
        echo "  $bot (:$port) -> FAILED — response: ${resp:-<no response>}"
        FAILED=1
    fi
done

echo
echo "=== pm2 status ==="
pm2 list

if [ "$FAILED" -ne 0 ]; then
    echo
    echo "!!! One or more bots failed their health check. Check 'pm2 logs <name> --err' before assuming this deploy is good. !!!"
    exit 1
fi

echo
echo "=== Deploy complete — all 4 bots healthy ==="
