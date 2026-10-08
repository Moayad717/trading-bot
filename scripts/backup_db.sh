#!/bin/bash
# Daily backup of each bot's signals.db, using SQLite's own .backup command
# (safe to run against a live WAL-mode DB a running process still has open —
# unlike `cp`, which can grab an inconsistent snapshot mid-write).
#
# One file per bot, overwritten every run — not a dated history. This guards
# against DB corruption, a bad migration, or an accidental DELETE/UPDATE from
# the last run backwards only; it is NOT off-box protection and it is NOT a
# point-in-time archive (yesterday's backup is gone once today's runs). True
# off-box backup, or multi-day retention, needs a destination (S3, another
# host, etc.) that isn't configured here.
set -euo pipefail

BOTS=(trading-bot-live trading-bot-8003 trading-bot-8005 trading-bot-aiko)
BACKUP_ROOT="/root/db-backups"
STAMP="$(date +%Y-%m-%d_%H-%M-%S)"

mkdir -p "$BACKUP_ROOT"

for bot in "${BOTS[@]}"; do
    src="/root/$bot/signals.db"
    dest_dir="$BACKUP_ROOT/$bot"
    mkdir -p "$dest_dir"

    if [ ! -f "$src" ]; then
        echo "WARNING: $src not found, skipping $bot"
        continue
    fi

    dest="$dest_dir/signals_latest.db"
    rm -f "$dest" "${dest}.gz"
    sqlite3 "$src" ".backup '$dest'"
    gzip "$dest"
    echo "backed up $bot -> ${dest}.gz (as of ${STAMP})"
done

echo "--- current backup disk usage ---"
du -sh "$BACKUP_ROOT"
