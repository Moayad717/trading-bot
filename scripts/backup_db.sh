#!/bin/bash
# Daily backup of each bot's signals.db, using SQLite's own .backup command
# (safe to run against a live WAL-mode DB a running process still has open —
# unlike `cp`, which can grab an inconsistent snapshot mid-write).
#
# Retains 14 days locally on this same server. This is NOT off-box protection
# — it guards against DB corruption, a bad migration, or an accidental
# DELETE/UPDATE, but not against loss of the whole server. True off-box
# backup needs a destination (S3, another host, etc.) that isn't configured
# here; wire one in by adding an `rsync`/`aws s3 cp` line at the bottom once
# a destination exists.
set -euo pipefail

BOTS=(trading-bot-live trading-bot-8003 trading-bot-8005 trading-bot-aiko)
BACKUP_ROOT="/root/db-backups"
RETAIN_DAYS=14
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

    dest="$dest_dir/signals_${STAMP}.db"
    sqlite3 "$src" ".backup '$dest'"
    gzip "$dest"
    echo "backed up $bot -> ${dest}.gz"
done

echo "--- pruning backups older than ${RETAIN_DAYS} days ---"
find "$BACKUP_ROOT" -name '*.db.gz' -mtime "+${RETAIN_DAYS}" -print -delete

echo "--- current backup disk usage ---"
du -sh "$BACKUP_ROOT"
