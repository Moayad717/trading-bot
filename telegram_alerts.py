"""Minimal Telegram sender for alerts raised from inside the live app
process (2026-10-19) — e.g. main.py's missing-TP notifications.

Deliberately separate from scripts/integrity_check.py's own send_telegram:
that one runs as a standalone cron script and reads /root/telegram_alert.env
directly via the shell wrapper that invokes it. This one runs inside the
pm2-managed app process, so it reads the same kind of credentials through
the normal settings/.env path instead — TELEGRAM_BOT_TOKEN and
TELEGRAM_CHAT_ID must also be present in each bot's own .env for this to
actually send anything.
"""
import logging
import urllib.parse
import urllib.request

from config import settings

logger = logging.getLogger(__name__)


def send_telegram(text: str) -> None:
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        logger.warning(
            "Telegram not configured (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID) — "
            "not sent: %s", text,
        )
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text}).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10)
    except Exception as exc:
        logger.error("Telegram send failed: %s", exc)
