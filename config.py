from datetime import datetime
from zoneinfo import ZoneInfo

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # testnet = true, mainnet = false
    TESTNET: bool = True

    # paper trading — parse and record signals but skip all exchange calls
    PAPER_TRADING: bool = False

    BYBIT_API_KEY: str = ""
    BYBIT_API_SECRET: str = ""

    WEBHOOK_SECRET: str = ""

    DB_PATH: str = "signals.db"

    TIMEZONE: str = "Asia/Beirut"

    # Net-delta cap — prevents the bot from building an outsized directional imbalance.
    NET_DELTA_CAP_ENABLED: bool  = False   # master switch (.env overrides to True)
    NET_DELTA_CAP_PCT:     float = 1.00    # max |net_delta| as multiple of equity in coin
    NET_DELTA_CAP_SHADOW:  bool  = True    # True = log only, never refuse
    NET_DELTA_CACHE_SEC:   int   = 5       # seconds to cache position/order/equity snapshot

    # Symbols the missing-TP checks (main.py's _fix_missing_tps, missing_tp.py)
    # are allowed to look at — comma-separated (2026-10-19). Added after that
    # mechanism was found scanning and logging about a user's own unrelated
    # manual positions on other coins (ILV/WAL/BLUR) sharing the same Bybit
    # account; this keeps it scoped to the bot's own actual trading pair(s).
    FIX_TP_SYMBOLS: str = "LINKUSDT"

    # Telegram credentials for main.py's own missing-TP notifications
    # (2026-10-19) — separate from scripts/integrity_check.py's alerting,
    # which runs as a standalone cron script reading /root/telegram_alert.env
    # directly. This one runs inside the live app process, so it reads these
    # the normal way (via .env), which must be set here too for it to send.
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID:   str = ""

    @property
    def active_exchange(self) -> str:
        return "bybit"


settings = Settings()


def now_local() -> datetime:
    """Current time in the configured local timezone, without tzinfo (naive local)."""
    return datetime.now(ZoneInfo(settings.TIMEZONE)).replace(tzinfo=None)


def today_local() -> str:
    """Today's date string (YYYY-MM-DD) in the configured local timezone."""
    return datetime.now(ZoneInfo(settings.TIMEZONE)).date().isoformat()
