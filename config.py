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

    # Position reconciler watch-only mode (2026-09-16) — when True, the
    # background reconciliation loop only logs what it found and what it
    # would place; it never calls place_tp_order itself. Per-bot via .env,
    # not a global default, since it was requested for specific accounts.
    RECONCILER_WATCH_ONLY: bool = False

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
