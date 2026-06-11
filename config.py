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
