from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Toggle between testnet and live — single variable change
    TESTNET: bool = True

    # Bybit credentials (never hardcode; load from .env)
    BYBIT_API_KEY: str = ""
    BYBIT_API_SECRET: str = ""

    # Optional shared secret to authenticate incoming webhooks
    WEBHOOK_SECRET: str = ""

    DB_PATH: str = "signals.db"

    @property
    def active_exchange(self) -> str:
        return "bybit"


settings = Settings()
