from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWSTOCK_", case_sensitive=False)

    # Database
    database_url: str = "postgresql+asyncpg://twstock:twstock@localhost:5432/twstock"

    # External APIs
    finmind_api_token: str = ""
    fred_api_key: str = ""          # https://fred.stlouisfed.org/docs/api/api_key.html

    # Telegram notifier (Phase 2)
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Scraping throttle (seconds between TWSE requests)
    request_delay: float = 1.0

    log_level: str = "INFO"


settings = Settings()
