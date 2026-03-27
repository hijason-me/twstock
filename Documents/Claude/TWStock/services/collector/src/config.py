from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWSTOCK_", case_sensitive=False)

    # Database
    database_url: str = "postgresql+asyncpg://twstock:twstock@localhost:5432/twstock"

    # External API tokens (optional but recommended for higher rate limits)
    finmind_api_token: str = ""
    fred_api_key: str = ""         # https://fred.stlouisfed.org/docs/api/api_key.html

    # TWSE / TPEx scraping throttle (seconds between requests)
    request_delay: float = 1.0

    # Log level
    log_level: str = "INFO"


settings = Settings()
