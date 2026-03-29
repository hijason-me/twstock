from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWSTOCK_", case_sensitive=False)

    # Database
    database_url: str = "postgresql+asyncpg://twstock:twstock@localhost:5432/twstock"

    # External APIs
    finmind_api_token: str = ""
    # FinMind free tier: 600 req/hr with token. Set 0 = no limit (requires paid plan).
    finmind_tickers_limit: int = 200
    # Delay between per-ticker FinMind requests (seconds). 10s = ~360 req/hr (safe).
    finmind_request_delay: float = 10.0
    fred_api_key: str = ""          # https://fred.stlouisfed.org/docs/api/api_key.html

    # Data source selection
    # monthly_revenue:      twse (bulk, latest month only) | mops (bulk, historical) | finmind (per-ticker)
    # quarterly_financials: twse (bulk, latest quarter only) | finmind (per-ticker, historical)
    # major_holders:        tdcc (bulk, latest week only, free) | finmind (per-ticker, paid plan)
    revenue_source:    str = "twse"
    financials_source: str = "twse"
    holders_source:    str = "tdcc"

    # Telegram notifier (Phase 2)
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Scraping throttle (seconds between TWSE requests)
    request_delay: float = 1.0

    log_level: str = "INFO"


settings = Settings()
