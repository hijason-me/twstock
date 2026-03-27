from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWSTOCK_", case_sensitive=False)

    database_url: str = "postgresql+asyncpg://twstock:twstock@localhost:5432/twstock"
    redis_url: str = "redis://localhost:6379/0"
    log_level: str = "INFO"
    cache_ttl_seconds: int = 300  # 5-minute default cache TTL


settings = Settings()
