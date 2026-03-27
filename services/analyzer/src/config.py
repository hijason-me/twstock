from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TWSTOCK_", case_sensitive=False)

    database_url: str = "postgresql+asyncpg://twstock:twstock@localhost:5432/twstock"
    log_level: str = "INFO"

    # DCF model parameters
    dcf_terminal_growth_rate: float = 0.025   # 長期成長率 2.5%
    dcf_margin_of_safety: float = 0.80        # 安全邊際 80%

    # Multi-factor filter thresholds
    mf_revenue_yoy_min: float = 15.0          # Filter 1: YoY > 15%
    mf_trust_net_ratio_min: float = 0.5       # Filter 2: 投信買超佔股本 > 0.5%
    mf_pe_std_threshold: float = 1.0          # Filter 3: 在均值 ±1 標準差內
    mf_volume_multiplier: float = 2.0         # Filter 4: 量 > 5日均量 × 2


settings = Settings()
