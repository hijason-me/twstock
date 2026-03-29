"""
Macro indicator collector.
  - UST 10yr yield, USD/TWD  via yfinance
  - Fed Funds Rate, CPI       via FRED API
"""
import logging
from datetime import date, datetime, timedelta, timezone

import httpx
import yfinance as yf

from .base import build_client

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

YFINANCE_SYMBOLS = {
    "UST_10Y": "^TNX",   # 10 年期美債殖利率
    "USDTWD":  "TWD=X",  # 美元/台幣匯率
}

FRED_SERIES = {
    "FED_RATE": "FEDFUNDS",  # 聯邦基金利率
    "CPI_YOY":  "CPIAUCSL",  # CPI (月度，需自行算 YoY)
}


class MacroCollector:
    def __init__(self, fred_api_key: str = ""):
        self._fred_key = fred_api_key

    async def fetch_yfinance(self, lookback_days: int = 5) -> list[dict]:
        start = (date.today() - timedelta(days=lookback_days)).isoformat()
        records = []
        for indicator, symbol in YFINANCE_SYMBOLS.items():
            try:
                df = yf.Ticker(symbol).history(start=start)
                if df.empty:
                    continue
                for ts, row in df.iterrows():
                    records.append({
                        "time":      ts.to_pydatetime(),
                        "indicator": indicator,
                        "value":     float(row["Close"]),
                        "source":    "yfinance",
                    })
                logger.info("yfinance %s: %d records", indicator, len(df))
            except Exception as e:
                logger.warning("yfinance %s failed: %s", indicator, e)
        return records

    async def fetch_fred(self, lookback_days: int = 35) -> list[dict]:
        if not self._fred_key:
            logger.info("No FRED API key — skipping FRED fetch")
            return []
        start = (date.today() - timedelta(days=lookback_days)).isoformat()
        records = []
        async with build_client() as client:
            for indicator, series_id in FRED_SERIES.items():
                try:
                    resp = await client.get(FRED_BASE, params={
                        "series_id":         series_id,
                        "api_key":           self._fred_key,
                        "file_type":         "json",
                        "observation_start": start,
                    })
                    resp.raise_for_status()
                    for obs in resp.json().get("observations", []):
                        if obs["value"] == ".":
                            continue
                        records.append({
                            "time":      datetime.fromisoformat(obs["date"] + "T00:00:00+00:00"),
                            "indicator": indicator,
                            "value":     float(obs["value"]),
                            "source":    "FRED",
                        })
                    logger.info("FRED %s: %d records", indicator, len(records))
                except Exception as e:
                    logger.warning("FRED %s failed: %s", series_id, e)
        return records
