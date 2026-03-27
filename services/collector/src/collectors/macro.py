"""
Macro indicator collector.
- US Treasury 10yr yield, USD/TWD via yfinance
- Fed Funds Rate, CPI via FRED API (free key)
"""
import logging
from datetime import date, timedelta

import yfinance as yf
import httpx

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

YFINANCE_SYMBOLS = {
    "UST_10Y": "^TNX",    # 10年期美債殖利率
    "USDTWD":  "TWD=X",   # 美元/台幣匯率
}

FRED_SERIES = {
    "FED_RATE": "FEDFUNDS",  # 聯邦基金利率
    "CPI_YOY":  "CPIAUCSL",  # 美國 CPI (需自行計算 YoY)
}


class MacroCollector:
    def __init__(self, fred_api_key: str = ""):
        self._fred_key = fred_api_key

    async def fetch_yfinance_indicators(self, lookback_days: int = 5) -> list[dict]:
        """Fetch market-based macro indicators from Yahoo Finance."""
        start = (date.today() - timedelta(days=lookback_days)).isoformat()
        records = []
        for indicator, symbol in YFINANCE_SYMBOLS.items():
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(start=start)
                if df.empty:
                    continue
                for ts, row in df.iterrows():
                    records.append({
                        "time":      ts.isoformat(),
                        "indicator": indicator,
                        "value":     float(row["Close"]),
                        "source":    "yfinance",
                    })
                logger.info("Fetched %d records for %s", len(df), indicator)
            except Exception as e:
                logger.warning("Failed to fetch %s: %s", indicator, e)
        return records

    async def fetch_fred_indicators(self, lookback_days: int = 35) -> list[dict]:
        """Fetch macro indicators from FRED API."""
        if not self._fred_key:
            logger.info("No FRED API key; skipping FRED fetch")
            return []

        observation_start = (date.today() - timedelta(days=lookback_days)).isoformat()
        records = []
        async with httpx.AsyncClient(timeout=20.0) as client:
            for indicator, series_id in FRED_SERIES.items():
                try:
                    resp = await client.get(FRED_BASE, params={
                        "series_id":         series_id,
                        "api_key":           self._fred_key,
                        "file_type":         "json",
                        "observation_start": observation_start,
                    })
                    resp.raise_for_status()
                    observations = resp.json().get("observations", [])
                    for obs in observations:
                        if obs["value"] == ".":
                            continue
                        records.append({
                            "time":      obs["date"] + "T00:00:00",
                            "indicator": indicator,
                            "value":     float(obs["value"]),
                            "source":    "FRED",
                        })
                    logger.info("Fetched %d FRED records for %s", len(observations), indicator)
                except Exception as e:
                    logger.warning("Failed to fetch FRED series %s: %s", series_id, e)
        return records
