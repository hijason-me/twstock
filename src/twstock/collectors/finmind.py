"""
FinMind API collector.
Free tier: 600 req/day. Paid plan unlocks EPS forecast and higher limits.
Docs: https://finmindtrade.com/analysis/#/Announcement/api
"""
import logging
from datetime import datetime, timezone

from .base import build_client

logger = logging.getLogger(__name__)

FINMIND_BASE = "https://api.finmindtrade.com/api/v4/data"

# 千張大戶：持股 >= 1,000,000 股 (千張 × 1000 股/張)
_MAJOR_TIERS  = ["1,000,001", "1000001", "over 1,000,000"]
# 散戶：持股 <= 10,000 股 (10 張以下)
_RETAIL_TIERS = ["1 to 999", "1,000 to 5,000", "5,001 to 10,000"]


class FinMindCollector:
    def __init__(self, api_token: str = ""):
        self._token = api_token

    def _params(self, dataset: str, ticker: str, start_date: str) -> dict:
        p = {"dataset": dataset, "data_id": ticker, "start_date": start_date}
        if self._token:
            p["token"] = self._token
        return p

    # ------------------------------------------------------------------
    # 月營收
    # ------------------------------------------------------------------
    async def fetch_monthly_revenue(
        self, tickers: list[str], start_date: str = "2022-01-01"
    ) -> list[dict]:
        records = []
        async with build_client() as client:
            for ticker in tickers:
                try:
                    resp = await client.get(
                        FINMIND_BASE,
                        params=self._params("TaiwanStockMonthRevenue", ticker, start_date),
                    )
                    resp.raise_for_status()
                    for row in resp.json().get("data", []):
                        records.append({
                            "year_month":  row["date"][:7],
                            "ticker":      ticker,
                            "revenue":     int(row.get("revenue", 0)),
                            "revenue_mom": float(row.get("revenue_month_increase", 0) or 0),
                            "revenue_yoy": float(row.get("revenue_year_increase", 0) or 0),
                        })
                except Exception as e:
                    logger.warning("FinMind revenue %s: %s", ticker, e)
        logger.info("Monthly revenue: %d records", len(records))
        return records

    # ------------------------------------------------------------------
    # 財報三率 + EPS
    # ------------------------------------------------------------------
    async def fetch_financial_statements(
        self, tickers: list[str], start_date: str = "2022-01-01"
    ) -> list[dict]:
        records = []
        async with build_client() as client:
            for ticker in tickers:
                try:
                    resp = await client.get(
                        FINMIND_BASE,
                        params=self._params("TaiwanStockFinancialStatements", ticker, start_date),
                    )
                    resp.raise_for_status()
                    # Pivot line items by quarter date
                    pivot: dict[str, dict] = {}
                    for row in resp.json().get("data", []):
                        key = row["date"]
                        if key not in pivot:
                            pivot[key] = {"year_quarter": key, "ticker": ticker}
                        val = float(row.get("value", 0) or 0)
                        t   = row.get("type", "")
                        if t == "GrossProfit":
                            pivot[key]["gross_profit"] = val
                        elif t == "Revenue":
                            pivot[key]["revenue"] = val
                        elif t == "OperatingIncome":
                            pivot[key]["operating_income"] = val
                        elif t == "NetIncome":
                            pivot[key]["net_income"] = val
                        elif t == "EPS":
                            pivot[key]["eps"] = val

                    for entry in pivot.values():
                        rev = entry.get("revenue", 0)
                        records.append({
                            "year_quarter":        entry["year_quarter"],
                            "ticker":              entry["ticker"],
                            "gross_profit_margin": (entry.get("gross_profit", 0) / rev * 100) if rev else None,
                            "operating_margin":    (entry.get("operating_income", 0) / rev * 100) if rev else None,
                            "net_profit_margin":   (entry.get("net_income", 0) / rev * 100) if rev else None,
                            "eps":                 entry.get("eps"),
                        })
                except Exception as e:
                    logger.warning("FinMind financials %s: %s", ticker, e)
        logger.info("Financial statements: %d records", len(records))
        return records

    # ------------------------------------------------------------------
    # Forward EPS 預估 (付費方案)
    # ------------------------------------------------------------------
    async def fetch_eps_forecast(
        self, tickers: list[str], start_date: str = "2024-01-01"
    ) -> list[dict]:
        """
        FinMind paid dataset: TaiwanStockEarningForecast
        Returns analyst consensus EPS forecasts by year_quarter.
        """
        records = []
        async with build_client() as client:
            for ticker in tickers:
                try:
                    resp = await client.get(
                        FINMIND_BASE,
                        params=self._params("TaiwanStockEarningForecast", ticker, start_date),
                    )
                    resp.raise_for_status()
                    for row in resp.json().get("data", []):
                        records.append({
                            "year_quarter": row.get("date", ""),
                            "ticker":       ticker,
                            "eps_forecast": float(row.get("EPS", 0) or 0),
                        })
                except Exception as e:
                    logger.warning("FinMind EPS forecast %s: %s", ticker, e)
        logger.info("EPS forecasts: %d records", len(records))
        return records

    # ------------------------------------------------------------------
    # 千張大戶持股比例 (週頻)
    # ------------------------------------------------------------------
    async def fetch_major_holders(
        self, tickers: list[str], start_date: str = "2022-01-01"
    ) -> list[dict]:
        """
        FinMind dataset: TaiwanStockHoldingSharesPer
        千張大戶 = 持股 >= 1,000,000 股；散戶 = 持股 <= 10,000 股
        """
        records = []
        async with build_client() as client:
            for ticker in tickers:
                try:
                    resp = await client.get(
                        FINMIND_BASE,
                        params=self._params("TaiwanStockHoldingSharesPer", ticker, start_date),
                    )
                    resp.raise_for_status()
                    date_map: dict[str, dict] = {}
                    for row in resp.json().get("data", []):
                        d     = row["date"]
                        level = str(row.get("HoldingSharesLevel", ""))
                        pct   = float(row.get("percent", 0) or 0)
                        if d not in date_map:
                            date_map[d] = {
                                "date": datetime.fromisoformat(d + "T00:00:00+00:00"), "ticker": ticker,
                                "holders_1000_ratio": 0.0, "retail_ratio": 0.0,
                            }
                        if any(kw in level for kw in _MAJOR_TIERS):
                            date_map[d]["holders_1000_ratio"] += pct
                        if any(kw in level for kw in _RETAIL_TIERS):
                            date_map[d]["retail_ratio"] += pct
                    records.extend(date_map.values())
                except Exception as e:
                    logger.warning("FinMind major_holders %s: %s", ticker, e)
        logger.info("Major holders: %d records", len(records))
        return records
