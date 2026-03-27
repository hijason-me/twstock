"""
FinMind API collector for financial statements and monthly revenue.
FinMind provides free tier (600 req/day) and paid plans.
Docs: https://finmindtrade.com/analysis/#/Announcement/api
"""
import logging
from datetime import date

import httpx

logger = logging.getLogger(__name__)

FINMIND_BASE = "https://api.finmindtrade.com/api/v4/data"


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
        self,
        tickers: list[str],
        start_date: str = "2022-01-01",
    ) -> list[dict]:
        """Fetch monthly revenue for a list of tickers."""
        records = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for ticker in tickers:
                try:
                    resp = await client.get(FINMIND_BASE, params=self._params(
                        "TaiwanStockMonthRevenue", ticker, start_date
                    ))
                    resp.raise_for_status()
                    for row in resp.json().get("data", []):
                        # date format: YYYY-MM-01 → year_month YYYY-MM
                        year_month = row["date"][:7]
                        records.append({
                            "year_month":  year_month,
                            "ticker":      ticker,
                            "revenue":     int(row.get("revenue", 0)),
                            "revenue_mom": float(row.get("revenue_month_increase", 0) or 0),
                            "revenue_yoy": float(row.get("revenue_year_increase", 0) or 0),
                        })
                except Exception as e:
                    logger.warning("FinMind revenue fetch failed for %s: %s", ticker, e)
        logger.info("Fetched %d monthly revenue records", len(records))
        return records

    # ------------------------------------------------------------------
    # 財報三率 (損益表)
    # ------------------------------------------------------------------
    async def fetch_financial_statements(
        self,
        tickers: list[str],
        start_date: str = "2022-01-01",
    ) -> list[dict]:
        """Fetch quarterly income statements and balance sheet data."""
        records = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for ticker in tickers:
                try:
                    # Income statement
                    resp = await client.get(FINMIND_BASE, params=self._params(
                        "TaiwanStockFinancialStatements", ticker, start_date
                    ))
                    resp.raise_for_status()
                    income_map: dict[str, dict] = {}
                    for row in resp.json().get("data", []):
                        key = row["date"][:7].replace("-Q", "-Q")  # YYYY-QN
                        # FinMind uses type field to categorize line items
                        t = row.get("type", "")
                        if row["date"] not in income_map:
                            income_map[row["date"]] = {"year_quarter": row["date"], "ticker": ticker}
                        entry = income_map[row["date"]]
                        val = float(row.get("value", 0) or 0)
                        if t == "GrossProfit":
                            entry["gross_profit"] = val
                        elif t == "Revenue":
                            entry["revenue"] = val
                        elif t == "OperatingIncome":
                            entry["operating_income"] = val
                        elif t == "NetIncome":
                            entry["net_income"] = val
                        elif t == "EPS":
                            entry["eps"] = val

                    for date_key, entry in income_map.items():
                        rev = entry.get("revenue", 0)
                        records.append({
                            "year_quarter":        entry["year_quarter"],
                            "ticker":              ticker,
                            "gross_profit_margin": (entry.get("gross_profit", 0) / rev * 100) if rev else None,
                            "operating_margin":    (entry.get("operating_income", 0) / rev * 100) if rev else None,
                            "net_profit_margin":   (entry.get("net_income", 0) / rev * 100) if rev else None,
                            "eps":                 entry.get("eps"),
                        })

                except Exception as e:
                    logger.warning("FinMind financials fetch failed for %s: %s", ticker, e)

        logger.info("Fetched %d financial statement records", len(records))
        return records
