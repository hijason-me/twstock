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

# 千張大戶：持股 >= 1,000,000 股 (千張 × 1000 股/張)
# TWSE 股權分散表最高一級
_MAJOR_TIER_KEYWORDS = ["1,000,001", "1000001", "over 1,000,000"]
# 散戶：持股 <= 10,000 股 (10 張以下)
_RETAIL_TIER_KEYWORDS = ["1 to 999", "1,000 to 5,000", "5,001 to 10,000"]


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

    # ------------------------------------------------------------------
    # 千張大戶持股比例 (週頻)
    # ------------------------------------------------------------------
    async def fetch_major_holders(
        self,
        tickers: list[str],
        start_date: str = "2022-01-01",
    ) -> list[dict]:
        """Fetch weekly shareholding distribution (千張大戶持股比例).

        FinMind dataset: TaiwanStockHoldingSharesPer
        千張大戶 = 持股 >= 1,000,000 股 (1000張 × 1000股/張)
        散戶     = 持股 <= 10,000 股 (10張以下)
        """
        records = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for ticker in tickers:
                try:
                    resp = await client.get(FINMIND_BASE, params=self._params(
                        "TaiwanStockHoldingSharesPer", ticker, start_date
                    ))
                    resp.raise_for_status()
                    data = resp.json().get("data", [])

                    # 依日期彙總各持股層級的比例
                    date_map: dict[str, dict] = {}
                    for row in data:
                        d = row["date"]
                        if d not in date_map:
                            date_map[d] = {
                                "date": d,
                                "ticker": ticker,
                                "holders_1000_ratio": 0.0,
                                "retail_ratio": 0.0,
                            }
                        level = str(row.get("HoldingSharesLevel", ""))
                        pct = float(row.get("percent", 0) or 0)

                        if any(kw in level for kw in _MAJOR_TIER_KEYWORDS):
                            date_map[d]["holders_1000_ratio"] += pct
                        if any(kw in level for kw in _RETAIL_TIER_KEYWORDS):
                            date_map[d]["retail_ratio"] += pct

                    records.extend(date_map.values())
                except Exception as e:
                    logger.warning("FinMind major_holders fetch failed for %s: %s", ticker, e)

        logger.info("Fetched %d major holder records", len(records))
        return records
