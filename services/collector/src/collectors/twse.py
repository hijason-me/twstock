"""
TWSE (台灣證券交易所) data collector.
Uses the official TWSE OpenAPI: https://openapi.twse.com.tw/
and legacy CSV endpoints for institutional data.
"""
import asyncio
import logging
from datetime import date, timedelta
from typing import Any

import httpx

logger = logging.getLogger(__name__)

TWSE_OPENAPI = "https://openapi.twse.com.tw/v1"
TWSE_BASE    = "https://www.twse.com.tw"


class TWSECollector:
    def __init__(self, delay: float = 1.0):
        self._delay = delay
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=30.0, headers={
            "User-Agent": "TWStock-Analyzer/1.0"
        })
        return self

    async def __aexit__(self, *_):
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # 上市股票清單
    # ------------------------------------------------------------------
    async def fetch_listed_stocks(self) -> list[dict[str, str]]:
        """回傳所有上市股票基本資料 (ticker, name, industry)。"""
        resp = await self._client.get(
            f"{TWSE_OPENAPI}/exchangeReport/TWTB4U",
        )
        resp.raise_for_status()
        rows = resp.json()
        result = []
        for r in rows:
            ticker = r.get("公司代號", "").strip()
            name   = r.get("公司簡稱", "").strip()
            if ticker and name:
                result.append({
                    "ticker":   ticker,
                    "name":     name,
                    "market":   "TWSE",
                    "industry": r.get("產業別", ""),
                })
        logger.info("Fetched %d listed stocks from TWSE", len(result))
        return result

    # ------------------------------------------------------------------
    # 日收盤價 (全市場)
    # ------------------------------------------------------------------
    async def fetch_daily_prices(self, trade_date: date | None = None) -> list[dict]:
        """取得整個上市市場的當日收盤 OHLCV。"""
        if trade_date is None:
            trade_date = date.today()
        date_str = trade_date.strftime("%Y%m%d")

        resp = await self._client.get(
            f"{TWSE_BASE}/exchangeReport/MI_INDEX",
            params={"response": "json", "date": date_str, "type": "ALLBUT0999"},
        )
        resp.raise_for_status()
        data = resp.json()

        fields = data.get("fields9", [])
        rows   = data.get("data9", [])
        if not fields or not rows:
            logger.warning("No price data returned for %s", date_str)
            return []

        col = {name: idx for idx, name in enumerate(fields)}
        result = []
        for row in rows:
            try:
                ticker = row[col["證券代號"]].strip()
                close  = float(row[col["收盤價"]].replace(",", ""))
                open_  = float(row[col["開盤價"]].replace(",", ""))
                high   = float(row[col["最高價"]].replace(",", ""))
                low    = float(row[col["最低價"]].replace(",", ""))
                volume = int(row[col["成交股數"]].replace(",", "")) // 1000  # 換算張
                result.append({
                    "date":   trade_date.isoformat(),
                    "ticker": ticker,
                    "open":   open_,
                    "high":   high,
                    "low":    low,
                    "close":  close,
                    "volume": volume,
                })
            except (ValueError, KeyError):
                continue

        logger.info("Fetched %d price records for %s", len(result), date_str)
        return result

    # ------------------------------------------------------------------
    # 三大法人買賣超
    # ------------------------------------------------------------------
    async def fetch_institutional_flows(self, trade_date: date | None = None) -> list[dict]:
        """取得全市場三大法人買賣超 (張)。"""
        if trade_date is None:
            trade_date = date.today()
        date_str = trade_date.strftime("%Y%m%d")

        await asyncio.sleep(self._delay)
        resp = await self._client.get(
            f"{TWSE_BASE}/fund/T86",
            params={"response": "json", "date": date_str, "selectType": "ALLBUT0999"},
        )
        resp.raise_for_status()
        data = resp.json()

        fields = data.get("fields", [])
        rows   = data.get("data", [])
        if not fields or not rows:
            return []

        col = {name: idx for idx, name in enumerate(fields)}
        result = []
        for row in rows:
            try:
                ticker       = row[col["證券代號"]].strip()
                foreign_net  = int(row[col["外陸資買賣超股數(不含外資自營商)"]].replace(",", "")) // 1000
                trust_net    = int(row[col["投信買賣超股數"]].replace(",", "")) // 1000
                dealer_net   = int(row[col["自營商買賣超股數"]].replace(",", "")) // 1000
                total_net    = foreign_net + trust_net + dealer_net
                result.append({
                    "date":        trade_date.isoformat(),
                    "ticker":      ticker,
                    "foreign_net": foreign_net,
                    "trust_net":   trust_net,
                    "dealer_net":  dealer_net,
                    "total_net":   total_net,
                })
            except (ValueError, KeyError):
                continue

        logger.info("Fetched %d institutional flow records for %s", len(result), date_str)
        return result

    # ------------------------------------------------------------------
    # 外資台指期未平倉
    # ------------------------------------------------------------------
    async def fetch_futures_positions(self, trade_date: date | None = None) -> dict | None:
        """取得外資與自營商台指期淨未平倉口數。"""
        if trade_date is None:
            trade_date = date.today()
        date_str = trade_date.strftime("%Y%m%d")

        await asyncio.sleep(self._delay)
        resp = await self._client.get(
            f"{TWSE_BASE}/derivatives/institutional_derivative",
            params={"response": "json", "date": date_str},
        )
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("data", [])
        record: dict[str, Any] = {"date": trade_date.isoformat()}
        for row in rows:
            name = row[0]
            try:
                if "外資" in name and "臺股期貨" in name:
                    record["foreign_long"]  = int(str(row[2]).replace(",", ""))
                    record["foreign_short"] = int(str(row[3]).replace(",", ""))
                    record["foreign_net"]   = int(str(row[4]).replace(",", ""))
                elif "自營商" in name and "臺股期貨" in name:
                    record["dealer_long"]  = int(str(row[2]).replace(",", ""))
                    record["dealer_short"] = int(str(row[3]).replace(",", ""))
                    record["dealer_net"]   = int(str(row[4]).replace(",", ""))
            except (ValueError, IndexError):
                continue

        if "foreign_net" not in record:
            return None
        return record

    # ------------------------------------------------------------------
    # 融資融券餘額
    # ------------------------------------------------------------------
    async def fetch_margin_trading(self, trade_date: date | None = None) -> list[dict]:
        """取得融資融券餘額。"""
        if trade_date is None:
            trade_date = date.today()
        date_str = trade_date.strftime("%Y%m%d")

        await asyncio.sleep(self._delay)
        resp = await self._client.get(
            f"{TWSE_BASE}/exchangeReport/MI_MARGN",
            params={"response": "json", "date": date_str, "selectType": "ALL"},
        )
        resp.raise_for_status()
        data = resp.json()

        fields = data.get("fields", [])
        rows   = data.get("data", [])
        if not fields or not rows:
            return []

        col = {name: idx for idx, name in enumerate(fields)}
        result = []
        for row in rows:
            try:
                ticker         = row[col["股票代號"]].strip()
                margin_balance = int(row[col["融資餘額"]].replace(",", ""))
                short_balance  = int(row[col["融券餘額"]].replace(",", ""))
                result.append({
                    "date":           trade_date.isoformat(),
                    "ticker":         ticker,
                    "margin_balance": margin_balance,
                    "short_balance":  short_balance,
                })
            except (ValueError, KeyError):
                continue

        logger.info("Fetched %d margin trading records for %s", len(result), date_str)
        return result
