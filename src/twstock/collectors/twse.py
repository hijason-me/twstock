"""
TWSE (台灣證券交易所) collector.
Data sources:
  - OpenAPI v1: https://openapi.twse.com.tw/v1  (股票清單)
  - Legacy JSON endpoints (MI_INDEX, T86, MI_MARGN, institutional_derivative)
"""
import asyncio
import logging
from datetime import date
from typing import Any

from .base import build_client, throttle

logger = logging.getLogger(__name__)

OPENAPI  = "https://openapi.twse.com.tw/v1"
TWSE     = "https://www.twse.com.tw"


class TWSECollector:
    def __init__(self, delay: float = 1.0):
        self._delay = delay

    async def __aenter__(self):
        self._client = build_client()
        return self

    async def __aexit__(self, *_):
        await self._client.aclose()

    # ------------------------------------------------------------------
    # 上市股票清單
    # ------------------------------------------------------------------
    async def fetch_listed_stocks(self) -> list[dict]:
        resp = await self._client.get(f"{OPENAPI}/exchangeReport/TWTB4U")
        resp.raise_for_status()
        result = []
        for r in resp.json():
            ticker = r.get("公司代號", "").strip()
            name   = r.get("公司簡稱", "").strip()
            if ticker and name:
                result.append({
                    "ticker":   ticker,
                    "name":     name,
                    "market":   "TWSE",
                    "industry": r.get("產業別", ""),
                })
        logger.info("Fetched %d listed stocks", len(result))
        return result

    # ------------------------------------------------------------------
    # 日收盤 OHLCV
    # ------------------------------------------------------------------
    async def fetch_daily_prices(self, trade_date: date) -> list[dict]:
        date_str = trade_date.strftime("%Y%m%d")
        resp = await self._client.get(
            f"{TWSE}/exchangeReport/MI_INDEX",
            params={"response": "json", "date": date_str, "type": "ALLBUT0999"},
        )
        resp.raise_for_status()
        data   = resp.json()
        fields = data.get("fields9", [])
        rows   = data.get("data9", [])
        if not fields or not rows:
            logger.warning("No price data for %s", date_str)
            return []

        col = {name: idx for idx, name in enumerate(fields)}
        result = []
        for row in rows:
            try:
                result.append({
                    "date":   trade_date.isoformat(),
                    "ticker": row[col["證券代號"]].strip(),
                    "open":   float(row[col["開盤價"]].replace(",", "")),
                    "high":   float(row[col["最高價"]].replace(",", "")),
                    "low":    float(row[col["最低價"]].replace(",", "")),
                    "close":  float(row[col["收盤價"]].replace(",", "")),
                    "volume": int(row[col["成交股數"]].replace(",", "")) // 1000,
                })
            except (ValueError, KeyError):
                continue
        logger.info("Fetched %d price records for %s", len(result), date_str)
        return result

    # ------------------------------------------------------------------
    # 三大法人買賣超
    # ------------------------------------------------------------------
    async def fetch_institutional_flows(self, trade_date: date) -> list[dict]:
        await throttle(self._delay)
        date_str = trade_date.strftime("%Y%m%d")
        resp = await self._client.get(
            f"{TWSE}/fund/T86",
            params={"response": "json", "date": date_str, "selectType": "ALLBUT0999"},
        )
        resp.raise_for_status()
        data   = resp.json()
        fields = data.get("fields", [])
        rows   = data.get("data", [])
        if not fields or not rows:
            return []

        col = {name: idx for idx, name in enumerate(fields)}
        result = []
        for row in rows:
            try:
                foreign_net = int(row[col["外陸資買賣超股數(不含外資自營商)"]].replace(",", "")) // 1000
                trust_net   = int(row[col["投信買賣超股數"]].replace(",", "")) // 1000
                dealer_net  = int(row[col["自營商買賣超股數"]].replace(",", "")) // 1000
                result.append({
                    "date":        trade_date.isoformat(),
                    "ticker":      row[col["證券代號"]].strip(),
                    "foreign_net": foreign_net,
                    "trust_net":   trust_net,
                    "dealer_net":  dealer_net,
                    "total_net":   foreign_net + trust_net + dealer_net,
                })
            except (ValueError, KeyError):
                continue
        logger.info("Fetched %d institutional flow records for %s", len(result), date_str)
        return result

    # ------------------------------------------------------------------
    # 外資台指期淨未平倉
    # ------------------------------------------------------------------
    async def fetch_futures_positions(self, trade_date: date) -> dict | None:
        await throttle(self._delay)
        date_str = trade_date.strftime("%Y%m%d")
        resp = await self._client.get(
            f"{TWSE}/derivatives/institutional_derivative",
            params={"response": "json", "date": date_str},
        )
        resp.raise_for_status()
        rows: list[list[Any]] = resp.json().get("data", [])
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
        return record if "foreign_net" in record else None

    # ------------------------------------------------------------------
    # 融資融券餘額
    # ------------------------------------------------------------------
    async def fetch_margin_trading(self, trade_date: date) -> list[dict]:
        await throttle(self._delay)
        date_str = trade_date.strftime("%Y%m%d")
        resp = await self._client.get(
            f"{TWSE}/exchangeReport/MI_MARGN",
            params={"response": "json", "date": date_str, "selectType": "ALL"},
        )
        resp.raise_for_status()
        data   = resp.json()
        fields = data.get("fields", [])
        rows   = data.get("data", [])
        if not fields or not rows:
            return []

        col = {name: idx for idx, name in enumerate(fields)}
        result = []
        for row in rows:
            try:
                result.append({
                    "date":           trade_date.isoformat(),
                    "ticker":         row[col["股票代號"]].strip(),
                    "margin_balance": int(row[col["融資餘額"]].replace(",", "")),
                    "short_balance":  int(row[col["融券餘額"]].replace(",", "")),
                })
            except (ValueError, KeyError):
                continue
        logger.info("Fetched %d margin records for %s", len(result), date_str)
        return result
