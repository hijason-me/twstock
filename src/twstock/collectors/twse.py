"""
TWSE (台灣證券交易所) collector.
Data sources:
  - OpenAPI v1:  https://openapi.twse.com.tw/v1  (股票清單、月營收、季財報)
  - Legacy JSON: MI_INDEX, T86, MI_MARGN
  - TAIFEX:      https://www.taifex.com.tw       (外資台指期未平倉)

月營收 endpoint:  GET /v1/opendata/t187ap05_P
  → 一次回傳全部公司、僅最新月、免費免 key

季財報 endpoints: GET /v1/opendata/t187ap06_L_{type}
  type: ci(一般) | basi(銀行) | fh(金控) | ins(保險) | bd(證券期貨) | mim(其他)
  → 各 ~1 call 回傳該產業全部公司、僅最新季、免費免 key
"""
import asyncio
import json
import logging
from datetime import date, datetime, timezone
from typing import Any

from .base import build_client, throttle

logger = logging.getLogger(__name__)

OPENAPI  = "https://openapi.twse.com.tw/v1"
TWSE     = "https://www.twse.com.tw"
TAIFEX   = "https://www.taifex.com.tw"


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
        # 主清單：代號 + 名稱（TWTB4U 不含產業別）
        resp = await self._client.get(f"{OPENAPI}/exchangeReport/TWTB4U")
        resp.raise_for_status()
        stocks: dict[str, dict] = {}
        for r in resp.json():
            ticker = r.get("Code", r.get("公司代號", "")).strip()
            name   = r.get("Name", r.get("公司簡稱", "")).strip()
            if ticker and name:
                stocks[ticker] = {"ticker": ticker, "name": name, "market": "TWSE", "industry": ""}

        # 產業別：t187ap03_L 含 CFICode / 產業別
        try:
            resp2 = await self._client.get(f"{OPENAPI}/opendata/t187ap03_L")
            resp2.raise_for_status()
            for r in resp2.json():
                ticker = r.get("公司代號", r.get("SecuritiesCompanyCode", "")).strip()
                ind    = r.get("產業別", r.get("IndustryType", r.get("TypeOfBusiness", ""))).strip()
                if ticker in stocks and ind:
                    stocks[ticker]["industry"] = ind
        except Exception as e:
            logger.warning("fetch industry from t187ap03_L failed: %s", e)

        result = list(stocks.values())
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
        data = resp.json()
        # 新版 MI_INDEX 把各類別放在 tables 陣列內，每個 table 有 fields + data
        # 找含有 "證券代號" 欄位的 table（普通股那張）
        fields, rows = [], []
        for table in data.get("tables", []):
            f = table.get("fields", [])
            if "證券代號" in f:
                fields = f
                rows   = table.get("data", [])
                break
        # 舊版 fallback
        if not fields:
            fields = data.get("fields9") or data.get("fields8", [])
            rows   = data.get("data9")   or data.get("data8",   [])
        if not fields or not rows:
            logger.warning("No price data for %s (keys: %s)", date_str, list(data.keys()))
            return []

        col = {name: idx for idx, name in enumerate(fields)}
        result = []
        for row in rows:
            try:
                result.append({
                    "date":   datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=timezone.utc),
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
                    "date":        datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=timezone.utc),
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
    # 外資台指期淨未平倉  (來源：TAIFEX)
    # ------------------------------------------------------------------
    async def fetch_futures_positions(self, trade_date: date) -> dict | None:
        """
        抓取外資與自營商台指期淨未平倉口數。
        資料來源為 TAIFEX（台灣期貨交易所）三大法人期貨未平倉報表。
        非交易日或資料未發布時回傳 None，不中斷 job。
        """
        await throttle(self._delay)
        date_str = trade_date.strftime("%Y/%m/%d")
        try:
            resp = await self._client.post(
                f"{TAIFEX}/cht/3/futContractsDate",
                data={
                    "queryStartDate": date_str,
                    "queryEndDate":   date_str,
                    "commodityId":    "TXF",  # 臺股期貨
                },
                headers={"Referer": f"{TAIFEX}/cht/3/futContractsDate"},
            )
            resp.raise_for_status()
            data = resp.json()
        except (json.JSONDecodeError, Exception) as e:
            logger.warning("fetch_futures_positions failed for %s: %s", date_str, e)
            return None

        record: dict[str, Any] = {
            "date": datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=timezone.utc)
        }
        for row in data if isinstance(data, list) else data.get("data", []):
            try:
                identity = str(row[2]).strip()   # 身份別欄位
                if "外資" in identity:
                    record["foreign_long"]  = int(str(row[3]).replace(",", ""))
                    record["foreign_short"] = int(str(row[4]).replace(",", ""))
                    record["foreign_net"]   = int(str(row[5]).replace(",", ""))
                elif "自營商" in identity:
                    record["dealer_long"]  = int(str(row[3]).replace(",", ""))
                    record["dealer_short"] = int(str(row[4]).replace(",", ""))
                    record["dealer_net"]   = int(str(row[5]).replace(",", ""))
            except (ValueError, IndexError):
                continue

        if "foreign_net" not in record:
            logger.warning("No futures position data for %s", date_str)
            return None
        return record

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
        data = resp.json()

        # 新版 MI_MARGN 同 MI_INDEX，資料在 tables 陣列中
        # 找含有 "代號" 欄位的 table（個股融資融券那張）
        # 欄位有重複名稱（今日餘額出現兩次），須用索引存取：
        #   idx 0: 代號, idx 6: 融資今日餘額, idx 12: 融券今日餘額
        rows = []
        for table in data.get("tables", []):
            fields = table.get("fields", [])
            if "代號" in fields:
                rows = table.get("data", [])
                break
        # 舊版 fallback
        if not rows:
            fields = data.get("fields", [])
            rows   = data.get("data", [])
            if fields and rows:
                col = {name: idx for idx, name in enumerate(fields)}
                result = []
                for row in rows:
                    try:
                        result.append({
                            "date":           datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=timezone.utc),
                            "ticker":         row[col["股票代號"]].strip(),
                            "margin_balance": int(row[col["融資餘額"]].replace(",", "")),
                            "short_balance":  int(row[col["融券餘額"]].replace(",", "")),
                        })
                    except (ValueError, KeyError):
                        continue
                logger.info("Fetched %d margin records for %s", len(result), date_str)
                return result
            return []

        result = []
        for row in rows:
            try:
                result.append({
                    "date":           datetime(trade_date.year, trade_date.month, trade_date.day, tzinfo=timezone.utc),
                    "ticker":         row[0].strip(),
                    "margin_balance": int(str(row[6]).replace(",", "")),
                    "short_balance":  int(str(row[12]).replace(",", "")),
                })
            except (ValueError, IndexError):
                continue
        logger.info("Fetched %d margin records for %s", len(result), date_str)
        return result

    # ------------------------------------------------------------------
    # 月營收 (TWSE OpenAPI bulk — 免費、一次取全部、僅最新月)
    # ------------------------------------------------------------------
    async def fetch_monthly_revenue(self) -> list[dict]:
        """
        GET /v1/opendata/t187ap05_L  (L = 上市公司)
        資料年月格式: "11502" = ROC 115年02月 = 2026-02
        一次回傳全部上市公司當月營收，無需逐支查詢。
        """
        resp = await self._client.get(f"{OPENAPI}/opendata/t187ap05_L")
        resp.raise_for_status()
        result = []
        for r in resp.json():
            try:
                # 資料年月: "11502" → "2026-02"
                ym_raw = str(r.get("資料年月", "")).strip()
                roc_y  = int(ym_raw[:3])
                month  = int(ym_raw[3:])
                year_month = f"{roc_y + 1911}-{month:02d}"

                revenue = r.get("營業收入-當月營收", "").replace(",", "").strip()
                mom     = r.get("營業收入-上月比較增減(%)", "").strip()
                yoy     = r.get("營業收入-去年同月增減(%)", "").strip()

                def _pct(v: str) -> float:
                    """Parse % string, clamp to NUMERIC(12,4) safe range."""
                    try:
                        return max(-99999999.0, min(99999999.0, float(v)))
                    except (ValueError, TypeError):
                        return 0.0

                result.append({
                    "year_month":  year_month,
                    "ticker":      r.get("公司代號", "").strip(),
                    "revenue":     int(revenue) if revenue else 0,
                    "revenue_mom": _pct(mom) if mom not in ("", "--", "-") else 0.0,
                    "revenue_yoy": _pct(yoy) if yoy not in ("", "--", "-") else 0.0,
                })
            except (ValueError, KeyError):
                continue
        logger.info("TWSE monthly_revenue: %d records", len(result))
        return result

    # ------------------------------------------------------------------
    # 季財報三率 + EPS (TWSE OpenAPI bulk — 免費、~6 calls、僅最新季)
    # ------------------------------------------------------------------
    # 各產業 endpoint 的 type 後綴
    _FINANCIAL_TYPES = ["ci", "basi", "fh", "ins", "bd", "mim"]

    async def fetch_quarterly_financials(self) -> list[dict]:
        """
        GET /v1/opendata/t187ap06_L_{type}
        年度格式: "114" = ROC 114年 = 2025, 季別: "4" → "2025-Q4"
        合併 6 個產業 endpoint，一次取全部公司最新季財報。
        """
        result = []
        for ftype in self._FINANCIAL_TYPES:
            try:
                resp = await self._client.get(
                    f"{OPENAPI}/opendata/t187ap06_L_{ftype}"
                )
                resp.raise_for_status()
                for r in resp.json():
                    try:
                        roc_y  = int(str(r.get("年度", "0")).strip())
                        season = str(r.get("季別", "")).strip()
                        year_quarter = f"{roc_y + 1911}-Q{season}"

                        rev = r.get("營業收入", "").replace(",", "").strip()
                        gp  = r.get("營業毛利（毛損）淨額", "").replace(",", "").strip()
                        oi  = r.get("營業利益（損失）", "").replace(",", "").strip()
                        ni  = r.get("本期淨利（淨損）", "").replace(",", "").strip()
                        eps = r.get("基本每股盈餘（元）", "").replace(",", "").strip()

                        revenue = float(rev) if rev else None
                        result.append({
                            "year_quarter":        year_quarter,
                            "ticker":              r.get("公司代號", "").strip(),
                            "gross_profit_margin": (float(gp) / revenue * 100) if revenue and gp else None,
                            "operating_margin":    (float(oi) / revenue * 100) if revenue and oi else None,
                            "net_profit_margin":   (float(ni) / revenue * 100) if revenue and ni else None,
                            "eps":                 float(eps) if eps else None,
                        })
                    except (ValueError, KeyError, ZeroDivisionError):
                        continue
                logger.info("TWSE financials type=%s: %d records so far", ftype, len(result))
            except Exception as e:
                logger.warning("TWSE financials type=%s failed: %s", ftype, e)
        logger.info("TWSE quarterly_financials total: %d records", len(result))
        return result
