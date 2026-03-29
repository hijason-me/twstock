"""
TDCC (集保結算所) collector.
OpenAPI: GET https://openapi.tdcc.com.tw/v1/opendata/1-5
返回最新週所有證券的持股分散表，免費、無需 API key、一次取全部。

持股分級對照:
  1  = 1~999 股
  2  = 1,000~5,000 股
  3  = 5,001~10,000 股   ← 散戶上限 (≤10 張)
  ...
  15 = 1,000,001 股以上  ← 千張大戶
  16 = 其他
  17 = 合計 (略過)
"""
import logging
from datetime import datetime, timezone

from .base import build_client

logger = logging.getLogger(__name__)

TDCC_OPENAPI = "https://openapi.tdcc.com.tw/v1/opendata"

# 千張大戶：持股分級 15（≥1,000,001 股）
_MAJOR_LEVELS  = {"15"}
# 散戶：持股分級 1, 2, 3（≤10,000 股 = ≤10 張）
_RETAIL_LEVELS = {"1", "2", "3"}
# 合計列，略過
_SKIP_LEVELS   = {"17"}


class TDCCCollector:
    """TDCC OpenAPI collector（集保結算所）。"""

    async def fetch_major_holders(self) -> list[dict]:
        """
        取得最新週所有證券持股分散表。
        回傳每支股票的千張大戶比例與散戶比例。
        一次 API 呼叫取全部，免費不限量。
        """
        async with build_client() as client:
            try:
                resp = await client.get(f"{TDCC_OPENAPI}/1-5")
                resp.raise_for_status()
                rows = resp.json()
            except Exception as e:
                logger.error("TDCC fetch_major_holders failed: %s", e)
                return []

        ticker_map: dict[str, dict] = {}
        for row in rows:
            ticker = row.get("證券代號", "").strip()
            level  = str(row.get("持股分級", "")).strip()
            if not ticker or level in _SKIP_LEVELS:
                continue

            # 注意：資料日期欄位有 BOM 字元，需同時嘗試兩種 key
            date_str = (
                row.get("\ufeff資料日期")
                or row.get("資料日期", "")
            ).strip()

            pct = 0.0
            try:
                pct = float(row.get("占集保庫存數比例%", 0) or 0)
            except (ValueError, TypeError):
                pass

            if ticker not in ticker_map:
                try:
                    dt = datetime(
                        int(date_str[:4]),
                        int(date_str[4:6]),
                        int(date_str[6:]),
                        tzinfo=timezone.utc,
                    )
                except (ValueError, IndexError):
                    logger.debug("TDCC bad date for %s: %r", ticker, date_str)
                    continue
                ticker_map[ticker] = {
                    "date":              dt,
                    "ticker":            ticker,
                    "holders_1000_ratio": 0.0,
                    "retail_ratio":       0.0,
                }

            if level in _MAJOR_LEVELS:
                ticker_map[ticker]["holders_1000_ratio"] += pct
            if level in _RETAIL_LEVELS:
                ticker_map[ticker]["retail_ratio"] += pct

        records = list(ticker_map.values())
        logger.info("TDCC major_holders: %d tickers", len(records))
        return records
