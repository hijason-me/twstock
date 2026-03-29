"""
TWStock CLI entry point.

Usage:
    python -m twstock --job <job_name>
    twstock --job <job_name>          # if installed via pip

Available jobs:
    daily_prices          TWSE 日收盤 OHLCV
    daily_institutional   三大法人 + 台指期未平倉 + 融資融券
    macro_indicators      Fed rate / CPI / UST 10Y / USD/TWD
    monthly_revenue       月營收 YoY / MoM  (FinMind)
    quarterly_financials  財報三率 + EPS    (FinMind)
    weekly_major_holders  千張大戶持股比例  (FinMind)
    backfill_prices       yfinance 歷史股價回補 (--backfill YYYY-MM-DD)
"""
import argparse
import asyncio
import logging
import sys
from datetime import date

from sqlalchemy import text

from .config import settings
from .database import AsyncSessionLocal
from .collectors import TWSECollector, MacroCollector, FinMindCollector, TDCCCollector

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("twstock.cli")

# ─────────────────────────────────────────────────────────────
# Upsert helpers (raw SQL for bulk performance)
# ─────────────────────────────────────────────────────────────

async def _upsert(session, sql: str, records: list[dict]) -> None:
    if records:
        await session.execute(text(sql), records)
        await session.commit()


SQL_STOCKS = """
    INSERT INTO stocks (ticker, name, market, industry)
    VALUES (:ticker, :name, :market, :industry)
    ON CONFLICT (ticker) DO UPDATE
      SET name = EXCLUDED.name, industry = EXCLUDED.industry, updated_at = NOW()
"""

SQL_PRICES = """
    INSERT INTO price_history (time, ticker, open, high, low, close, volume)
    VALUES (:date, :ticker, :open, :high, :low, :close, :volume)
    ON CONFLICT (time, ticker) DO UPDATE
      SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
          close=EXCLUDED.close, volume=EXCLUDED.volume
"""

SQL_FLOWS = """
    INSERT INTO institutional_flows (time, ticker, foreign_net, trust_net, dealer_net, total_net)
    VALUES (:date, :ticker, :foreign_net, :trust_net, :dealer_net, :total_net)
    ON CONFLICT (time, ticker) DO UPDATE
      SET foreign_net=EXCLUDED.foreign_net, trust_net=EXCLUDED.trust_net,
          dealer_net=EXCLUDED.dealer_net,   total_net=EXCLUDED.total_net
"""

SQL_FUTURES = """
    INSERT INTO futures_positions
      (time, foreign_long, foreign_short, foreign_net, dealer_long, dealer_short, dealer_net)
    VALUES (:date,
            :foreign_long, :foreign_short, :foreign_net,
            :dealer_long,  :dealer_short,  :dealer_net)
    ON CONFLICT (time) DO UPDATE
      SET foreign_long=EXCLUDED.foreign_long, foreign_short=EXCLUDED.foreign_short,
          foreign_net=EXCLUDED.foreign_net,   dealer_long=EXCLUDED.dealer_long,
          dealer_short=EXCLUDED.dealer_short, dealer_net=EXCLUDED.dealer_net
"""

SQL_MARGIN = """
    INSERT INTO margin_trading (time, ticker, margin_balance, short_balance)
    VALUES (:date, :ticker, :margin_balance, :short_balance)
    ON CONFLICT (time, ticker) DO UPDATE
      SET margin_balance=EXCLUDED.margin_balance, short_balance=EXCLUDED.short_balance
"""

SQL_MACRO = """
    INSERT INTO macro_indicators (time, indicator, value, source)
    VALUES (:time, :indicator, :value, :source)
    ON CONFLICT (time, indicator) DO UPDATE SET value=EXCLUDED.value
"""

SQL_REVENUE = """
    INSERT INTO monthly_revenue (year_month, ticker, revenue, revenue_mom, revenue_yoy)
    VALUES (:year_month, :ticker, :revenue, :revenue_mom, :revenue_yoy)
    ON CONFLICT (year_month, ticker) DO UPDATE
      SET revenue=EXCLUDED.revenue, revenue_mom=EXCLUDED.revenue_mom,
          revenue_yoy=EXCLUDED.revenue_yoy
"""

SQL_FINANCIALS = """
    INSERT INTO financial_statements
      (year_quarter, ticker, gross_profit_margin, operating_margin, net_profit_margin, eps)
    VALUES (:year_quarter, :ticker, :gross_profit_margin, :operating_margin, :net_profit_margin, :eps)
    ON CONFLICT (year_quarter, ticker) DO UPDATE
      SET gross_profit_margin=EXCLUDED.gross_profit_margin,
          operating_margin=EXCLUDED.operating_margin,
          net_profit_margin=EXCLUDED.net_profit_margin,
          eps=EXCLUDED.eps
"""

SQL_MAJOR_HOLDERS = """
    INSERT INTO major_holders (time, ticker, holders_1000_ratio, retail_ratio)
    VALUES (:date, :ticker, :holders_1000_ratio, :retail_ratio)
    ON CONFLICT (time, ticker) DO UPDATE
      SET holders_1000_ratio=EXCLUDED.holders_1000_ratio,
          retail_ratio=EXCLUDED.retail_ratio
"""

# ─────────────────────────────────────────────────────────────
# Jobs
# ─────────────────────────────────────────────────────────────

async def job_daily_prices() -> None:
    today = date.today()
    async with TWSECollector(delay=settings.request_delay) as col:
        stocks = await col.fetch_listed_stocks()
        prices = await col.fetch_daily_prices(today)
    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_STOCKS, stocks)
        valid  = {st["ticker"] for st in stocks}
        prices = [p for p in prices if p["ticker"] in valid]
        await _upsert(s, SQL_PRICES, prices)
    logger.info("daily_prices done: %d stocks, %d prices", len(stocks), len(prices))


async def job_daily_institutional() -> None:
    today = date.today()
    async with TWSECollector(delay=settings.request_delay) as col:
        stocks  = await col.fetch_listed_stocks()
        flows   = await col.fetch_institutional_flows(today)
        futures = await col.fetch_futures_positions(today)
        margin  = await col.fetch_margin_trading(today)

    async with AsyncSessionLocal() as s:
        if stocks:
            await _upsert(s, SQL_STOCKS, stocks)
            valid = {st["ticker"] for st in stocks}
        else:
            logger.warning("fetch_listed_stocks returned 0; falling back to DB stocks")
            result = await s.execute(text("SELECT ticker FROM stocks"))
            valid = {r[0] for r in result.fetchall()}

        if not valid:
            # DB 也是空的 — 從 flows 萃取 ticker 建立 minimal stocks 記錄
            logger.warning("stocks table empty; bootstrapping from flow tickers")
            seen: set[str] = set()
            minimal: list[dict] = []
            for r in flows + margin:
                t = r["ticker"]
                if t not in seen:
                    minimal.append({"ticker": t, "name": t, "market": "TWSE", "industry": ""})
                    seen.add(t)
            await _upsert(s, SQL_STOCKS, minimal)
            valid = seen

        # 過濾掉不在 stocks 清單的 ticker (債券 ETF、認購權證等)
        flows  = [r for r in flows  if r["ticker"] in valid]
        margin = [r for r in margin if r["ticker"] in valid]

        await _upsert(s, SQL_FLOWS,  flows)
        if futures:
            await _upsert(s, SQL_FUTURES, [futures])
        await _upsert(s, SQL_MARGIN, margin)
    logger.info("daily_institutional done: %d flows, %d margin", len(flows), len(margin))


async def job_macro_indicators() -> None:
    col = MacroCollector(fred_api_key=settings.fred_api_key)
    yf_recs   = await col.fetch_yfinance(lookback_days=5)
    fred_recs = await col.fetch_fred(lookback_days=35)
    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_MACRO, yf_recs + fred_recs)
    logger.info("macro_indicators done: %d records", len(yf_recs) + len(fred_recs))


async def _get_tickers(limit: int | None = None) -> list[str]:
    """Fetch tickers from DB.

    Ordered by ticker length ASC then lexically, so 4-digit real stocks (1xxx~9xxx)
    come before 5-6 char ETF/bond codes (00xxx, 009xxx).
    Pass limit=None to get all.
    """
    clause = f"LIMIT {limit}" if limit else ""
    async with AsyncSessionLocal() as s:
        result = await s.execute(
            text(f"SELECT ticker FROM stocks ORDER BY LENGTH(ticker), ticker {clause}")
        )
        return [r[0] for r in result.fetchall()]


def _require_finmind() -> "FinMindCollector | None":
    """Return a FinMindCollector if token is configured, else log and return None."""
    if not settings.finmind_api_token:
        logger.warning(
            "TWSTOCK_FINMIND_API_TOKEN is not set — skipping FinMind job.\n"
            "  Free token: https://finmindtrade.com  (600 req/day)\n"
            "  Set in .env:  TWSTOCK_FINMIND_API_TOKEN=<your_token>\n"
            "  Limit tickers: TWSTOCK_FINMIND_TICKERS_LIMIT=200  (default)"
        )
        return None
    return FinMindCollector(
        api_token=settings.finmind_api_token,
        request_delay=settings.finmind_request_delay,
    )


async def job_monthly_revenue(source: str | None = None, backfill: str | None = None, backfill_end: str | None = None) -> None:
    """
    來源選擇:
      twse    — TWSE OpenAPI bulk (1 call, 僅最新月, 免費) [預設]
      finmind — FinMind per-ticker (可指定 --backfill YYYY-MM)
    """
    src = source or settings.revenue_source
    records: list[dict] = []

    if src == "finmind":
        col = _require_finmind()
        if col is None:
            return
        limit   = settings.finmind_tickers_limit or None
        tickers = await _get_tickers(limit=limit)
        start   = backfill or "2022-01-01"
        logger.info("monthly_revenue[finmind] %d tickers from %s", len(tickers), start)
        records = await col.fetch_monthly_revenue(tickers, start_date=start)
    else:
        # twse bulk (default)
        async with TWSECollector(delay=settings.request_delay) as col:
            records = await col.fetch_monthly_revenue()

    # 過濾只保留 stocks 表中已存在的 ticker
    async with AsyncSessionLocal() as s:
        result = await s.execute(text("SELECT ticker FROM stocks"))
        valid  = {r[0] for r in result.fetchall()}
    records = [r for r in records if r["ticker"] in valid]

    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_REVENUE, records)
    logger.info("monthly_revenue[%s] done: %d records", src, len(records))


async def job_quarterly_financials(source: str | None = None, backfill: str | None = None, backfill_end: str | None = None) -> None:
    """
    來源選擇:
      twse    — TWSE OpenAPI bulk (~6 calls, 僅最新季, 免費) [預設]
      finmind — FinMind per-ticker (可指定 --backfill YYYY-MM-DD)
    """
    src = source or settings.financials_source
    records: list[dict] = []

    if src == "finmind":
        col = _require_finmind()
        if col is None:
            return
        limit   = settings.finmind_tickers_limit or None
        tickers = await _get_tickers(limit=limit)
        start   = backfill or "2022-01-01"
        logger.info("quarterly_financials[finmind] %d tickers from %s", len(tickers), start)
        records = await col.fetch_financial_statements(tickers, start_date=start)
    else:
        # twse bulk (default)
        async with TWSECollector(delay=settings.request_delay) as col:
            records = await col.fetch_quarterly_financials()

    # 過濾只保留 stocks 表中已存在的 ticker
    async with AsyncSessionLocal() as s:
        result = await s.execute(text("SELECT ticker FROM stocks"))
        valid  = {r[0] for r in result.fetchall()}
    records = [r for r in records if r.get("ticker") in valid]

    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_FINANCIALS, records)
    logger.info("quarterly_financials[%s] done: %d records", src, len(records))


async def job_backfill_institutional(backfill: str | None = None, backfill_end: str | None = None) -> None:
    """
    用 FinMind 回補歷史三大法人買賣超 + 融資融券。
    預設起始日：2022-01-01
    """
    col = _require_finmind()
    if col is None:
        return

    limit   = settings.finmind_tickers_limit or None
    tickers = await _get_tickers(limit=limit)
    start   = backfill or "2022-01-01"
    logger.info("backfill_institutional: %d tickers from %s", len(tickers), start)

    flows  = await col.fetch_institutional_flows(tickers, start_date=start)
    margin = await col.fetch_margin_trading(tickers, start_date=start)

    # 過濾只保留 stocks 表中已存在的 ticker
    async with AsyncSessionLocal() as s:
        result = await s.execute(text("SELECT ticker FROM stocks"))
        valid  = {r[0] for r in result.fetchall()}

    flows  = [r for r in flows  if r["ticker"] in valid]
    margin = [r for r in margin if r["ticker"] in valid]

    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_FLOWS,  flows)
        await _upsert(s, SQL_MARGIN, margin)

    logger.info("backfill_institutional done: %d flows, %d margin", len(flows), len(margin))


async def job_backfill_prices(backfill: str | None = None, backfill_end: str | None = None) -> None:
    """
    用 yfinance 回補歷史日 OHLCV。
    預設回補 2024-01-01 到今天。
    yfinance 代碼格式：TWSE → {ticker}.TW, TPEx → {ticker}.TWO
    """
    import yfinance as yf
    import pandas as pd

    start = backfill or "2024-01-01"
    end   = backfill_end or date.today().strftime("%Y-%m-%d")
    logger.info("backfill_prices: %s → %s", start, end)

    # 讀取所有股票及市場別
    async with AsyncSessionLocal() as s:
        result = await s.execute(text("SELECT ticker, market FROM stocks ORDER BY ticker"))
        rows = result.fetchall()

    if not rows:
        logger.error("stocks 表為空，請先執行 daily_prices 建立股票清單")
        return

    # 建立 yfinance ticker 對照表
    ticker_map: dict[str, str] = {}  # yf_ticker → db_ticker
    for db_ticker, market in rows:
        suffix = ".TW" if market == "TWSE" else ".TWO"
        ticker_map[f"{db_ticker}{suffix}"] = db_ticker

    yf_tickers = list(ticker_map.keys())
    logger.info("共 %d 支股票，開始下載…", len(yf_tickers))

    BATCH = 200   # yfinance 每次最多同時下載的數量
    total = 0

    for i in range(0, len(yf_tickers), BATCH):
        batch = yf_tickers[i : i + BATCH]
        batch_no = i // BATCH + 1
        total_batches = (len(yf_tickers) + BATCH - 1) // BATCH
        logger.info("Batch %d/%d (%d tickers)", batch_no, total_batches, len(batch))

        try:
            df = yf.download(
                batch, start=start, end=end,
                auto_adjust=True, progress=False, threads=True,
            )
        except Exception as exc:
            logger.error("yfinance batch %d 下載失敗: %s", batch_no, exc)
            continue

        if df.empty:
            logger.warning("Batch %d 無資料", batch_no)
            continue

        records: list[dict] = []
        for yf_tk in batch:
            db_tk = ticker_map.get(yf_tk)
            if not db_tk:
                continue
            try:
                # 多 ticker → MultiIndex columns (metric, ticker)
                # 單 ticker → 一般 columns
                if isinstance(df.columns, pd.MultiIndex):
                    sub = df.xs(yf_tk, level=1, axis=1)
                else:
                    sub = df
                for ts, row in sub.iterrows():
                    close = row.get("Close")
                    if pd.isna(close):
                        continue
                    vol_raw = row.get("Volume", 0)
                    records.append({
                        "date":   ts.to_pydatetime(),
                        "ticker": db_tk,
                        "open":   float(row["Open"])  if not pd.isna(row.get("Open"))  else None,
                        "high":   float(row["High"])  if not pd.isna(row.get("High"))  else None,
                        "low":    float(row["Low"])   if not pd.isna(row.get("Low"))   else None,
                        "close":  float(close),
                        # yfinance 單位為股，÷1000 轉換為張
                        "volume": int(vol_raw / 1000) if not pd.isna(vol_raw) else 0,
                    })
            except Exception as exc:
                logger.debug("解析 %s 失敗: %s", yf_tk, exc)

        if records:
            async with AsyncSessionLocal() as s:
                await _upsert(s, SQL_PRICES, records)
            total += len(records)
            logger.info("Batch %d 寫入 %d 筆 (累計 %d)", batch_no, len(records), total)

    logger.info("backfill_prices 完成，共 %d 筆", total)


async def job_weekly_major_holders(source: str | None = None, backfill: str | None = None, backfill_end: str | None = None) -> None:
    """
    來源選擇:
      tdcc    — TDCC OpenAPI bulk (1 call, 最新週, 免費免 key) [預設]
      finmind — FinMind per-ticker (可指定 --backfill, 需付費方案)
    """
    src = source or settings.holders_source
    records: list[dict] = []

    if src == "finmind":
        col = _require_finmind()
        if col is None:
            return
        limit   = settings.finmind_tickers_limit or None
        tickers = await _get_tickers(limit=limit)
        start   = backfill or "2022-01-01"
        logger.info("weekly_major_holders[finmind] %d tickers from %s", len(tickers), start)
        records = await col.fetch_major_holders(tickers, start_date=start)
    else:
        # tdcc bulk (default)
        col = TDCCCollector()
        records = await col.fetch_major_holders()

    # 過濾只保留 stocks 表中已存在的 ticker
    async with AsyncSessionLocal() as s:
        result = await s.execute(text("SELECT ticker FROM stocks"))
        valid  = {r[0] for r in result.fetchall()}
    records = [r for r in records if r["ticker"] in valid]

    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_MAJOR_HOLDERS, records)
    logger.info("weekly_major_holders[%s] done: %d records", src, len(records))


JOBS = {
    "daily_prices":           job_daily_prices,
    "daily_institutional":    job_daily_institutional,
    "macro_indicators":       job_macro_indicators,
    "monthly_revenue":        job_monthly_revenue,
    "quarterly_financials":   job_quarterly_financials,
    "weekly_major_holders":   job_weekly_major_holders,
    "backfill_prices":        job_backfill_prices,
    "backfill_institutional": job_backfill_institutional,
}


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

_SOURCE_JOBS    = {"monthly_revenue", "quarterly_financials", "weekly_major_holders"}
_BACKFILL_JOBS  = {"monthly_revenue", "quarterly_financials", "weekly_major_holders",
                   "backfill_prices", "backfill_institutional"}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="TWStock data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
來源選擇 (--source) 僅適用於 monthly_revenue / quarterly_financials / weekly_major_holders:
  monthly_revenue:      twse (預設, bulk) | finmind (per-ticker, 需 token)
  quarterly_financials: twse (預設, bulk) | finmind (per-ticker, 需 token)
  weekly_major_holders: tdcc (預設, bulk) | finmind (per-ticker, 需付費)

歷史回補 (--backfill) 僅適用於 finmind 來源:
  格式: YYYY-MM-DD 或 YYYY-MM
  例: --source finmind --backfill 2022-01-01
""",
    )
    parser.add_argument("--job", required=True, choices=list(JOBS.keys()))
    parser.add_argument(
        "--source",
        default=None,
        help="覆蓋 .env 的來源設定: twse | mops | tdcc | finmind",
    )
    parser.add_argument(
        "--backfill",
        default=None,
        metavar="YYYY-MM[-DD]",
        help="歷史回補起始日期 (backfill_prices 或 finmind 來源有效)",
    )
    parser.add_argument(
        "--backfill-end",
        default=None,
        metavar="YYYY-MM[-DD]",
        dest="backfill_end",
        help="歷史回補截止日期 (預設: 今天)",
    )
    args = parser.parse_args()

    if args.source and args.job not in _SOURCE_JOBS:
        logger.warning("--source 對 %s 無效，忽略", args.job)

    logger.info("Starting job: %s (source=%s, backfill=%s)", args.job, args.source, args.backfill)

    job_fn = JOBS[args.job]
    if args.job in ("backfill_prices", "backfill_institutional"):
        asyncio.run(job_fn(backfill=args.backfill, backfill_end=args.backfill_end))
    elif args.job in _SOURCE_JOBS:
        asyncio.run(job_fn(source=args.source, backfill=args.backfill, backfill_end=args.backfill_end))
    else:
        asyncio.run(job_fn())

    logger.info("Job %s complete", args.job)


if __name__ == "__main__":
    main()
