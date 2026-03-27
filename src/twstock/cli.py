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
"""
import argparse
import asyncio
import logging
import sys
from datetime import date

from sqlalchemy import text

from .config import settings
from .database import AsyncSessionLocal
from .collectors import TWSECollector, MacroCollector, FinMindCollector

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
    VALUES (:date::timestamptz, :ticker, :open, :high, :low, :close, :volume)
    ON CONFLICT (time, ticker) DO UPDATE
      SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
          close=EXCLUDED.close, volume=EXCLUDED.volume
"""

SQL_FLOWS = """
    INSERT INTO institutional_flows (time, ticker, foreign_net, trust_net, dealer_net, total_net)
    VALUES (:date::timestamptz, :ticker, :foreign_net, :trust_net, :dealer_net, :total_net)
    ON CONFLICT (time, ticker) DO UPDATE
      SET foreign_net=EXCLUDED.foreign_net, trust_net=EXCLUDED.trust_net,
          dealer_net=EXCLUDED.dealer_net,   total_net=EXCLUDED.total_net
"""

SQL_FUTURES = """
    INSERT INTO futures_positions
      (time, foreign_long, foreign_short, foreign_net, dealer_long, dealer_short, dealer_net)
    VALUES (:date::timestamptz,
            :foreign_long, :foreign_short, :foreign_net,
            :dealer_long,  :dealer_short,  :dealer_net)
    ON CONFLICT (time) DO UPDATE
      SET foreign_long=EXCLUDED.foreign_long, foreign_short=EXCLUDED.foreign_short,
          foreign_net=EXCLUDED.foreign_net,   dealer_long=EXCLUDED.dealer_long,
          dealer_short=EXCLUDED.dealer_short, dealer_net=EXCLUDED.dealer_net
"""

SQL_MARGIN = """
    INSERT INTO margin_trading (time, ticker, margin_balance, short_balance)
    VALUES (:date::timestamptz, :ticker, :margin_balance, :short_balance)
    ON CONFLICT (time, ticker) DO UPDATE
      SET margin_balance=EXCLUDED.margin_balance, short_balance=EXCLUDED.short_balance
"""

SQL_MACRO = """
    INSERT INTO macro_indicators (time, indicator, value, source)
    VALUES (:time::timestamptz, :indicator, :value, :source)
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
    VALUES (:date::timestamptz, :ticker, :holders_1000_ratio, :retail_ratio)
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
        await _upsert(s, SQL_PRICES, prices)
    logger.info("daily_prices done: %d stocks, %d prices", len(stocks), len(prices))


async def job_daily_institutional() -> None:
    today = date.today()
    async with TWSECollector(delay=settings.request_delay) as col:
        flows   = await col.fetch_institutional_flows(today)
        futures = await col.fetch_futures_positions(today)
        margin  = await col.fetch_margin_trading(today)
    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_FLOWS,   flows)
        if futures:
            await _upsert(s, SQL_FUTURES, [futures])
        await _upsert(s, SQL_MARGIN,  margin)
    logger.info("daily_institutional done")


async def job_macro_indicators() -> None:
    col = MacroCollector(fred_api_key=settings.fred_api_key)
    yf_recs   = await col.fetch_yfinance(lookback_days=5)
    fred_recs = await col.fetch_fred(lookback_days=35)
    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_MACRO, yf_recs + fred_recs)
    logger.info("macro_indicators done: %d records", len(yf_recs) + len(fred_recs))


async def _get_tickers(limit: int = 500) -> list[str]:
    async with AsyncSessionLocal() as s:
        result = await s.execute(text(f"SELECT ticker FROM stocks LIMIT {limit}"))
        return [r[0] for r in result.fetchall()]


async def job_monthly_revenue() -> None:
    tickers = await _get_tickers()
    col     = FinMindCollector(api_token=settings.finmind_api_token)
    records = await col.fetch_monthly_revenue(tickers)
    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_REVENUE, records)
    logger.info("monthly_revenue done: %d records", len(records))


async def job_quarterly_financials() -> None:
    tickers = await _get_tickers()
    col     = FinMindCollector(api_token=settings.finmind_api_token)
    records = await col.fetch_financial_statements(tickers)
    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_FINANCIALS, records)
    logger.info("quarterly_financials done: %d records", len(records))


async def job_weekly_major_holders() -> None:
    tickers = await _get_tickers()
    col     = FinMindCollector(api_token=settings.finmind_api_token)
    records = await col.fetch_major_holders(tickers)
    async with AsyncSessionLocal() as s:
        await _upsert(s, SQL_MAJOR_HOLDERS, records)
    logger.info("weekly_major_holders done: %d records", len(records))


JOBS = {
    "daily_prices":         job_daily_prices,
    "daily_institutional":  job_daily_institutional,
    "macro_indicators":     job_macro_indicators,
    "monthly_revenue":      job_monthly_revenue,
    "quarterly_financials": job_quarterly_financials,
    "weekly_major_holders": job_weekly_major_holders,
}


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="TWStock data pipeline")
    parser.add_argument("--job", required=True, choices=list(JOBS.keys()))
    args = parser.parse_args()
    logger.info("Starting job: %s", args.job)
    asyncio.run(JOBS[args.job]())
    logger.info("Job %s complete", args.job)


if __name__ == "__main__":
    main()
