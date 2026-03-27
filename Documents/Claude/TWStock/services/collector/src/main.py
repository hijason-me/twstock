"""
Collector service entry point.
Usage:
    python -m src.main --job <job_name>

Available jobs:
    daily_prices        - TWSE 收盤行情 (每日盤後)
    daily_institutional - 三大法人 + 期貨未平倉 + 融資融券 (每日盤後)
    monthly_revenue     - 月營收 (每月 10 日後)
    quarterly_financials- 季報財務三率 (每季財報公佈後)
    macro_indicators    - 總經指標 (每日)
"""
import argparse
import asyncio
import logging
import sys
from datetime import date, timedelta

from sqlalchemy import text

from .config import settings
from .database import AsyncSessionLocal, engine
from .collectors import TWSECollector, MacroCollector, FinMindCollector

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("collector")


# ------------------------------------------------------------------
# Upsert helpers (raw SQL for performance)
# ------------------------------------------------------------------

async def upsert_stocks(session, stocks: list[dict]):
    if not stocks:
        return
    await session.execute(text("""
        INSERT INTO stocks (ticker, name, market, industry)
        VALUES (:ticker, :name, :market, :industry)
        ON CONFLICT (ticker) DO UPDATE
          SET name = EXCLUDED.name,
              industry = EXCLUDED.industry,
              updated_at = NOW()
    """), stocks)
    await session.commit()


async def upsert_prices(session, records: list[dict]):
    if not records:
        return
    await session.execute(text("""
        INSERT INTO price_history (time, ticker, open, high, low, close, volume)
        VALUES (:date::timestamptz, :ticker, :open, :high, :low, :close, :volume)
        ON CONFLICT (time, ticker) DO UPDATE
          SET open = EXCLUDED.open, high = EXCLUDED.high,
              low  = EXCLUDED.low,  close = EXCLUDED.close,
              volume = EXCLUDED.volume
    """), records)
    await session.commit()


async def upsert_institutional(session, records: list[dict]):
    if not records:
        return
    await session.execute(text("""
        INSERT INTO institutional_flows
          (time, ticker, foreign_net, trust_net, dealer_net, total_net)
        VALUES (:date::timestamptz, :ticker,
                :foreign_net, :trust_net, :dealer_net, :total_net)
        ON CONFLICT (time, ticker) DO UPDATE
          SET foreign_net = EXCLUDED.foreign_net,
              trust_net   = EXCLUDED.trust_net,
              dealer_net  = EXCLUDED.dealer_net,
              total_net   = EXCLUDED.total_net
    """), records)
    await session.commit()


async def upsert_futures(session, record: dict | None):
    if not record:
        return
    await session.execute(text("""
        INSERT INTO futures_positions
          (time, foreign_long, foreign_short, foreign_net,
           dealer_long, dealer_short, dealer_net)
        VALUES (:date::timestamptz,
                :foreign_long, :foreign_short, :foreign_net,
                :dealer_long,  :dealer_short,  :dealer_net)
        ON CONFLICT (time) DO UPDATE
          SET foreign_long  = EXCLUDED.foreign_long,
              foreign_short = EXCLUDED.foreign_short,
              foreign_net   = EXCLUDED.foreign_net,
              dealer_long   = EXCLUDED.dealer_long,
              dealer_short  = EXCLUDED.dealer_short,
              dealer_net    = EXCLUDED.dealer_net
    """), record)
    await session.commit()


async def upsert_margin(session, records: list[dict]):
    if not records:
        return
    await session.execute(text("""
        INSERT INTO margin_trading (time, ticker, margin_balance, short_balance)
        VALUES (:date::timestamptz, :ticker, :margin_balance, :short_balance)
        ON CONFLICT (time, ticker) DO UPDATE
          SET margin_balance = EXCLUDED.margin_balance,
              short_balance  = EXCLUDED.short_balance
    """), records)
    await session.commit()


async def upsert_macro(session, records: list[dict]):
    if not records:
        return
    await session.execute(text("""
        INSERT INTO macro_indicators (time, indicator, value, source)
        VALUES (:time::timestamptz, :indicator, :value, :source)
        ON CONFLICT (time, indicator) DO UPDATE
          SET value = EXCLUDED.value
    """), records)
    await session.commit()


async def upsert_revenue(session, records: list[dict]):
    if not records:
        return
    await session.execute(text("""
        INSERT INTO monthly_revenue
          (year_month, ticker, revenue, revenue_mom, revenue_yoy)
        VALUES (:year_month, :ticker, :revenue, :revenue_mom, :revenue_yoy)
        ON CONFLICT (year_month, ticker) DO UPDATE
          SET revenue     = EXCLUDED.revenue,
              revenue_mom = EXCLUDED.revenue_mom,
              revenue_yoy = EXCLUDED.revenue_yoy
    """), records)
    await session.commit()


async def upsert_financials(session, records: list[dict]):
    if not records:
        return
    await session.execute(text("""
        INSERT INTO financial_statements
          (year_quarter, ticker, gross_profit_margin,
           operating_margin, net_profit_margin, eps)
        VALUES (:year_quarter, :ticker, :gross_profit_margin,
                :operating_margin, :net_profit_margin, :eps)
        ON CONFLICT (year_quarter, ticker) DO UPDATE
          SET gross_profit_margin = EXCLUDED.gross_profit_margin,
              operating_margin    = EXCLUDED.operating_margin,
              net_profit_margin   = EXCLUDED.net_profit_margin,
              eps                 = EXCLUDED.eps
    """), records)
    await session.commit()


# ------------------------------------------------------------------
# Job implementations
# ------------------------------------------------------------------

async def job_daily_prices():
    today = date.today()
    async with TWSECollector(delay=settings.request_delay) as col:
        stocks = await col.fetch_listed_stocks()
        prices = await col.fetch_daily_prices(today)

    async with AsyncSessionLocal() as session:
        await upsert_stocks(session, stocks)
        await upsert_prices(session, prices)
    logger.info("job_daily_prices complete: %d prices", len(prices))


async def job_daily_institutional():
    today = date.today()
    async with TWSECollector(delay=settings.request_delay) as col:
        flows   = await col.fetch_institutional_flows(today)
        futures = await col.fetch_futures_positions(today)
        margin  = await col.fetch_margin_trading(today)

    async with AsyncSessionLocal() as session:
        await upsert_institutional(session, flows)
        await upsert_futures(session, futures)
        await upsert_margin(session, margin)
    logger.info("job_daily_institutional complete")


async def job_macro_indicators():
    collector = MacroCollector(fred_api_key=settings.fred_api_key)
    yf_records   = await collector.fetch_yfinance_indicators(lookback_days=5)
    fred_records = await collector.fetch_fred_indicators(lookback_days=35)
    all_records  = yf_records + fred_records

    async with AsyncSessionLocal() as session:
        await upsert_macro(session, all_records)
    logger.info("job_macro_indicators complete: %d records", len(all_records))


async def job_monthly_revenue():
    # Collect the watchlist from DB
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("SELECT ticker FROM stocks LIMIT 500"))
        tickers = [r[0] for r in result.fetchall()]

    collector = FinMindCollector(api_token=settings.finmind_api_token)
    records = await collector.fetch_monthly_revenue(tickers)

    async with AsyncSessionLocal() as session:
        await upsert_revenue(session, records)
    logger.info("job_monthly_revenue complete: %d records", len(records))


async def job_quarterly_financials():
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("SELECT ticker FROM stocks LIMIT 500"))
        tickers = [r[0] for r in result.fetchall()]

    collector = FinMindCollector(api_token=settings.finmind_api_token)
    records = await collector.fetch_financial_statements(tickers)

    async with AsyncSessionLocal() as session:
        await upsert_financials(session, records)
    logger.info("job_quarterly_financials complete: %d records", len(records))


JOBS = {
    "daily_prices":         job_daily_prices,
    "daily_institutional":  job_daily_institutional,
    "macro_indicators":     job_macro_indicators,
    "monthly_revenue":      job_monthly_revenue,
    "quarterly_financials": job_quarterly_financials,
}


def main():
    parser = argparse.ArgumentParser(description="TWStock data collector")
    parser.add_argument("--job", required=True, choices=list(JOBS.keys()),
                        help="Job to run")
    args = parser.parse_args()

    logger.info("Starting job: %s", args.job)
    asyncio.run(JOBS[args.job]())
    logger.info("Job %s finished", args.job)


if __name__ == "__main__":
    main()
