"""
Analyzer service entry point.

Available jobs:
    valuations    - 計算所有股票的估值分數
    multi_factor  - 執行多因子篩選策略
    alerts        - 產生告警 (總經 + 個股)
"""
import argparse
import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import text

from .config import settings
from .database import AsyncSessionLocal
from .valuation import PEModel, PBModel, PEGModel, DividendYieldModel, DCFModel
from .filters.multi_factor import MultiFactorFilter
from .alerts.engine import AlertEngine

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("analyzer")

NOW = datetime.now(timezone.utc)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

async def get_tickers(session) -> list[str]:
    result = await session.execute(text("SELECT ticker FROM stocks"))
    return [r[0] for r in result.fetchall()]


async def get_latest_price(session, ticker: str) -> float | None:
    row = await session.execute(text(
        "SELECT close FROM price_history WHERE ticker = :t ORDER BY time DESC LIMIT 1"
    ), {"t": ticker})
    r = row.fetchone()
    return float(r[0]) if r else None


async def get_historical_pe(session, ticker: str, periods: int = 60) -> list[float]:
    rows = await session.execute(text("""
        SELECT ph.close / NULLIF(fs.eps, 0)
        FROM price_history ph
        JOIN financial_statements fs ON fs.ticker = ph.ticker
        WHERE ph.ticker = :t AND fs.eps > 0
        ORDER BY ph.time DESC LIMIT :n
    """), {"t": ticker, "n": periods})
    return [float(r[0]) for r in rows.fetchall() if r[0]]


async def save_valuation(session, ticker: str, model: str, result):
    if result is None:
        return
    await session.execute(text("""
        INSERT INTO valuation_scores
          (time, ticker, model, intrinsic_value, current_price, ratio, signal, metadata)
        VALUES (:time, :ticker, :model, :iv, :cp, :ratio, :signal, :metadata::jsonb)
        ON CONFLICT (time, ticker, model) DO UPDATE
          SET intrinsic_value = EXCLUDED.intrinsic_value,
              current_price   = EXCLUDED.current_price,
              ratio           = EXCLUDED.ratio,
              signal          = EXCLUDED.signal,
              metadata        = EXCLUDED.metadata
    """), {
        "time":    NOW.isoformat(),
        "ticker":  ticker,
        "model":   model,
        "iv":      getattr(result, "intrinsic_value", None),
        "cp":      result.current_price,
        "ratio":   result.ratio,
        "signal":  result.signal,
        "metadata": str(result.metadata),
    })


async def save_signal(session, signal_obj):
    await session.execute(text("""
        INSERT INTO multi_factor_signals
          (time, ticker, filter1_fundamental, filter2_institutional,
           filter3_valuation, filter4_technical, score, all_passed)
        VALUES (:time, :ticker, :f1, :f2, :f3, :f4, :score, :all_passed)
        ON CONFLICT (time, ticker) DO UPDATE
          SET filter1_fundamental   = EXCLUDED.filter1_fundamental,
              filter2_institutional = EXCLUDED.filter2_institutional,
              filter3_valuation     = EXCLUDED.filter3_valuation,
              filter4_technical     = EXCLUDED.filter4_technical,
              score                 = EXCLUDED.score,
              all_passed            = EXCLUDED.all_passed
    """), {
        "time":       NOW.isoformat(),
        "ticker":     signal_obj.ticker,
        "f1":         signal_obj.filter1_fundamental,
        "f2":         signal_obj.filter2_institutional,
        "f3":         signal_obj.filter3_valuation,
        "f4":         signal_obj.filter4_technical,
        "score":      signal_obj.score,
        "all_passed": signal_obj.all_passed,
    })


async def save_alerts(session, alerts: list):
    for alert in alerts:
        await session.execute(text("""
            INSERT INTO alerts (ticker, alert_type, severity, message, metadata)
            VALUES (:ticker, :alert_type, :severity, :message, :metadata::jsonb)
        """), {
            "ticker":     alert.ticker,
            "alert_type": alert.alert_type,
            "severity":   alert.severity,
            "message":    alert.message,
            "metadata":   str(alert.metadata),
        })


# ------------------------------------------------------------------
# Job: valuations
# ------------------------------------------------------------------

async def job_valuations():
    pe_model  = PEModel()
    pb_model  = PBModel()
    peg_model = PEGModel()
    div_model = DividendYieldModel()
    dcf_model = DCFModel()

    async with AsyncSessionLocal() as session:
        tickers = await get_tickers(session)

    processed = 0
    for ticker in tickers:
        async with AsyncSessionLocal() as session:
            price = await get_latest_price(session, ticker)
            if not price:
                continue

            # Get latest financials
            row = await session.execute(text("""
                SELECT eps, bvps, gross_profit_margin, free_cash_flow, shares_outstanding,
                       dividend_per_share
                FROM financial_statements
                WHERE ticker = :t ORDER BY year_quarter DESC LIMIT 1
            """), {"t": ticker})
            fin = row.fetchone()
            if not fin:
                continue

            eps, bvps, _, fcf, shares, div = fin
            hist_pe = await get_historical_pe(session, ticker, 60)

            # P/E
            if eps and eps > 0:
                r = pe_model.evaluate(ticker, price, float(eps), hist_pe)
                await save_valuation(session, ticker, "PE", r)

            # P/B
            if bvps and bvps > 0:
                # Build historical P/B from price / bvps history
                hist_pb = [p / float(bvps) for p in hist_pe if p] if hist_pe else []
                r = pb_model.evaluate(ticker, price, float(bvps), hist_pb)
                await save_valuation(session, ticker, "PB", r)

            # Dividend Yield (use last 5 recorded dividends)
            divs_row = await session.execute(text("""
                SELECT dividend_per_share FROM financial_statements
                WHERE ticker = :t AND dividend_per_share > 0
                ORDER BY year_quarter DESC LIMIT 5
            """), {"t": ticker})
            divs = [float(d[0]) for d in divs_row.fetchall()]
            if divs:
                r = div_model.evaluate(ticker, price, divs)
                await save_valuation(session, ticker, "DIV_YIELD", r)

            # DCF
            fcf_rows = await session.execute(text("""
                SELECT free_cash_flow FROM financial_statements
                WHERE ticker = :t AND free_cash_flow IS NOT NULL
                ORDER BY year_quarter DESC LIMIT 8
            """), {"t": ticker})
            fcf_list = [float(f[0]) for f in fcf_rows.fetchall()]
            if fcf_list and shares:
                macro_row = await session.execute(text("""
                    SELECT value FROM macro_indicators
                    WHERE indicator = 'UST_10Y'
                    ORDER BY time DESC LIMIT 1
                """))
                ust = macro_row.fetchone()
                rfr = float(ust[0]) / 100 if ust else 0.04
                r = dcf_model.evaluate(ticker, price, fcf_list, int(shares),
                                       risk_free_rate=rfr,
                                       margin_of_safety=settings.dcf_margin_of_safety)
                await save_valuation(session, ticker, "DCF", r)

            await session.commit()
            processed += 1

    logger.info("job_valuations complete: %d tickers processed", processed)


# ------------------------------------------------------------------
# Job: multi_factor
# ------------------------------------------------------------------

async def job_multi_factor():
    mf = MultiFactorFilter(
        revenue_yoy_min=settings.mf_revenue_yoy_min,
        trust_net_ratio_min=settings.mf_trust_net_ratio_min,
        pe_std_threshold=settings.mf_pe_std_threshold,
        volume_multiplier=settings.mf_volume_multiplier,
    )

    async with AsyncSessionLocal() as session:
        tickers = await get_tickers(session)

    passed = 0
    for ticker in tickers:
        async with AsyncSessionLocal() as session:
            # F1: Revenue YoY
            rev_rows = await session.execute(text("""
                SELECT revenue_yoy FROM monthly_revenue
                WHERE ticker = :t ORDER BY year_month DESC LIMIT 3
            """), {"t": ticker})
            yoy_3m = [float(r[0]) for r in rev_rows.fetchall() if r[0] is not None]

            # F2: Trust net & major holders
            inst_rows = await session.execute(text("""
                SELECT trust_net FROM institutional_flows
                WHERE ticker = :t ORDER BY time DESC LIMIT 5
            """), {"t": ticker})
            trust_5d = [int(r[0]) for r in inst_rows.fetchall() if r[0] is not None]

            shares_row = await session.execute(text("""
                SELECT shares_outstanding FROM financial_statements
                WHERE ticker = :t ORDER BY year_quarter DESC LIMIT 1
            """), {"t": ticker})
            shares_r = shares_row.fetchone()
            shares = int(shares_r[0]) if shares_r and shares_r[0] else 0

            major_rows = await session.execute(text("""
                SELECT holders_1000_ratio FROM major_holders
                WHERE ticker = :t ORDER BY time DESC LIMIT 3
            """), {"t": ticker})
            major_trend = [float(r[0]) for r in major_rows.fetchall() if r[0] is not None]

            # F3: P/E
            price = await get_latest_price(session, ticker)
            hist_pe = await get_historical_pe(session, ticker, 60)
            pe_row = await session.execute(text("""
                SELECT eps FROM financial_statements
                WHERE ticker = :t AND eps > 0 ORDER BY year_quarter DESC LIMIT 1
            """), {"t": ticker})
            pe_fin = pe_row.fetchone()
            current_pe = (price / float(pe_fin[0])) if price and pe_fin else None

            # F4: Technical
            price_rows = await session.execute(text("""
                SELECT close FROM price_history
                WHERE ticker = :t ORDER BY time DESC LIMIT 21
            """), {"t": ticker})
            closes = [float(r[0]) for r in price_rows.fetchall()]
            closes.reverse()

            vol_rows = await session.execute(text("""
                SELECT volume FROM price_history
                WHERE ticker = :t ORDER BY time DESC LIMIT 6
            """), {"t": ticker})
            vols = [int(r[0]) for r in vol_rows.fetchall() if r[0] is not None]
            vols.reverse()

            result = mf.evaluate(
                ticker=ticker,
                revenue_yoy_3m=yoy_3m,
                trust_net_5d=trust_5d,
                shares_outstanding_k=shares,
                major_holders_trend=major_trend,
                retail_holders_trend=[],   # optional
                current_pe=current_pe,
                historical_pe=hist_pe,
                close_prices_21d=closes,
                volumes_6d=vols,
            )
            await save_signal(session, result)
            await session.commit()

            if result.all_passed:
                passed += 1
                logger.info("SIGNAL [%s] score=%d ALL PASSED", ticker, result.score)

    logger.info("job_multi_factor complete: %d / %d tickers passed all filters", passed, len(tickers))


# ------------------------------------------------------------------
# Job: alerts
# ------------------------------------------------------------------

async def job_alerts():
    engine_obj = AlertEngine()
    async with AsyncSessionLocal() as session:
        # Macro data
        macro_rows = await session.execute(text("""
            SELECT indicator, value FROM macro_indicators
            WHERE time = (SELECT MAX(time) FROM macro_indicators WHERE indicator = macro_indicators.indicator)
        """))
        macro = {r[0]: float(r[1]) for r in macro_rows.fetchall()}

        usd_rows = await session.execute(text("""
            SELECT value FROM macro_indicators
            WHERE indicator = 'USDTWD' ORDER BY time DESC LIMIT 5
        """))
        usdtwd_5d = [float(r[0]) for r in usd_rows.fetchall()]

        futures_row = await session.execute(text("""
            SELECT foreign_net FROM futures_positions ORDER BY time DESC LIMIT 1
        """))
        futures_net = futures_row.fetchone()
        futures_net_short = -int(futures_net[0]) if futures_net and futures_net[0] < 0 else 0

        macro_alerts = engine_obj.check_macro(
            ust_10y=macro.get("UST_10Y", 0),
            usdtwd=macro.get("USDTWD", 30.0),
            usdtwd_5d=usdtwd_5d,
            futures_net_short=futures_net_short,
        )
        await save_alerts(session, macro_alerts)
        await session.commit()

    logger.info("job_alerts complete: %d macro alerts", len(macro_alerts))


JOBS = {
    "valuations":   job_valuations,
    "multi_factor": job_multi_factor,
    "alerts":       job_alerts,
}


def main():
    parser = argparse.ArgumentParser(description="TWStock analyzer")
    parser.add_argument("--job", required=True, choices=list(JOBS.keys()))
    args = parser.parse_args()

    logger.info("Starting job: %s", args.job)
    asyncio.run(JOBS[args.job]())
    logger.info("Job %s finished", args.job)


if __name__ == "__main__":
    main()
