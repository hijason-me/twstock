from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..schemas import ValuationScore, MultiFactorSignal, MacroIndicator

router = APIRouter(prefix="/analysis", tags=["analysis"])


@router.get("/valuations/{ticker}", response_model=list[ValuationScore])
async def get_valuations(
    ticker: str,
    model: str | None = Query(None, description="PE | PB | PEG | DIV_YIELD | DCF"),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT time, ticker, model, intrinsic_value, current_price, ratio, signal, metadata
        FROM valuation_scores
        WHERE ticker = :t
    """
    params = {"t": ticker}
    if model:
        sql += " AND model = :model"
        params["model"] = model
    sql += " ORDER BY time DESC LIMIT 20"
    rows = await db.execute(text(sql), params)
    return [ValuationScore(**dict(zip(
        ["time","ticker","model","intrinsic_value","current_price","ratio","signal","metadata"], r
    ))) for r in rows.fetchall()]


@router.get("/signals", response_model=list[MultiFactorSignal])
async def list_signals(
    all_passed: bool | None = Query(None),
    min_score: int = Query(0, ge=0, le=4),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """List latest multi-factor signals, optionally filtered by pass status."""
    sql = """
        SELECT DISTINCT ON (ticker) time, ticker,
               filter1_fundamental, filter2_institutional,
               filter3_valuation, filter4_technical, score, all_passed
        FROM multi_factor_signals
        WHERE score >= :min_score
    """
    params: dict = {"min_score": min_score, "n": limit}
    if all_passed is not None:
        sql += " AND all_passed = :all_passed"
        params["all_passed"] = all_passed
    sql += " ORDER BY ticker, time DESC LIMIT :n"
    rows = await db.execute(text(sql), params)
    return [MultiFactorSignal(**dict(zip(
        ["time","ticker","f1","f2","f3","f4","score","all_passed"], r
    ))) for r in rows.fetchall()]


@router.get("/macro", response_model=list[MacroIndicator])
async def get_macro(
    indicator: str | None = Query(None),
    limit: int = Query(30, ge=1, le=120),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT time, indicator, value, source
        FROM macro_indicators
    """
    params: dict = {"n": limit}
    if indicator:
        sql += " WHERE indicator = :indicator"
        params["indicator"] = indicator
    sql += " ORDER BY time DESC LIMIT :n"
    rows = await db.execute(text(sql), params)
    return [MacroIndicator(**dict(zip(["time","indicator","value","source"], r)))
            for r in rows.fetchall()]
