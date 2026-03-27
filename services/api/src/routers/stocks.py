from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..schemas import StockBase, PriceRecord, InstitutionalFlow, FuturesPosition

router = APIRouter(prefix="/stocks", tags=["stocks"])


@router.get("/", response_model=list[StockBase])
async def list_stocks(
    market: str | None = Query(None, description="TWSE or TPEx"),
    db: AsyncSession = Depends(get_db),
):
    sql = "SELECT ticker, name, market, industry FROM stocks"
    params = {}
    if market:
        sql += " WHERE market = :market"
        params["market"] = market
    sql += " ORDER BY ticker"
    result = await db.execute(text(sql), params)
    return [StockBase(**dict(zip(["ticker","name","market","industry"], r)))
            for r in result.fetchall()]


@router.get("/{ticker}", response_model=StockBase)
async def get_stock(ticker: str, db: AsyncSession = Depends(get_db)):
    row = await db.execute(
        text("SELECT ticker, name, market, industry FROM stocks WHERE ticker = :t"),
        {"t": ticker},
    )
    r = row.fetchone()
    if not r:
        raise HTTPException(status_code=404, detail=f"Stock {ticker} not found")
    return StockBase(**dict(zip(["ticker","name","market","industry"], r)))


@router.get("/{ticker}/prices", response_model=list[PriceRecord])
async def get_prices(
    ticker: str,
    limit: int = Query(60, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    rows = await db.execute(text("""
        SELECT time, ticker, open, high, low, close, volume
        FROM price_history
        WHERE ticker = :t
        ORDER BY time DESC LIMIT :n
    """), {"t": ticker, "n": limit})
    return [PriceRecord(**dict(zip(
        ["time","ticker","open","high","low","close","volume"], r
    ))) for r in rows.fetchall()]


@router.get("/{ticker}/institutional", response_model=list[InstitutionalFlow])
async def get_institutional(
    ticker: str,
    limit: int = Query(20, ge=1, le=120),
    db: AsyncSession = Depends(get_db),
):
    rows = await db.execute(text("""
        SELECT time, ticker, foreign_net, trust_net, dealer_net, total_net
        FROM institutional_flows
        WHERE ticker = :t
        ORDER BY time DESC LIMIT :n
    """), {"t": ticker, "n": limit})
    return [InstitutionalFlow(**dict(zip(
        ["time","ticker","foreign_net","trust_net","dealer_net","total_net"], r
    ))) for r in rows.fetchall()]


@router.get("/market/futures", response_model=list[FuturesPosition])
async def get_futures_positions(
    limit: int = Query(20, ge=1, le=60),
    db: AsyncSession = Depends(get_db),
):
    rows = await db.execute(text("""
        SELECT time, foreign_long, foreign_short, foreign_net,
               dealer_long, dealer_short, dealer_net
        FROM futures_positions ORDER BY time DESC LIMIT :n
    """), {"n": limit})
    return [FuturesPosition(**dict(zip(
        ["time","foreign_long","foreign_short","foreign_net",
         "dealer_long","dealer_short","dealer_net"], r
    ))) for r in rows.fetchall()]
