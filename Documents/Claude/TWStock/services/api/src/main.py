import logging

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import settings
from .routers import stocks, analysis, alerts

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

app = FastAPI(
    title="TWStock Analyzer API",
    description="台股量化分析系統 REST API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(stocks.router)
app.include_router(analysis.router)
app.include_router(alerts.router)


@app.get("/healthz", tags=["ops"])
async def health():
    return {"status": "ok"}


@app.get("/readyz", tags=["ops"])
async def ready():
    """Check DB and Redis connectivity."""
    from .database import engine
    from sqlalchemy import text
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as e:
        return {"status": "db_error", "detail": str(e)}, 503

    try:
        r = aioredis.from_url(settings.redis_url)
        await r.ping()
        await r.aclose()
    except Exception as e:
        return {"status": "redis_error", "detail": str(e)}, 503

    return {"status": "ok"}
