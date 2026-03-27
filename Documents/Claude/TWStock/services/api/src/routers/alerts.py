from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..schemas import AlertRecord, AlertAcknowledge

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("/", response_model=list[AlertRecord])
async def list_alerts(
    ticker: str | None = Query(None),
    severity: str | None = Query(None, description="INFO | WARNING | CRITICAL"),
    unacknowledged_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    sql = """
        SELECT id, created_at, ticker, alert_type, severity, message, metadata, acknowledged
        FROM alerts WHERE 1=1
    """
    params: dict = {"n": limit}
    if ticker:
        sql += " AND ticker = :ticker"
        params["ticker"] = ticker
    if severity:
        sql += " AND severity = :severity"
        params["severity"] = severity
    if unacknowledged_only:
        sql += " AND acknowledged = FALSE"
    sql += " ORDER BY created_at DESC LIMIT :n"

    rows = await db.execute(text(sql), params)
    return [AlertRecord(**dict(zip(
        ["id","created_at","ticker","alert_type","severity","message","metadata","acknowledged"], r
    ))) for r in rows.fetchall()]


@router.patch("/{alert_id}", response_model=AlertRecord)
async def acknowledge_alert(
    alert_id: int,
    body: AlertAcknowledge,
    db: AsyncSession = Depends(get_db),
):
    await db.execute(text("""
        UPDATE alerts SET acknowledged = :ack WHERE id = :id
    """), {"ack": body.acknowledged, "id": alert_id})
    await db.commit()
    row = await db.execute(text("""
        SELECT id, created_at, ticker, alert_type, severity, message, metadata, acknowledged
        FROM alerts WHERE id = :id
    """), {"id": alert_id})
    r = row.fetchone()
    return AlertRecord(**dict(zip(
        ["id","created_at","ticker","alert_type","severity","message","metadata","acknowledged"], r
    )))
