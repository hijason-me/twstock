from datetime import datetime
from typing import Any
from pydantic import BaseModel


class StockBase(BaseModel):
    ticker: str
    name: str
    market: str
    industry: str | None = None


class PriceRecord(BaseModel):
    time: datetime
    ticker: str
    open: float | None
    high: float | None
    low: float | None
    close: float
    volume: int | None


class InstitutionalFlow(BaseModel):
    time: datetime
    ticker: str
    foreign_net: int | None
    trust_net: int | None
    dealer_net: int | None
    total_net: int | None


class MacroIndicator(BaseModel):
    time: datetime
    indicator: str
    value: float
    source: str | None


class ValuationScore(BaseModel):
    time: datetime
    ticker: str
    model: str
    intrinsic_value: float | None
    current_price: float | None
    ratio: float | None
    signal: str
    metadata: dict[str, Any] | None


class MultiFactorSignal(BaseModel):
    time: datetime
    ticker: str
    filter1_fundamental: bool
    filter2_institutional: bool
    filter3_valuation: bool
    filter4_technical: bool
    score: int
    all_passed: bool


class AlertRecord(BaseModel):
    id: int
    created_at: datetime
    ticker: str | None
    alert_type: str
    severity: str
    message: str
    metadata: dict[str, Any] | None
    acknowledged: bool


class AlertAcknowledge(BaseModel):
    acknowledged: bool = True


class FuturesPosition(BaseModel):
    time: datetime
    foreign_long: int | None
    foreign_short: int | None
    foreign_net: int | None
    dealer_long: int | None
    dealer_short: int | None
    dealer_net: int | None
