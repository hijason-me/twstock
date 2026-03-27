"""
殖利率反推合理股價模型。

公式：合理價 = 近5年平均配息 ÷ 期望殖利率

適用：防禦型定存股、高股息 ETF 成分股、電信網通股。

判定：
  - 現價 ≤ 合理價          → 買進區間 (UNDERVALUED)
  - 合理價 < 現價 ≤ 偏貴價 → 合理 (FAIR)
  - 現價 > 偏貴價           → 過熱 (OVERVALUED)

偏貴價 = 近5年平均配息 ÷ (期望殖利率 × 0.8)  [殖利率壓低 20%]
"""
from dataclasses import dataclass


@dataclass
class DividendYieldResult:
    ticker: str
    avg_dividend: float
    target_yield: float     # %
    intrinsic_value: float  # avg_dividend / target_yield
    rich_value: float       # avg_dividend / (target_yield * 0.8)
    current_price: float
    current_yield: float    # %
    ratio: float
    signal: str
    metadata: dict


class DividendYieldModel:
    def evaluate(
        self,
        ticker: str,
        current_price: float,
        dividends_5y: list[float],   # 近5年每股配息
        target_yield_pct: float = 5.0,
    ) -> DividendYieldResult | None:
        if not dividends_5y or current_price <= 0:
            return None

        avg_div       = sum(dividends_5y) / len(dividends_5y)
        target_rate   = target_yield_pct / 100.0
        intrinsic     = avg_div / target_rate if target_rate > 0 else None
        if intrinsic is None:
            return None

        rich_val      = avg_div / (target_rate * 0.8)
        current_yield = (avg_div / current_price) * 100
        ratio         = current_price / intrinsic

        if current_price <= intrinsic:
            signal = "UNDERVALUED"
        elif current_price <= rich_val:
            signal = "FAIR"
        else:
            signal = "OVERVALUED"

        return DividendYieldResult(
            ticker=ticker,
            avg_dividend=round(avg_div, 4),
            target_yield=target_yield_pct,
            intrinsic_value=round(intrinsic, 2),
            rich_value=round(rich_val, 2),
            current_price=round(current_price, 2),
            current_yield=round(current_yield, 2),
            ratio=round(ratio, 4),
            signal=signal,
            metadata={"dividends_used": dividends_5y},
        )
