"""
現金流折現 (DCF) 估值模型。

公式：
  DCF = Σ [ CFt / (1+r)^t ] + TV / (1+r)^n

  Terminal Value (Gordon Growth): TV = CF_n × (1 + g) / (r - g)

適用：具備穩定自由現金流的公用事業、基礎設施。

注意：
  - 折現率 r = 無風險利率(美債10y) + 風險溢酬(預設 5%)
  - 結果乘以安全邊際係數 (預設 0.80) 作為買入價
"""
from dataclasses import dataclass


@dataclass
class DCFResult:
    ticker: str
    dcf_value: float           # 不含安全邊際的內在價值
    intrinsic_value: float     # × 安全邊際
    current_price: float
    ratio: float
    signal: str
    metadata: dict


class DCFModel:
    DEFAULT_RISK_PREMIUM = 0.05   # 5% equity risk premium
    DEFAULT_MARGIN_OF_SAFETY = 0.80

    def evaluate(
        self,
        ticker: str,
        current_price: float,
        free_cash_flows: list[float],  # 歷史 FCF (最近→最舊)，單位千元
        shares_outstanding: int,       # 流通股數 (千股)
        risk_free_rate: float = 0.04,  # 美債10y (小數)
        terminal_growth_rate: float = 0.025,
        margin_of_safety: float = DEFAULT_MARGIN_OF_SAFETY,
    ) -> DCFResult | None:
        if not free_cash_flows or shares_outstanding <= 0:
            return None
        if len(free_cash_flows) < 3:
            return None

        discount_rate = risk_free_rate + self.DEFAULT_RISK_PREMIUM
        if discount_rate <= terminal_growth_rate:
            discount_rate = terminal_growth_rate + 0.01  # safety guard

        # Project FCF using avg growth of historical data
        import numpy as np
        fcf_arr   = [f for f in free_cash_flows if f and f != 0]
        if len(fcf_arr) < 3:
            return None

        # Average FCF (千元)
        base_fcf  = float(np.mean(fcf_arr[-4:]))  # use last 4 periods avg

        # Project 10 years
        growth_rate = 0.08  # conservative 8% near-term growth
        projected = [base_fcf * ((1 + growth_rate) ** t) for t in range(1, 11)]

        # Terminal value (Gordon Growth at year 10)
        terminal_value = projected[-1] * (1 + terminal_growth_rate) / (
            discount_rate - terminal_growth_rate
        )

        # Discount back
        pv_fcfs = sum(
            cf / ((1 + discount_rate) ** t)
            for t, cf in enumerate(projected, 1)
        )
        pv_tv  = terminal_value / ((1 + discount_rate) ** 10)
        total_dcf_value = pv_fcfs + pv_tv   # 千元

        # Per share value (shares in 千股, FCF in 千元 → per share in 元)
        dcf_per_share = total_dcf_value / shares_outstanding * 1000  # → 元/股
        intrinsic     = dcf_per_share * margin_of_safety
        ratio         = current_price / intrinsic if intrinsic > 0 else None

        if ratio is None:
            signal = "N/A"
        elif ratio < 1.0:
            signal = "UNDERVALUED"
        elif ratio < 1.2:
            signal = "FAIR"
        else:
            signal = "OVERVALUED"

        return DCFResult(
            ticker=ticker,
            dcf_value=round(dcf_per_share, 2),
            intrinsic_value=round(intrinsic, 2),
            current_price=round(current_price, 2),
            ratio=round(ratio, 4) if ratio else None,
            signal=signal,
            metadata={
                "discount_rate":       round(discount_rate, 4),
                "terminal_growth_rate": terminal_growth_rate,
                "margin_of_safety":    margin_of_safety,
                "base_fcf_thousand":   round(base_fcf, 0),
            },
        )
