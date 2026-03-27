"""
本益成長比 (PEG) 估值模型。

公式：PEG = P/E ÷ EPS成長率(%)

適用：高速成長中的中小型股、IP 矽智財、AI 伺服器概念股。

判定：
  - PEG < 1.0  → 嚴重低估 (UNDERVALUED)
  - 1.0 ~ 1.5  → 合理 (FAIR)
  - > 2.0      → 透支未來 (OVERVALUED)
"""
from dataclasses import dataclass


@dataclass
class PEGResult:
    ticker: str
    pe_ratio: float
    growth_rate: float   # %
    peg: float
    signal: str
    metadata: dict


class PEGModel:
    PEG_UNDERVALUED = 1.0
    PEG_FAIR_MAX    = 1.5
    PEG_OVERVALUED  = 2.0

    def evaluate(
        self,
        ticker: str,
        current_price: float,
        forward_eps: float,
        eps_growth_rate: float,  # 預估 EPS YoY 成長率 (%)
    ) -> PEGResult | None:
        if forward_eps <= 0 or eps_growth_rate <= 0:
            return None

        pe  = current_price / forward_eps
        peg = pe / eps_growth_rate

        if peg < self.PEG_UNDERVALUED:
            signal = "UNDERVALUED"
        elif peg > self.PEG_OVERVALUED:
            signal = "OVERVALUED"
        elif peg <= self.PEG_FAIR_MAX:
            signal = "FAIR"
        else:
            signal = "OVERVALUED"

        return PEGResult(
            ticker=ticker,
            pe_ratio=round(pe, 2),
            growth_rate=round(eps_growth_rate, 2),
            peg=round(peg, 4),
            signal=signal,
            metadata={
                "current_price": current_price,
                "forward_eps":   forward_eps,
            },
        )
