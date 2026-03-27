"""
本益比 (P/E) 河流圖估值模型。

方法：
  1. 計算歷史 P/E 序列（每日收盤價 / 滾動 TTM EPS）。
  2. 計算歷史均值 (μ) 與標準差 (σ)。
  3. 以目前 Forward P/E 對照歷史區間判定估值。

判定邏輯：
  - 低估 (UNDERVALUED) : current_pe < μ - 1σ
  - 合理 (FAIR)         : μ - 1σ ≤ current_pe ≤ μ + 1σ
  - 高估 (OVERVALUED)  : current_pe > μ + 1σ
"""
from dataclasses import dataclass

import numpy as np


@dataclass
class PEResult:
    ticker: str
    current_pe: float
    mean_pe: float
    std_pe: float
    intrinsic_value: float   # 均值 P/E × Forward EPS
    current_price: float
    ratio: float             # current_price / intrinsic_value
    signal: str
    metadata: dict


class PEModel:
    def evaluate(
        self,
        ticker: str,
        current_price: float,
        forward_eps: float,
        historical_pe: list[float],
    ) -> PEResult | None:
        if forward_eps <= 0 or len(historical_pe) < 8:
            return None

        current_pe = current_price / forward_eps
        arr = np.array(historical_pe, dtype=float)
        arr = arr[arr > 0]  # remove invalid
        if len(arr) < 8:
            return None

        mu  = float(np.mean(arr))
        sig = float(np.std(arr))

        intrinsic = mu * forward_eps
        ratio     = current_price / intrinsic if intrinsic > 0 else None

        if current_pe < mu - sig:
            signal = "UNDERVALUED"
        elif current_pe > mu + sig:
            signal = "OVERVALUED"
        else:
            signal = "FAIR"

        return PEResult(
            ticker=ticker,
            current_pe=round(current_pe, 2),
            mean_pe=round(mu, 2),
            std_pe=round(sig, 2),
            intrinsic_value=round(intrinsic, 2),
            current_price=round(current_price, 2),
            ratio=round(ratio, 4) if ratio else None,
            signal=signal,
            metadata={
                "lower_band": round(mu - sig, 2),
                "upper_band": round(mu + sig, 2),
                "forward_eps": round(forward_eps, 4),
            },
        )
