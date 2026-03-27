"""
股價淨值比 (P/B) 估值模型。

適用：景氣循環股、金融股、資產股。
判定：
  - 低估 : current_pb < historical 下四分位數 (Q1)
  - 合理 : Q1 ≤ current_pb ≤ Q3
  - 高估 : current_pb > Q3
"""
from dataclasses import dataclass

import numpy as np


@dataclass
class PBResult:
    ticker: str
    current_pb: float
    q1_pb: float
    median_pb: float
    q3_pb: float
    intrinsic_value: float   # median P/B × BVPS
    current_price: float
    ratio: float
    signal: str
    metadata: dict


class PBModel:
    def evaluate(
        self,
        ticker: str,
        current_price: float,
        bvps: float,              # 每股淨值
        historical_pb: list[float],
    ) -> PBResult | None:
        if bvps <= 0 or len(historical_pb) < 4:
            return None

        current_pb = current_price / bvps
        arr = np.array(historical_pb, dtype=float)
        arr = arr[arr > 0]
        if len(arr) < 4:
            return None

        q1     = float(np.percentile(arr, 25))
        median = float(np.median(arr))
        q3     = float(np.percentile(arr, 75))

        intrinsic = median * bvps
        ratio     = current_price / intrinsic if intrinsic > 0 else None

        if current_pb < q1:
            signal = "UNDERVALUED"
        elif current_pb > q3:
            signal = "OVERVALUED"
        else:
            signal = "FAIR"

        return PBResult(
            ticker=ticker,
            current_pb=round(current_pb, 2),
            q1_pb=round(q1, 2),
            median_pb=round(median, 2),
            q3_pb=round(q3, 2),
            intrinsic_value=round(intrinsic, 2),
            current_price=round(current_price, 2),
            ratio=round(ratio, 4) if ratio else None,
            signal=signal,
            metadata={"bvps": round(bvps, 2)},
        )
