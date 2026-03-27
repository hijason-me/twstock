"""
多因子篩選策略 (Multi-Factor Filter Pipeline)

四道篩選：
  Filter 1 (基本面)   : 近3個月營收 YoY > 15%
  Filter 2 (籌碼面)   : 近5日投信買超佔股本比例 > 0.5%，且千張大戶持股增加
  Filter 3 (估值面)   : 目前 Forward P/E 落在歷史均值 ±1σ 以內
  Filter 4 (技術面)   : 收盤突破近20日高點，且成交量 > 5日均量 × 2

全部通過 → all_passed = True → 列入高勝率波段突破候選名單
"""
from dataclasses import dataclass, field


@dataclass
class MultiFactorResult:
    ticker: str
    filter1_fundamental: bool = False
    filter2_institutional: bool = False
    filter3_valuation: bool = False
    filter4_technical: bool = False
    score: int = 0
    all_passed: bool = False
    details: dict = field(default_factory=dict)


class MultiFactorFilter:
    def __init__(
        self,
        revenue_yoy_min: float = 15.0,
        trust_net_ratio_min: float = 0.5,
        pe_std_threshold: float = 1.0,
        volume_multiplier: float = 2.0,
    ):
        self.revenue_yoy_min     = revenue_yoy_min
        self.trust_net_ratio_min = trust_net_ratio_min
        self.pe_std_threshold    = pe_std_threshold
        self.volume_multiplier   = volume_multiplier

    def evaluate(
        self,
        ticker: str,
        # Filter 1 inputs
        revenue_yoy_3m: list[float],      # 最近3個月 YoY (%)
        # Filter 2 inputs
        trust_net_5d: list[int],           # 近5日投信買賣超 (張)
        shares_outstanding_k: int,         # 股本 (千股)
        major_holders_trend: list[float],  # 近3週千張大戶持股比例
        # Filter 3 inputs
        current_pe: float | None,
        historical_pe: list[float],
        # Filter 4 inputs
        close_prices_21d: list[float],    # 近21日收盤 [最舊...最新]
        volumes_6d: list[int],             # 近6日成交量 [最舊...最新]
    ) -> MultiFactorResult:
        result = MultiFactorResult(ticker=ticker)

        # ── Filter 1: Fundamental ──────────────────────────────
        if revenue_yoy_3m:
            avg_yoy = sum(revenue_yoy_3m) / len(revenue_yoy_3m)
            f1      = avg_yoy > self.revenue_yoy_min
            result.filter1_fundamental = f1
            result.details["f1_avg_yoy"] = round(avg_yoy, 2)

        # ── Filter 2: Institutional ────────────────────────────
        if trust_net_5d and shares_outstanding_k > 0:
            total_trust_5d = sum(trust_net_5d)
            # 換算佔股本比例 (%)  — trust in 張, shares in 千股 → 千張
            trust_ratio = total_trust_5d / (shares_outstanding_k / 1000) * 100
            major_up    = (
                len(major_holders_trend) >= 2
                and major_holders_trend[-1] > major_holders_trend[-2]
            )
            f2 = trust_ratio > self.trust_net_ratio_min and major_up
            result.filter2_institutional = f2
            result.details["f2_trust_ratio_pct"] = round(trust_ratio, 4)
            result.details["f2_major_holders_up"] = major_up

        # ── Filter 3: Valuation ────────────────────────────────
        if current_pe and historical_pe and len(historical_pe) >= 8:
            import numpy as np
            arr = [x for x in historical_pe if x > 0]
            if len(arr) >= 8:
                mu, sig = float(np.mean(arr)), float(np.std(arr))
                f3 = (mu - self.pe_std_threshold * sig) <= current_pe <= (mu + self.pe_std_threshold * sig)
                result.filter3_valuation = f3
                result.details["f3_current_pe"] = round(current_pe, 2)
                result.details["f3_pe_band"]    = [round(mu - sig, 2), round(mu + sig, 2)]

        # ── Filter 4: Technical ────────────────────────────────
        if len(close_prices_21d) >= 21 and len(volumes_6d) >= 6:
            today_close    = close_prices_21d[-1]
            high_20d       = max(close_prices_21d[-21:-1])   # 前20日高點
            today_volume   = volumes_6d[-1]
            avg_volume_5d  = sum(volumes_6d[-6:-1]) / 5
            breakout = today_close > high_20d
            vol_surge = avg_volume_5d > 0 and today_volume > avg_volume_5d * self.volume_multiplier
            f4 = breakout and vol_surge
            result.filter4_technical = f4
            result.details["f4_breakout"]  = breakout
            result.details["f4_vol_surge"] = vol_surge

        # ── Aggregate ──────────────────────────────────────────
        flags = [
            result.filter1_fundamental,
            result.filter2_institutional,
            result.filter3_valuation,
            result.filter4_technical,
        ]
        result.score      = sum(flags)
        result.all_passed = all(flags)

        return result
