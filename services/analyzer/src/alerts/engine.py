"""
告警引擎。根據觀測指標觸發 INFO / WARNING / CRITICAL 告警，
並寫入 alerts 資料表。
"""
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Alert:
    ticker: str | None
    alert_type: str
    severity: str        # INFO | WARNING | CRITICAL
    message: str
    metadata: dict


class AlertEngine:
    """Rules-based alert evaluator."""

    # ── 總經告警 ────────────────────────────────────────────
    def check_macro(
        self,
        ust_10y: float,
        usdtwd: float,
        usdtwd_5d: list[float],  # 近5日台幣匯率
        futures_net_short: int,  # 外資期貨淨空單口數 (正值 = 空單)
    ) -> list[Alert]:
        alerts = []

        # 美債殖利率突破 4.5%
        if ust_10y >= 4.5:
            alerts.append(Alert(
                ticker=None,
                alert_type="MACRO_UST10Y_BREAKOUT",
                severity="WARNING",
                message=f"美債10年期殖利率達 {ust_10y:.2f}%，超過 4.5% 警戒線，高成長股估值面臨下修壓力。",
                metadata={"ust_10y": ust_10y},
            ))

        # 台幣連續急貶 (近5日累計貶值 > 1.5%)
        if len(usdtwd_5d) >= 5:
            chg_pct = (usdtwd - usdtwd_5d[0]) / usdtwd_5d[0] * 100
            if chg_pct > 1.5:
                alerts.append(Alert(
                    ticker=None,
                    alert_type="MACRO_TWD_DEPRECIATION",
                    severity="WARNING",
                    message=f"台幣近5日對美元貶值 {chg_pct:.2f}%，外資提款風險升高。",
                    metadata={"chg_pct": round(chg_pct, 2)},
                ))

        # 外資期貨淨空單 > 30,000 口
        if futures_net_short > 30_000:
            alerts.append(Alert(
                ticker=None,
                alert_type="FUTURES_LARGE_SHORT",
                severity="CRITICAL",
                message=f"外資台指期淨空單達 {futures_net_short:,} 口，觸發系統性做空訊號。",
                metadata={"futures_net_short": futures_net_short},
            ))

        return alerts

    # ── 個股籌碼告警 ────────────────────────────────────────
    def check_stock_institutional(
        self,
        ticker: str,
        trust_consecutive_buy_days: int,
        trust_volume_ratio: float,  # 投信買超佔當日成交量 %
        major_holders_trend: list[float],
        retail_holders_trend: list[float],
    ) -> list[Alert]:
        alerts = []

        # 投信連買3日且佔成交量 > 10%
        if trust_consecutive_buy_days >= 3 and trust_volume_ratio > 10.0:
            alerts.append(Alert(
                ticker=ticker,
                alert_type="INSTITUTIONAL_TRUST_BUYING",
                severity="INFO",
                message=f"{ticker} 投信連續 {trust_consecutive_buy_days} 日買超，佔成交量 {trust_volume_ratio:.1f}%，疑似投信作帳行情。",
                metadata={
                    "trust_consecutive_days": trust_consecutive_buy_days,
                    "trust_volume_ratio":     trust_volume_ratio,
                },
            ))

        # 籌碼集中：大戶增持 + 散戶減持 連續3週
        if (len(major_holders_trend) >= 3
                and len(retail_holders_trend) >= 3
                and all(major_holders_trend[i] > major_holders_trend[i-1]
                        for i in range(1, min(3, len(major_holders_trend))))
                and all(retail_holders_trend[i] < retail_holders_trend[i-1]
                        for i in range(1, min(3, len(retail_holders_trend))))):
            alerts.append(Alert(
                ticker=ticker,
                alert_type="CHIP_CONCENTRATION",
                severity="INFO",
                message=f"{ticker} 籌碼集中訊號：大戶連3週增持，散戶持股下降。",
                metadata={
                    "major_holders_latest": major_holders_trend[-1],
                    "retail_latest":        retail_holders_trend[-1],
                },
            ))

        return alerts

    # ── 個股技術面告警 ────────────────────────────────────
    def check_technical_divergence(
        self,
        ticker: str,
        price_new_high: bool,
        macd_new_high: bool,
        price_high: float,
        macd_value: float,
    ) -> list[Alert]:
        alerts = []
        if price_new_high and not macd_new_high:
            alerts.append(Alert(
                ticker=ticker,
                alert_type="TECHNICAL_DIVERGENCE",
                severity="WARNING",
                message=f"{ticker} 技術背離警告：股價創新高但 MACD ({macd_value:.4f}) 未創新高。",
                metadata={"price_high": price_high, "macd_value": macd_value},
            ))
        return alerts

    # ── 個股基本面告警 ────────────────────────────────────
    def check_fundamental(
        self,
        ticker: str,
        revenue_yoy: float,
        was_negative: bool,   # 上個月 YoY 為負
        is_all_time_high: bool,
        gross_margin_trend: list[float],  # 近4季毛利率
    ) -> list[Alert]:
        alerts = []

        # 月營收 YoY 由負轉正
        if was_negative and revenue_yoy > 0:
            alerts.append(Alert(
                ticker=ticker,
                alert_type="REVENUE_TURNAROUND",
                severity="INFO",
                message=f"{ticker} 月營收 YoY 由負轉正 ({revenue_yoy:+.1f}%)，基本面觸底訊號。",
                metadata={"revenue_yoy": revenue_yoy},
            ))

        # 月營收創歷史新高
        if is_all_time_high:
            alerts.append(Alert(
                ticker=ticker,
                alert_type="REVENUE_ALL_TIME_HIGH",
                severity="INFO",
                message=f"{ticker} 月營收創歷史新高，營運動能強勁。",
                metadata={},
            ))

        # 毛利率連2季成長
        if len(gross_margin_trend) >= 2:
            if all(gross_margin_trend[i] > gross_margin_trend[i-1]
                   for i in range(1, min(2, len(gross_margin_trend)))):
                alerts.append(Alert(
                    ticker=ticker,
                    alert_type="GROSS_MARGIN_RISING",
                    severity="INFO",
                    message=f"{ticker} 毛利率連2季成長，產品結構改善或漲價成功。",
                    metadata={"gross_margin_latest": gross_margin_trend[-1]},
                ))

        return alerts
