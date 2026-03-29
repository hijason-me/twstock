-- ============================================================
-- TWStock Database Schema  v2
-- Requires: PostgreSQL 14+ with TimescaleDB extension
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================
-- 1. 股票主檔
-- ============================================================
CREATE TABLE IF NOT EXISTS stocks (
    ticker      VARCHAR(10)  PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    market      VARCHAR(10)  NOT NULL CHECK (market IN ('TWSE', 'TPEx')),
    industry    VARCHAR(50),
    created_at  TIMESTAMPTZ  DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  DEFAULT NOW()
);

-- ============================================================
-- 2. 日收盤 OHLCV  (hypertable)
-- ============================================================
CREATE TABLE IF NOT EXISTS price_history (
    time    TIMESTAMPTZ   NOT NULL,
    ticker  VARCHAR(10)   NOT NULL,
    open    NUMERIC(12,2),
    high    NUMERIC(12,2),
    low     NUMERIC(12,2),
    close   NUMERIC(12,2) NOT NULL,
    volume  BIGINT,                   -- 單位：張
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);
SELECT create_hypertable('price_history', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS ux_price_history ON price_history (time DESC, ticker);

-- ============================================================
-- 3. 三大法人買賣超  (hypertable)
-- ============================================================
CREATE TABLE IF NOT EXISTS institutional_flows (
    time        TIMESTAMPTZ NOT NULL,
    ticker      VARCHAR(10) NOT NULL,
    foreign_net BIGINT,   -- 外資買賣超 (張)
    trust_net   BIGINT,   -- 投信買賣超
    dealer_net  BIGINT,   -- 自營商買賣超
    total_net   BIGINT,   -- 三大法人合計
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);
SELECT create_hypertable('institutional_flows', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS ux_inst_flows ON institutional_flows (time DESC, ticker);

-- ============================================================
-- 4. 外資台指期淨未平倉  (hypertable)
-- ============================================================
CREATE TABLE IF NOT EXISTS futures_positions (
    time          TIMESTAMPTZ NOT NULL,
    foreign_long  BIGINT,
    foreign_short BIGINT,
    foreign_net   BIGINT,   -- 外資淨多單口數
    dealer_long   BIGINT,
    dealer_short  BIGINT,
    dealer_net    BIGINT
);
SELECT create_hypertable('futures_positions', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS ux_futures_positions ON futures_positions (time DESC);

-- ============================================================
-- 5. 融資融券餘額  (hypertable)
-- ============================================================
CREATE TABLE IF NOT EXISTS margin_trading (
    time           TIMESTAMPTZ NOT NULL,
    ticker         VARCHAR(10) NOT NULL,
    margin_balance BIGINT,   -- 融資餘額 (千元)
    short_balance  BIGINT,   -- 融券餘額 (張)
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);
SELECT create_hypertable('margin_trading', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS ux_margin_trading ON margin_trading (time DESC, ticker);

-- ============================================================
-- 6. 千張大戶持股比例  (hypertable, 週頻)
-- ============================================================
CREATE TABLE IF NOT EXISTS major_holders (
    time               TIMESTAMPTZ NOT NULL,
    ticker             VARCHAR(10) NOT NULL,
    holders_1000_ratio NUMERIC(6,2),  -- 千張以上持股比例 (%)
    retail_ratio       NUMERIC(6,2),  -- 散戶 (<10張) 持股比例 (%)
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);
SELECT create_hypertable('major_holders', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS ux_major_holders ON major_holders (time DESC, ticker);

-- ============================================================
-- 7. 月營收
-- ============================================================
CREATE TABLE IF NOT EXISTS monthly_revenue (
    year_month  CHAR(7)      NOT NULL,  -- YYYY-MM
    ticker      VARCHAR(10)  NOT NULL,
    revenue     BIGINT,                 -- 當月營收 (千元)
    revenue_mom NUMERIC(12,4),          -- 月增率 (%)，小公司極端值可能超過 ±9999
    revenue_yoy NUMERIC(12,4),          -- 年增率 (%)
    PRIMARY KEY (year_month, ticker),
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);

-- ============================================================
-- 8. 財報三率 + EPS  (季頻)
-- ============================================================
CREATE TABLE IF NOT EXISTS financial_statements (
    year_quarter        CHAR(7)      NOT NULL,  -- YYYY-QN
    ticker              VARCHAR(10)  NOT NULL,
    gross_profit_margin NUMERIC(12,4),
    operating_margin    NUMERIC(12,4),
    net_profit_margin   NUMERIC(12,4),
    eps                 NUMERIC(10,4),
    eps_forecast        NUMERIC(10,4),          -- FinMind 分析師預估 (付費)
    bvps                NUMERIC(12,4),
    free_cash_flow      BIGINT,
    dividend_per_share  NUMERIC(8,4),
    shares_outstanding  BIGINT,
    PRIMARY KEY (year_quarter, ticker),
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);

-- ============================================================
-- 9. 總經指標  (hypertable)
-- ============================================================
CREATE TABLE IF NOT EXISTS macro_indicators (
    time      TIMESTAMPTZ  NOT NULL,
    indicator VARCHAR(30)  NOT NULL,  -- FED_RATE | CPI_YOY | UST_10Y | USDTWD
    value     NUMERIC(15,6) NOT NULL,
    source    VARCHAR(50)
);
SELECT create_hypertable('macro_indicators', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS ux_macro_indicators ON macro_indicators (time DESC, indicator);

-- ============================================================
-- 10. 告警
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts (
    id           BIGSERIAL   PRIMARY KEY,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ticker       VARCHAR(10),
    alert_type   VARCHAR(60) NOT NULL,
    severity     VARCHAR(10) NOT NULL CHECK (severity IN ('INFO','WARNING','CRITICAL')),
    message      TEXT        NOT NULL,
    metadata     JSONB,
    acknowledged BOOLEAN     DEFAULT FALSE,
    notified_at  TIMESTAMPTZ DEFAULT NULL  -- Telegram 推送時間
);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_unnotified
    ON alerts (created_at DESC)
    WHERE notified_at IS NULL AND acknowledged = FALSE;

-- ============================================================
-- Continuous Aggregate：週 OHLCV rollup
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS price_weekly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 week', time) AS week,
    ticker,
    first(open,  time) AS open,
    max(high)          AS high,
    min(low)           AS low,
    last(close, time)  AS close,
    sum(volume)        AS volume
FROM price_history
GROUP BY 1, 2
WITH NO DATA;

SELECT add_continuous_aggregate_policy('price_weekly',
    start_offset      => INTERVAL '1 month',
    end_offset        => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists     => TRUE
);
