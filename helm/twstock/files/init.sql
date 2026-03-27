-- ============================================================
-- TWStock Database Schema
-- Requires: PostgreSQL 14+ with TimescaleDB extension
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================
-- Master Data
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
-- Price & Volume (TimescaleDB hypertable)
-- ============================================================

CREATE TABLE IF NOT EXISTS price_history (
    time    TIMESTAMPTZ    NOT NULL,
    ticker  VARCHAR(10)    NOT NULL,
    open    NUMERIC(12,2),
    high    NUMERIC(12,2),
    low     NUMERIC(12,2),
    close   NUMERIC(12,2)  NOT NULL,
    volume  BIGINT,
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);
SELECT create_hypertable('price_history', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_price_history_time_ticker ON price_history (time DESC, ticker);

-- ============================================================
-- Institutional Flow / 三大法人買賣超 (TimescaleDB hypertable)
-- ============================================================

CREATE TABLE IF NOT EXISTS institutional_flows (
    time            TIMESTAMPTZ  NOT NULL,
    ticker          VARCHAR(10)  NOT NULL,
    foreign_net     BIGINT,   -- 外資買賣超 (張)
    trust_net       BIGINT,   -- 投信買賣超
    dealer_net      BIGINT,   -- 自營商買賣超
    total_net       BIGINT,   -- 三大法人合計
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);
SELECT create_hypertable('institutional_flows', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_inst_flows_time_ticker ON institutional_flows (time DESC, ticker);

-- ============================================================
-- Futures Positions / 外資台指期淨未平倉
-- ============================================================

CREATE TABLE IF NOT EXISTS futures_positions (
    time            TIMESTAMPTZ  NOT NULL,
    foreign_long    BIGINT,
    foreign_short   BIGINT,
    foreign_net     BIGINT,   -- 外資淨未平倉口數
    dealer_long     BIGINT,
    dealer_short    BIGINT,
    dealer_net      BIGINT
);
SELECT create_hypertable('futures_positions', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_futures_positions_time ON futures_positions (time DESC);

-- ============================================================
-- Margin Trading / 融資融券餘額
-- ============================================================

CREATE TABLE IF NOT EXISTS margin_trading (
    time            TIMESTAMPTZ  NOT NULL,
    ticker          VARCHAR(10)  NOT NULL,
    margin_balance  BIGINT,   -- 融資餘額 (千元)
    short_balance   BIGINT,   -- 融券餘額 (張)
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);
SELECT create_hypertable('margin_trading', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_margin_time_ticker ON margin_trading (time DESC, ticker);

-- ============================================================
-- Major Holders / 千張大戶持股 (weekly)
-- ============================================================

CREATE TABLE IF NOT EXISTS major_holders (
    time                TIMESTAMPTZ  NOT NULL,
    ticker              VARCHAR(10)  NOT NULL,
    holders_1000_ratio  NUMERIC(6,2),  -- 千張大戶持股比例 (%)
    retail_ratio        NUMERIC(6,2),  -- 散戶 (<10張) 持股比例 (%)
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);
SELECT create_hypertable('major_holders', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_major_holders_time_ticker ON major_holders (time DESC, ticker);

-- ============================================================
-- Monthly Revenue / 月營收
-- ============================================================

CREATE TABLE IF NOT EXISTS monthly_revenue (
    year_month      CHAR(7)       NOT NULL,  -- YYYY-MM
    ticker          VARCHAR(10)   NOT NULL,
    revenue         BIGINT,       -- 當月營收 (千元)
    revenue_mom     NUMERIC(8,4), -- 月增率 (%)
    revenue_yoy     NUMERIC(8,4), -- 年增率 (%)
    PRIMARY KEY (year_month, ticker),
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);

-- ============================================================
-- Financial Statements / 財報三率 (quarterly)
-- ============================================================

CREATE TABLE IF NOT EXISTS financial_statements (
    year_quarter            CHAR(7)       NOT NULL,  -- YYYY-QN
    ticker                  VARCHAR(10)   NOT NULL,
    gross_profit_margin     NUMERIC(8,4), -- 毛利率 (%)
    operating_margin        NUMERIC(8,4), -- 營益率 (%)
    net_profit_margin       NUMERIC(8,4), -- 淨利率 (%)
    eps                     NUMERIC(10,4),
    bvps                    NUMERIC(12,4), -- 每股淨值
    free_cash_flow          BIGINT,        -- 自由現金流 (千元)
    dividend_per_share      NUMERIC(8,4),  -- 每股配息
    shares_outstanding      BIGINT,        -- 流通股數 (千股)
    PRIMARY KEY (year_quarter, ticker),
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);

-- ============================================================
-- Macro Indicators / 總經指標
-- ============================================================

CREATE TABLE IF NOT EXISTS macro_indicators (
    time        TIMESTAMPTZ  NOT NULL,
    indicator   VARCHAR(30)  NOT NULL,
    -- Indicators: FED_RATE, CPI_YOY, PCE_YOY, UST_10Y, USDTWD
    value       NUMERIC(15,6) NOT NULL,
    source      VARCHAR(50)
);
SELECT create_hypertable('macro_indicators', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_macro_time_indicator ON macro_indicators (time DESC, indicator);

-- ============================================================
-- Analysis Results
-- ============================================================

CREATE TABLE IF NOT EXISTS valuation_scores (
    time             TIMESTAMPTZ   NOT NULL,
    ticker           VARCHAR(10)   NOT NULL,
    model            VARCHAR(20)   NOT NULL, -- PE, PB, PEG, DIV_YIELD, DCF
    intrinsic_value  NUMERIC(12,2),
    current_price    NUMERIC(12,2),
    ratio            NUMERIC(10,4), -- current_price / intrinsic_value
    signal           VARCHAR(20)   CHECK (signal IN ('UNDERVALUED','FAIR','OVERVALUED','N/A')),
    metadata         JSONB
);
SELECT create_hypertable('valuation_scores', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_valuation_time_ticker_model ON valuation_scores (time DESC, ticker, model);

CREATE TABLE IF NOT EXISTS multi_factor_signals (
    time                    TIMESTAMPTZ  NOT NULL,
    ticker                  VARCHAR(10)  NOT NULL,
    filter1_fundamental     BOOLEAN      DEFAULT FALSE, -- YoY revenue > 15%
    filter2_institutional   BOOLEAN      DEFAULT FALSE, -- 投信買超 + 大戶增持
    filter3_valuation       BOOLEAN      DEFAULT FALSE, -- Forward P/E in range
    filter4_technical       BOOLEAN      DEFAULT FALSE, -- 突破20日高點 + 量能
    score                   SMALLINT     DEFAULT 0,     -- 通過篩選數 (0-4)
    all_passed              BOOLEAN      DEFAULT FALSE
);
SELECT create_hypertable('multi_factor_signals', 'time', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS idx_mf_signals_time_ticker ON multi_factor_signals (time DESC, ticker);

CREATE TABLE IF NOT EXISTS alerts (
    id           BIGSERIAL    PRIMARY KEY,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ticker       VARCHAR(10),
    alert_type   VARCHAR(60)  NOT NULL,
    severity     VARCHAR(10)  NOT NULL CHECK (severity IN ('INFO','WARNING','CRITICAL')),
    message      TEXT         NOT NULL,
    metadata     JSONB,
    acknowledged BOOLEAN      DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_ticker ON alerts (ticker) WHERE ticker IS NOT NULL;

-- ============================================================
-- Continuous Aggregates (TimescaleDB)
-- Weekly OHLCV rollup
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

-- Refresh policy: refresh weekly data every day
SELECT add_continuous_aggregate_policy('price_weekly',
    start_offset => INTERVAL '1 month',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);
