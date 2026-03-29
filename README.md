# 台股量化分析系統 (TWStock Quant Pipeline)

> Branch: `feat/pipeline-v2` — Data Pipeline MVP

## 概覽

以 Python 3.11 單套件架構實作的台股量化資料蒐集管線，涵蓋四大資料維度：
總經流動性、法人籌碼、基本面 / 技術面，並預留估值模型欄位。

```
資料來源 → Collector → TimescaleDB → (Grafana / Analyzer)
```

---

## 架構

```
src/twstock/
├── __main__.py          # python -m twstock 入口
├── cli.py               # 6 個 collector jobs + --source / --backfill CLI
├── config.py            # pydantic-settings (TWSTOCK_ 前綴環境變數)
├── database.py          # asyncpg + SQLAlchemy async session
└── collectors/
    ├── base.py          # httpx async client + tenacity retry
    ├── twse.py          # TWSE OpenAPI + MI_INDEX / T86 / MI_MARGN
    ├── macro.py         # yfinance (UST_10Y / USDTWD) + FRED
    ├── finmind.py       # FinMind API (月營收 / 季財報 / 大戶持股 備援)
    └── tdcc.py          # TDCC OpenAPI 集保結算所大戶持股
```

---

## 資料來源與 Jobs

### 每日 Jobs（盤後觸發）

| Job | 來源 | 說明 | API 次數 |
|-----|------|------|---------|
| `daily_prices` | TWSE MI_INDEX | 1174 支上市股 OHLCV | 2 calls |
| `daily_institutional` | TWSE T86 + MI_MARGN | 三大法人買賣超 + 融資融券 | 3 calls |
| `macro_indicators` | yfinance + FRED | UST_10Y / USDTWD / Fed Rate / CPI | 2–6 calls |

### 低頻 Jobs

| Job | 預設來源 | 說明 | API 次數 |
|-----|---------|------|---------|
| `monthly_revenue` | TWSE OpenAPI `t187ap05_L` | 上市公司月營收 YoY/MoM | **1 call** |
| `quarterly_financials` | TWSE OpenAPI `t187ap06_L_*` | 財報三率 + EPS（6 產業別） | **6 calls** |
| `weekly_major_holders` | TDCC OpenAPI `1-5` | 千張大戶 / 散戶持股比例 | **1 call** |

> 低頻 jobs 預設使用政府 Open Data（免費、無需 API Key、Bulk 回傳全部股票），
> 可透過 `--source finmind` 切換為 FinMind per-ticker 並加上 `--backfill` 歷史回補。

---

## 資料庫 Schema（TimescaleDB）

| Table | 類型 | 說明 |
|-------|------|------|
| `stocks` | 主表 | 股票代號 / 名稱 / 市場 / 產業 |
| `price_history` | hypertable | 日 OHLCV（單位：張） |
| `institutional_flows` | hypertable | 三大法人買賣超（外資 / 投信 / 自營） |
| `futures_positions` | hypertable | 外資台指期淨未平倉口數 |
| `margin_trading` | hypertable | 融資融券餘額 |
| `major_holders` | hypertable | 千張大戶 / 散戶持股比例（週頻） |
| `monthly_revenue` | 一般表 | 月營收 + MoM / YoY |
| `financial_statements` | 一般表 | 毛利率 / 營益率 / 淨利率 / EPS |
| `macro_indicators` | hypertable | UST_10Y / USDTWD / FED_RATE / CPI |
| `alerts` | 一般表 | 告警記錄（Phase 2 Telegram 推送預留） |

---

## 快速啟動

### 前置需求

- Docker Desktop
- Docker Compose v2

### 本地開發

```powershell
# 啟動 TimescaleDB
docker compose up -d postgres

# 執行各 job（範例）
docker compose run --rm twstock --job daily_prices
docker compose run --rm twstock --job daily_institutional
docker compose run --rm twstock --job macro_indicators
docker compose run --rm twstock --job monthly_revenue
docker compose run --rm twstock --job quarterly_financials
docker compose run --rm twstock --job weekly_major_holders
```

### 歷史回補（FinMind，需 token）

```powershell
# 月營收 2022 至今
docker compose run --rm twstock --job monthly_revenue --source finmind --backfill 2022-01-01

# 千張大戶歷史（需付費方案）
docker compose run --rm twstock --job weekly_major_holders --source finmind --backfill 2022-01-01
```

---

## 環境變數設定（`.env`）

```dotenv
# Database
TWSTOCK_DATABASE_URL=postgresql+asyncpg://twstock:twstock@postgres:5432/twstock

# External API tokens（選填）
TWSTOCK_FINMIND_API_TOKEN=          # finmindtrade.com 免費註冊 (600 req/hr)
TWSTOCK_FRED_API_KEY=               # fred.stlouisfed.org 免費

# Data source selection（預設值即可，不需修改）
TWSTOCK_REVENUE_SOURCE=twse         # twse | finmind
TWSTOCK_FINANCIALS_SOURCE=twse      # twse | finmind
TWSTOCK_HOLDERS_SOURCE=tdcc         # tdcc | finmind

# FinMind 限流設定
TWSTOCK_FINMIND_TICKERS_LIMIT=200   # free tier 建議 200；付費設 0（無限）
TWSTOCK_FINMIND_REQUEST_DELAY=10.0  # 秒/req，10s ≈ 360 req/hr

# Telegram 告警（Phase 2）
TWSTOCK_TELEGRAM_BOT_TOKEN=
TWSTOCK_TELEGRAM_CHAT_ID=
```

---

## Helm / Kubernetes 部署

```powershell
# Helm lint
helm lint helm/twstock

# 部署（覆蓋 token）
helm upgrade --install twstock helm/twstock `
  --namespace twstock --create-namespace `
  --set secrets.finmindApiToken=YOUR_TOKEN `
  --set secrets.fredApiKey=YOUR_FRED_KEY `
  --set postgresql.credentials.password=YOUR_PG_PASSWORD
```

### ArgoCD

`argocd/application.yaml` 已預留所有 token 注入欄位，
在 ArgoCD UI → App Details → Parameters 填入即可，無需修改 Git。

```yaml
# 可在 ArgoCD UI 覆蓋的 parameters
secrets.finmindApiToken
secrets.fredApiKey
secrets.telegramBotToken
secrets.telegramChatId
postgresql.credentials.password
config.revenueSource       # twse | finmind
config.financialsSource    # twse | finmind
config.holdersSource       # tdcc | finmind
config.finmindTickersLimit # 200 (free) | 0 (paid)
config.finmindRequestDelay # 10.0 秒
```

---

## 已知限制

| 項目 | 狀態 | 說明 |
|------|------|------|
| TAIFEX 台指期部位 | ⚠️ Skip | 需要 browser session，回傳 HTML，待改用其他方式 |
| 月營收歷史回補 | ⚠️ 需 token | TWSE API 僅提供最新月；歷史需 FinMind |
| 季財報歷史回補 | ⚠️ 需 token | TWSE API 僅提供最新季；歷史需 FinMind |
| 大戶持股歷史 | ⚠️ 需付費 | TDCC API 僅提供最新週；歷史需 FinMind 付費方案 |
| FRED 總經指標 | ⚠️ 選填 | 設定 `TWSTOCK_FRED_API_KEY` 才會拉取 |

---

## Phase 2 規劃（預留）

- [ ] Telegram 告警推送（`alerts` 表已有 `notified_at` 欄位）
- [ ] 估值模型：P/E、P/B、EV/EBITDA、DCF、PEG、股息殖利率
- [ ] Grafana Dashboard（四大維度）
- [ ] TAIFEX 期貨部位替代來源
- [ ] MOPS 月營收歷史爬蟲（免費替代 FinMind 歷史回補）
