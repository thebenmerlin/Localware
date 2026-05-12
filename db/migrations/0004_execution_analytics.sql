-- Localware Phase 3: execution NAV history + Vercel-ready analytics.
--
-- Sizing for 5y daily history:
--   portfolio.nav_daily              ~1260 rows  ~110B   = ~140KB
--   analytics.equity_curve           ~1260 rows  ~80B    = ~100KB
--   analytics.rolling_metrics        ~1260 rows  ~50B    = ~60KB
--   analytics.var_daily              ~1260 rows  ~40B    = ~50KB
--   analytics.monthly_returns        ~60 rows                ~3KB
--   analytics.drawdown_periods       ~5-15 rows              negligible
--   analytics.performance_summary    ~5 rows (per period)    <1KB
--
-- All analytics tables are designed for direct Vercel reads:
--   - Equity-curve chart:  SELECT * FROM analytics.equity_curve ORDER BY date
--   - Heatmap:             SELECT * FROM analytics.monthly_returns
--   - KPI cards:           SELECT * FROM analytics.performance_summary WHERE period='all'
--   - Rolling Sharpe:      SELECT date,rolling_1y_sharpe FROM analytics.rolling_metrics
--   - Drawdown table:      SELECT * FROM analytics.drawdown_periods ORDER BY depth
-- Every chart query hits a single table by primary key or sorted index — no
-- JOINs, no aggregations, well under the 10s Vercel ceiling.

CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- Portfolio NAV history (the source-of-truth equity series)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS portfolio.nav_daily (
  date               DATE PRIMARY KEY,
  nav                DOUBLE PRECISION NOT NULL,
  daily_return       DOUBLE PRECISION,
  cumulative_return  DOUBLE PRECISION,
  gross_exposure     DOUBLE PRECISION NOT NULL DEFAULT 0,
  net_exposure       DOUBLE PRECISION NOT NULL DEFAULT 0,
  turnover           DOUBLE PRECISION NOT NULL DEFAULT 0,
  execution_cost_bps DOUBLE PRECISION NOT NULL DEFAULT 0,
  computed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- analytics.* (Vercel-ready, flat, single-table-per-chart)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.equity_curve (
  date              DATE PRIMARY KEY,
  nav               DOUBLE PRECISION NOT NULL,
  daily_return      DOUBLE PRECISION,
  cumulative_return DOUBLE PRECISION,
  peak_nav          DOUBLE PRECISION,
  drawdown          DOUBLE PRECISION,
  benchmark_cumret  DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS analytics.performance_summary (
  period         TEXT PRIMARY KEY,    -- 'all', 'ytd', '1y', '3m', '1m'
  as_of          DATE NOT NULL,
  total_return   DOUBLE PRECISION,
  ann_return     DOUBLE PRECISION,
  ann_vol        DOUBLE PRECISION,
  sharpe         DOUBLE PRECISION,
  sortino        DOUBLE PRECISION,
  max_drawdown   DOUBLE PRECISION,
  calmar         DOUBLE PRECISION,
  hit_rate       DOUBLE PRECISION,
  beta           DOUBLE PRECISION,
  alpha          DOUBLE PRECISION,
  best_day       DOUBLE PRECISION,
  worst_day      DOUBLE PRECISION,
  trading_days   INTEGER,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.rolling_metrics (
  date              DATE PRIMARY KEY,
  rolling_1y_return DOUBLE PRECISION,
  rolling_1y_vol    DOUBLE PRECISION,
  rolling_1y_sharpe DOUBLE PRECISION,
  rolling_3m_return DOUBLE PRECISION,
  rolling_1y_max_dd DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS analytics.monthly_returns (
  year         INTEGER NOT NULL,
  month        INTEGER NOT NULL,    -- 1..12
  total_return DOUBLE PRECISION NOT NULL,
  trading_days INTEGER NOT NULL,
  PRIMARY KEY (year, month)
);

CREATE TABLE IF NOT EXISTS analytics.var_daily (
  date               DATE PRIMARY KEY,
  var_95             DOUBLE PRECISION,
  var_99             DOUBLE PRECISION,
  expected_shortfall DOUBLE PRECISION,
  realized_vol       DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS analytics.drawdown_periods (
  id            BIGSERIAL PRIMARY KEY,
  start_date    DATE NOT NULL,
  trough_date   DATE NOT NULL,
  end_date      DATE,                  -- NULL if not yet recovered
  depth         DOUBLE PRECISION NOT NULL,
  duration_days INTEGER,
  recovery_days INTEGER,
  ongoing       BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS dd_periods_depth_idx ON analytics.drawdown_periods(depth);
CREATE INDEX IF NOT EXISTS dd_periods_start_idx ON analytics.drawdown_periods(start_date);
