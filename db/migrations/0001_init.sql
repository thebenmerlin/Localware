-- Localware automated hedge fund — initial schema
-- All state lives here. Workers write; Next.js reads.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Universe -------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS securities (
  id          SERIAL PRIMARY KEY,
  ticker      TEXT NOT NULL UNIQUE,
  name        TEXT,
  sector      TEXT,
  industry    TEXT,
  asset_class TEXT NOT NULL DEFAULT 'equity',
  listed_at   DATE,
  active      BOOLEAN NOT NULL DEFAULT TRUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS securities_active_idx ON securities(active);

CREATE TABLE IF NOT EXISTS universe (
  security_id  INT NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  included_at  DATE NOT NULL,
  removed_at   DATE,
  reason       TEXT,
  PRIMARY KEY (security_id, included_at)
);

-- Market data ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS prices (
  security_id  INT NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  date         DATE NOT NULL,
  open         NUMERIC(14,4),
  high         NUMERIC(14,4),
  low          NUMERIC(14,4),
  close        NUMERIC(14,4),
  adj_close    NUMERIC(14,4),
  volume       BIGINT,
  PRIMARY KEY (security_id, date)
);
CREATE INDEX IF NOT EXISTS prices_date_idx ON prices(date);

CREATE TABLE IF NOT EXISTS fundamentals (
  security_id      INT NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  date             DATE NOT NULL,
  pe               NUMERIC(12,4),
  pb               NUMERIC(12,4),
  roe              NUMERIC(12,4),
  debt_to_equity   NUMERIC(12,4),
  market_cap       NUMERIC(20,2),
  earnings_growth  NUMERIC(12,4),
  PRIMARY KEY (security_id, date)
);

-- Strategies & signals -------------------------------------------------------

CREATE TABLE IF NOT EXISTS strategies (
  id                SERIAL PRIMARY KEY,
  name              TEXT NOT NULL UNIQUE,
  description       TEXT,
  allocation_weight NUMERIC(6,4) NOT NULL DEFAULT 0.25,
  params            JSONB NOT NULL DEFAULT '{}'::jsonb,
  enabled           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS signals (
  id           BIGSERIAL PRIMARY KEY,
  strategy_id  INT NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
  security_id  INT NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  date         DATE NOT NULL,
  signal       NUMERIC(8,4) NOT NULL,    -- target weight contribution, signed
  score        NUMERIC(12,6),            -- raw factor score
  metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (strategy_id, security_id, date)
);
CREATE INDEX IF NOT EXISTS signals_date_idx ON signals(date);
CREATE INDEX IF NOT EXISTS signals_strategy_date_idx ON signals(strategy_id, date);

-- Portfolio state ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS positions (
  id             BIGSERIAL PRIMARY KEY,
  security_id    INT NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  quantity       NUMERIC(18,6) NOT NULL,
  avg_cost       NUMERIC(14,4) NOT NULL,
  market_value   NUMERIC(18,2) NOT NULL,
  weight         NUMERIC(8,6) NOT NULL,
  unrealized_pnl NUMERIC(18,2) NOT NULL DEFAULT 0,
  peak_price     NUMERIC(14,4),           -- for trailing stops
  as_of          DATE NOT NULL,
  UNIQUE (security_id, as_of)
);
CREATE INDEX IF NOT EXISTS positions_as_of_idx ON positions(as_of);

CREATE TABLE IF NOT EXISTS trades (
  id           BIGSERIAL PRIMARY KEY,
  security_id  INT NOT NULL REFERENCES securities(id),
  strategy_id  INT REFERENCES strategies(id),
  side         TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  quantity     NUMERIC(18,6) NOT NULL,
  price        NUMERIC(14,4) NOT NULL,
  slippage_bps NUMERIC(8,4) NOT NULL DEFAULT 0,
  commission   NUMERIC(12,4) NOT NULL DEFAULT 0,
  notional     NUMERIC(18,2) NOT NULL,
  executed_at  TIMESTAMPTZ NOT NULL,
  reason       TEXT
);
CREATE INDEX IF NOT EXISTS trades_executed_idx ON trades(executed_at);
CREATE INDEX IF NOT EXISTS trades_security_idx ON trades(security_id);

CREATE TABLE IF NOT EXISTS portfolio_nav (
  date            DATE PRIMARY KEY,
  nav             NUMERIC(18,2) NOT NULL,
  cash            NUMERIC(18,2) NOT NULL,
  gross_exposure  NUMERIC(18,2) NOT NULL,
  net_exposure    NUMERIC(18,2) NOT NULL,
  leverage        NUMERIC(6,4) NOT NULL,
  daily_return    NUMERIC(10,6),
  cumulative_return NUMERIC(12,6)
);

-- Performance & risk --------------------------------------------------------

CREATE TABLE IF NOT EXISTS performance_metrics (
  id            BIGSERIAL PRIMARY KEY,
  period        TEXT NOT NULL,           -- 'all', 'ytd', '1y', '3m', '1m'
  as_of         DATE NOT NULL,
  total_return  NUMERIC(10,6),
  ann_return    NUMERIC(10,6),
  ann_vol       NUMERIC(10,6),
  sharpe        NUMERIC(10,4),
  sortino       NUMERIC(10,4),
  max_drawdown  NUMERIC(10,6),
  calmar        NUMERIC(10,4),
  hit_rate      NUMERIC(6,4),
  beta          NUMERIC(8,4),
  alpha         NUMERIC(10,6),
  computed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (period, as_of)
);

CREATE TABLE IF NOT EXISTS risk_metrics (
  date               DATE PRIMARY KEY,
  var_95             NUMERIC(10,6),
  var_99             NUMERIC(10,6),
  expected_shortfall NUMERIC(10,6),
  realized_vol       NUMERIC(10,6),
  factor_exposures   JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS strategy_performance (
  strategy_id       INT NOT NULL REFERENCES strategies(id) ON DELETE CASCADE,
  date              DATE NOT NULL,
  contribution      NUMERIC(10,6),
  cumulative_return NUMERIC(12,6),
  PRIMARY KEY (strategy_id, date)
);

CREATE TABLE IF NOT EXISTS backtests (
  id              SERIAL PRIMARY KEY,
  name            TEXT NOT NULL,
  strategy_config JSONB NOT NULL,
  start_date      DATE NOT NULL,
  end_date        DATE NOT NULL,
  results         JSONB NOT NULL,        -- summary metrics
  equity_curve    JSONB NOT NULL,        -- [{date, nav, ret}, ...]
  status          TEXT NOT NULL DEFAULT 'completed',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed strategies ------------------------------------------------------------

INSERT INTO strategies (name, description, allocation_weight, params) VALUES
  ('momentum',     'Cross-sectional 12-1 month momentum, top/bottom 30, monthly rebalance', 0.40,
    '{"lookback_days": 252, "skip_days": 21, "longs": 30, "shorts": 30, "rebalance": "monthly"}'::jsonb),
  ('quality',      'High ROE, low D/E, positive earnings growth — long only top 30',         0.25,
    '{"longs": 30, "min_roe": 0.15, "max_de": 1.0}'::jsonb),
  ('low_volatility','Bottom-quintile 60-day realized vol, equal weight long only',           0.20,
    '{"lookback_days": 60, "longs": 50}'::jsonb),
  ('mean_reversion','RSI<30 above 200-DMA, 5-day hold',                                      0.15,
    '{"rsi_period": 14, "rsi_threshold": 30, "ma_period": 200, "hold_days": 5}'::jsonb)
ON CONFLICT (name) DO NOTHING;
