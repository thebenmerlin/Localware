-- Localware Phase 1: raw.* schema for source-of-truth market & fundamentals data.
--
-- Design notes (sized for ~1000 names, 5y daily history):
--   raw.ohlcv_daily         ~1.26M rows × ~80B = ~100MB + indexes
--   raw.fundamentals_quarterly  ~20k rows × ~200B = ~4MB
--   raw.fundamentals_snapshot   ~1k rows (one row per security)
--   raw.ticker_health           ~1k rows
--
-- Types: DOUBLE PRECISION (float8 = 8B) instead of NUMERIC (variable-width,
-- 16-32B) — saves ~50% on the dominant table. Precision is irrelevant for
-- prices that already round-trip through yfinance.
--
-- 90-day fundamentals lag is materialized as `available_at` so downstream
-- queries can do `WHERE available_at <= as_of` without recomputing.

CREATE SCHEMA IF NOT EXISTS raw;

-- ---------------------------------------------------------------------------
-- OHLCV
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.ohlcv_daily (
  security_id INTEGER NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  date        DATE    NOT NULL,
  open        DOUBLE PRECISION,
  high        DOUBLE PRECISION,
  low         DOUBLE PRECISION,
  close       DOUBLE PRECISION,
  adj_close   DOUBLE PRECISION,
  volume      BIGINT,
  PRIMARY KEY (security_id, date)
);
CREATE INDEX IF NOT EXISTS ohlcv_daily_date_idx ON raw.ohlcv_daily(date);

-- ---------------------------------------------------------------------------
-- Fundamentals (quarterly time series — point-in-time via available_at)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.fundamentals_quarterly (
  security_id        INTEGER NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  fiscal_period_end  DATE    NOT NULL,
  available_at       DATE    NOT NULL,       -- fiscal_period_end + 90 days
  total_revenue      DOUBLE PRECISION,
  net_income         DOUBLE PRECISION,
  total_assets       DOUBLE PRECISION,
  total_equity       DOUBLE PRECISION,
  total_debt         DOUBLE PRECISION,
  operating_cashflow DOUBLE PRECISION,
  eps_diluted        DOUBLE PRECISION,
  shares_diluted     DOUBLE PRECISION,
  fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (security_id, fiscal_period_end)
);
CREATE INDEX IF NOT EXISTS fundamentals_q_avail_idx
  ON raw.fundamentals_quarterly(available_at);

-- Slow-changing per-ticker snapshot (market cap, current ratios, beta).
-- One row per security; overwrite on each fetch.
CREATE TABLE IF NOT EXISTS raw.fundamentals_snapshot (
  security_id        INTEGER PRIMARY KEY REFERENCES securities(id) ON DELETE CASCADE,
  market_cap         DOUBLE PRECISION,
  shares_outstanding DOUBLE PRECISION,
  trailing_pe        DOUBLE PRECISION,
  forward_pe         DOUBLE PRECISION,
  price_to_book      DOUBLE PRECISION,
  trailing_eps       DOUBLE PRECISION,
  beta               DOUBLE PRECISION,
  fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Ticker health (persistent failure tracker for circuit-breaker / skip logic)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS raw.ticker_health (
  security_id          INTEGER PRIMARY KEY REFERENCES securities(id) ON DELETE CASCADE,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  last_failure_at      TIMESTAMPTZ,
  last_failure_reason  TEXT,
  last_success_at      TIMESTAMPTZ,
  skip_until           DATE
);
CREATE INDEX IF NOT EXISTS ticker_health_skip_idx ON raw.ticker_health(skip_until);
