-- Localware Phase 2: derived signals + portfolio target positions.
--
-- Sizing (per the ~1000-name × 5y plan):
--   derived.signals_daily         ~300 rows/day  × 1260 days = ~380k rows × ~80B = ~30MB
--   portfolio.positions_daily     ~200 rows/day  × 1260 days = ~250k rows × ~60B = ~15MB
--
-- Design notes:
--   * `derived.signals_daily.attribution` is JSONB so adding a 5th sleeve is a
--     code change, not a migration.
--   * `portfolio.positions_daily.date` is the date the target APPLIES TO
--     (i.e. the next business day after computation), not when it was computed.
--     The executor only needs to read `WHERE date = today` to know what to hold.

CREATE SCHEMA IF NOT EXISTS derived;
CREATE SCHEMA IF NOT EXISTS portfolio;

-- ---------------------------------------------------------------------------
-- Blended cross-sectional signal (one row per (security, date))
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS derived.signals_daily (
  security_id    INTEGER NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  date           DATE    NOT NULL,
  blended_signal DOUBLE PRECISION NOT NULL,
  attribution    JSONB   NOT NULL DEFAULT '{}'::jsonb,
  computed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (security_id, date)
);
CREATE INDEX IF NOT EXISTS signals_daily_date_idx ON derived.signals_daily(date);

-- ---------------------------------------------------------------------------
-- Risk-managed target positions (one row per (security, target-date))
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS portfolio.positions_daily (
  security_id        INTEGER NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
  date               DATE    NOT NULL,         -- target effective date (next business day)
  target_weight      DOUBLE PRECISION NOT NULL,
  pre_overlay_weight DOUBLE PRECISION NOT NULL, -- before vol-target / DD / caps
  sector             TEXT,
  computed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (security_id, date)
);
CREATE INDEX IF NOT EXISTS positions_daily_date_idx ON portfolio.positions_daily(date);
