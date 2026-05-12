-- Localware Phase 4: drop tables orphaned by the Phase 1–3 redesign.
--
-- Kept (still in active use):
--   securities         — universe roster, referenced by every raw/derived table
--   strategies         — allocation weights + sleeve params, read by strategy_runner
--   universe           — historical inclusion/exclusion records
--   backtests          — per-run registry, written by backtest.py, read by the
--                        Next.js /backtest pages
--
-- Dropped (replaced by Phase 1–3 tables):
--   prices                  → raw.ohlcv_daily
--   fundamentals            → raw.fundamentals_quarterly + raw.fundamentals_snapshot
--   signals                 → derived.signals_daily
--   positions               → portfolio.positions_daily (now stores target weights)
--   portfolio_nav           → portfolio.nav_daily
--   performance_metrics     → analytics.performance_summary
--   risk_metrics            → analytics.var_daily
--   strategy_performance    → derivable from derived.signals_daily.attribution
--   trades                  → synthesized in queries.ts from positions_daily diffs
--
-- CASCADE handles FKs that referenced these tables (there shouldn't be any
-- outside this orphan set, but be safe).

BEGIN;

DROP TABLE IF EXISTS trades                CASCADE;
DROP TABLE IF EXISTS strategy_performance  CASCADE;
DROP TABLE IF EXISTS risk_metrics          CASCADE;
DROP TABLE IF EXISTS performance_metrics   CASCADE;
DROP TABLE IF EXISTS portfolio_nav         CASCADE;
DROP TABLE IF EXISTS positions             CASCADE;
DROP TABLE IF EXISTS signals               CASCADE;
DROP TABLE IF EXISTS fundamentals          CASCADE;
DROP TABLE IF EXISTS prices                CASCADE;

COMMIT;
