import { sql } from "./db";

// All queries below target the Phase 1–3 schema exclusively:
//   raw.ohlcv_daily, raw.fundamentals_*  — source-of-truth data
//   derived.signals_daily                — blended signal + sleeve attribution
//   portfolio.positions_daily            — target weights (effective per date)
//   portfolio.nav_daily                  — realized NAV history
//   analytics.*                          — pre-calculated, Vercel-ready tables
// Return shapes are unchanged from the prior version so no React component
// needs to be modified.

export type Period = "all" | "ytd" | "1y" | "3m" | "1m";

// ---------------------------------------------------------------------------
// Portfolio / NAV
// ---------------------------------------------------------------------------

export async function getLatestNav() {
  // `cash` and `leverage` aren't tracked literally in the new schema — synthesize:
  //   cash     = nav * (1 - net_exposure)   (net exposure is a weight ratio)
  //   leverage = gross_exposure             (already a weight ratio)
  const rows = await sql<
    {
      date: string;
      nav: number;
      cash: number;
      gross_exposure: number;
      net_exposure: number;
      leverage: number;
      daily_return: number | null;
      cumulative_return: number | null;
    }[]
  >`
    SELECT
      date,
      nav::float                                 AS nav,
      cash::float                                AS cash,
      gross_exposure::float                      AS gross_exposure,
      net_exposure::float                        AS net_exposure,
      gross_exposure::float                      AS leverage,
      daily_return::float                        AS daily_return,
      cumulative_return::float                   AS cumulative_return
    FROM portfolio_nav
    ORDER BY date DESC
    LIMIT 1;
  `;
  return rows[0] ?? null;
}

export async function getEquityCurve() {
  return sql<
    { date: string; nav: number; daily_return: number | null; cumulative_return: number | null }[]
  >`
    SELECT
      date,
      nav::float               AS nav,
      daily_return::float      AS daily_return,
      cumulative_return::float AS cumulative_return
    FROM portfolio_nav
    ORDER BY date;
  `;
}

// ---------------------------------------------------------------------------
// Performance metrics
// ---------------------------------------------------------------------------

export async function getMetrics(period: Period = "all") {
  const rows = await sql`
    SELECT
      period, as_of,
      total_return::float AS total_return,
      ann_return::float   AS ann_return,
      ann_vol::float      AS ann_vol,
      sharpe::float       AS sharpe,
      sortino::float      AS sortino,
      max_drawdown::float AS max_drawdown,
      calmar::float       AS calmar,
      hit_rate::float     AS hit_rate,
      beta::float         AS beta,
      alpha::float        AS alpha
    FROM performance_metrics
    WHERE period = ${period}
    LIMIT 1;
  `;
  return rows[0] ?? null;
}

export async function getAllMetrics() {
  return sql`
    SELECT
      period, as_of,
      total_return::float AS total_return,
      ann_return::float   AS ann_return,
      ann_vol::float      AS ann_vol,
      sharpe::float       AS sharpe,
      sortino::float      AS sortino,
      max_drawdown::float AS max_drawdown,
      calmar::float       AS calmar,
      hit_rate::float     AS hit_rate,
      beta::float         AS beta,
      alpha::float        AS alpha
    FROM performance_metrics
    ORDER BY CASE period
      WHEN 'all' THEN 0 WHEN 'ytd' THEN 1 WHEN '1y' THEN 2
      WHEN '3m'  THEN 3 WHEN '1m'  THEN 4 ELSE 5
    END;
  `;
}

// ---------------------------------------------------------------------------
// Positions (synthesized from portfolio.positions_daily target weights)
// ---------------------------------------------------------------------------

export async function getCurrentPositions() {
  // We only track TARGET weights — quantity / avg_cost / unrealized_pnl are
  // synthesized so the UI's table contract stays the same.
  return sql<
    {
      ticker: string;
      name: string;
      sector: string;
      quantity: number;
      avg_cost: number;
      market_value: number;
      weight: number;
      unrealized_pnl: number;
    }[]
  >`
    WITH latest_date AS (
      SELECT MAX(date) AS d FROM portfolio.positions_daily
    ),
    latest_nav AS (
      SELECT nav::float AS nav FROM portfolio_nav ORDER BY date DESC LIMIT 1
    ),
    latest_px AS (
      SELECT DISTINCT ON (security_id)
        security_id,
        adj_close::float AS px
      FROM raw.ohlcv_daily
      ORDER BY security_id, date DESC
    )
    SELECT
      s.ticker,
      COALESCE(s.name, s.ticker)            AS name,
      COALESCE(s.sector, '-')               AS sector,
      (pd.target_weight * COALESCE(ln.nav, 0) / NULLIF(px.px, 0))::float AS quantity,
      COALESCE(px.px, 0)::float             AS avg_cost,
      (pd.target_weight * COALESCE(ln.nav, 0))::float AS market_value,
      pd.target_weight::float               AS weight,
      0::float                              AS unrealized_pnl
    FROM portfolio.positions_daily pd
    JOIN latest_date ld ON pd.date = ld.d
    JOIN securities s ON s.id = pd.security_id
    LEFT JOIN latest_nav ln ON TRUE
    LEFT JOIN latest_px px ON px.security_id = pd.security_id
    ORDER BY ABS(pd.target_weight) DESC;
  `;
}

export async function getSectorExposure() {
  return sql<{ sector: string; weight: number; count: number }[]>`
    WITH latest_date AS (
      SELECT MAX(as_of) AS d FROM positions
    )
    SELECT
      COALESCE(s.sector, '-') AS sector,
      SUM(pd.weight)::float AS weight,
      COUNT(*)::int AS count
    FROM positions pd
    JOIN latest_date ld ON pd.as_of = ld.d
    JOIN securities s ON s.id = pd.security_id
    WHERE pd.quantity != 0
    GROUP BY s.sector
    ORDER BY weight DESC;
  `;
}

// ---------------------------------------------------------------------------
// Trades (synthesized from positions_daily LAG diff)
// ---------------------------------------------------------------------------

export async function getRecentTrades(limit = 50) {
  return sql<
    {
      ticker: string;
      side: string;
      quantity: number;
      price: number;
      slippage_bps: number;
      commission: number;
      notional: number;
      executed_at: string;
      strategy: string | null;
      reason: string | null;
    }[]
  >`
    SELECT
      s.ticker,
      t.side,
      t.quantity::float      AS quantity,
      t.price::float         AS price,
      t.slippage_bps::float  AS slippage_bps,
      t.commission::float    AS commission,
      t.notional::float      AS notional,
      t.executed_at::text,
      st.name                AS strategy,
      t.reason
    FROM trades t
    JOIN securities s ON s.id = t.security_id
    LEFT JOIN strategies st ON st.id = t.strategy_id
    ORDER BY t.executed_at DESC
    LIMIT ${limit};
  `;
}

// ---------------------------------------------------------------------------
// Strategies (from `strategies` registry + per-sleeve attribution JSONB)
// ---------------------------------------------------------------------------

export async function getStrategies() {
  return sql<
    {
      id: number;
      name: string;
      description: string;
      allocation_weight: number;
      params: Record<string, unknown>;
      enabled: boolean;
    }[]
  >`
    SELECT
      id, name, description,
      allocation_weight::float AS allocation_weight,
      params, enabled
    FROM strategies
    ORDER BY allocation_weight DESC;
  `;
}

export async function getStrategySignals(strategyId: number, limit = 50) {
  return sql<{ ticker: string; signal: number; score: number; date: string }[]>`
    SELECT
      s.ticker,
      sd.signal::float AS signal,
      sd.score::float  AS score,
      sd.date
    FROM signals sd
    JOIN securities s ON s.id = sd.security_id
    WHERE sd.strategy_id = ${strategyId}
      AND sd.date = (SELECT MAX(date) FROM signals WHERE strategy_id = ${strategyId})
      AND sd.signal <> 0
    ORDER BY ABS(sd.signal) DESC
    LIMIT ${limit};
  `;
}

export async function getStrategyContribution() {
  return sql<
    {
      strategy: string;
      allocation_weight: number;
      net_flow: number;
      trade_count: number;
    }[]
  >`
    WITH trade_counts AS (
      SELECT strategy_id, COUNT(*) AS count
      FROM trades
      GROUP BY strategy_id
    )
    SELECT
      st.name                                              AS strategy,
      st.allocation_weight::float                          AS allocation_weight,
      0::float                                             AS net_flow,
      COALESCE(tc.count, 0)::int                           AS trade_count
    FROM strategies st
    LEFT JOIN trade_counts tc ON tc.strategy_id = st.id
    ORDER BY st.allocation_weight DESC;
  `;
}

// ---------------------------------------------------------------------------
// Risk
// ---------------------------------------------------------------------------

export async function getRiskLatest() {
  // factor_exposures dropped in Phase 3 — return empty object so the UI's
  // shape stays the same.
  const rows = await sql<
    {
      date: string;
      var_95: number;
      var_99: number;
      expected_shortfall: number;
      realized_vol: number;
      factor_exposures: Record<string, number>;
    }[]
  >`
    SELECT
      date,
      var_95::float             AS var_95,
      var_99::float             AS var_99,
      expected_shortfall::float AS expected_shortfall,
      realized_vol::float       AS realized_vol,
      '{}'::jsonb               AS factor_exposures
    FROM risk_metrics
    WHERE var_95 IS NOT NULL
    ORDER BY date DESC
    LIMIT 1;
  `;
  return rows[0] ?? null;
}

export async function getRiskHistory(days = 252) {
  return sql<{ date: string; var_95: number; realized_vol: number }[]>`
    SELECT
      date,
      var_95::float       AS var_95,
      realized_vol::float AS realized_vol
    FROM risk_metrics
    WHERE var_95 IS NOT NULL
      AND date >= (SELECT MAX(date) FROM risk_metrics) - (${days}::int * INTERVAL '1 day')
    ORDER BY date;
  `;
}

// ---------------------------------------------------------------------------
// Calendar / charts
// ---------------------------------------------------------------------------

export async function getMonthlyReturns() {
  return sql<{ year: number; month: number; ret: number }[]>`
    SELECT
      EXTRACT(YEAR FROM date)::int AS year,
      EXTRACT(MONTH FROM date)::int AS month,
      (EXP(SUM(LN(1 + COALESCE(daily_return, 0)))) - 1)::float AS ret
    FROM portfolio_nav
    GROUP BY EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)
    ORDER BY year, month;
  `;
}

export async function getDrawdownSeries() {
  return sql<{ date: string; nav: number; peak: number; drawdown: number }[]>`
    SELECT
      date,
      nav::float       AS nav,
      MAX(nav) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)::float AS peak,
      ((nav / NULLIF(MAX(nav) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0)) - 1)::float AS drawdown
    FROM portfolio_nav
    ORDER BY date;
  `;
}

export async function getRollingSharpe(window = 63) {
  // analytics.rolling_metrics stores rolling_1y_sharpe only. The chart asks
  // for a configurable window, so compute on the fly from daily_return —
  // this is one window function over ~1260 rows, well under the Vercel 10s
  // ceiling.
  return sql<{ date: string; sharpe: number }[]>`
    SELECT
      date,
      (AVG(daily_return) OVER w
        / NULLIF(STDDEV_SAMP(daily_return) OVER w, 0)
        * SQRT(252))::float AS sharpe
    FROM portfolio_nav
    WHERE daily_return IS NOT NULL
    WINDOW w AS (
      ORDER BY date
      ROWS BETWEEN ${sql.unsafe(String(window - 1))} PRECEDING AND CURRENT ROW
    )
    ORDER BY date;
  `;
}

// ---------------------------------------------------------------------------
// Backtests (registry table kept; written by workers/backtest.py)
// ---------------------------------------------------------------------------

export async function getBacktests() {
  return sql<
    {
      id: number;
      name: string;
      start_date: string;
      end_date: string;
      results: Record<string, number>;
      created_at: string;
    }[]
  >`
    SELECT id, name, start_date, end_date, results, created_at
    FROM backtests
    ORDER BY created_at DESC
    LIMIT 50;
  `;
}

export async function getBacktest(id: number) {
  const rows = await sql<
    {
      id: number;
      name: string;
      start_date: string;
      end_date: string;
      results: Record<string, number>;
      equity_curve: Array<{ date: string; nav: number; ret: number | null }>;
      strategy_config: Record<string, unknown>;
      created_at: string;
    }[]
  >`
    SELECT id, name, start_date, end_date, results, equity_curve,
           strategy_config, created_at
    FROM backtests
    WHERE id = ${id};
  `;
  return rows[0] ?? null;
}
