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
      (nav * (1 - net_exposure))::float          AS cash,
      gross_exposure::float                      AS gross_exposure,
      net_exposure::float                        AS net_exposure,
      gross_exposure::float                      AS leverage,
      daily_return::float                        AS daily_return,
      cumulative_return::float                   AS cumulative_return
    FROM portfolio.nav_daily
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
    FROM analytics.equity_curve
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
    FROM analytics.performance_summary
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
    FROM analytics.performance_summary
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
  // synthesized:
  //   quantity        = target_weight * NAV / current_price
  //   avg_cost        = price on the security's first nonzero-weight date
  //                     (used as a cost-basis proxy since no trade ledger exists)
  //   unrealized_pnl  = quantity * (current_price - avg_cost)
  // adj_close can be NULL when the price feed only fills `close`, so coalesce.
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
      SELECT nav::float AS nav FROM portfolio.nav_daily
      WHERE nav IS NOT NULL
      ORDER BY date DESC LIMIT 1
    ),
    latest_px AS (
      SELECT DISTINCT ON (security_id)
        security_id,
        COALESCE(adj_close, close)::float AS px
      FROM raw.ohlcv_daily
      WHERE COALESCE(adj_close, close) IS NOT NULL
      ORDER BY security_id, date DESC
    ),
    entry_date AS (
      SELECT security_id, MIN(date) AS first_date
      FROM portfolio.positions_daily
      WHERE target_weight <> 0
      GROUP BY security_id
    ),
    entry_px AS (
      SELECT
        ed.security_id,
        (SELECT COALESCE(o.adj_close, o.close) FROM raw.ohlcv_daily o
          WHERE o.security_id = ed.security_id
            AND o.date <= ed.first_date
            AND COALESCE(o.adj_close, o.close) IS NOT NULL
          ORDER BY o.date DESC LIMIT 1)::float AS px
      FROM entry_date ed
    )
    SELECT
      s.ticker,
      COALESCE(s.name, s.ticker)            AS name,
      COALESCE(s.sector, '-')               AS sector,
      (pd.target_weight * COALESCE(ln.nav, 0) / NULLIF(px.px, 0))::float AS quantity,
      COALESCE(ep.px, px.px, 0)::float      AS avg_cost,
      (pd.target_weight * COALESCE(ln.nav, 0))::float AS market_value,
      pd.target_weight::float               AS weight,
      ((pd.target_weight * COALESCE(ln.nav, 0) / NULLIF(px.px, 0))
        * (COALESCE(px.px, 0) - COALESCE(ep.px, px.px, 0)))::float AS unrealized_pnl
    FROM portfolio.positions_daily pd
    JOIN latest_date ld ON pd.date = ld.d
    JOIN securities s ON s.id = pd.security_id
    LEFT JOIN latest_nav ln ON TRUE
    LEFT JOIN latest_px px ON px.security_id = pd.security_id
    LEFT JOIN entry_px ep ON ep.security_id = pd.security_id
    ORDER BY ABS(pd.target_weight) DESC;
  `;
}

export async function getSectorExposure() {
  return sql<{ sector: string; weight: number; count: number }[]>`
    WITH latest_date AS (
      SELECT MAX(date) AS d FROM portfolio.positions_daily
    )
    SELECT
      COALESCE(pd.sector, '-') AS sector,
      SUM(pd.target_weight)::float AS weight,
      COUNT(*)::int AS count
    FROM portfolio.positions_daily pd
    JOIN latest_date ld ON pd.date = ld.d
    GROUP BY pd.sector
    ORDER BY weight DESC;
  `;
}

// ---------------------------------------------------------------------------
// Trades (synthesized from positions_daily LAG diff)
// ---------------------------------------------------------------------------

export async function getRecentTrades(limit = 50) {
  // No trades table exists in the new schema. Each row here represents an
  // implied rebalance: the per-name target_weight change at a given date,
  // scaled to dollar notional by that day's NAV.
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
    WITH diffs AS (
      SELECT
        pd.security_id,
        pd.date,
        pd.target_weight::float AS target_weight,
        LAG(pd.target_weight) OVER (PARTITION BY pd.security_id ORDER BY pd.date)::float
          AS prev_weight
      FROM portfolio.positions_daily pd
    )
    SELECT
      s.ticker,
      CASE WHEN (d.target_weight - COALESCE(d.prev_weight, 0)) > 0
        THEN 'BUY' ELSE 'SELL' END                                       AS side,
      ABS((d.target_weight - COALESCE(d.prev_weight, 0))
          * COALESCE(n.nav, 0) / NULLIF(o.adj_close, 0))::float          AS quantity,
      COALESCE(o.adj_close, 0)::float                                    AS price,
      10.0::float                                                        AS slippage_bps,
      0::float                                                           AS commission,
      ABS((d.target_weight - COALESCE(d.prev_weight, 0))
          * COALESCE(n.nav, 0))::float                                   AS notional,
      ((o.date::timestamp) + INTERVAL '16 hours')::text                  AS executed_at,
      NULL::text                                                         AS strategy,
      ('weight ' || ROUND(COALESCE(d.prev_weight, 0)::numeric, 4)
                 || ' → ' || ROUND(d.target_weight::numeric, 4))         AS reason
    FROM diffs d
    JOIN securities s                ON s.id = d.security_id
    LEFT JOIN LATERAL (
      SELECT COALESCE(adj_close, close) AS adj_close, date
      FROM raw.ohlcv_daily
      WHERE security_id = d.security_id
        AND date < d.date
        AND COALESCE(adj_close, close) IS NOT NULL
      ORDER BY date DESC LIMIT 1
    ) o ON TRUE
    LEFT JOIN LATERAL (
      SELECT nav
      FROM portfolio.nav_daily
      WHERE date < d.date AND nav IS NOT NULL
      ORDER BY date DESC LIMIT 1
    ) n ON TRUE
    WHERE (d.target_weight - COALESCE(d.prev_weight, 0)) <> 0
    ORDER BY d.date DESC, ABS(d.target_weight - COALESCE(d.prev_weight, 0)) DESC
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
  // The sleeve name lives on `strategies`; the sleeve's per-name contribution
  // lives in `derived.signals_daily.attribution` (JSONB keyed by sleeve name).
  const stratRows = await sql<{ name: string }[]>`
    SELECT name FROM strategies WHERE id = ${strategyId} LIMIT 1;
  `;
  if (!stratRows[0]) return [];
  const name = stratRows[0].name;

  return sql<{ ticker: string; signal: number; score: number; date: string }[]>`
    SELECT
      s.ticker,
      (sd.attribution->>${name})::float AS signal,
      sd.blended_signal::float          AS score,
      sd.date
    FROM derived.signals_daily sd
    JOIN securities s ON s.id = sd.security_id
    WHERE sd.date = (SELECT MAX(date) FROM derived.signals_daily)
      AND sd.attribution ? ${name}
      AND (sd.attribution->>${name})::float <> 0
    ORDER BY ABS((sd.attribution->>${name})::float) DESC
    LIMIT ${limit};
  `;
}

export async function getStrategyContribution() {
  // Synthesized: per-sleeve dollar exposure on the latest signal date, plus
  // the count of names that sleeve is currently picking. `net_flow` is now
  // "current dollar contribution" rather than historical cash flow, but the
  // sign/magnitude story is the same.
  return sql<
    {
      strategy: string;
      allocation_weight: number;
      net_flow: number;
      trade_count: number;
    }[]
  >`
    WITH latest AS (
      SELECT MAX(date) AS d FROM derived.signals_daily
    ),
    latest_nav AS (
      SELECT nav::float AS nav FROM portfolio.nav_daily ORDER BY date DESC LIMIT 1
    ),
    contribs AS (
      SELECT
        kv.key                    AS strategy,
        SUM((kv.value)::float)    AS net_signal,
        COUNT(*) FILTER (WHERE (kv.value)::float <> 0) AS active_names
      FROM derived.signals_daily sd
      CROSS JOIN LATERAL jsonb_each_text(sd.attribution) AS kv(key, value)
      WHERE sd.date = (SELECT d FROM latest)
      GROUP BY kv.key
    )
    SELECT
      st.name                                              AS strategy,
      st.allocation_weight::float                          AS allocation_weight,
      COALESCE(c.net_signal * (SELECT nav FROM latest_nav), 0)::float AS net_flow,
      COALESCE(c.active_names, 0)::int                     AS trade_count
    FROM strategies st
    LEFT JOIN contribs c ON c.strategy = st.name
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
    FROM analytics.var_daily
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
    FROM analytics.var_daily
    WHERE var_95 IS NOT NULL
      AND date >= (SELECT MAX(date) FROM analytics.var_daily) - (${days}::int * INTERVAL '1 day')
    ORDER BY date;
  `;
}

// ---------------------------------------------------------------------------
// Calendar / charts
// ---------------------------------------------------------------------------

export async function getMonthlyReturns() {
  return sql<{ year: number; month: number; ret: number }[]>`
    SELECT
      year,
      month,
      total_return::float AS ret
    FROM analytics.monthly_returns
    ORDER BY year, month;
  `;
}

export async function getDrawdownSeries() {
  return sql<{ date: string; nav: number; peak: number; drawdown: number }[]>`
    SELECT
      date,
      nav::float       AS nav,
      peak_nav::float  AS peak,
      drawdown::float  AS drawdown
    FROM analytics.equity_curve
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
    FROM analytics.equity_curve
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

// ---------------------------------------------------------------------------
// Per-ticker historical prices (powers the hover sparkline)
// ---------------------------------------------------------------------------

export async function getTickerPrices(ticker: string, days = 30) {
  return sql<{ date: string; close: number }[]>`
    SELECT
      o.date,
      COALESCE(o.adj_close, o.close)::float AS close
    FROM raw.ohlcv_daily o
    JOIN securities s ON s.id = o.security_id
    WHERE UPPER(s.ticker) = UPPER(${ticker})
      AND COALESCE(o.adj_close, o.close) IS NOT NULL
    ORDER BY o.date DESC
    LIMIT ${days};
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
