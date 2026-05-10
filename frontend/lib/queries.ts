import { sql } from "./db";

export type Period = "all" | "ytd" | "1y" | "3m" | "1m";

export async function getLatestNav() {
  const rows = await sql<{ date: string; nav: number; cash: number; gross_exposure: number; net_exposure: number; leverage: number; daily_return: number | null; cumulative_return: number | null }[]>`
    SELECT date, nav, cash, gross_exposure, net_exposure, leverage, daily_return, cumulative_return
    FROM portfolio_nav ORDER BY date DESC LIMIT 1;
  `;
  return rows[0] ?? null;
}

export async function getEquityCurve() {
  return sql<{ date: string; nav: number; daily_return: number | null; cumulative_return: number | null }[]>`
    SELECT date, nav, daily_return, cumulative_return
    FROM portfolio_nav ORDER BY date;
  `;
}

export async function getMetrics(period: Period = "all") {
  const rows = await sql`
    SELECT * FROM performance_metrics
    WHERE period = ${period}
    ORDER BY as_of DESC LIMIT 1;
  `;
  return rows[0] ?? null;
}

export async function getAllMetrics() {
  return sql`
    SELECT DISTINCT ON (period) period, as_of, total_return, ann_return, ann_vol,
           sharpe, sortino, max_drawdown, calmar, hit_rate, beta, alpha
    FROM performance_metrics
    ORDER BY period, as_of DESC;
  `;
}

export async function getCurrentPositions() {
  return sql<{ ticker: string; name: string; sector: string; quantity: number; avg_cost: number; market_value: number; weight: number; unrealized_pnl: number; }[]>`
    SELECT s.ticker, s.name, s.sector,
           p.quantity, p.avg_cost, p.market_value, p.weight, p.unrealized_pnl
    FROM positions p
    JOIN securities s ON s.id = p.security_id
    WHERE p.as_of = (SELECT MAX(as_of) FROM positions)
    ORDER BY p.market_value DESC;
  `;
}

export async function getRecentTrades(limit = 50) {
  return sql<{ ticker: string; side: string; quantity: number; price: number; slippage_bps: number; commission: number; notional: number; executed_at: string; strategy: string | null; reason: string | null }[]>`
    SELECT s.ticker, t.side, t.quantity, t.price, t.slippage_bps, t.commission,
           t.notional, t.executed_at, st.name AS strategy, t.reason
    FROM trades t
    JOIN securities s ON s.id = t.security_id
    LEFT JOIN strategies st ON st.id = t.strategy_id
    ORDER BY t.executed_at DESC LIMIT ${limit};
  `;
}

export async function getStrategies() {
  return sql<{ id: number; name: string; description: string; allocation_weight: number; params: Record<string, unknown>; enabled: boolean }[]>`
    SELECT id, name, description, allocation_weight, params, enabled
    FROM strategies ORDER BY allocation_weight DESC;
  `;
}

export async function getStrategySignals(strategyId: number, limit = 50) {
  return sql`
    SELECT s.ticker, sg.signal, sg.score, sg.date
    FROM signals sg JOIN securities s ON s.id = sg.security_id
    WHERE sg.strategy_id = ${strategyId} AND sg.date = (
      SELECT MAX(date) FROM signals WHERE strategy_id = ${strategyId}
    )
    ORDER BY ABS(sg.signal) DESC LIMIT ${limit};
  `;
}

export async function getRiskLatest() {
  const rows = await sql<{ date: string; var_95: number; var_99: number; expected_shortfall: number; realized_vol: number; factor_exposures: Record<string, number> }[]>`
    SELECT date, var_95, var_99, expected_shortfall, realized_vol, factor_exposures
    FROM risk_metrics ORDER BY date DESC LIMIT 1;
  `;
  return rows[0] ?? null;
}

export async function getRiskHistory(days = 252) {
  return sql<{ date: string; var_95: number; realized_vol: number }[]>`
    SELECT date, var_95, realized_vol
    FROM risk_metrics
    WHERE date >= (SELECT MAX(date) FROM risk_metrics) - INTERVAL '${sql.unsafe(String(days))} days'
    ORDER BY date;
  `;
}

export async function getMonthlyReturns() {
  return sql<{ year: number; month: number; ret: number }[]>`
    WITH monthly AS (
      SELECT DATE_TRUNC('month', date)::date AS m,
             (PRODUCT(1 + COALESCE(daily_return, 0)) - 1) AS ret
      FROM portfolio_nav
      GROUP BY 1
    )
    SELECT EXTRACT(YEAR FROM m)::int AS year,
           EXTRACT(MONTH FROM m)::int AS month,
           ret
    FROM monthly
    ORDER BY m;
  `;
}

export async function getDrawdownSeries() {
  return sql<{ date: string; nav: number; peak: number; drawdown: number }[]>`
    WITH eq AS (
      SELECT date, nav,
             MAX(nav) OVER (ORDER BY date) AS peak
      FROM portfolio_nav
    )
    SELECT date, nav, peak, (nav - peak) / peak AS drawdown
    FROM eq ORDER BY date;
  `;
}

export async function getRollingSharpe(window = 63) {
  return sql<{ date: string; sharpe: number }[]>`
    WITH r AS (
      SELECT date, daily_return FROM portfolio_nav WHERE daily_return IS NOT NULL
    )
    SELECT date,
           AVG(daily_return) OVER w / NULLIF(STDDEV_SAMP(daily_return) OVER w, 0) * SQRT(252) AS sharpe
    FROM r
    WINDOW w AS (ORDER BY date ROWS BETWEEN ${sql.unsafe(String(window - 1))} PRECEDING AND CURRENT ROW)
    ORDER BY date;
  `;
}

export async function getStrategyContribution() {
  return sql`
    SELECT st.name AS strategy, st.allocation_weight,
           COALESCE(SUM(t.notional * CASE WHEN t.side = 'BUY' THEN -1 ELSE 1 END), 0) AS net_flow,
           COUNT(*) AS trade_count
    FROM strategies st
    LEFT JOIN trades t ON t.strategy_id = st.id
    GROUP BY st.id, st.name, st.allocation_weight
    ORDER BY st.allocation_weight DESC;
  `;
}

export async function getBacktests() {
  return sql<{ id: number; name: string; start_date: string; end_date: string; results: Record<string, number>; created_at: string }[]>`
    SELECT id, name, start_date, end_date, results, created_at
    FROM backtests ORDER BY created_at DESC LIMIT 50;
  `;
}

export async function getBacktest(id: number) {
  const rows = await sql<{ id: number; name: string; start_date: string; end_date: string; results: Record<string, number>; equity_curve: Array<{ date: string; nav: number; ret: number | null }>; strategy_config: Record<string, unknown>; created_at: string }[]>`
    SELECT * FROM backtests WHERE id = ${id};
  `;
  return rows[0] ?? null;
}

export async function getSectorExposure() {
  return sql<{ sector: string; weight: number; count: number }[]>`
    SELECT s.sector, SUM(p.weight)::float AS weight, COUNT(*)::int AS count
    FROM positions p JOIN securities s ON s.id = p.security_id
    WHERE p.as_of = (SELECT MAX(as_of) FROM positions)
    GROUP BY s.sector ORDER BY weight DESC;
  `;
}
