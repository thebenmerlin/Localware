"use client";

import { LineChart, type LineChartPoint } from "./LineChart";

function fullMoney(x: number | null): string {
  if (x === null || Number.isNaN(x)) return "—";
  const sign = x < 0 ? "-" : "";
  return `${sign}$${Math.abs(x).toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
}

function signedPct(x: number | null): string {
  if (x === null || Number.isNaN(x)) return "—";
  return `${x >= 0 ? "+" : ""}${(x * 100).toFixed(1)}%`;
}

/**
 * The performance page's flagship chart: a full-width, animated, glowing
 * equity curve with an optional benchmark overlay. Everything else on the
 * page (drawdown, rolling Sharpe) is a supporting ChartCard beneath this.
 */
export function HeroEquityChart({
  navSeries,
  benchmarkSeries,
  currentNav,
  portfolioReturn,
  benchmarkReturn,
}: {
  navSeries: LineChartPoint[];
  benchmarkSeries: LineChartPoint[];
  currentNav: number | null;
  portfolioReturn: number | null;
  benchmarkReturn: number | null;
}) {
  return (
    <div className="hover-glow w-full rounded-lg border border-[var(--faint)]/30 px-6 py-6 md:px-8 md:py-8">
      <div className="flex flex-wrap items-end justify-between gap-6">
        <div>
          <div className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
            Equity curve
          </div>
          <div
            className="tabular font-display text-4xl md:text-5xl font-semibold mt-1"
            style={{ color: "var(--positive)" }}
          >
            {fullMoney(currentNav)}
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-x-6 gap-y-2 font-mono text-xs">
          <div className="flex items-center gap-2">
            <span className="inline-block h-2 w-2 rounded-full" style={{ background: "var(--positive)" }} />
            <span className="text-[var(--muted)]">Portfolio</span>
            <span className="tabular" style={{ color: "var(--positive)" }}>
              {signedPct(portfolioReturn)}
            </span>
          </div>
          {benchmarkSeries.length > 0 && (
            <div className="flex items-center gap-2">
              <span
                className="inline-block h-2 w-3 rounded-sm"
                style={{
                  backgroundImage: "repeating-linear-gradient(90deg, var(--accent) 0 3px, transparent 3px 5px)",
                }}
              />
              <span className="text-[var(--muted)]">S&amp;P 500</span>
              <span className="tabular" style={{ color: "var(--accent)" }}>
                {signedPct(benchmarkReturn)}
              </span>
            </div>
          )}
        </div>
      </div>

      <div className="mt-8">
        <LineChart
          data={navSeries}
          color="var(--positive)"
          format="money"
          height={380}
          glow
          animate
          secondary={benchmarkSeries.length ? { data: benchmarkSeries, color: "var(--accent)" } : undefined}
        />
      </div>
    </div>
  );
}
