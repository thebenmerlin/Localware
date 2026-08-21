import { getEquityCurve, getDrawdownSeries, getRollingSharpe, getAllMetrics, getMonthlyReturns } from "@/lib/queries";
import { ChartCard } from "@/components/ChartCard";
import { HeroEquityChart } from "@/components/HeroEquityChart";
import { PerformanceMetricsTable } from "@/components/PerformanceMetricsTable";
import { MonthlyReturnsHeatmap } from "@/components/MonthlyReturnsHeatmap";
import { pct, num } from "@/lib/format";

export const revalidate = 600;

export default async function PerformancePage() {
  const [equity, drawdown, rollingSharpe, allMetrics, monthly] = await Promise.all([
    getEquityCurve(),
    getDrawdownSeries(),
    getRollingSharpe(63),
    getAllMetrics(),
    getMonthlyReturns(),
  ]);

  // Prefer the intersection of nav + benchmark rows so both series line up
  // point-for-point (LineChart's secondary overlay maps by index, not date).
  // Falls back to the full nav history, benchmark-free, if benchmark_cumret
  // hasn't been backfilled yet.
  const navAll = equity.filter((r) => r.nav !== null);
  const withBenchmark = navAll.filter((r) => r.benchmark_cumret !== null);
  const hasBenchmark = withBenchmark.length > 0;
  const base = hasBenchmark ? withBenchmark : navAll;
  const firstNav = base[0]?.nav ?? null;

  const navSeries = base.map((r) => ({ date: r.date, value: Number(r.nav) }));
  const benchmarkSeries =
    hasBenchmark && firstNav !== null
      ? withBenchmark.map((r) => ({ date: r.date, value: firstNav * (1 + Number(r.benchmark_cumret)) }))
      : [];

  const currentNav = navSeries.at(-1)?.value ?? null;
  const portfolioReturn = base.at(-1)?.cumulative_return ?? null;
  const benchmarkReturn = hasBenchmark ? withBenchmark.at(-1)?.benchmark_cumret ?? null : null;

  const ddSeries = drawdown
    .filter((r) => r.drawdown !== null)
    .map((r) => ({ date: r.date, value: Number(r.drawdown) * 100 }));

  const sharpeSeries = rollingSharpe
    .filter((r) => r.sharpe !== null)
    .map((r) => ({ date: r.date, value: Number(r.sharpe) }));

  const sinceInception = allMetrics.find((m) => m.period === "all") ?? null;
  const ddCurr = drawdown.at(-1)?.drawdown ?? null;

  return (
    <div className="max-w-wide flex flex-col gap-12">
      {sinceInception && (
        <p className="font-display text-lg md:text-xl leading-relaxed max-w-3xl text-[var(--ink)]">
          Since inception, the fund has returned{" "}
          <span
            className="tabular font-semibold"
            style={{ color: (sinceInception.total_return ?? 0) >= 0 ? "var(--positive)" : "var(--negative)" }}
          >
            {pct(sinceInception.total_return, 1)}
          </span>
          , annualizing to <span className="tabular font-semibold">{pct(sinceInception.ann_return, 1)}</span> at{" "}
          <span className="tabular font-semibold">{pct(sinceInception.ann_vol, 1)}</span> volatility (Sharpe{" "}
          <span className="tabular font-semibold">{num(sinceInception.sharpe, 2)}</span>). The book is currently{" "}
          <span className="tabular font-semibold" style={{ color: "var(--negative)" }}>
            {pct(ddCurr !== null ? Math.abs(ddCurr) : null, 1)}
          </span>{" "}
          below its high-water mark.
        </p>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 lg:gap-10 items-stretch">
        <div className="lg:col-span-2">
          <HeroEquityChart
            navSeries={navSeries}
            benchmarkSeries={benchmarkSeries}
            currentNav={currentNav}
            portfolioReturn={portfolioReturn}
            benchmarkReturn={benchmarkReturn}
            height={280}
          />
        </div>
        <div className="flex flex-col gap-6">
          <ChartCard title="Drawdown from peak" data={ddSeries} color="var(--negative)" format="percent" zeroLine height={130} />
          <ChartCard title="Rolling Sharpe (63d)" data={sharpeSeries} color="var(--ink)" format="number" zeroLine height={130} />
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 lg:gap-10">
        <section>
          <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">Key metrics</h2>
          <div className="mt-5">
            <PerformanceMetricsTable metrics={allMetrics} />
          </div>
        </section>

        {monthly.length > 0 && (
          <section>
            <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
              Monthly returns
            </h2>
            <div className="mt-5">
              <MonthlyReturnsHeatmap rows={monthly} />
            </div>
          </section>
        )}
      </div>
    </div>
  );
}
