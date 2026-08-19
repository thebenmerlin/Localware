import { getEquityCurve, getDrawdownSeries, getRollingSharpe } from "@/lib/queries";
import { ChartCard } from "@/components/ChartCard";
import { HeroEquityChart } from "@/components/HeroEquityChart";

export const revalidate = 600;

export default async function PerformancePage() {
  const [equity, drawdown, rollingSharpe] = await Promise.all([
    getEquityCurve(),
    getDrawdownSeries(),
    getRollingSharpe(63),
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

  return (
    <div className="max-w-wide flex flex-col gap-10">
      <HeroEquityChart
        navSeries={navSeries}
        benchmarkSeries={benchmarkSeries}
        currentNav={currentNav}
        portfolioReturn={portfolioReturn}
        benchmarkReturn={benchmarkReturn}
      />

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-8 lg:gap-10">
        <ChartCard title="Drawdown from peak" data={ddSeries} color="var(--negative)" format="percent" zeroLine />
        <ChartCard title="Rolling Sharpe (63d)" data={sharpeSeries} color="var(--ink)" format="number" zeroLine />
      </div>
    </div>
  );
}
