import { getEquityCurve, getDrawdownSeries, getRollingSharpe } from "@/lib/queries";
import { ChartCard } from "@/components/ChartCard";

export const revalidate = 600;

export default async function PerformancePage() {
  const [equity, drawdown, rollingSharpe] = await Promise.all([
    getEquityCurve(),
    getDrawdownSeries(),
    getRollingSharpe(63),
  ]);

  const navSeries = equity
    .filter((r) => r.nav !== null)
    .map((r) => ({ date: r.date, value: Number(r.nav) }));

  const ddSeries = drawdown
    .filter((r) => r.drawdown !== null)
    .map((r) => ({ date: r.date, value: Number(r.drawdown) * 100 }));

  const sharpeSeries = rollingSharpe
    .filter((r) => r.sharpe !== null)
    .map((r) => ({ date: r.date, value: Number(r.sharpe) }));

  return (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-6">
      <ChartCard title="Equity curve" data={navSeries} color="var(--positive)" format="money" />
      <ChartCard title="Drawdown from peak" data={ddSeries} color="var(--negative)" format="percent" zeroLine />
      <ChartCard title="Rolling Sharpe (63d)" data={sharpeSeries} color="var(--ink)" format="number" zeroLine />
    </div>
  );
}
