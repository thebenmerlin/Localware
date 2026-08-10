import { getEquityCurve, getDrawdownSeries, getRollingSharpe } from "@/lib/queries";
import { LineChart } from "@/components/LineChart";

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
    <div className="flex flex-col gap-16">
      <section>
        <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
          Equity curve
        </h2>
        <div className="mt-4">
          <LineChart data={navSeries} color="var(--positive)" format="money" />
        </div>
      </section>

      <section>
        <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
          Drawdown from peak
        </h2>
        <div className="mt-4">
          <LineChart data={ddSeries} color="var(--negative)" format="percent" zeroLine />
        </div>
      </section>

      <section>
        <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
          Rolling Sharpe (63d)
        </h2>
        <div className="mt-4">
          <LineChart data={sharpeSeries} color="var(--ink)" format="number" zeroLine />
        </div>
      </section>
    </div>
  );
}
