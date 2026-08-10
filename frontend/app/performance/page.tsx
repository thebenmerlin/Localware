import { getEquityCurve, getDrawdownSeries, getRollingSharpe } from "@/lib/queries";
import { LineChart } from "@/components/LineChart";
import { num } from "@/lib/format";

export const revalidate = 600;

function fmtDate(d: string) {
  return String(d).slice(0, 10);
}

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
          <LineChart
            data={navSeries}
            color="var(--positive)"
            formatValue={(v) => `$${v.toLocaleString("en-US", { maximumFractionDigits: 0 })}`}
            formatDate={fmtDate}
          />
        </div>
      </section>

      <section>
        <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
          Drawdown from peak
        </h2>
        <div className="mt-4">
          <LineChart
            data={ddSeries}
            color="var(--negative)"
            formatValue={(v) => `${v.toFixed(1)}%`}
            formatDate={fmtDate}
            zeroLine
          />
        </div>
      </section>

      <section>
        <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
          Rolling Sharpe (63d)
        </h2>
        <div className="mt-4">
          <LineChart
            data={sharpeSeries}
            color="var(--ink)"
            formatValue={(v) => num(v)}
            formatDate={fmtDate}
            zeroLine
          />
        </div>
      </section>
    </div>
  );
}
