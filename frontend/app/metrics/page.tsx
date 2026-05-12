import { headers } from "next/headers";
import { Figure } from "@/components/paper/Figure";
import { KPI } from "@/components/paper/KPI";
import { AcademicLine } from "@/components/charts/AcademicLine";
import { MonthlyHeatmap } from "@/components/charts/Heatmap";
import { pct, num, signed } from "@/lib/format";

export const revalidate = 300;

type Metric = {
  period: string; as_of: string; total_return: number; ann_return: number;
  ann_vol: number; sharpe: number; sortino: number; max_drawdown: number;
  calmar: number; hit_rate: number; beta: number | null; alpha: number | null;
};
type EquityPt = { date: string; nav: number; daily_return: number | null; cumulative_return: number | null };
type DrawdownPt = { date: string; nav: number; peak: number; drawdown: number };
type MonthlyPt = { year: number; month: number; ret: number };
type RollingPt = { date: string; sharpe: number | null };

async function fetchPerformance(): Promise<{
  equity: EquityPt[]; metrics: Metric[]; drawdown: DrawdownPt[];
  monthly: MonthlyPt[]; rollingSharpe: RollingPt[];
}> {
  const h = await headers();
  const host = h.get("host") ?? "localhost:3000";
  const proto = h.get("x-forwarded-proto") ?? "http";
  const res = await fetch(`${proto}://${host}/api/performance`, { cache: "no-store" });
  if (!res.ok) throw new Error(`/api/performance ${res.status}`);
  return res.json();
}

const MONTH_NAMES = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

export default async function Page() {
  const data = await fetchPerformance();
  const all = data.metrics.find((m) => m.period === "all");
  const ytd = data.metrics.find((m) => m.period === "ytd");
  const y1  = data.metrics.find((m) => m.period === "1y");
  const m3  = data.metrics.find((m) => m.period === "3m");
  const m1  = data.metrics.find((m) => m.period === "1m");

  const months = (data.monthly || []).map((m) => ({ ...m, ret: Number(m.ret) }));
  const valid = months.filter((m) => Number.isFinite(m.ret));
  const best  = valid.length ? valid.reduce((a, b) => (b.ret > a.ret ? b : a)) : null;
  const worst = valid.length ? valid.reduce((a, b) => (b.ret < a.ret ? b : a)) : null;
  const posMonths = valid.filter((m) => m.ret > 0).length;
  const monthHitRate = valid.length ? posMonths / valid.length : 0;

  const rollData = (data.rollingSharpe || []).map((r) => ({
    date: String(r.date).slice(0, 10),
    sharpe: r.sharpe == null ? null : Number(r.sharpe),
  }));
  const ddData = (data.drawdown || []).map((r) => ({
    date: String(r.date).slice(0, 10),
    drawdown: Number(r.drawdown),
  }));

  const cur = ddData.length ? ddData[ddData.length - 1].drawdown : 0;
  const inceptionDate = data.equity?.[0]?.date ? String(data.equity[0].date).slice(0,10) : "—";
  const asOfDate = data.equity?.length ? String(data.equity[data.equity.length-1].date).slice(0,10) : "—";

  const ann = Number(all?.ann_return ?? 0);
  const sharpe = Number(all?.sharpe ?? 0);
  const sortino = Number(all?.sortino ?? 0);
  const calmar = Number(all?.calmar ?? 0);
  const vol = Number(all?.ann_vol ?? 0);
  const mdd = Number(all?.max_drawdown ?? 0);
  const beta = Number(all?.beta ?? 0);
  const alpha = Number(all?.alpha ?? 0);
  const winRate = Number(all?.hit_rate ?? 0);

  function fmtMonth(m: MonthlyPt | null) {
    if (!m) return "—";
    return `${MONTH_NAMES[m.month - 1]} ${m.year}`;
  }

  return (
    <div className="space-y-2">
      <header className="text-center">
        <div className="smallcaps">Fund Fact Sheet</div>
        <h1 className="!mt-1">Localware Capital — Multi-Factor Equity</h1>
        <div className="caption !mt-1 not-italic">
          As of {asOfDate} · Inception {inceptionDate} · Reporting currency USD
        </div>
      </header>

      <hr className="rule" />

      <h4>Headline statistics — since inception</h4>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3 mt-2">
        <KPI label="Sharpe ratio"        value={num(sharpe, 2)}
             tone={sharpe >= 1 ? "positive" : sharpe < 0 ? "negative" : "neutral"}
             sub="ann. risk-adjusted" />
        <KPI label="Annualised return"   value={pct(ann)}
             tone={ann >= 0 ? "positive" : "negative"}
             sub={`Total ${pct(Number(all?.total_return ?? 0))}`} />
        <KPI label="Max drawdown"        value={pct(mdd)} tone="negative"
             sub={`Now ${pct(-cur, 1)} below peak`} />
        <KPI label="Sortino ratio"       value={num(sortino, 2)}
             tone={sortino >= 1 ? "positive" : "neutral"}
             sub="downside-only" />
        <KPI label="Calmar ratio"        value={num(calmar, 2)}
             tone={calmar >= 0.5 ? "positive" : "neutral"}
             sub="return ÷ |max DD|" />
        <KPI label="Annualised vol"      value={pct(vol)}
             sub="ex-post realised" />
        <KPI label="Best month"          value={best ? pct(best.ret, 1) : "—"}
             tone="positive"
             sub={fmtMonth(best)} />
        <KPI label="Worst month"         value={worst ? pct(worst.ret, 1) : "—"}
             tone="negative"
             sub={fmtMonth(worst)} />
        <KPI label="Win rate (daily)"    value={pct(winRate, 1)}
             sub={`Monthly ${pct(monthHitRate, 1)}`} />
        <KPI label="Beta (vs SPY)"       value={num(beta, 2)}
             sub="OLS, daily" />
        <KPI label="Alpha (annualised)"  value={pct(alpha, 2)}
             tone={alpha >= 0 ? "positive" : "negative"}
             sub="Jensen, vs SPY" />
        <KPI label="Total return"        value={pct(Number(all?.total_return ?? 0))}
             tone={Number(all?.total_return ?? 0) >= 0 ? "positive" : "negative"}
             sub={`Since ${inceptionDate}`} />
      </div>

      <h4>Trailing returns</h4>
      <table className="paper">
        <thead>
          <tr>
            <th>Window</th>
            <th className="num">Return</th>
            <th className="num">Ann. return</th>
            <th className="num">Ann. vol</th>
            <th className="num">Sharpe</th>
            <th className="num">Sortino</th>
            <th className="num">Max DD</th>
          </tr>
        </thead>
        <tbody>
          {[
            ["1m", m1, "Trailing 1 month"],
            ["3m", m3, "Trailing 3 months"],
            ["ytd", ytd, "Year to date"],
            ["1y", y1, "Trailing 12 months"],
            ["all", all, "Since inception"],
          ].map(([code, m, label]) => {
            const r = (m as Metric | undefined) ?? null;
            return (
              <tr key={code as string}>
                <td>{label as string}</td>
                <td className={`num ${Number(r?.total_return ?? 0) >= 0 ? "text-positive" : "text-negative"}`}>
                  {pct(Number(r?.total_return))}
                </td>
                <td className="num">{pct(Number(r?.ann_return))}</td>
                <td className="num">{pct(Number(r?.ann_vol))}</td>
                <td className="num">{num(Number(r?.sharpe))}</td>
                <td className="num">{num(Number(r?.sortino))}</td>
                <td className="num text-negative">{pct(Number(r?.max_drawdown))}</td>
              </tr>
            );
          })}
        </tbody>
      </table>

      <Figure
        number="A"
        title="Rolling 63-day Sharpe"
        caption={
          <>
            Rolling annualised Sharpe over a three-month window. Reference line at zero;
            sustained values above one indicate persistent risk-adjusted skill.
          </>
        }
      >
        <AcademicLine
          data={rollData}
          xKey="date"
          yKeys={[{ key: "sharpe", color: "ink" }]}
          height={200}
          refY={0}
          yFmt="num2"
          xFmt="YYYY-MM"
        />
      </Figure>

      <Figure
        number="B"
        title="Drawdown waterfall"
        caption={
          <>
            Underwater plot: peak-to-trough decline from running maximum NAV. The dashed line
            marks the 9% risk-overlay threshold above which gross exposure is cut to 60%.
          </>
        }
      >
        <AcademicLine
          data={ddData}
          xKey="date"
          yKeys={[{ key: "drawdown", color: "crimson" }]}
          height={220}
          fill
          refY={-0.09}
          yFmt="pct1"
          xFmt="YYYY-MM"
        />
      </Figure>

      <Figure
        number="C"
        title="Monthly returns"
        caption={
          <>
            Each cell is a calendar-month total return (basis points), shaded by sign and
            magnitude. The right column is the calendar-year cumulative total.
          </>
        }
      >
        <MonthlyHeatmap data={months} />
      </Figure>

      <h4>Definitions</h4>
      <div className="grid lg:grid-cols-2 gap-x-8 text-small text-muted">
        <ul className="list-none ml-0 space-y-1">
          <li><span className="font-mono text-ink">Sharpe</span> &mdash; mean(r) ÷ stdev(r) × √252.</li>
          <li><span className="font-mono text-ink">Sortino</span> &mdash; mean(r) ÷ stdev(r | r&lt;0) × √252.</li>
          <li><span className="font-mono text-ink">Calmar</span> &mdash; annualised return ÷ |max drawdown|.</li>
          <li><span className="font-mono text-ink">Beta</span> &mdash; OLS slope of daily returns on SPY.</li>
        </ul>
        <ul className="list-none ml-0 space-y-1">
          <li><span className="font-mono text-ink">Alpha</span> &mdash; OLS intercept × 252.</li>
          <li><span className="font-mono text-ink">Win rate</span> &mdash; fraction of days with strictly positive return.</li>
          <li><span className="font-mono text-ink">Best / worst month</span> &mdash; extremes of calendar-month returns.</li>
          <li><span className="font-mono text-ink">Max drawdown</span> &mdash; worst (NAV − running peak) / running peak.</li>
        </ul>
      </div>

      <div className="caption text-center mt-6">
        Live calculation pulled from <span className="font-mono not-italic">GET /api/performance</span>{" "}
        &middot; updated on every page load.
      </div>
    </div>
  );
}
