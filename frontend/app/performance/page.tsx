import {
  getEquityCurve,
  getAllMetrics,
  getDrawdownSeries,
  getMonthlyReturns,
  getRollingSharpe,
} from "@/lib/queries";
import { Figure } from "@/components/paper/Figure";
import { PaperTable } from "@/components/paper/PaperTable";
import { AcademicLine } from "@/components/charts/AcademicLine";
import { MonthlyHeatmap } from "@/components/charts/Heatmap";
import { pct, num, money } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function Page() {
  const [equity, metricsRows, drawdown, monthly, rolling] = await Promise.all([
    getEquityCurve(),
    getAllMetrics(),
    getDrawdownSeries(),
    getMonthlyReturns(),
    getRollingSharpe(63),
  ]);

  const eqData = equity.map((r) => ({
    date: String(r.date).slice(0, 10),
    nav: Number(r.nav),
  }));
  const ddData = drawdown.map((r) => ({
    date: String(r.date).slice(0, 10),
    drawdown: Number(r.drawdown),
  }));
  const rollData = rolling.map((r) => ({
    date: String(r.date).slice(0, 10),
    sharpe: r.sharpe == null ? null : Number(r.sharpe),
  }));
  const months = monthly.map((m) => ({ year: Number(m.year), month: Number(m.month), ret: Number(m.ret) }));

  const periods = ["all", "ytd", "1y", "3m", "1m"] as const;
  const byPeriod: Record<string, Record<string, number | string>> = {};
  for (const m of metricsRows as Record<string, unknown>[]) {
    byPeriod[String(m.period)] = m as unknown as Record<string, number | string>;
  }

  return (
    <div className="space-y-2">
      <div>
        <div className="smallcaps">Section II</div>
        <h1>Performance Analysis</h1>
        <p className="text-muted italic">
          Realised return, volatility, drawdown, and rolling Sharpe over the live and simulated periods.
        </p>
      </div>

      <hr className="rule" />

      <PaperTable
        number="4"
        title="Performance metrics by period"
        caption={
          <>
            Annualised figures use 252 trading days. <span className="font-mono">all</span> covers
            since inception; <span className="font-mono">ytd</span>, <span className="font-mono">1y</span>,
            <span className="font-mono"> 3m</span>, and <span className="font-mono">1m</span> are the
            corresponding trailing windows.
          </>
        }
      >
        <thead>
          <tr>
            <th>Period</th>
            <th className="num">Total Ret</th>
            <th className="num">Ann Ret</th>
            <th className="num">Ann Vol</th>
            <th className="num">Sharpe</th>
            <th className="num">Sortino</th>
            <th className="num">Max DD</th>
            <th className="num">Calmar</th>
            <th className="num">Hit %</th>
          </tr>
        </thead>
        <tbody>
          {periods.map((p) => {
            const r = byPeriod[p];
            return (
              <tr key={p}>
                <td className="font-mono">{p.toUpperCase()}</td>
                <td className="num">{pct(Number(r?.total_return))}</td>
                <td className="num">{pct(Number(r?.ann_return))}</td>
                <td className="num">{pct(Number(r?.ann_vol))}</td>
                <td className="num">{num(Number(r?.sharpe))}</td>
                <td className="num">{num(Number(r?.sortino))}</td>
                <td className="num text-negative">{pct(Number(r?.max_drawdown))}</td>
                <td className="num">{num(Number(r?.calmar))}</td>
                <td className="num">{pct(Number(r?.hit_rate), 1)}</td>
              </tr>
            );
          })}
        </tbody>
      </PaperTable>

      <Figure
        number="2"
        title="Equity curve"
        caption={<>NAV in USD on log scale; reference dashed line marks the inception value.</>}
      >
        <AcademicLine
          data={eqData}
          xKey="date"
          yKeys={[{ key: "nav", color: "academic" }]}
          height={300}
          yFmt={(v) => money(v)}
          xFmt={(d) => String(d).slice(0, 7)}
        />
      </Figure>

      <Figure
        number="3"
        title="Drawdown waterfall"
        caption={
          <>
            Drawdown defined as <span className="font-mono">(NAVₜ − maxₛ≤ₜ NAVₛ)/maxₛ≤ₜ NAVₛ</span>.
            The 8% horizontal reference line marks the threshold at which gross exposure is halved
            by the risk overlay.
          </>
        }
      >
        <AcademicLine
          data={ddData}
          xKey="date"
          yKeys={[{ key: "drawdown", color: "crimson" }]}
          height={220}
          fill
          refY={-0.08}
          yFmt={(v) => pct(v, 1)}
          xFmt={(d) => String(d).slice(0, 7)}
        />
      </Figure>

      <Figure
        number="4"
        title="Rolling 63-day Sharpe"
        caption={<>Standard error widens at the start of the series where the window is unfilled.</>}
      >
        <AcademicLine
          data={rollData}
          xKey="date"
          yKeys={[{ key: "sharpe", color: "ink" }]}
          height={200}
          refY={0}
          yFmt={(v) => num(v, 2)}
          xFmt={(d) => String(d).slice(0, 7)}
        />
      </Figure>

      <Figure
        number="5"
        title="Monthly returns"
        caption={<>Cells in basis points; rightmost column is calendar-year cumulative.</>}
      >
        <MonthlyHeatmap data={months} />
      </Figure>
    </div>
  );
}
