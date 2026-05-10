import { notFound } from "next/navigation";
import { getBacktest } from "@/lib/queries";
import { Figure } from "@/components/paper/Figure";
import { PaperTable } from "@/components/paper/PaperTable";
import { AcademicLine } from "@/components/charts/AcademicLine";
import { pct, num, money, date as fmtDate } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function Page({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const bt = await getBacktest(Number(id));
  if (!bt) return notFound();
  const r = (bt.results || {}) as Record<string, number>;
  const eq = (bt.equity_curve || []) as Array<{ date: string; nav: number; ret: number | null }>;
  const data = eq.map((p) => ({ date: String(p.date).slice(0, 10), nav: Number(p.nav) }));

  return (
    <div className="space-y-2">
      <div>
        <div className="smallcaps">Backtest #{bt.id}</div>
        <h1>{bt.name}</h1>
        <p className="text-muted italic">
          {fmtDate(bt.start_date)} → {fmtDate(bt.end_date)}, {data.length} days
        </p>
      </div>

      <hr className="rule" />

      <PaperTable number={`${bt.id}.1`} title="Summary statistics">
        <thead>
          <tr>
            <th className="num">Total Ret</th>
            <th className="num">Ann Ret</th>
            <th className="num">Ann Vol</th>
            <th className="num">Sharpe</th>
            <th className="num">Sortino</th>
            <th className="num">Max DD</th>
            <th className="num">Calmar</th>
            <th className="num">Hit %</th>
            <th className="num">Beta</th>
            <th className="num">Alpha</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td className="num">{pct(Number(r.total_return))}</td>
            <td className="num">{pct(Number(r.ann_return))}</td>
            <td className="num">{pct(Number(r.ann_vol))}</td>
            <td className="num">{num(Number(r.sharpe))}</td>
            <td className="num">{num(Number(r.sortino))}</td>
            <td className="num text-negative">{pct(Number(r.max_drawdown))}</td>
            <td className="num">{num(Number(r.calmar))}</td>
            <td className="num">{pct(Number(r.hit_rate), 1)}</td>
            <td className="num">{num(Number(r.beta))}</td>
            <td className="num">{pct(Number(r.alpha))}</td>
          </tr>
        </tbody>
      </PaperTable>

      <Figure
        number={`${bt.id}.1`}
        title="Equity curve"
        caption={
          <>
            NAV starts at the configured initial capital and is rebalanced weekly under the live
            risk overlay. Slippage and commissions are deducted from cash on each fill.
          </>
        }
      >
        <AcademicLine
          data={data}
          xKey="date"
          yKeys={[{ key: "nav", color: "academic" }]}
          height={300}
          fill
          yFmt={(v) => money(v)}
          xFmt={(d) => String(d).slice(0, 7)}
        />
      </Figure>

      <h3>Configuration</h3>
      <pre className="font-mono text-tiny bg-sheet border border-ink/30 p-3 overflow-x-auto">
{JSON.stringify(bt.strategy_config, null, 2)}
      </pre>
    </div>
  );
}
