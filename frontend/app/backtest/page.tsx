import Link from "next/link";
import { getBacktests } from "@/lib/queries";
import { PaperTable } from "@/components/paper/PaperTable";
import { pct, num, date as fmtDate } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function Page() {
  const list = await getBacktests();
  return (
    <div className="space-y-2">
      <div>
        <div className="smallcaps">Section VII</div>
        <h1>Backtests</h1>
        <p className="text-muted italic">
          Historical replays of the live strategy stack. Each run uses the same code path as the
          production scheduler — the equity curve below would have been the live curve had we
          launched on the start date.
        </p>
      </div>

      <hr className="rule" />

      <PaperTable
        number="9"
        title="Catalogued backtests"
        caption={
          <>
            Click a row to see its equity curve and per-period statistics. Annualised return uses
            252 trading days; Sharpe is annualised likewise.
          </>
        }
      >
        <thead>
          <tr>
            <th>ID</th>
            <th>Name</th>
            <th>Range</th>
            <th className="num">Ann&nbsp;Ret</th>
            <th className="num">Ann&nbsp;Vol</th>
            <th className="num">Sharpe</th>
            <th className="num">Max&nbsp;DD</th>
            <th className="num">Calmar</th>
          </tr>
        </thead>
        <tbody>
          {list.map((b) => {
            const r = (b.results || {}) as Record<string, number>;
            return (
              <tr key={b.id}>
                <td className="font-mono">#{b.id}</td>
                <td>
                  <Link href={`/backtest/${b.id}`}>{b.name}</Link>
                </td>
                <td className="text-small text-muted">
                  {fmtDate(b.start_date)} → {fmtDate(b.end_date)}
                </td>
                <td className="num">{pct(Number(r.ann_return))}</td>
                <td className="num">{pct(Number(r.ann_vol))}</td>
                <td className="num">{num(Number(r.sharpe))}</td>
                <td className="num text-negative">{pct(Number(r.max_drawdown))}</td>
                <td className="num">{num(Number(r.calmar))}</td>
              </tr>
            );
          })}
          {list.length === 0 && (
            <tr><td colSpan={8} className="caption">No backtests catalogued yet.</td></tr>
          )}
        </tbody>
      </PaperTable>
    </div>
  );
}
