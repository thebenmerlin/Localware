import { getRecentTrades } from "@/lib/queries";
import { PaperTable } from "@/components/paper/PaperTable";
import { money, num } from "@/lib/format";

export const dynamic = "force-dynamic";

export default async function Page() {
  const trades = await getRecentTrades(500);
  const buys = trades.filter((t) => t.side === "BUY").length;
  const sells = trades.filter((t) => t.side === "SELL").length;
  const totalNotional = trades.reduce((a, t) => a + Number(t.notional), 0);
  const totalCommission = trades.reduce((a, t) => a + Number(t.commission), 0);
  const avgSlippage = trades.length
    ? trades.reduce((a, t) => a + Number(t.slippage_bps), 0) / trades.length
    : 0;

  return (
    <div className="space-y-2">
      <div>
        <div className="smallcaps">Section V</div>
        <h1>Trade Ledger</h1>
        <p className="text-muted italic">
          All simulated executions, ordered most-recent first. Slippage and commission included.
        </p>
      </div>

      <hr className="rule" />

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="kpi-card">
          <div className="label">Buys / Sells</div>
          <div className="value">{buys} / {sells}</div>
          <div className="sub">Last {trades.length} trades</div>
        </div>
        <div className="kpi-card">
          <div className="label">Gross notional</div>
          <div className="value">{money(totalNotional)}</div>
          <div className="sub">Sum of all fills</div>
        </div>
        <div className="kpi-card">
          <div className="label">Commission paid</div>
          <div className="value">{money(totalCommission)}</div>
          <div className="sub">$0.005/share, $1 min</div>
        </div>
        <div className="kpi-card">
          <div className="label">Avg slippage</div>
          <div className="value">{num(avgSlippage, 1)} bp</div>
          <div className="sub">Including impact tail</div>
        </div>
      </div>

      <PaperTable number="7" title="Most recent executions">
        <thead>
          <tr>
            <th>Executed&nbsp;at</th>
            <th>Ticker</th>
            <th>Side</th>
            <th>Strategy</th>
            <th className="num">Quantity</th>
            <th className="num">Fill&nbsp;price</th>
            <th className="num">Notional</th>
            <th className="num">Slippage</th>
            <th className="num">Commission</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((t, i) => (
            <tr key={i}>
              <td className="text-small text-muted">{String(t.executed_at).slice(0, 16).replace("T", " ")}</td>
              <td className="font-mono">{t.ticker}</td>
              <td className={t.side === "BUY" ? "text-positive" : "text-negative"}>{t.side}</td>
              <td className="text-small text-muted italic">{t.strategy ?? "—"}</td>
              <td className="num">{Number(t.quantity).toFixed(0)}</td>
              <td className="num">${num(Number(t.price), 2)}</td>
              <td className="num">{money(Number(t.notional))}</td>
              <td className="num text-muted">{num(Number(t.slippage_bps), 1)} bp</td>
              <td className="num text-muted">${num(Number(t.commission), 2)}</td>
            </tr>
          ))}
          {trades.length === 0 && (
            <tr><td colSpan={9} className="caption">No trades recorded.</td></tr>
          )}
        </tbody>
      </PaperTable>
    </div>
  );
}
