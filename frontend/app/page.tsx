import {
  getLatestNav,
  getEquityCurve,
  getMetrics,
  getCurrentPositions,
  getRecentTrades,
  getSectorExposure,
} from "@/lib/queries";
import { KPI } from "@/components/paper/KPI";
import { Figure } from "@/components/paper/Figure";
import { PaperTable } from "@/components/paper/PaperTable";
import { AcademicLine } from "@/components/charts/AcademicLine";
import { pct, money, num, signed } from "@/lib/format";
import { Ticker } from "@/components/Ticker";

export const revalidate = 300;

export default async function Page() {
  const [nav, equity, metrics, positions, trades, sectors] = await Promise.all([
    getLatestNav(),
    getEquityCurve(),
    getMetrics("all"),
    getCurrentPositions(),
    getRecentTrades(8),
    getSectorExposure(),
  ]);

  const eqData = equity.map((r) => ({
    date: String(r.date).slice(0, 10),
    nav: Number(r.nav),
    cum: r.cumulative_return ? Number(r.cumulative_return) : 0,
  }));
  const annRet = Number(metrics?.ann_return ?? 0);
  const sharpe = Number(metrics?.sharpe ?? 0);
  const dd = Number(metrics?.max_drawdown ?? 0);
  const annVol = Number(metrics?.ann_vol ?? 0);

  return (
    <div className="space-y-2">
      <div className="text-center mb-2">
        <h1 className="font-display tracking-tight !mt-1" style={{ fontSize: "2.15rem" }}>
          Localware: A multi-factor systematic portfolio
        </h1>
        <div className="caption !mt-1 not-italic">by Gajanan Barve</div>
      </div>

      <hr className="rule" />

      <h4>I. Headline figures</h4>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mt-3">
        <KPI
          label="Annualised return"
          value={pct(annRet)}
          tone={annRet >= 0 ? "positive" : "negative"}
          sub={`Total ${pct(Number(metrics?.total_return ?? 0))} since inception`}
        />
        <KPI
          label="Sharpe ratio"
          value={num(sharpe)}
          tone={sharpe >= 1 ? "positive" : "neutral"}
          sub={`Sortino ${num(Number(metrics?.sortino ?? 0))}`}
        />
        <KPI
          label="Max drawdown"
          value={pct(dd)}
          tone="negative"
          sub={`Calmar ${num(Number(metrics?.calmar ?? 0))}`}
        />
        <KPI
          label="Realised volatility"
          value={pct(annVol)}
          sub={`Hit rate ${pct(Number(metrics?.hit_rate ?? 0), 1)}`}
        />
      </div>

      <Figure
        number="1"
        title="Equity curve"
        caption={
          <>
            Cumulative growth of $1 invested at inception. Net of slippage (5–25 bps) and
            commissions ($0.005/share, $1 minimum). Blue trace is the strategy; the dashed
            reference is the starting capital baseline.
          </>
        }
      >
        <AcademicLine
          data={eqData}
          xKey="date"
          yKeys={[{ key: "nav", color: "academic", label: "NAV" }]}
          height={300}
          fill
          yFmt="money"
          xFmt="YYYY-MM"
        />
      </Figure>

      <div className="grid lg:grid-cols-3 gap-8">
        <div className="lg:col-span-2">
          <h4>II. Top positions, current snapshot</h4>
          <PaperTable
            number="1"
            title="Largest holdings by market value"
            caption={<>Includes both long and short legs. Weights calculated on portfolio NAV.</>}
          >
            <thead>
              <tr>
                <th>Ticker</th>
                <th>Sector</th>
                <th className="num">Weight</th>
                <th className="num">Mkt&nbsp;Value</th>
                <th className="num">Unrealized&nbsp;P/L</th>
              </tr>
            </thead>
            <tbody>
              {positions.slice(0, 12).map((p) => (
                <tr key={p.ticker}>
                  <td><Ticker symbol={p.ticker} /></td>
                  <td className="text-muted text-small">{p.sector}</td>
                  <td className="num">{pct(Number(p.weight))}</td>
                  <td className="num">{money(Number(p.market_value))}</td>
                  <td className={`num ${Number(p.unrealized_pnl) >= 0 ? "text-positive" : "text-negative"}`}>
                    {signed(Number(p.unrealized_pnl) / 1000, 1)}K
                  </td>
                </tr>
              ))}
              {positions.length === 0 && (
                <tr>
                  <td colSpan={5} className="caption">
                    No live positions yet — bootstrap is still running.
                  </td>
                </tr>
              )}
            </tbody>
          </PaperTable>
        </div>

        <div>
          <h4>III. Sector composition</h4>
          <PaperTable number="2" title="Sector weight (long net)">
            <thead>
              <tr><th>Sector</th><th className="num">Weight</th><th className="num">N</th></tr>
            </thead>
            <tbody>
              {sectors.slice(0, 12).map((s) => (
                <tr key={s.sector}>
                  <td className="text-small">{s.sector || "—"}</td>
                  <td className="num">{pct(Number(s.weight))}</td>
                  <td className="num text-muted">{s.count}</td>
                </tr>
              ))}
            </tbody>
          </PaperTable>
        </div>
      </div>

      <div>
        <h4>IV. Most recent executions</h4>
        <PaperTable
          number="3"
          title="Trade tape (last 8)"
          caption={
            <>Slippage modelled as 5 bps base plus half-spread proxy and a 15 bps impact penalty above 1% ADV.</>
          }
        >
          <thead>
            <tr>
              <th>When</th>
              <th>Ticker</th>
              <th>Side</th>
              <th>Strategy</th>
              <th className="num">Qty</th>
              <th className="num">Price</th>
              <th className="num">Slippage</th>
            </tr>
          </thead>
          <tbody>
            {trades.map((t, i) => (
              <tr key={i}>
                <td className="text-small text-muted">{String(t.executed_at).slice(0, 10)}</td>
                <td><Ticker symbol={t.ticker} /></td>
                <td className={t.side === "BUY" ? "text-positive" : "text-negative"}>{t.side}</td>
                <td className="text-small text-muted italic">{t.strategy ?? "—"}</td>
                <td className="num">{Number(t.quantity).toFixed(0)}</td>
                <td className="num">${Number(t.price).toFixed(2)}</td>
                <td className="num text-muted">{Number(t.slippage_bps).toFixed(1)} bp</td>
              </tr>
            ))}
            {trades.length === 0 && (
              <tr><td colSpan={7} className="caption">No trades yet.</td></tr>
            )}
          </tbody>
        </PaperTable>
      </div>

      <div className="ornament"></div>
    </div>
  );
}
