import { getStrategies, getStrategyContribution, getStrategySignals } from "@/lib/queries";
import { PaperTable } from "@/components/paper/PaperTable";
import { pct, money, num } from "@/lib/format";
import { InlineEq } from "@/components/paper/Equation";
import { Ticker } from "@/components/Ticker";

export const revalidate = 300;

const FORMULAS: Record<string, string> = {
  momentum:        "r^{12,1}_i = \\frac{P_{t-21}}{P_{t-252}} - 1",
  quality:         "Q_i = z(\\text{ROE}_i) - z(\\text{D/E}_i) + z(\\text{EPS}_i)",
  low_volatility:  "\\sigma^{60}_i = \\sqrt{\\frac{1}{59}\\sum_{s=t-59}^{t}(r_{i,s}-\\bar r_i)^2}",
  mean_reversion:  "\\text{long if } \\text{RSI}_{14}(P) < 30 \\,\\wedge\\, P > \\overline{P}_{200}",
};

export default async function Page() {
  const strategies = await getStrategies();
  const contributions = (await getStrategyContribution()) as unknown as Array<{
    strategy: string; allocation_weight: number; net_flow: number; trade_count: number;
  }>;
  const contribByName: Record<string, { net_flow: number; trade_count: number }> = {};
  for (const c of contributions) {
    contribByName[c.strategy] = { net_flow: Number(c.net_flow), trade_count: Number(c.trade_count) };
  }
  const sigsByStrategy: Record<number, Array<{ ticker: string; signal: number; score: number; date: string }>> = {};
  for (const s of strategies) {
    sigsByStrategy[s.id] = (await getStrategySignals(s.id, 12)) as unknown as Array<{
      ticker: string; signal: number; score: number; date: string;
    }>;
  }

  return (
    <div className="space-y-2">
      <div>
        <div className="smallcaps">Section IV</div>
        <h1>Strategy Sleeves</h1>
        <p className="text-muted italic">
          The portfolio is constructed as a weighted blend of four orthogonal factor sleeves
          combined under a single risk overlay.
        </p>
      </div>

      <hr className="rule" />

      <PaperTable
        number="6"
        title="Sleeve weights and recent activity"
        caption={
          <>
            <span className="font-mono">Allocation</span> is the static weight applied to each
            sleeve before risk overlays. <span className="font-mono">Trades</span> counts all
            executions tagged to that sleeve over the live period.
          </>
        }
      >
        <thead>
          <tr>
            <th>Strategy</th>
            <th>Description</th>
            <th className="num">Allocation</th>
            <th className="num">Trades</th>
            <th className="num">Net flow</th>
          </tr>
        </thead>
        <tbody>
          {strategies.map((s) => (
            <tr key={s.id}>
              <td className="font-mono">{s.name}</td>
              <td className="text-small text-muted">{s.description}</td>
              <td className="num">{pct(Number(s.allocation_weight))}</td>
              <td className="num">{contribByName[s.name]?.trade_count ?? 0}</td>
              <td className="num">{money(contribByName[s.name]?.net_flow ?? 0)}</td>
            </tr>
          ))}
        </tbody>
      </PaperTable>

      {strategies.map((s) => {
        const sigs = sigsByStrategy[s.id];
        return (
          <section key={s.id} className="mt-8">
            <h3 className="font-display">{s.name}</h3>
            <p className="text-muted text-small italic">{s.description}</p>
            {FORMULAS[s.name] && (
              <div className="my-3">
                <InlineEq tex={FORMULAS[s.name]} />
              </div>
            )}
            <div className="caption mb-1">
              <span className="smallcaps">Parameters</span>{" "}
              <span className="font-mono text-tiny">{JSON.stringify(s.params)}</span>
            </div>
            <PaperTable>
              <thead>
                <tr>
                  <th>Top signals (latest)</th>
                  <th className="num">Score</th>
                  <th className="num">Weight contribution</th>
                  <th>Date</th>
                </tr>
              </thead>
              <tbody>
                {sigs.length === 0 && (
                  <tr><td colSpan={4} className="caption">No signals yet.</td></tr>
                )}
                {sigs.map((sig) => (
                  <tr key={sig.ticker}>
                    <td><Ticker symbol={sig.ticker} /></td>
                    <td className="num">{num(Number(sig.score), 4)}</td>
                    <td className={`num ${Number(sig.signal) >= 0 ? "text-positive" : "text-negative"}`}>
                      {pct(Number(sig.signal), 3)}
                    </td>
                    <td className="text-small text-muted">{String(sig.date).slice(0, 10)}</td>
                  </tr>
                ))}
              </tbody>
            </PaperTable>
          </section>
        );
      })}
    </div>
  );
}
