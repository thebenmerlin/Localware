import { getCurrentPositions, getSectorExposure } from "@/lib/queries";
import { PaperTable } from "@/components/paper/PaperTable";
import { Figure } from "@/components/paper/Figure";
import { AcademicBar } from "@/components/charts/BarChart";
import { pct, money, num, signed } from "@/lib/format";

export const revalidate = 300;

export default async function Page() {
  const [positions, sectors] = await Promise.all([getCurrentPositions(), getSectorExposure()]);
  const sectorData = sectors.map((s) => ({ sector: s.sector || "—", weight: Number(s.weight) }));
  const longCount = positions.filter((p) => Number(p.quantity) > 0).length;
  const shortCount = positions.filter((p) => Number(p.quantity) < 0).length;
  const totalGross = positions.reduce((acc, p) => acc + Math.abs(Number(p.market_value)), 0);
  const totalNet = positions.reduce((acc, p) => acc + Number(p.market_value), 0);

  return (
    <div className="space-y-2">
      <div>
        <div className="smallcaps">Section III</div>
        <h1>Current Positions</h1>
        <p className="text-muted italic">
          Snapshot of all live holdings, position weights, and unrealised P/L as of the last
          mark-to-market.
        </p>
      </div>

      <hr className="rule" />

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="kpi-card">
          <div className="label">Long positions</div>
          <div className="value">{longCount}</div>
          <div className="sub">Names with positive weight</div>
        </div>
        <div className="kpi-card">
          <div className="label">Short positions</div>
          <div className="value">{shortCount}</div>
          <div className="sub">Names with negative weight</div>
        </div>
        <div className="kpi-card">
          <div className="label">Gross exposure</div>
          <div className="value">{money(totalGross)}</div>
          <div className="sub">Sum of |market values|</div>
        </div>
        <div className="kpi-card">
          <div className="label">Net exposure</div>
          <div className="value">{money(totalNet)}</div>
          <div className="sub">Long minus short</div>
        </div>
      </div>

      <Figure
        number="6"
        title="Sector exposure"
        caption={<>Net portfolio weight per sector. The 25% sector cap is enforced post-construction.</>}
      >
        <AcademicBar
          data={sectorData}
          xKey="sector"
          yKey="weight"
          height={260}
          yFmt="pct1"
          signed
        />
      </Figure>

      <PaperTable
        number="5"
        title="Position ledger"
        caption={
          <>
            Weight is signed: positive for longs, negative for shorts. Average cost is the running
            cost basis after splits and reweights.
          </>
        }
      >
        <thead>
          <tr>
            <th>Ticker</th>
            <th>Sector</th>
            <th className="num">Qty</th>
            <th className="num">Avg&nbsp;Cost</th>
            <th className="num">Mkt&nbsp;Value</th>
            <th className="num">Weight</th>
            <th className="num">Unrealized&nbsp;P/L</th>
          </tr>
        </thead>
        <tbody>
          {positions.map((p) => (
            <tr key={p.ticker}>
              <td className="font-mono">{p.ticker}</td>
              <td className="text-muted text-small">{p.sector}</td>
              <td className="num">{Number(p.quantity).toFixed(0)}</td>
              <td className="num">${num(Number(p.avg_cost), 2)}</td>
              <td className="num">{money(Number(p.market_value))}</td>
              <td className={`num ${Number(p.weight) >= 0 ? "text-positive" : "text-negative"}`}>
                {pct(Number(p.weight))}
              </td>
              <td className={`num ${Number(p.unrealized_pnl) >= 0 ? "text-positive" : "text-negative"}`}>
                {signed(Number(p.unrealized_pnl), 0)}
              </td>
            </tr>
          ))}
          {positions.length === 0 && (
            <tr><td colSpan={7} className="caption">No live positions.</td></tr>
          )}
        </tbody>
      </PaperTable>
    </div>
  );
}
