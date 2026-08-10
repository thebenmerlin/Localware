import { getCurrentPositions, getSectorExposure } from "@/lib/queries";
import { pct, money } from "@/lib/format";

export const revalidate = 600;

export default async function PositionsPage() {
  const [positions, sectors] = await Promise.all([getCurrentPositions(), getSectorExposure()]);

  return (
    <div className="flex flex-col gap-16">
      <section>
        <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
          Holdings
        </h2>
        <table className="mt-4 w-full text-left border-collapse">
          <thead>
            <tr className="font-mono text-[0.65rem] tracking-wide uppercase text-[var(--faint)]">
              <th className="font-normal pb-2">Ticker</th>
              <th className="font-normal pb-2">Sector</th>
              <th className="font-normal pb-2 text-right">Weight</th>
              <th className="font-normal pb-2 text-right">Mkt value</th>
              <th className="font-normal pb-2 text-right">Unrl. P/L</th>
            </tr>
          </thead>
          <tbody className="tabular font-mono text-sm">
            {positions.map((p) => (
              <tr key={p.ticker} className="border-t border-[var(--faint)]/20">
                <td className="py-2 font-semibold">{p.ticker}</td>
                <td className="py-2 text-[var(--muted)]">{p.sector}</td>
                <td className="py-2 text-right">{pct(Number(p.weight))}</td>
                <td className="py-2 text-right">{money(Number(p.market_value))}</td>
                <td
                  className={`py-2 text-right ${
                    Number(p.unrealized_pnl) >= 0 ? "text-[var(--positive)]" : "text-[var(--negative)]"
                  }`}
                >
                  {money(Number(p.unrealized_pnl))}
                </td>
              </tr>
            ))}
            {positions.length === 0 && (
              <tr>
                <td colSpan={5} className="py-4 text-[var(--muted)]">
                  No live positions yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </section>

      <section>
        <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
          Sector exposure
        </h2>
        <table className="mt-4 w-full max-w-md text-left border-collapse">
          <thead>
            <tr className="font-mono text-[0.65rem] tracking-wide uppercase text-[var(--faint)]">
              <th className="font-normal pb-2">Sector</th>
              <th className="font-normal pb-2 text-right">Weight</th>
              <th className="font-normal pb-2 text-right">N</th>
            </tr>
          </thead>
          <tbody className="tabular font-mono text-sm">
            {sectors.map((s) => (
              <tr key={s.sector} className="border-t border-[var(--faint)]/20">
                <td className="py-2">{s.sector || "—"}</td>
                <td className="py-2 text-right">{pct(Number(s.weight))}</td>
                <td className="py-2 text-right text-[var(--muted)]">{s.count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}
