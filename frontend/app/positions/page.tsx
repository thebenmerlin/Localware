import { getCurrentPositions, getSectorExposure } from "@/lib/queries";
import { pct } from "@/lib/format";
import { HoldingsTable, type SectorGroup } from "@/components/HoldingsTable";

export const revalidate = 600;

export default async function PositionsPage() {
  const [positions, sectors] = await Promise.all([getCurrentPositions(), getSectorExposure()]);

  type PositionRow = Awaited<ReturnType<typeof getCurrentPositions>>[number];
  const bySector = new Map<string, PositionRow[]>();
  for (const p of positions) {
    const key = p.sector || "—";
    if (!bySector.has(key)) bySector.set(key, []);
    bySector.get(key)!.push(p);
  }
  const groups: SectorGroup[] = Array.from(bySector.entries())
    .map(([sector, rows]) => ({
      sector,
      totalWeight: rows.reduce((sum, r) => sum + Number(r.weight), 0),
      rows: [...rows]
        .sort((a, b) => Math.abs(Number(b.weight)) - Math.abs(Number(a.weight)))
        .map((r) => ({
          ticker: r.ticker,
          name: r.name,
          weight: Number(r.weight),
          market_value: Number(r.market_value),
          unrealized_pnl: Number(r.unrealized_pnl),
        })),
    }))
    .sort((a, b) => Math.abs(b.totalWeight) - Math.abs(a.totalWeight));

  const maxAbsWeight = Math.max(1e-9, ...positions.map((p) => Math.abs(Number(p.weight))));
  const maxSectorWeight = Math.max(1e-9, ...sectors.map((s) => Math.abs(Number(s.weight))));

  return (
    <div className="max-w-wide flex flex-col gap-16">
      <section>
        <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">Holdings</h2>
        <div className="mt-5">
          {groups.length === 0 ? (
            <div className="text-sm text-[var(--muted)] font-mono">No live positions yet.</div>
          ) : (
            <HoldingsTable groups={groups} maxAbsWeight={maxAbsWeight} />
          )}
        </div>
      </section>

      <section>
        <h2 className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
          Sector exposure
        </h2>
        <div className="mt-5 max-w-xl flex flex-col gap-3">
          {sectors.map((s) => {
            const w = Number(s.weight);
            const barWidth = (Math.abs(w) / maxSectorWeight) * 100;
            return (
              <div key={s.sector} className="flex items-center gap-3">
                <div className="w-32 shrink-0 font-mono text-xs text-[var(--muted)] truncate">
                  {s.sector || "—"}
                </div>
                <div className="flex-1 h-2 rounded-full bg-[var(--faint)]/15 overflow-hidden">
                  <div
                    className="h-full rounded-full"
                    style={{ width: `${barWidth}%`, background: w >= 0 ? "var(--positive)" : "var(--negative)" }}
                  />
                </div>
                <div className="w-14 shrink-0 text-right tabular font-mono text-xs">{pct(w)}</div>
                <div className="w-6 shrink-0 text-right tabular font-mono text-xs text-[var(--faint)]">
                  {s.count}
                </div>
              </div>
            );
          })}
          {sectors.length === 0 && (
            <div className="text-sm text-[var(--muted)] font-mono">No exposure data yet.</div>
          )}
        </div>
      </section>
    </div>
  );
}
