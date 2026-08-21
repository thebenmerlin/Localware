import { pct } from "@/lib/format";

export interface MonthlyReturn {
  year: number;
  month: number;
  ret: number;
}

const MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

// Monthly moves rarely clear a few percent, so an 8% cap keeps the tint
// scale meaningful — anything past it just tops out rather than blowing
// out the color range for one outlier month.
const INTENSITY_CAP = 0.08;

function cellBg(ret: number | null): string {
  if (ret === null) return "transparent";
  const intensity = Math.min(1, Math.abs(ret) / INTENSITY_CAP);
  const color = ret >= 0 ? "var(--positive)" : "var(--negative)";
  return `color-mix(in srgb, ${color} ${Math.round(10 + intensity * 55)}%, var(--paper))`;
}

/**
 * Year-by-month return calendar, the standard tearsheet complement to an
 * equity curve — cells tinted by sign/magnitude via color-mix (never via
 * `opacity`, which would fade the text along with the background; see the
 * same fix on the positions treemap). A trailing "Year" column compounds
 * whatever months exist so far, correct even for a partial current year.
 */
export function MonthlyReturnsHeatmap({ rows }: { rows: MonthlyReturn[] }) {
  const byKey = new Map(rows.map((r) => [`${r.year}-${r.month}`, r.ret]));
  const years = Array.from(new Set(rows.map((r) => r.year))).sort((a, b) => b - a);

  return (
    <div className="overflow-x-auto">
      <table className="border-collapse text-sm">
        <thead>
          <tr className="font-mono text-[0.62rem] tracking-[0.1em] uppercase text-[var(--faint)]">
            <th className="font-normal pb-2 pr-4 text-left">Year</th>
            {MONTH_LABELS.map((m) => (
              <th key={m} className="font-normal pb-2 px-1.5 text-right">
                {m}
              </th>
            ))}
            <th className="font-normal pb-2 pl-4 text-right">Year</th>
          </tr>
        </thead>
        <tbody className="tabular font-mono">
          {years.map((year) => {
            const monthVals = Array.from({ length: 12 }, (_, i) => byKey.get(`${year}-${i + 1}`) ?? null);
            const known = monthVals.filter((v): v is number => v !== null);
            const yearRet = known.length ? known.reduce((acc, r) => acc * (1 + r), 1) - 1 : null;
            return (
              <tr key={year} className="border-t border-[var(--faint)]/10">
                <td className="py-1.5 pr-4 text-[var(--muted)]">{year}</td>
                {monthVals.map((v, i) => (
                  <td key={i} className="py-1.5 px-1.5 text-right rounded-sm" style={{ background: cellBg(v) }}>
                    {v === null ? <span className="text-[var(--faint)]">—</span> : pct(v, 1)}
                  </td>
                ))}
                <td className="py-1.5 pl-4 text-right font-semibold">{yearRet === null ? "—" : pct(yearRet, 1)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
