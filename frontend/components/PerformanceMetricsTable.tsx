import type { PerformanceMetrics } from "@/lib/queries";
import { pct, num } from "@/lib/format";

const PERIOD_ORDER: PerformanceMetrics["period"][] = ["1m", "3m", "ytd", "1y", "all"];
const PERIOD_LABEL: Record<PerformanceMetrics["period"], string> = {
  "1m": "1M",
  "3m": "3M",
  ytd: "YTD",
  "1y": "1Y",
  all: "All",
};

type Tone = "positive" | "negative" | "neutral";
const toneClass: Record<Tone, string> = {
  positive: "text-[var(--positive)]",
  negative: "text-[var(--negative)]",
  neutral: "",
};

function signTone(v: number | null): Tone {
  if (v === null || v === 0) return "neutral";
  return v > 0 ? "positive" : "negative";
}

const ROWS: {
  key: keyof PerformanceMetrics;
  label: string;
  format: (v: number | null) => string;
  tone: (v: number | null) => Tone;
}[] = [
  { key: "total_return", label: "Total return", format: (v) => pct(v, 1), tone: signTone },
  { key: "ann_return", label: "Ann. return", format: (v) => pct(v, 1), tone: signTone },
  { key: "ann_vol", label: "Ann. volatility", format: (v) => pct(v, 1), tone: () => "neutral" },
  { key: "sharpe", label: "Sharpe", format: (v) => num(v, 2), tone: signTone },
  { key: "sortino", label: "Sortino", format: (v) => num(v, 2), tone: signTone },
  { key: "max_drawdown", label: "Max drawdown", format: (v) => pct(v, 1), tone: (v) => (v === null ? "neutral" : "negative") },
  { key: "calmar", label: "Calmar", format: (v) => num(v, 2), tone: signTone },
  { key: "hit_rate", label: "Hit rate", format: (v) => pct(v, 0), tone: () => "neutral" },
  { key: "beta", label: "Beta", format: (v) => num(v, 2), tone: () => "neutral" },
  { key: "alpha", label: "Alpha", format: (v) => pct(v, 1), tone: signTone },
];

/** Classic tearsheet block: every headline metric across every period,
 *  scannable in one table instead of five separate stat blocks. */
export function PerformanceMetricsTable({ metrics }: { metrics: PerformanceMetrics[] }) {
  const byPeriod = new Map(metrics.map((m) => [m.period, m]));

  return (
    <table className="w-full text-left border-collapse">
      <thead>
        <tr className="font-mono text-[0.65rem] tracking-[0.1em] uppercase text-[var(--faint)]">
          <th className="font-normal pb-2 pr-4">Metric</th>
          {PERIOD_ORDER.map((p) => (
            <th key={p} className="font-normal pb-2 text-right w-16">
              {PERIOD_LABEL[p]}
            </th>
          ))}
        </tr>
      </thead>
      <tbody className="tabular font-mono text-sm">
        {ROWS.map((row) => (
          <tr key={row.key} className="border-t border-[var(--faint)]/15">
            <td className="py-2 pr-4 text-[var(--muted)] whitespace-nowrap">{row.label}</td>
            {PERIOD_ORDER.map((p) => {
              const m = byPeriod.get(p);
              const v = m ? (m[row.key] as number | null) : null;
              return (
                <td key={p} className={`py-2 text-right ${toneClass[row.tone(v)]}`}>
                  {v === null ? "—" : row.format(v)}
                </td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
