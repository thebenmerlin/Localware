"use client";

import { useThemeColors } from "@/lib/useThemeColors";

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

function rgba(rgb: string, alpha: number): string {
  // accepts "#rrggbb" or "rgb(r,g,b)" or a CSS var-resolved color
  const m = /^#([0-9a-fA-F]{6})$/.exec(rgb.trim());
  if (m) {
    const n = parseInt(m[1], 16);
    return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha.toFixed(2)})`;
  }
  // fall back to color-mix for non-hex strings (e.g., oklch)
  return `color-mix(in oklab, ${rgb} ${(alpha * 100).toFixed(0)}%, transparent)`;
}

export function MonthlyHeatmap({ data }: { data: { year: number; month: number; ret: number }[] }) {
  const t = useThemeColors();
  const byYear: Record<number, Record<number, number>> = {};
  for (const d of data) {
    byYear[d.year] = byYear[d.year] || {};
    byYear[d.year][d.month] = d.ret;
  }
  const years = Object.keys(byYear).map(Number).sort();
  if (years.length === 0) {
    return <div className="caption">No monthly returns yet.</div>;
  }
  const all = data.map(d => d.ret).filter(x => Number.isFinite(x));
  const maxAbs = Math.max(...all.map(Math.abs), 0.01);

  function color(v: number | undefined) {
    if (v === undefined) return t.hairline;
    const r = Math.max(-1, Math.min(1, v / maxAbs));
    const a = Math.abs(r) * 0.55;
    return rgba(r >= 0 ? t.positive : t.negative, a);
  }

  function textColor(v: number | undefined) {
    if (v === undefined) return t.muted;
    const r = Math.abs(v) / maxAbs;
    return r > 0.5 ? t.paper : t.ink;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full" style={{ borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th className="heatmap-cell" style={{ background: "transparent" }}></th>
            {MONTHS.map(m => (
              <th key={m} className="heatmap-cell" style={{ color: t.muted, background: "transparent" }}>{m}</th>
            ))}
            <th className="heatmap-cell" style={{ color: t.muted, background: "transparent" }}>YTD</th>
          </tr>
        </thead>
        <tbody>
          {years.map(y => {
            const months = byYear[y];
            const ytd = MONTHS.reduce((acc, _m, i) => {
              const r = months[i + 1];
              return r === undefined ? acc : (1 + acc) * (1 + r) - 1;
            }, 0);
            return (
              <tr key={y}>
                <td className="heatmap-cell" style={{ color: t.muted, background: "transparent" }}>{y}</td>
                {MONTHS.map((_, i) => {
                  const v = months[i + 1];
                  return (
                    <td
                      key={i}
                      className="heatmap-cell"
                      style={{ background: color(v), color: textColor(v) }}
                    >
                      {v === undefined ? "·" : `${(v * 100).toFixed(1)}`}
                    </td>
                  );
                })}
                <td
                  className="heatmap-cell"
                  style={{ background: color(ytd), color: textColor(ytd), fontWeight: 600 }}
                >
                  {(ytd * 100).toFixed(1)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
