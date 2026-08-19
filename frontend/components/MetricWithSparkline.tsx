"use client";

import { useState } from "react";
import { toneClass, type BigStatDelta } from "./BigStat";
import { PixelArrow } from "./PixelArrow";
import { Sparkline, type SparklinePoint } from "./Sparkline";
import { money, num } from "@/lib/format";

type SeriesFormat = "money" | "percent" | "number";

function formatEdge(v: number, format: SeriesFormat): string {
  if (format === "money") return money(v);
  if (format === "percent") return `${v.toFixed(1)}%`;
  return num(v);
}

/**
 * Same layout as BigStat (label / value / delta), but the hover trigger for
 * the sparkline is scoped to just the label text — not the value or delta —
 * so mousing over the big number doesn't pop a graph. Reimplements BigStat's
 * JSX rather than wrapping it, since BigStat renders label+value as one
 * block and there's no seam to hang a narrower hover zone on.
 */
export function MetricWithSparkline({
  label,
  value,
  delta,
  size = "secondary",
  series,
  seriesLabel,
  seriesFormat = "number",
  seriesColor = "var(--ink)",
}: {
  label: string;
  value: string;
  delta?: BigStatDelta;
  size?: "hero" | "secondary";
  series: SparklinePoint[];
  seriesLabel: string;
  seriesFormat?: SeriesFormat;
  seriesColor?: string;
}) {
  const [hover, setHover] = useState(false);

  return (
    <div className="flex flex-col items-start text-left">
      <div
        className="relative inline-block cursor-default"
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => setHover(false)}
      >
        <div className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">{label}</div>
        {hover && series.length > 1 && (
          <div
            className={`absolute z-40 rounded-md border border-[var(--faint)]/40 bg-[var(--paper)] px-3 py-2.5 shadow-lg w-[190px] ${
              size === "hero" ? "left-0 bottom-full mb-2" : "left-full top-1/2 -translate-y-1/2 ml-3"
            }`}
          >
            <div className="font-mono text-[0.62rem] tracking-wide uppercase text-[var(--muted)]">{seriesLabel}</div>
            <div className="mt-1.5">
              <Sparkline points={series} color={seriesColor} />
            </div>
            <div className="mt-1 flex items-center justify-between tabular font-mono text-xs text-[var(--muted)]">
              <span>{formatEdge(series[0].value, seriesFormat)}</span>
              <span>{formatEdge(series.at(-1)!.value, seriesFormat)}</span>
            </div>
          </div>
        )}
      </div>
      <div
        className={`tabular font-display font-semibold leading-none ${
          size === "hero" ? "mt-3 text-5xl md:text-6xl lg:text-7xl" : "mt-2 text-3xl md:text-4xl"
        }`}
      >
        {value}
      </div>
      {delta && (
        <div className={`mt-2 flex items-center gap-1.5 ${toneClass[delta.tone]}`}>
          <PixelArrow direction={delta.direction} className="h-3 w-3" />
          {delta.label && <span className="tabular font-mono text-sm">{delta.label}</span>}
        </div>
      )}
    </div>
  );
}
