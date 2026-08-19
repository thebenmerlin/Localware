"use client";

import { useState } from "react";
import { BigStat, type BigStatDelta } from "./BigStat";
import { Sparkline, type SparklinePoint } from "./Sparkline";
import { money, num } from "@/lib/format";

type SeriesFormat = "money" | "percent" | "number";

function formatEdge(v: number, format: SeriesFormat): string {
  if (format === "money") return money(v);
  if (format === "percent") return `${v.toFixed(1)}%`;
  return num(v);
}

/**
 * Wraps BigStat with a hover-to-reveal sparkline, same pattern as the
 * positions treemap's ticker hover: park the pointer over the metric name
 * and its history shows up in a small floating card. Anchored under the
 * stat (these are fixed-position blocks, not scattered like treemap tiles)
 * rather than tracking the cursor.
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
    <div
      className="relative inline-block cursor-default"
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
    >
      <BigStat label={label} value={value} delta={delta} size={size} />
      {hover && series.length > 1 && (
        <div
          className={`absolute z-40 rounded-md border border-[var(--faint)]/40 bg-[var(--paper)] px-3 py-2.5 shadow-lg w-[190px] ${
            size === "hero"
              ? "left-full top-0 ml-6"
              : "left-1/2 top-full -translate-x-1/2 mt-2"
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
  );
}
