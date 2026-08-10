"use client";

import { useMemo, useRef, useState } from "react";

export interface LineChartPoint {
  date: string;
  value: number;
}

type ValueFormat = "money" | "percent" | "number";

// Formatting lives here as plain data (a format "kind"), not function props —
// Server Components can't pass functions to a "use client" component (they
// aren't serializable across the RSC boundary), so callers pass a string enum.
function formatValue(v: number, format: ValueFormat): string {
  if (format === "money") return `$${v.toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
  if (format === "percent") return `${v.toFixed(1)}%`;
  return v.toFixed(2);
}

function formatDate(d: string): string {
  return String(d).slice(0, 10);
}

/**
 * A single-series line chart: thin 2px stroke, hairline zero/baseline,
 * hover crosshair that snaps to the nearest point, end value direct-labeled.
 * One color in, one series shown — no legend needed per dataviz's single-series rule.
 */
export function LineChart({
  data,
  color,
  height = 220,
  format = "number",
  zeroLine = false,
  compact = false,
}: {
  data: LineChartPoint[];
  color: string;
  height?: number;
  format?: ValueFormat;
  zeroLine?: boolean;
  /** Thumbnail mode: no hover/tooltip, no end-label, tighter padding.
   *  Meant to be glanced at inside a small card, not interacted with —
   *  the card itself is the click target that opens the full chart. */
  compact?: boolean;
}) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const width = 720;
  const padding = compact
    ? { top: 6, right: 4, bottom: 4, left: 4 }
    : { top: 12, right: 8, bottom: 20, left: 8 };

  const { path, area, points, yMin, yMax } = useMemo(() => {
    if (data.length === 0) {
      return { path: "", area: "", points: [] as { x: number; y: number }[], yMin: 0, yMax: 0 };
    }
    const values = data.map((d) => d.value);
    let yMin = Math.min(...values);
    let yMax = Math.max(...values);
    if (zeroLine) yMin = Math.min(yMin, 0);
    if (yMin === yMax) {
      yMin -= 1;
      yMax += 1;
    }
    const innerW = width - padding.left - padding.right;
    const innerH = height - padding.top - padding.bottom;
    const points = data.map((d, i) => ({
      x: padding.left + (i / (data.length - 1 || 1)) * innerW,
      y: padding.top + innerH - ((d.value - yMin) / (yMax - yMin)) * innerH,
    }));
    const path = points.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(" ");
    const baseline = padding.top + innerH;
    const area =
      path +
      ` L${points[points.length - 1].x.toFixed(2)},${baseline} L${points[0].x.toFixed(2)},${baseline} Z`;
    return { path, area, points, yMin, yMax };
  }, [data, height, zeroLine]);

  function handleMove(e: React.PointerEvent<SVGSVGElement>) {
    if (compact || !svgRef.current || points.length === 0) return;
    const rect = svgRef.current.getBoundingClientRect();
    const px = ((e.clientX - rect.left) / rect.width) * width;
    let nearest = 0;
    let nearestDist = Infinity;
    points.forEach((p, i) => {
      const d = Math.abs(p.x - px);
      if (d < nearestDist) {
        nearestDist = d;
        nearest = i;
      }
    });
    setHoverIdx(nearest);
  }

  if (data.length === 0) {
    return <div className="text-sm text-[var(--muted)] font-mono">No data yet.</div>;
  }

  const hover = hoverIdx !== null ? { point: points[hoverIdx], datum: data[hoverIdx] } : null;
  const endPoint = points[points.length - 1];
  const endDatum = data[data.length - 1];

  return (
    <div className="relative w-full">
      <svg
        ref={svgRef}
        viewBox={`0 0 ${width} ${height}`}
        className="w-full h-auto"
        onPointerMove={handleMove}
        onPointerLeave={() => setHoverIdx(null)}
      >
        {zeroLine && yMin < 0 && yMax > 0 && (
          <line
            x1={padding.left}
            x2={width - padding.right}
            y1={padding.top + (height - padding.top - padding.bottom) * (yMax / (yMax - yMin))}
            y2={padding.top + (height - padding.top - padding.bottom) * (yMax / (yMax - yMin))}
            stroke="var(--faint)"
            strokeWidth={1}
          />
        )}

        <path d={area} fill={color} opacity={0.08} stroke="none" />
        <path
          d={path}
          fill="none"
          stroke={color}
          strokeWidth={compact ? 1.5 : 2}
          strokeLinejoin="round"
          strokeLinecap="round"
        />

        {/* end marker + direct label */}
        {!compact && (
          <>
            <circle cx={endPoint.x} cy={endPoint.y} r={4} fill={color} stroke="var(--paper)" strokeWidth={2} />
            <text
              x={endPoint.x}
              y={endPoint.y - 10}
              textAnchor="end"
              className="tabular"
              fontSize={12}
              fontFamily="JetBrains Mono, monospace"
              fill="var(--ink)"
            >
              {formatValue(endDatum.value, format)}
            </text>
          </>
        )}

        {hover && (
          <>
            <line
              x1={hover.point.x}
              x2={hover.point.x}
              y1={padding.top}
              y2={height - padding.bottom}
              stroke="var(--faint)"
              strokeWidth={1}
            />
            <circle cx={hover.point.x} cy={hover.point.y} r={4} fill={color} stroke="var(--paper)" strokeWidth={2} />
          </>
        )}
      </svg>

      {hover && (
        <div
          className="absolute top-0 -translate-x-1/2 -translate-y-full bg-[var(--paper)] border border-[var(--faint)] rounded px-2 py-1 text-xs font-mono pointer-events-none"
          style={{ left: `${(hover.point.x / width) * 100}%` }}
        >
          <div className="tabular font-semibold">{formatValue(hover.datum.value, format)}</div>
          <div className="text-[var(--muted)]">{formatDate(hover.datum.date)}</div>
        </div>
      )}
    </div>
  );
}
