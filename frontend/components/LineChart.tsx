"use client";

import { useId, useMemo, useRef, useState } from "react";

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

function formatAxisValue(v: number, format: ValueFormat): string {
  if (format === "money") {
    const abs = Math.abs(v);
    if (abs >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(v / 1e3).toFixed(0)}K`;
    return `$${v.toFixed(0)}`;
  }
  if (format === "percent") return `${v.toFixed(0)}%`;
  return v.toFixed(1);
}

function formatDate(d: string): string {
  const dt = new Date(d);
  if (Number.isNaN(dt.getTime())) return String(d).slice(0, 10);
  return dt.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

/** Pick ~`count` evenly spaced "nice" values between min and max. */
function niceTicks(min: number, max: number, count: number): number[] {
  if (min === max) return [min];
  const range = max - min;
  const rawStep = range / count;
  const magnitude = Math.pow(10, Math.floor(Math.log10(rawStep)));
  const residual = rawStep / magnitude;
  const step = (residual >= 5 ? 10 : residual >= 2 ? 5 : residual >= 1 ? 2 : 1) * magnitude;
  const start = Math.ceil(min / step) * step;
  const ticks: number[] = [];
  for (let v = start; v <= max + step * 1e-9; v += step) ticks.push(Math.round(v * 1e6) / 1e6);
  return ticks;
}

/**
 * A single-series line chart: thin 2px stroke, recessive hairline axes/
 * gridlines, hover crosshair that snaps to the nearest point. The current
 * value is never drawn inside the plot (it used to collide with the line
 * near series highs) — callers show it in a header above the chart instead.
 * One color in, one series shown — no legend needed per dataviz's
 * single-series rule (a `secondary` overlay is the one exception, for
 * benchmark comparison — its legend lives in the caller, not here).
 */
export function LineChart({
  data,
  color,
  height = 240,
  format = "number",
  zeroLine = false,
  compact = false,
  interactive = true,
  glow = false,
  animate = false,
  secondary,
}: {
  data: LineChartPoint[];
  color: string;
  height?: number;
  format?: ValueFormat;
  zeroLine?: boolean;
  /** Compact mode: no axis ticks/gridlines, tighter padding, thinner
   *  stroke — for a card-sized preview. Hover/drag scrubbing still works
   *  by default (see `interactive`) so values are readable without
   *  opening anything bigger. */
  compact?: boolean;
  /** Set false only for a truly static, non-scrubbable preview. */
  interactive?: boolean;
  /** Soft blurred halo behind the stroke — reserve for a single hero chart. */
  glow?: boolean;
  /** One-time stroke draw-in on mount. Off by default so scrubbing a card
   *  never replays it (it wouldn't anyway — same element, no remount — but
   *  compact previews don't need the motion at all). */
  animate?: boolean;
  /** A second series sharing this chart's y-domain, drawn under the
   *  primary line (e.g. a benchmark). Must be the same length/order as
   *  `data` — the caller aligns dates, this just plots by index. */
  secondary?: { data: LineChartPoint[]; color: string };
}) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);
  const glowId = useId();
  const width = 720;
  const padding = compact
    ? { top: 16, right: 4, bottom: 4, left: 4 }
    : { top: 12, right: 16, bottom: 28, left: 56 };

  const { path, area, points, secondaryPath, secondaryPoints, yMin, yMax, yTicks, xTickIdx } = useMemo(() => {
    if (data.length === 0) {
      return {
        path: "",
        area: "",
        points: [] as { x: number; y: number }[],
        secondaryPath: "",
        secondaryPoints: [] as { x: number; y: number }[],
        yMin: 0,
        yMax: 0,
        yTicks: [] as number[],
        xTickIdx: [] as number[],
      };
    }
    const values = data.map((d) => d.value);
    if (secondary) values.push(...secondary.data.map((d) => d.value));
    let yMin = Math.min(...values);
    let yMax = Math.max(...values);
    if (zeroLine) yMin = Math.min(yMin, 0);
    const yTicks = compact ? [] : niceTicks(yMin, yMax, 4);
    if (yTicks.length) {
      yMin = Math.min(yMin, yTicks[0]);
      yMax = Math.max(yMax, yTicks[yTicks.length - 1]);
    }
    if (yMin === yMax) {
      yMin -= 1;
      yMax += 1;
    }
    const innerW = width - padding.left - padding.right;
    const innerH = height - padding.top - padding.bottom;
    const toPoints = (series: LineChartPoint[]) =>
      series.map((d, i) => ({
        x: padding.left + (i / (series.length - 1 || 1)) * innerW,
        y: padding.top + innerH - ((d.value - yMin) / (yMax - yMin)) * innerH,
      }));
    const toPath = (pts: { x: number; y: number }[]) =>
      pts.map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(2)},${p.y.toFixed(2)}`).join(" ");

    const points = toPoints(data);
    const path = toPath(points);
    const baseline = padding.top + innerH;
    const area =
      path +
      ` L${points[points.length - 1].x.toFixed(2)},${baseline} L${points[0].x.toFixed(2)},${baseline} Z`;

    const secondaryPoints = secondary ? toPoints(secondary.data) : [];
    const secondaryPath = secondary ? toPath(secondaryPoints) : "";

    // Up to 5 evenly spaced x-axis labels, always including first/last.
    const xTickCount = Math.min(5, data.length);
    const xTickIdx =
      xTickCount <= 1
        ? [0]
        : Array.from({ length: xTickCount }, (_, i) => Math.round((i * (data.length - 1)) / (xTickCount - 1)));

    return { path, area, points, secondaryPath, secondaryPoints, yMin, yMax, yTicks, xTickIdx };
  }, [data, secondary, height, zeroLine, compact, padding.left, padding.right, padding.top, padding.bottom]);

  function handleMove(e: React.PointerEvent<SVGSVGElement>) {
    if (!interactive || !svgRef.current || points.length === 0) return;
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
  const innerH = height - padding.top - padding.bottom;
  const yToPx = (v: number) => padding.top + innerH - ((v - yMin) / (yMax - yMin)) * innerH;

  return (
    <div className="relative w-full">
      <svg
        ref={svgRef}
        viewBox={`0 0 ${width} ${height}`}
        className="w-full h-auto"
        style={{ touchAction: interactive ? "none" : undefined }}
        onPointerMove={handleMove}
        onPointerDown={handleMove}
        onPointerLeave={() => setHoverIdx(null)}
      >
        {glow && (
          <defs>
            <filter id={glowId} x="-40%" y="-40%" width="180%" height="180%">
              <feGaussianBlur stdDeviation="5" result="blur" />
              <feMerge>
                <feMergeNode in="blur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>
        )}

        {/* y-axis gridlines + ticks */}
        {!compact &&
          yTicks.map((t) => (
            <g key={t}>
              <line
                x1={padding.left}
                x2={width - padding.right}
                y1={yToPx(t)}
                y2={yToPx(t)}
                stroke="var(--faint)"
                strokeOpacity={0.3}
                strokeWidth={1}
              />
              <text
                x={padding.left - 8}
                y={yToPx(t)}
                textAnchor="end"
                dominantBaseline="middle"
                className="tabular"
                fontSize={11}
                fontFamily="JetBrains Mono, monospace"
                fill="var(--muted)"
              >
                {formatAxisValue(t, format)}
              </text>
            </g>
          ))}

        {zeroLine && yMin < 0 && yMax > 0 && (
          <line
            x1={padding.left}
            x2={width - padding.right}
            y1={yToPx(0)}
            y2={yToPx(0)}
            stroke="var(--faint)"
            strokeWidth={1}
          />
        )}

        {secondary && (
          <path
            d={secondaryPath}
            fill="none"
            stroke={secondary.color}
            strokeWidth={compact ? 1.25 : 1.75}
            strokeDasharray="5 4"
            strokeLinejoin="round"
            strokeLinecap="round"
            opacity={0.85}
          />
        )}

        <path d={area} fill={color} opacity={0.08} stroke="none" />
        <path
          d={path}
          pathLength={animate ? 1000 : undefined}
          fill="none"
          stroke={color}
          strokeWidth={compact ? 1.5 : 2}
          strokeLinejoin="round"
          strokeLinecap="round"
          className={animate ? "chart-draw-in" : undefined}
          filter={glow ? `url(#${glowId})` : undefined}
        />

        {/* x-axis date ticks */}
        {!compact &&
          xTickIdx.map((i) => (
            <text
              key={i}
              x={points[i].x}
              y={height - padding.bottom + 16}
              textAnchor={i === 0 ? "start" : i === data.length - 1 ? "end" : "middle"}
              fontSize={11}
              fontFamily="JetBrains Mono, monospace"
              fill="var(--muted)"
            >
              {formatDate(data[i].date)}
            </text>
          ))}

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
            {secondary && secondaryPoints[hoverIdx!] && (
              <circle
                cx={secondaryPoints[hoverIdx!].x}
                cy={secondaryPoints[hoverIdx!].y}
                r={3.5}
                fill={secondary.color}
                stroke="var(--paper)"
                strokeWidth={2}
              />
            )}
            <circle cx={hover.point.x} cy={hover.point.y} r={4} fill={color} stroke="var(--paper)" strokeWidth={2} />
          </>
        )}
      </svg>

      {hover && (
        <div
          className="absolute bg-[var(--paper)] border border-[var(--faint)] rounded px-2 py-1 text-xs font-mono pointer-events-none whitespace-nowrap"
          style={{
            left: `${(hover.point.x / width) * 100}%`,
            top: `${(Math.max(hover.point.y, (compact ? padding.top : padding.top) + 6) / height) * 100}%`,
            transform: `translateX(${
              hover.point.x < width * 0.12 ? "0%" : hover.point.x > width * 0.88 ? "-100%" : "-50%"
            }) translateY(calc(-100% - 8px))`,
          }}
        >
          <div className="tabular font-semibold" style={{ color }}>
            {formatValue(hover.datum.value, format)}
          </div>
          {secondary && secondary.data[hoverIdx!] && (
            <div className="tabular font-semibold" style={{ color: secondary.color }}>
              {formatValue(secondary.data[hoverIdx!].value, format)}
            </div>
          )}
          <div className="text-[var(--muted)]">{formatDate(hover.datum.date)}</div>
        </div>
      )}
    </div>
  );
}
