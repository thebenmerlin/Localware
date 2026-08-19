export interface SparklinePoint {
  date: string;
  value: number;
}

/**
 * A minimal single-series line, no axes — used inside floating hover cards
 * (positions treemap, home page metrics) where a full LineChart would be
 * too heavy. Callers own the wrapping card/positioning; this just draws.
 */
export function Sparkline({
  points,
  color,
  width = 140,
  height = 40,
}: {
  points: SparklinePoint[];
  color: string;
  width?: number;
  height?: number;
}) {
  if (points.length < 2) {
    return (
      <div
        style={{ width, height }}
        className="flex items-center justify-center text-[10px] text-[var(--muted)] font-mono"
      >
        not enough data
      </div>
    );
  }
  const values = points.map((p) => p.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const step = width / (points.length - 1);
  const coords = points.map((p, i) => ({ x: i * step, y: height - ((p.value - min) / range) * height }));
  const path = coords.map((c, i) => `${i === 0 ? "M" : "L"}${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(" ");
  const area = `${path} L${coords[coords.length - 1].x.toFixed(1)},${height} L0,${height} Z`;
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
      <path d={area} fill={color} opacity={0.14} stroke="none" />
      <path d={path} fill="none" stroke={color} strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}
