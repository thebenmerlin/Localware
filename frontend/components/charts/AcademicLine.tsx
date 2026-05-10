"use client";

import {
  Area, AreaChart, CartesianGrid, Line, LineChart, ReferenceLine,
  ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import { useThemeColors } from "@/lib/useThemeColors";

type Pt = Record<string, number | string | null>;
type YKey = { key: string; label?: string; color?: keyof Pick<ReturnType<typeof useThemeColors>, "academic"|"crimson"|"ink"|"positive"|"negative"|"muted">; dash?: string };

export function AcademicLine({
  data, xKey, yKeys, height = 240, yFmt, xFmt, fill, refY,
}: {
  data: Pt[];
  xKey: string;
  yKeys: YKey[];
  height?: number;
  yFmt?: (v: number) => string;
  xFmt?: (v: string) => string;
  fill?: boolean;
  refY?: number;
}) {
  const t = useThemeColors();
  const C = fill ? AreaChart : LineChart;
  return (
    <ResponsiveContainer width="100%" height={height}>
      <C data={data} margin={{ top: 8, right: 16, left: 8, bottom: 8 }}>
        <CartesianGrid stroke={t.grid} strokeDasharray="1 2" />
        <XAxis
          dataKey={xKey}
          tickFormatter={xFmt}
          tickLine={false}
          axisLine={{ stroke: t.ink, strokeWidth: 0.5 }}
          minTickGap={48}
          tick={{ fill: t.muted, fontSize: 11 }}
        />
        <YAxis
          tickFormatter={yFmt}
          tickLine={false}
          axisLine={{ stroke: t.ink, strokeWidth: 0.5 }}
          width={56}
          tick={{ fill: t.muted, fontSize: 11 }}
        />
        <Tooltip
          contentStyle={{
            background: t.paper,
            border: `0.5px solid ${t.ink}`,
            borderRadius: 0,
            fontSize: 12,
            fontFamily: "JetBrains Mono",
            color: t.ink,
          }}
          itemStyle={{ color: t.ink }}
          labelStyle={{ color: t.ink }}
          formatter={(v: number) => (yFmt ? yFmt(v) : v)}
        />
        {refY !== undefined && (
          <ReferenceLine y={refY} stroke={t.ink} strokeDasharray="2 3" strokeWidth={0.5} />
        )}
        {yKeys.map((y) => {
          const stroke = y.color ? t[y.color] : t.ink;
          const fillCol = y.color ? t[y.color] : t.crimson;
          return fill ? (
            <Area
              key={y.key}
              type="monotone"
              dataKey={y.key}
              stroke={stroke}
              strokeWidth={1.2}
              fill={fillCol}
              fillOpacity={0.1}
              name={y.label || y.key}
              isAnimationActive={false}
            />
          ) : (
            <Line
              key={y.key}
              type="monotone"
              dataKey={y.key}
              stroke={stroke}
              strokeWidth={1.2}
              strokeDasharray={y.dash}
              dot={false}
              name={y.label || y.key}
              isAnimationActive={false}
            />
          );
        })}
      </C>
    </ResponsiveContainer>
  );
}
