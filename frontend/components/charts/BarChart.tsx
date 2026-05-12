"use client";

import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis, Cell } from "recharts";
import { useThemeColors } from "@/lib/useThemeColors";

export function AcademicBar({
  data, xKey, yKey, height = 240, yFmt, signed,
}: {
  data: Record<string, number | string>[];
  xKey: string;
  yKey: string;
  height?: number;
  yFmt?: (v: number) => string;
  signed?: boolean;
}) {
  const t = useThemeColors();
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 8, right: 16, left: 8, bottom: 8 }}>
        <CartesianGrid stroke={t.grid} strokeDasharray="1 2" vertical={false} />
        <XAxis
          dataKey={xKey}
          tickLine={false}
          axisLine={{ stroke: t.ink, strokeWidth: 0.5 }}
          interval={0}
          angle={-30}
          textAnchor="end"
          height={60}
          tick={{ fill: t.muted, fontSize: 11 }}
        />
        <YAxis
          tickFormatter={(v) => formatY(v as number, yFmt)}
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
          formatter={(v: number) => formatY(v, yFmt)}
        />
        <Bar dataKey={yKey} isAnimationActive={false}>
          {data.map((d, i) => {
            const v = Number(d[yKey]) || 0;
            const color = signed ? (v >= 0 ? t.positive : t.negative) : t.academic;
            return <Cell key={i} fill={color} />;
          })}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
 Number(d[yKey]) || 0;
            const color = signed ? (v >= 0 ? t.positive : t.negative) : t.academic;
            return <Cell key={i} fill={color} />;
          })}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
}
