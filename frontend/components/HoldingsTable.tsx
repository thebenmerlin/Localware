"use client";

import { useState } from "react";
import { pct, money } from "@/lib/format";

export interface HoldingRow {
  ticker: string;
  name: string;
  weight: number;
  market_value: number;
  unrealized_pnl: number;
}

export interface SectorGroup {
  sector: string;
  totalWeight: number;
  rows: HoldingRow[];
}

type PricePoint = { date: string; close: number };
type PriceEntry = PricePoint[] | "loading" | "error";

const SPARK_W = 140;
const SPARK_H = 40;

function Sparkline({ points, color }: { points: PricePoint[]; color: string }) {
  if (points.length < 2) {
    return (
      <div
        style={{ width: SPARK_W, height: SPARK_H }}
        className="flex items-center justify-center text-[10px] text-[var(--muted)] font-mono"
      >
        not enough data
      </div>
    );
  }
  const values = points.map((p) => p.close);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const step = SPARK_W / (points.length - 1);
  const coords = points.map((p, i) => ({
    x: i * step,
    y: SPARK_H - ((p.close - min) / range) * SPARK_H,
  }));
  const path = coords.map((c, i) => `${i === 0 ? "M" : "L"}${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(" ");
  const area = `${path} L${coords[coords.length - 1].x.toFixed(1)},${SPARK_H} L0,${SPARK_H} Z`;
  return (
    <svg width={SPARK_W} height={SPARK_H} viewBox={`0 0 ${SPARK_W} ${SPARK_H}`}>
      <path d={area} fill={color} opacity={0.14} stroke="none" />
      <path d={path} fill="none" stroke={color} strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

/**
 * Holdings grouped by sector, each row carrying an inline weight bar. Hovering
 * a ticker opens a small floating sparkline (last 30 sessions) that tracks the
 * pointer — data is fetched once per ticker per visit and cached in state.
 */
export function HoldingsTable({ groups, maxAbsWeight }: { groups: SectorGroup[]; maxAbsWeight: number }) {
  const [hoverTicker, setHoverTicker] = useState<string | null>(null);
  const [pos, setPos] = useState({ x: 0, y: 0 });
  const [prices, setPrices] = useState<Record<string, PriceEntry>>({});

  function handleEnter(ticker: string, e: React.MouseEvent) {
    setHoverTicker(ticker);
    setPos({ x: e.clientX, y: e.clientY });
    if (prices[ticker]) return;
    setPrices((p) => ({ ...p, [ticker]: "loading" }));
    fetch(`/api/ticker-prices/${encodeURIComponent(ticker)}`)
      .then((r) => (r.ok ? (r.json() as Promise<PricePoint[]>) : Promise.reject()))
      .then((data) => setPrices((p) => ({ ...p, [ticker]: [...data].reverse() })))
      .catch(() => setPrices((p) => ({ ...p, [ticker]: "error" })));
  }

  function handleMove(e: React.MouseEvent) {
    setPos({ x: e.clientX, y: e.clientY });
  }

  const hoverData = hoverTicker ? prices[hoverTicker] : undefined;
  const tooltipLeft = hoverTicker ? Math.min(pos.x + 18, (typeof window !== "undefined" ? window.innerWidth : 1200) - 190) : 0;
  const tooltipTop = hoverTicker ? Math.min(pos.y + 18, (typeof window !== "undefined" ? window.innerHeight : 800) - 110) : 0;

  return (
    <div className="flex flex-col gap-10">
      {groups.map((g) => (
        <div key={g.sector}>
          <div className="flex items-baseline justify-between font-mono text-[0.65rem] tracking-[0.14em] uppercase text-[var(--faint)] pb-2 border-b border-[var(--faint)]/25">
            <span>{g.sector}</span>
            <span className="text-[var(--muted)] normal-case tracking-normal">
              {pct(g.totalWeight)} · {g.rows.length} {g.rows.length === 1 ? "name" : "names"}
            </span>
          </div>
          <table className="w-full text-left border-collapse">
            <tbody className="tabular font-mono text-sm">
              {g.rows.map((p) => {
                const barWidth = maxAbsWeight > 0 ? (Math.abs(p.weight) / maxAbsWeight) * 100 : 0;
                return (
                  <tr key={p.ticker} className="border-t border-[var(--faint)]/10">
                    <td
                      className="py-2.5 pr-4 font-semibold cursor-default"
                      onMouseEnter={(e) => handleEnter(p.ticker, e)}
                      onMouseMove={handleMove}
                      onMouseLeave={() => setHoverTicker(null)}
                    >
                      {p.ticker}
                    </td>
                    <td className="py-2.5 pr-4 text-[var(--muted)] hidden sm:table-cell truncate max-w-[16rem]">
                      {p.name}
                    </td>
                    <td className="py-2.5 pr-4">
                      <div className="flex items-center gap-2.5">
                        <div className="h-1.5 w-16 shrink-0 rounded-full bg-[var(--faint)]/15 overflow-hidden">
                          <div
                            className="h-full rounded-full"
                            style={{
                              width: `${barWidth}%`,
                              background: p.weight >= 0 ? "var(--positive)" : "var(--negative)",
                            }}
                          />
                        </div>
                        <span className="w-14 text-right">{pct(Number(p.weight))}</span>
                      </div>
                    </td>
                    <td className="py-2.5 pr-4 text-right">{money(Number(p.market_value))}</td>
                    <td
                      className={`py-2.5 text-right ${
                        Number(p.unrealized_pnl) >= 0 ? "text-[var(--positive)]" : "text-[var(--negative)]"
                      }`}
                    >
                      {money(Number(p.unrealized_pnl))}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ))}

      {hoverTicker && (
        <div
          className="fixed z-50 pointer-events-none rounded-md border border-[var(--faint)]/40 bg-[var(--paper)] px-3 py-2.5 shadow-lg"
          style={{ left: tooltipLeft, top: tooltipTop }}
        >
          <div className="font-mono text-[0.65rem] tracking-wide uppercase text-[var(--muted)]">
            {hoverTicker} <span className="text-[var(--faint)]">· 30d</span>
          </div>
          <div className="mt-1.5">
            {hoverData === "loading" || hoverData === undefined ? (
              <div
                style={{ width: SPARK_W, height: SPARK_H }}
                className="flex items-center justify-center text-[10px] text-[var(--muted)] font-mono"
              >
                loading…
              </div>
            ) : hoverData === "error" ? (
              <div
                style={{ width: SPARK_W, height: SPARK_H }}
                className="flex items-center justify-center text-[10px] text-[var(--muted)] font-mono"
              >
                no data
              </div>
            ) : (
              <>
                <Sparkline
                  points={hoverData}
                  color={
                    hoverData.length > 1 && hoverData[hoverData.length - 1].close >= hoverData[0].close
                      ? "var(--positive)"
                      : "var(--negative)"
                  }
                />
                <div className="mt-1 flex items-center justify-between tabular font-mono text-xs">
                  <span>${hoverData.at(-1)?.close.toFixed(2)}</span>
                  {hoverData.length > 1 && (
                    <span
                      style={{
                        color: hoverData.at(-1)!.close >= hoverData[0].close ? "var(--positive)" : "var(--negative)",
                      }}
                    >
                      {(((hoverData.at(-1)!.close - hoverData[0].close) / hoverData[0].close) * 100).toFixed(1)}%
                    </span>
                  )}
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
