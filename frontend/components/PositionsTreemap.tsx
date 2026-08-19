"use client";

import { useMemo, useState } from "react";
import { squarify, type TreemapRect } from "@/lib/treemap";
import { pct, money } from "@/lib/format";

export interface HoldingRow {
  ticker: string;
  name: string;
  sector: string;
  weight: number;
  market_value: number;
  unrealized_pnl: number;
}

type PricePoint = { date: string; close: number };
type PriceEntry = PricePoint[] | "loading" | "error";

// Virtual coordinate system the layout is computed in, then rendered as
// percentages — keeps the map responsive without a ResizeObserver.
const W = 1000;
const H = 560;
const SECTOR_PAD = 4;
const SECTOR_LABEL_H = 26;

// Return-on-position (not raw $ P/L) drives tile color, so a big position
// with a small gain doesn't out-glow a small position with a big one.
const PNL_SATURATION_CAP = 0.15;

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
  const coords = points.map((p, i) => ({ x: i * step, y: SPARK_H - ((p.close - min) / range) * SPARK_H }));
  const path = coords.map((c, i) => `${i === 0 ? "M" : "L"}${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(" ");
  const area = `${path} L${coords[coords.length - 1].x.toFixed(1)},${SPARK_H} L0,${SPARK_H} Z`;
  return (
    <svg width={SPARK_W} height={SPARK_H} viewBox={`0 0 ${SPARK_W} ${SPARK_H}`}>
      <path d={area} fill={color} opacity={0.14} stroke="none" />
      <path d={path} fill="none" stroke={color} strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}

interface SectorLayout {
  sector: string;
  rect: TreemapRect;
  tiles: (TreemapRect & { row: HoldingRow })[];
}

/**
 * The whole book as a squarified treemap: sector blocks sized by gross
 * sector weight, position tiles inside sized by gross position weight and
 * colored by return. Hovering a tile opens a 30-day sparkline that tracks
 * the pointer, matching the old table's hover behavior.
 */
export function PositionsTreemap({ positions }: { positions: HoldingRow[] }) {
  const [hoverTicker, setHoverTicker] = useState<string | null>(null);
  const [pos, setPos] = useState({ x: 0, y: 0 });
  const [prices, setPrices] = useState<Record<string, PriceEntry>>({});

  const layout = useMemo<SectorLayout[]>(() => {
    const bySector = new Map<string, HoldingRow[]>();
    for (const p of positions) {
      const key = p.sector || "—";
      if (!bySector.has(key)) bySector.set(key, []);
      bySector.get(key)!.push(p);
    }

    const sectorItems = Array.from(bySector.entries()).map(([sector, rows]) => ({
      id: sector,
      value: rows.reduce((s, r) => s + Math.abs(r.weight), 0),
    }));
    const sectorRects = squarify(sectorItems, 0, 0, W, H);

    return sectorRects.map((rect) => {
      const rows = bySector.get(rect.id)!;
      const innerX = rect.x + SECTOR_PAD;
      const innerY = rect.y + SECTOR_LABEL_H;
      const innerW = Math.max(0, rect.w - SECTOR_PAD * 2);
      const innerH = Math.max(0, rect.h - SECTOR_LABEL_H - SECTOR_PAD);
      const tileItems = rows.map((r) => ({ id: r.ticker, value: Math.abs(r.weight) }));
      const tileRects = squarify(tileItems, innerX, innerY, innerW, innerH);
      const rowByTicker = new Map(rows.map((r) => [r.ticker, r]));
      return {
        sector: rect.id,
        rect,
        tiles: tileRects.map((t) => ({ ...t, row: rowByTicker.get(t.id)! })),
      };
    });
  }, [positions]);

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
  const tooltipTop = hoverTicker ? Math.min(pos.y + 18, (typeof window !== "undefined" ? window.innerHeight : 800) - 130) : 0;

  return (
    <div>
      <div className="relative w-full" style={{ aspectRatio: `${W} / ${H}` }}>
        {layout.map(({ sector, rect, tiles }) => (
          <div
            key={sector}
            className="absolute"
            style={{
              left: `${(rect.x / W) * 100}%`,
              top: `${(rect.y / H) * 100}%`,
              width: `${(rect.w / W) * 100}%`,
              height: `${(rect.h / H) * 100}%`,
            }}
          >
            <div className="absolute inset-x-0 top-0 truncate px-1 font-mono text-[0.62rem] tracking-[0.1em] uppercase text-[var(--muted)]">
              {sector}
            </div>
            {tiles.map(({ x, y, w, h, row }) => {
              const area = w * h;
              const showTicker = area / (W * H) > 0.007;
              const showWeight = area / (W * H) > 0.02;
              const pnlPct = row.market_value !== 0 ? row.unrealized_pnl / Math.abs(row.market_value) : 0;
              const intensity = Math.min(1, Math.abs(pnlPct) / PNL_SATURATION_CAP);
              const tone = pnlPct >= 0 ? "var(--positive)" : "var(--negative)";
              return (
                <div
                  key={row.ticker}
                  className="absolute flex flex-col items-start justify-start overflow-hidden border border-[var(--paper)] px-1.5 py-1 cursor-default transition-[filter] hover:brightness-110"
                  style={{
                    left: `${((x - rect.x) / rect.w) * 100}%`,
                    top: `${((y - rect.y) / rect.h) * 100}%`,
                    width: `${(w / rect.w) * 100}%`,
                    height: `${(h / rect.h) * 100}%`,
                    background: tone,
                    opacity: 0.16 + intensity * 0.55,
                  }}
                  onMouseEnter={(e) => handleEnter(row.ticker, e)}
                  onMouseMove={handleMove}
                  onMouseLeave={() => setHoverTicker(null)}
                >
                  {showTicker && (
                    <span className="tabular font-mono text-[0.68rem] font-semibold text-[var(--ink)]">
                      {row.ticker}
                    </span>
                  )}
                  {showWeight && (
                    <span className="tabular font-mono text-[0.6rem] text-[var(--ink)]/70">
                      {pct(row.weight, 1)}
                    </span>
                  )}
                </div>
              );
            })}
          </div>
        ))}
      </div>

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
                    hoverData.length > 1 && hoverData.at(-1)!.close >= hoverData[0].close
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
          {(() => {
            const row = positions.find((p) => p.ticker === hoverTicker);
            if (!row) return null;
            return (
              <div className="mt-1.5 border-t border-[var(--faint)]/25 pt-1.5 tabular font-mono text-[0.65rem] text-[var(--muted)] flex flex-col gap-0.5">
                <div className="flex justify-between gap-4">
                  <span>weight</span>
                  <span>{pct(row.weight)}</span>
                </div>
                <div className="flex justify-between gap-4">
                  <span>mkt value</span>
                  <span>{money(row.market_value)}</span>
                </div>
              </div>
            );
          })()}
        </div>
      )}
    </div>
  );
}
