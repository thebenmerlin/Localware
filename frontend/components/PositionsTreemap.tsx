"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { squarify } from "@/lib/treemap";
import { pct, money } from "@/lib/format";
import { Sparkline } from "./Sparkline";
import { StockDetailModal } from "./StockDetailModal";

export interface HoldingRow {
  ticker: string;
  name: string;
  sector: string;
  weight: number;
  market_value: number;
  unrealized_pnl: number;
  quantity: number;
  avg_cost: number;
}

type PricePoint = { date: string; close: number };
type PriceEntry = PricePoint[] | "loading" | "error";

// Virtual coordinate system the sector *blocks* are arranged in (not the
// tiles inside them — see SectorMap, which measures its own real size).
const W = 1000;
const H = 560;
const SECTOR_LABEL_H = 22;
const SECTOR_PAD = 4;

// Return-on-position (not raw $ P/L) drives tile color, so a big position
// with a small gain doesn't out-glow a small position with a big one.
const PNL_SATURATION_CAP = 0.15;

function toneOf(row: HoldingRow) {
  const pnlPct = row.market_value !== 0 ? row.unrealized_pnl / Math.abs(row.market_value) : 0;
  const intensity = Math.min(1, Math.abs(pnlPct) / PNL_SATURATION_CAP);
  return { pnlPct, color: pnlPct >= 0 ? "var(--positive)" : "var(--negative)", opacity: 0.16 + intensity * 0.55 };
}

// Largest font (px) that fits `text` in a `widthPx` x `heightPx` box, down to
// MIN_FONT_PX — below that a label just isn't readable, so the tile goes
// unlabeled rather than rendering illegible text. JetBrains Mono runs about
// 0.62em per character; 0.62 * heightPx approximates "font fits one line."
const MIN_FONT_PX = 5;
const MAX_FONT_PX = 11;
function fitFontPx(text: string, widthPx: number, heightPx: number): number {
  const byWidth = widthPx / (text.length * 0.62 + 0.3);
  const byHeight = heightPx * 0.62;
  return Math.floor(Math.min(MAX_FONT_PX, byWidth, byHeight));
}

/**
 * One sector's tiles, laid out against this component's *own* measured
 * pixel size (ResizeObserver) rather than a shared global virtual box.
 * That's what makes ticker labels show up correctly regardless of how many
 * positions there are or how big the container ends up being — a fixed
 * area-ratio threshold (the old approach) breaks down once a sector has
 * hundreds of names, since every tile ends up tiny relative to the whole.
 * Used both inline (small, in the compact grid) and full-size (in the
 * expanded-sector modal) — same component, just a differently sized parent.
 */
function SectorMap({
  rows,
  onTileEnter,
  onTileMove,
  onTileLeave,
  onTileClick,
}: {
  rows: HoldingRow[];
  onTileEnter: (ticker: string, e: React.MouseEvent) => void;
  onTileMove: (e: React.MouseEvent) => void;
  onTileLeave: () => void;
  onTileClick: (row: HoldingRow) => void;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [box, setBox] = useState({ w: 0, h: 0 });

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const ro = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      setBox({ w: width, h: height });
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const localW = 1000;
  const localH = box.w > 0 ? Math.max(1, localW * (box.h / box.w)) : 560;
  const scale = box.w > 0 ? box.w / localW : 0;

  const tiles = useMemo(() => {
    const items = rows.map((r) => ({ id: r.ticker, value: Math.abs(r.weight) }));
    return squarify(items, 0, 0, localW, localH);
  }, [rows, localH]);

  const byTicker = useMemo(() => new Map(rows.map((r) => [r.ticker, r])), [rows]);

  return (
    <div ref={containerRef} className="relative w-full h-full">
      {tiles.map((t) => {
        const row = byTicker.get(t.id);
        if (!row) return null;
        const realW = t.w * scale;
        const realH = t.h * scale;
        const innerW = Math.max(0, realW - 5);
        const innerH = Math.max(0, realH - 4);
        const tickerFontPx = scale > 0 ? fitFontPx(row.ticker, innerW, innerH) : 0;
        const showTicker = tickerFontPx >= MIN_FONT_PX;
        const weightText = pct(row.weight, 1);
        const weightFontPx = Math.max(MIN_FONT_PX, tickerFontPx - 2);
        const showWeight =
          showTicker &&
          tickerFontPx >= 7 &&
          innerH >= tickerFontPx + weightFontPx + 3 &&
          innerW >= weightText.length * weightFontPx * 0.62;
        const tone = toneOf(row);
        return (
          <div
            key={row.ticker}
            className="group absolute overflow-hidden border border-[var(--paper)] cursor-pointer"
            style={{
              left: `${(t.x / localW) * 100}%`,
              top: `${(t.y / localH) * 100}%`,
              width: `${(t.w / localW) * 100}%`,
              height: `${(t.h / localH) * 100}%`,
            }}
            onMouseEnter={(e) => onTileEnter(row.ticker, e)}
            onMouseMove={onTileMove}
            onMouseLeave={onTileLeave}
            onClick={() => onTileClick(row)}
          >
            {/* Separate layer for the P/L color so its opacity never fades
                the ticker/weight text sitting on top of it. */}
            <div
              className="absolute inset-0 transition-[filter] group-hover:brightness-110"
              style={{ background: tone.color, opacity: tone.opacity }}
            />
            <div className="relative flex flex-col items-start justify-start px-1.5 py-1 h-full">
              {showTicker && (
                <span
                  className="tabular font-mono font-semibold leading-none text-[var(--ink)] whitespace-nowrap"
                  style={{ fontSize: `${tickerFontPx}px` }}
                >
                  {row.ticker}
                </span>
              )}
              {showWeight && (
                <span
                  className="tabular font-mono leading-none text-[var(--ink)]/70 whitespace-nowrap mt-0.5"
                  style={{ fontSize: `${weightFontPx}px` }}
                >
                  {weightText}
                </span>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

/**
 * The whole book as a squarified treemap: sector blocks sized by gross
 * sector weight, position tiles inside sized by gross position weight and
 * colored by return. Hover opens a 30-day sparkline + key figures that
 * track the pointer; click a tile for the full detail modal, or click a
 * sector's name to expand just that sector so small tiles are legible.
 */
export function PositionsTreemap({ positions }: { positions: HoldingRow[] }) {
  const [hoverTicker, setHoverTicker] = useState<string | null>(null);
  const [pos, setPos] = useState({ x: 0, y: 0 });
  const [prices, setPrices] = useState<Record<string, PriceEntry>>({});
  const [expandedSector, setExpandedSector] = useState<string | null>(null);
  const [detailRow, setDetailRow] = useState<HoldingRow | null>(null);

  const bySector = useMemo(() => {
    const m = new Map<string, HoldingRow[]>();
    for (const p of positions) {
      const key = p.sector || "—";
      if (!m.has(key)) m.set(key, []);
      m.get(key)!.push(p);
    }
    return m;
  }, [positions]);

  const sectorRects = useMemo(() => {
    const items = Array.from(bySector.entries()).map(([sector, rows]) => ({
      id: sector,
      value: rows.reduce((s, r) => s + Math.abs(r.weight), 0),
    }));
    return squarify(items, 0, 0, W, H);
  }, [bySector]);

  useEffect(() => {
    if (!expandedSector) return;
    // Detail modal renders on top when both are open — let its own Escape
    // handler close that first, so Escape unwinds one layer at a time.
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape" && !detailRow) setExpandedSector(null);
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [expandedSector, detailRow]);

  function handleTileEnter(ticker: string, e: React.MouseEvent) {
    setHoverTicker(ticker);
    setPos({ x: e.clientX, y: e.clientY });
    if (prices[ticker]) return;
    setPrices((p) => ({ ...p, [ticker]: "loading" }));
    fetch(`/api/ticker-prices/${encodeURIComponent(ticker)}`)
      .then((r) => (r.ok ? (r.json() as Promise<PricePoint[]>) : Promise.reject()))
      .then((data) => setPrices((p) => ({ ...p, [ticker]: [...data].reverse() })))
      .catch(() => setPrices((p) => ({ ...p, [ticker]: "error" })));
  }
  function handleTileMove(e: React.MouseEvent) {
    setPos({ x: e.clientX, y: e.clientY });
  }
  function handleTileLeave() {
    setHoverTicker(null);
  }
  function handleTileClick(row: HoldingRow) {
    setHoverTicker(null);
    setDetailRow(row);
  }

  const hoverRow = hoverTicker ? positions.find((p) => p.ticker === hoverTicker) ?? null : null;
  const hoverData = hoverTicker ? prices[hoverTicker] : undefined;
  const viewportW = typeof window !== "undefined" ? window.innerWidth : 1200;
  const viewportH = typeof window !== "undefined" ? window.innerHeight : 800;
  const tooltipLeft = hoverTicker ? Math.min(pos.x + 18, viewportW - 210) : 0;
  const tooltipTop = hoverTicker ? Math.min(pos.y + 18, viewportH - 210) : 0;

  return (
    <div>
      <div className="relative w-full" style={{ aspectRatio: `${W} / ${H}` }}>
        {sectorRects.map((rect) => (
          <div
            key={rect.id}
            className="absolute flex flex-col"
            style={{
              left: `${(rect.x / W) * 100}%`,
              top: `${(rect.y / H) * 100}%`,
              width: `${(rect.w / W) * 100}%`,
              height: `${(rect.h / H) * 100}%`,
            }}
          >
            <button
              type="button"
              onClick={() => setExpandedSector(rect.id)}
              title={`Expand ${rect.id}`}
              className="shrink-0 truncate px-0.5 pb-1 text-left font-mono text-[0.62rem] tracking-[0.1em] uppercase text-[var(--muted)] hover:text-[var(--ink)] hover:underline decoration-dotted underline-offset-2"
              style={{ height: SECTOR_LABEL_H }}
            >
              {rect.id}
            </button>
            <div className="relative flex-1 min-h-0" style={{ margin: `0 ${SECTOR_PAD}px ${SECTOR_PAD}px 0` }}>
              <SectorMap
                rows={bySector.get(rect.id) ?? []}
                onTileEnter={handleTileEnter}
                onTileMove={handleTileMove}
                onTileLeave={handleTileLeave}
                onTileClick={handleTileClick}
              />
            </div>
          </div>
        ))}
      </div>

      {hoverTicker && hoverRow && (
        <div
          className="fixed z-[60] pointer-events-none rounded-md border border-[var(--faint)]/40 bg-[var(--paper)] px-3 py-2.5 shadow-lg w-[190px]"
          style={{ left: tooltipLeft, top: tooltipTop }}
        >
          <div className="font-mono text-[0.65rem] tracking-wide uppercase text-[var(--muted)] truncate">
            {hoverRow.ticker} <span className="text-[var(--faint)] normal-case">· {hoverRow.name}</span>
          </div>
          <div className="mt-1.5">
            {hoverData === "loading" || hoverData === undefined ? (
              <div className="h-10 flex items-center justify-center text-[10px] text-[var(--muted)] font-mono">
                loading…
              </div>
            ) : hoverData === "error" ? (
              <div className="h-10 flex items-center justify-center text-[10px] text-[var(--muted)] font-mono">
                no data
              </div>
            ) : (
              <>
                <Sparkline
                  points={hoverData.map((p) => ({ date: p.date, value: p.close }))}
                  color={hoverData.length > 1 && hoverData.at(-1)!.close >= hoverData[0].close ? "var(--positive)" : "var(--negative)"}
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
                      <span className="text-[var(--faint)]"> /30d</span>
                    </span>
                  )}
                </div>
              </>
            )}
          </div>
          <div className="mt-1.5 border-t border-[var(--faint)]/25 pt-1.5 tabular font-mono text-[0.65rem] text-[var(--muted)] flex flex-col gap-0.5">
            <div className="flex justify-between gap-4">
              <span>weight</span>
              <span>{pct(hoverRow.weight)}</span>
            </div>
            <div className="flex justify-between gap-4">
              <span>mkt value</span>
              <span>{money(hoverRow.market_value)}</span>
            </div>
            <div className="flex justify-between gap-4">
              <span>unrl. p/l</span>
              <span style={{ color: hoverRow.unrealized_pnl >= 0 ? "var(--positive)" : "var(--negative)" }}>
                {money(hoverRow.unrealized_pnl)}
              </span>
            </div>
          </div>
          <div className="mt-1.5 text-[0.6rem] text-[var(--faint)] font-mono">click for full detail</div>
        </div>
      )}

      {expandedSector && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-[var(--paper)]/85 backdrop-blur-sm px-6 py-10"
          onClick={() => setExpandedSector(null)}
        >
          <div
            className="w-full max-w-5xl h-full rounded-lg border border-[var(--faint)]/40 bg-[var(--paper)] px-8 py-8 shadow-xl flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between shrink-0">
              <div className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
                {expandedSector}
              </div>
              <button
                type="button"
                onClick={() => setExpandedSector(null)}
                aria-label="Close"
                className="font-mono text-xs text-[var(--muted)] hover:text-[var(--ink)]"
              >
                esc ✕
              </button>
            </div>
            <div className="relative flex-1 min-h-0 mt-4">
              <SectorMap
                rows={bySector.get(expandedSector) ?? []}
                onTileEnter={handleTileEnter}
                onTileMove={handleTileMove}
                onTileLeave={handleTileLeave}
                onTileClick={handleTileClick}
              />
            </div>
          </div>
        </div>
      )}

      {detailRow && <StockDetailModal row={detailRow} onClose={() => setDetailRow(null)} />}
    </div>
  );
}
