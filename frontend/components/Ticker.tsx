"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useThemeColors } from "@/lib/useThemeColors";

type Price = { date: string; close: number };

type CacheEntry = { prices: Price[]; ts: number };
const cache = new Map<string, CacheEntry>();
const inflight = new Map<string, Promise<Price[]>>();
const CACHE_TTL_MS = 5 * 60 * 1000;

async function loadPrices(ticker: string): Promise<Price[]> {
  const key = ticker.toUpperCase();
  const cached = cache.get(key);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) return cached.prices;
  const pending = inflight.get(key);
  if (pending) return pending;
  const p = fetch(`/api/prices/${encodeURIComponent(key)}`)
    .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`${r.status}`))))
    .then((j: { prices: Price[] }) => {
      cache.set(key, { prices: j.prices ?? [], ts: Date.now() });
      inflight.delete(key);
      return j.prices ?? [];
    })
    .catch((e) => {
      inflight.delete(key);
      throw e;
    });
  inflight.set(key, p);
  return p;
}

const POPUP_W = 240;
const POPUP_H = 130;
const SPARK_W = 220;
const SPARK_H = 56;
const SPARK_PAD = 4;

function Sparkline({ data, color }: { data: Price[]; color: string }) {
  if (data.length < 2) {
    return (
      <svg width={SPARK_W} height={SPARK_H}>
        <text x={SPARK_W / 2} y={SPARK_H / 2} textAnchor="middle"
              fill={color} fontSize="10" fontFamily="JetBrains Mono">
          insufficient data
        </text>
      </svg>
    );
  }
  const vals = data.map((d) => d.close);
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const range = max - min || 1;
  const w = SPARK_W - SPARK_PAD * 2;
  const h = SPARK_H - SPARK_PAD * 2;
  const pts = data.map((d, i) => {
    const x = SPARK_PAD + (i / (data.length - 1)) * w;
    const y = SPARK_PAD + h - ((d.close - min) / range) * h;
    return [x, y] as const;
  });
  const path = pts.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`).join(" ");
  const baseY = SPARK_PAD + h - ((data[0].close - min) / range) * h;
  return (
    <svg width={SPARK_W} height={SPARK_H}>
      <line x1={SPARK_PAD} x2={SPARK_W - SPARK_PAD} y1={baseY} y2={baseY}
            stroke={color} strokeOpacity={0.25} strokeDasharray="2 3" strokeWidth={0.5} />
      <path d={path} fill="none" stroke={color} strokeWidth={1.2} />
    </svg>
  );
}

export function Ticker({ symbol, className }: { symbol: string; className?: string }) {
  const t = useThemeColors();
  const [hover, setHover] = useState(false);
  const [pos, setPos] = useState<{ x: number; y: number } | null>(null);
  const [prices, setPrices] = useState<Price[] | null>(null);
  const [error, setError] = useState(false);
  const requestedRef = useRef(false);

  const sym = symbol.toUpperCase();

  const onEnter = useCallback(
    (e: React.MouseEvent) => {
      setHover(true);
      setPos({ x: e.clientX, y: e.clientY });
      if (requestedRef.current) return;
      requestedRef.current = true;
      const cached = cache.get(sym);
      if (cached) {
        setPrices(cached.prices);
        return;
      }
      loadPrices(sym)
        .then(setPrices)
        .catch(() => setError(true));
    },
    [sym],
  );

  const onMove = useCallback((e: React.MouseEvent) => {
    setPos({ x: e.clientX, y: e.clientY });
  }, []);

  const onLeave = useCallback(() => {
    setHover(false);
  }, []);

  const first = prices && prices.length > 0 ? prices[0] : null;
  const last = prices && prices.length > 0 ? prices[prices.length - 1] : null;
  const change = first && last ? (last.close - first.close) / first.close : 0;
  const isUp = change >= 0;
  const lineColor = prices && prices.length > 1 ? (isUp ? t.positive : t.negative) : t.academic;

  return (
    <>
      <span
        className={`font-mono cursor-help underline decoration-dotted decoration-1 underline-offset-2 ${className ?? ""}`}
        onMouseEnter={onEnter}
        onMouseMove={onMove}
        onMouseLeave={onLeave}
      >
        {sym}
      </span>
      {hover && pos && <Popup pos={pos} t={t}>
        <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between",
                      borderBottom: `0.5px solid ${t.hairline}`, paddingBottom: 4, marginBottom: 4 }}>
          <span style={{ fontFamily: "JetBrains Mono", fontWeight: 600, color: t.ink }}>{sym}</span>
          {last && (
            <span style={{ fontFamily: "JetBrains Mono", fontSize: 11, color: t.muted }}>
              ${last.close.toFixed(2)}{" "}
              <span style={{ color: isUp ? t.positive : t.negative }}>
                {isUp ? "+" : ""}{(change * 100).toFixed(2)}%
              </span>
            </span>
          )}
        </div>
        {error ? (
          <div style={{ height: SPARK_H, display: "flex", alignItems: "center",
                        justifyContent: "center", color: t.muted, fontSize: 11 }}>
            no data
          </div>
        ) : !prices ? (
          <div style={{ height: SPARK_H, display: "flex", alignItems: "center",
                        justifyContent: "center" }}>
            <div style={{ width: SPARK_W - 24, height: 1, background: t.hairline,
                          position: "relative", overflow: "hidden" }}>
              <div style={{ position: "absolute", inset: 0, background: t.muted, opacity: 0.4,
                            animation: "tickerPulse 1.1s ease-in-out infinite" }} />
            </div>
          </div>
        ) : (
          <Sparkline data={prices} color={lineColor} />
        )}
        {prices && prices.length > 0 && (
          <div style={{ display: "flex", justifyContent: "space-between",
                        fontFamily: "JetBrains Mono", fontSize: 10, color: t.muted, marginTop: 2 }}>
            <span>{prices[0].date}</span>
            <span>last {prices.length}d</span>
            <span>{prices[prices.length - 1].date}</span>
          </div>
        )}
      </Popup>}
    </>
  );
}

function Popup({
  pos, t, children,
}: {
  pos: { x: number; y: number };
  t: ReturnType<typeof useThemeColors>;
  children: React.ReactNode;
}) {
  const [mounted, setMounted] = useState(false);
  useEffect(() => {
    setMounted(true);
    if (document.getElementById("ticker-popup-style")) return;
    const style = document.createElement("style");
    style.id = "ticker-popup-style";
    style.textContent =
      "@keyframes tickerPulse { 0%,100% { transform: translateX(-100%); } 50% { transform: translateX(100%); } }";
    document.head.appendChild(style);
  }, []);
  if (!mounted || typeof document === "undefined") return null;

  const margin = 14;
  const vw = window.innerWidth;
  const vh = window.innerHeight;
  let x = pos.x + margin;
  let y = pos.y + margin;
  if (x + POPUP_W + 8 > vw) x = pos.x - POPUP_W - margin;
  if (y + POPUP_H + 8 > vh) y = pos.y - POPUP_H - margin;
  if (x < 4) x = 4;
  if (y < 4) y = 4;

  return createPortal(
    <div
      style={{
        position: "fixed",
        left: x,
        top: y,
        width: POPUP_W,
        background: t.paper,
        border: `0.5px solid ${t.ink}`,
        padding: 8,
        zIndex: 9999,
        pointerEvents: "none",
        boxShadow: "0 2px 8px rgba(0,0,0,0.08)",
        color: t.ink,
        fontFamily: "JetBrains Mono",
        fontSize: 12,
      }}
    >
      {children}
    </div>,
    document.body,
  );
}
