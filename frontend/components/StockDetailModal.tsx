"use client";

import { useEffect, useState } from "react";
import { LineChart, type LineChartPoint } from "./LineChart";
import { pct, money, num, signed } from "@/lib/format";
import type { HoldingRow } from "./PositionsTreemap";

type PricePoint = { date: string; close: number };
type PriceState = PricePoint[] | "loading" | "error";

function Field({ label, value, tone }: { label: string; value: string; tone?: "positive" | "negative" }) {
  return (
    <div className="flex flex-col gap-0.5">
      <div className="font-mono text-[0.62rem] tracking-[0.12em] uppercase text-[var(--faint)]">{label}</div>
      <div
        className={`tabular font-mono text-sm ${
          tone === "positive" ? "text-[var(--positive)]" : tone === "negative" ? "text-[var(--negative)]" : ""
        }`}
      >
        {value}
      </div>
    </div>
  );
}

/**
 * Everything we have on one holding, opened by clicking its treemap tile.
 * Fetches a wider (180d) price window than the hover sparkline uses, so the
 * chart here shows real trend rather than a 30-day sliver.
 */
export function StockDetailModal({ row, onClose }: { row: HoldingRow; onClose: () => void }) {
  const [prices, setPrices] = useState<PriceState>("loading");

  useEffect(() => {
    let cancelled = false;
    setPrices("loading");
    fetch(`/api/ticker-prices/${encodeURIComponent(row.ticker)}?days=180`)
      .then((r) => (r.ok ? (r.json() as Promise<PricePoint[]>) : Promise.reject()))
      .then((data) => {
        if (!cancelled) setPrices([...data].reverse());
      })
      .catch(() => {
        if (!cancelled) setPrices("error");
      });
    return () => {
      cancelled = true;
    };
  }, [row.ticker]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const series: LineChartPoint[] =
    prices === "loading" || prices === "error" ? [] : prices.map((p) => ({ date: p.date, value: p.close }));
  const latest = prices !== "loading" && prices !== "error" ? prices.at(-1) : undefined;
  const prev = prices !== "loading" && prices !== "error" ? prices.at(-2) : undefined;
  const dayChange = latest && prev ? (latest.close - prev.close) / prev.close : null;
  const asOf = latest?.date ? String(latest.date).slice(0, 10) : "—";
  const pnlPct = row.market_value !== 0 ? row.unrealized_pnl / Math.abs(row.market_value) : 0;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-[var(--paper)]/80 backdrop-blur-sm px-6"
      onClick={onClose}
    >
      <div
        className="w-full max-w-2xl rounded-lg border border-[var(--faint)]/40 bg-[var(--paper)] px-8 py-8 shadow-xl max-h-[85vh] overflow-y-auto"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
              {row.sector}
            </div>
            <div className="font-display text-3xl font-semibold mt-1">{row.ticker}</div>
            <div className="text-[var(--muted)] mt-0.5">{row.name}</div>
          </div>
          <div className="text-right shrink-0">
            {latest && (
              <div className="tabular font-display text-2xl font-semibold">${latest.close.toFixed(2)}</div>
            )}
            {dayChange !== null && (
              <div
                className="tabular font-mono text-sm mt-0.5"
                style={{ color: dayChange >= 0 ? "var(--positive)" : "var(--negative)" }}
              >
                {signed(dayChange * 100, 2)}%
              </div>
            )}
            <button
              type="button"
              onClick={onClose}
              aria-label="Close"
              className="mt-3 font-mono text-xs text-[var(--muted)] hover:text-[var(--ink)]"
            >
              esc ✕
            </button>
          </div>
        </div>

        <div className="mt-6">
          {prices === "loading" ? (
            <div className="h-[220px] flex items-center justify-center text-sm text-[var(--muted)] font-mono">
              loading price history…
            </div>
          ) : prices === "error" || series.length === 0 ? (
            <div className="h-[220px] flex items-center justify-center text-sm text-[var(--muted)] font-mono">
              no price history available
            </div>
          ) : (
            <LineChart
              data={series}
              color={dayChange !== null && dayChange < 0 ? "var(--negative)" : "var(--positive)"}
              format="money"
              height={220}
              animate
            />
          )}
        </div>

        <div className="mt-8 grid grid-cols-2 sm:grid-cols-3 gap-x-6 gap-y-5">
          <Field label="Weight" value={pct(row.weight)} />
          <Field label="Market value" value={money(row.market_value)} />
          <Field
            label="Unrealized P/L"
            value={`${money(row.unrealized_pnl)} (${signed(pnlPct * 100, 1)}%)`}
            tone={row.unrealized_pnl >= 0 ? "positive" : "negative"}
          />
          <Field label="Quantity" value={num(row.quantity, 2)} />
          <Field label="Avg cost" value={row.avg_cost ? `$${row.avg_cost.toFixed(2)}` : "—"} />
          <Field label="As of" value={asOf} />
        </div>
      </div>
    </div>
  );
}
