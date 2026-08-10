"use client";

import { useEffect, useState } from "react";
import { LineChart, type LineChartPoint } from "./LineChart";

type Format = "money" | "percent" | "number";

function formatCurrent(v: number, format: Format): string {
  if (format === "money") return `$${v.toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
  if (format === "percent") return `${v.toFixed(1)}%`;
  return v.toFixed(2);
}

/**
 * A minimal bordered box holding a small, non-interactive chart preview.
 * The current value sits in its own header row — never inside the plot,
 * so it can't collide with the line near a series high/low.
 * Clicking the card opens a full-size version in a centered modal.
 */
export function ChartCard({
  title,
  data,
  color,
  format,
  zeroLine = false,
}: {
  title: string;
  data: LineChartPoint[];
  color: string;
  format: Format;
  zeroLine?: boolean;
}) {
  const [expanded, setExpanded] = useState(false);
  const current = data.length ? data[data.length - 1].value : null;

  useEffect(() => {
    if (!expanded) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setExpanded(false);
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [expanded]);

  return (
    <>
      <button
        type="button"
        onClick={() => setExpanded(true)}
        className="text-left w-full rounded-lg border border-[var(--faint)]/30 px-5 py-5 transition-colors hover:border-[var(--faint)]/60"
      >
        <div className="font-mono text-[0.65rem] tracking-[0.14em] uppercase text-[var(--muted)]">
          {title}
        </div>
        {current !== null && (
          <div className="tabular font-display text-2xl font-semibold mt-1" style={{ color }}>
            {formatCurrent(current, format)}
          </div>
        )}
        <div className="mt-4">
          <LineChart data={data} color={color} format={format} zeroLine={zeroLine} height={64} compact />
        </div>
      </button>

      {expanded && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-[var(--paper)]/80 backdrop-blur-sm px-6"
          onClick={() => setExpanded(false)}
        >
          <div
            className="w-full max-w-3xl rounded-lg border border-[var(--faint)]/40 bg-[var(--paper)] px-8 py-8 shadow-xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-start justify-between">
              <div>
                <div className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
                  {title}
                </div>
                {current !== null && (
                  <div className="tabular font-display text-4xl font-semibold mt-2" style={{ color }}>
                    {formatCurrent(current, format)}
                  </div>
                )}
              </div>
              <button
                type="button"
                onClick={() => setExpanded(false)}
                aria-label="Close"
                className="font-mono text-xs text-[var(--muted)] hover:text-[var(--ink)]"
              >
                esc ✕
              </button>
            </div>
            <div className="mt-8">
              <LineChart data={data} color={color} format={format} zeroLine={zeroLine} height={340} />
            </div>
          </div>
        </div>
      )}
    </>
  );
}
