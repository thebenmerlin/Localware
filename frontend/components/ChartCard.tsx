"use client";

import { useEffect, useState } from "react";
import { LineChart, type LineChartPoint } from "./LineChart";

/**
 * A minimal bordered box holding a small, non-interactive chart preview.
 * Clicking it opens a full-size version in a centered modal overlay.
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
  format: "money" | "percent" | "number";
  zeroLine?: boolean;
}) {
  const [expanded, setExpanded] = useState(false);

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
        className="text-left w-full rounded-lg border border-[var(--faint)]/30 px-4 py-3 transition-colors hover:border-[var(--faint)]/60"
      >
        <div className="font-mono text-[0.65rem] tracking-[0.14em] uppercase text-[var(--muted)]">
          {title}
        </div>
        <div className="mt-2">
          <LineChart data={data} color={color} format={format} zeroLine={zeroLine} height={72} compact />
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
              <div className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
                {title}
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
            <div className="mt-6">
              <LineChart data={data} color={color} format={format} zeroLine={zeroLine} height={320} />
            </div>
          </div>
        </div>
      )}
    </>
  );
}
