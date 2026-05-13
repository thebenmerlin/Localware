"use client";

import { useEffect, useState } from "react";

type Parts = { h: number; m: number; s: number; wd: number };

function nowEastern(): Parts {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour12: false,
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  const parts = fmt.formatToParts(new Date());
  const get = (k: string) => parts.find((p) => p.type === k)?.value ?? "";
  const weekdayMap: Record<string, number> = {
    Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6,
  };
  const h = parseInt(get("hour"), 10) % 24;
  const m = parseInt(get("minute"), 10);
  const s = parseInt(get("second"), 10);
  return { h, m, s, wd: weekdayMap[get("weekday")] ?? 0 };
}

function pad(n: number) {
  return n.toString().padStart(2, "0");
}

function isMarketOpen({ h, m, wd }: Parts): boolean {
  if (wd === 0 || wd === 6) return false;
  const mins = h * 60 + m;
  return mins >= 9 * 60 + 30 && mins < 16 * 60;
}

export function MarketClock() {
  const [t, setT] = useState<Parts | null>(null);

  useEffect(() => {
    setT(nowEastern());
    const id = setInterval(() => setT(nowEastern()), 1000);
    return () => clearInterval(id);
  }, []);

  if (!t) {
    return (
      <span className="inline-flex items-baseline gap-2 font-mono text-tiny text-muted">
        <span style={{ width: 8, height: 8, display: "inline-block" }} />
        --:--:-- ET
      </span>
    );
  }

  const open = isMarketOpen(t);
  return (
    <>
      <style>{`
        @keyframes marketPulse {
          0%, 100% { opacity: 0.35; }
          50%      { opacity: 1; }
        }
      `}</style>
      <span className="inline-flex items-baseline gap-2 font-mono text-tiny">
        <span
          aria-label={open ? "market open" : "market closed"}
          style={{
            width: 8,
            height: 8,
            borderRadius: "50%",
            display: "inline-block",
            transform: "translateY(1px)",
            background: open ? "#15a34a" : "#7a7a72",
            boxShadow: open ? "0 0 6px rgba(21,163,74,0.55)" : "none",
            animation: open ? "marketPulse 2s ease-in-out infinite" : "none",
          }}
        />
        <span className="text-muted">
          {pad(t.h)}:{pad(t.m)}:{pad(t.s)} ET
        </span>
      </span>
    </>
  );
}
