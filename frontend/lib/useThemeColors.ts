"use client";

import { useEffect, useState } from "react";

export type ThemeColors = {
  paper: string;
  ink: string;
  muted: string;
  rule: string;
  hairline: string;
  academic: string;
  crimson: string;
  positive: string;
  negative: string;
  grid: string;
};

function read(): ThemeColors {
  if (typeof document === "undefined") {
    return {
      paper: "#fafaf7", ink: "#1a1a18", muted: "#5a5a55",
      rule: "#2c2c28", hairline: "#e7e5db",
      academic: "#1a3a8b", crimson: "#8b1a1a",
      positive: "#15613f", negative: "#8b1a1a", grid: "#d8d7cc",
    };
  }
  const s = getComputedStyle(document.documentElement);
  const v = (n: string) => s.getPropertyValue(n).trim() || "";
  return {
    paper: v("--paper"),
    ink: v("--ink"),
    muted: v("--muted"),
    rule: v("--rule"),
    hairline: v("--hairline"),
    academic: v("--academic"),
    crimson: v("--crimson"),
    positive: v("--positive"),
    negative: v("--negative"),
    grid: v("--grid"),
  };
}

export function useThemeColors(): ThemeColors {
  const [c, setC] = useState<ThemeColors>(read);
  useEffect(() => {
    setC(read());
    const obs = new MutationObserver(() => setC(read()));
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
    return () => obs.disconnect();
  }, []);
  return c;
}
