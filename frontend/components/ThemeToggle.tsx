"use client";

import { useEffect, useState } from "react";

type Theme = "light" | "terminal";
const STORAGE_KEY = "localware-theme";

/**
 * Manual override for the CRT/phosphor theme (see globals.css
 * `[data-theme="terminal"]`). Independent of prefers-color-scheme — this
 * always wins once set. The blocking script in layout.tsx applies the
 * stored value before paint so there's no flash on load; this component
 * only needs to stay in sync with it after hydration.
 */
export function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>("light");

  useEffect(() => {
    const current = document.documentElement.dataset.theme;
    setTheme(current === "terminal" ? "terminal" : "light");
  }, []);

  function toggle() {
    const next: Theme = theme === "terminal" ? "light" : "terminal";
    setTheme(next);
    if (next === "terminal") {
      document.documentElement.dataset.theme = "terminal";
    } else {
      delete document.documentElement.dataset.theme;
    }
    window.localStorage.setItem(STORAGE_KEY, next);
  }

  return (
    <button
      type="button"
      onClick={toggle}
      aria-label="Toggle terminal mode"
      aria-pressed={theme === "terminal"}
      className="font-mono text-[0.65rem] tracking-[0.14em] uppercase text-[var(--muted)] hover:text-[var(--ink)] transition-colors"
    >
      {theme === "terminal" ? "[■] terminal" : "[ ] terminal"}
    </button>
  );
}
