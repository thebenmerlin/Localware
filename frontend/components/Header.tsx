"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useRef, useState } from "react";

const TABS = [
  { href: "/", label: "Overview" },
  { href: "/performance", label: "Performance" },
  { href: "/positions", label: "Positions" },
];

// How long the tab row stays open after the pointer leaves — gives it time
// to travel from the title down to a tab without the row vanishing first.
const CLOSE_DELAY_MS = 350;

export function Header() {
  const [open, setOpen] = useState(false);
  const [hovered, setHovered] = useState<string | null>(null);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pathname = usePathname();

  function cancelClose() {
    if (closeTimer.current) {
      clearTimeout(closeTimer.current);
      closeTimer.current = null;
    }
  }

  function scheduleClose() {
    cancelClose();
    closeTimer.current = setTimeout(() => setOpen(false), CLOSE_DELAY_MS);
  }

  return (
    <div
      className="flex flex-col items-center"
      onMouseEnter={cancelClose}
      onMouseLeave={scheduleClose}
    >
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        onMouseEnter={() => setOpen(true)}
        className="terminal-cursor font-display text-3xl md:text-4xl tracking-tight text-[var(--ink)] cursor-default"
        aria-expanded={open}
        aria-haspopup="true"
      >
        Localware
      </button>

      <nav
        className={`mt-4 flex flex-row flex-wrap items-baseline justify-center gap-x-8 gap-y-2 transition-all duration-150 ${
          open ? "opacity-100 translate-y-0 pointer-events-auto" : "opacity-0 -translate-y-1 pointer-events-none"
        }`}
      >
        {TABS.map((tab) => {
          const active = pathname === tab.href;
          const isHovered = hovered === tab.href;
          return (
            <Link
              key={tab.href}
              href={tab.href}
              onMouseEnter={() => setHovered(tab.href)}
              onMouseLeave={() => setHovered(null)}
              className={`font-display text-base whitespace-nowrap transition-all duration-150 ${
                active ? "text-[var(--ink)]" : "text-[var(--muted)]"
              } ${isHovered ? "-translate-y-0.5 font-semibold text-[var(--ink)]" : "translate-y-0"}`}
            >
              {tab.label}
            </Link>
          );
        })}
      </nav>
    </div>
  );
}
