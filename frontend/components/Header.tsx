"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";

const TABS = [
  { href: "/", label: "Overview" },
  { href: "/performance", label: "Performance" },
  { href: "/positions", label: "Positions" },
];

export function Header() {
  const [open, setOpen] = useState(false);
  const pathname = usePathname();

  return (
    <div
      className="relative inline-block"
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
    >
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="font-display text-xl tracking-tight text-[var(--ink)] cursor-default"
        aria-expanded={open}
        aria-haspopup="true"
      >
        Localware
      </button>

      <nav
        className={`absolute left-0 top-full mt-2 flex flex-col gap-1 transition-all duration-150 ${
          open ? "opacity-100 translate-y-0 pointer-events-auto" : "opacity-0 -translate-y-1 pointer-events-none"
        }`}
      >
        {TABS.map((tab) => {
          const active = pathname === tab.href;
          return (
            <Link
              key={tab.href}
              href={tab.href}
              className={`font-mono text-xs uppercase tracking-wide whitespace-nowrap transition-colors ${
                active ? "text-[var(--ink)]" : "text-[var(--muted)] hover:text-[var(--ink)]"
              }`}
            >
              {tab.label}
            </Link>
          );
        })}
      </nav>
    </div>
  );
}
