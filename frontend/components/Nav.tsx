"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import clsx from "clsx";

const links = [
  { href: "/",                       label: "Overview" },
  { href: "/metrics",                label: "Fact Sheet" },
  { href: "/performance",            label: "Performance" },
  { href: "/positions",              label: "Positions" },
  { href: "/strategies",             label: "Strategies" },
  { href: "/trades",                 label: "Trades" },
  { href: "/risk",                   label: "Risk" },
  { href: "/backtest",               label: "Backtests" },
  { href: "/research/methodology",   label: "Methodology" },
];

export function Nav() {
  const path = usePathname();
  return (
    <nav className="flex flex-1 flex-wrap items-baseline justify-center gap-x-5 gap-y-1">
      {links.map((l) => {
        const active = path === l.href || (l.href !== "/" && path.startsWith(l.href));
        return (
          <Link
            key={l.href}
            href={l.href}
            className={clsx("nav-link", active && "active")}
          >
            {l.label}
          </Link>
        );
      })}
    </nav>
  );
}
