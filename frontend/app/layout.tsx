import type { Metadata } from "next";
import "./globals.css";
import Link from "next/link";
import { Nav } from "@/components/Nav";
import { ThemeToggle } from "@/components/ThemeToggle";
import { ThemeScript } from "@/components/ThemeScript";

export const metadata: Metadata = {
  title: "Localware — Quantitative Multi-Factor Fund",
  description:
    "An automated, multi-factor systematic equity strategy with risk-parity weighting and volatility targeting.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <ThemeScript />
      </head>
      <body>
        <header className="border-b border-rule bg-paper sticky top-0 z-10 backdrop-blur">
          <div className="max-w-wide mx-auto px-6 py-3.5 flex items-baseline gap-x-8 gap-y-2 flex-wrap">
            <div className="flex items-baseline gap-3">
              <Link href="/" className="no-underline hover:no-underline">
                <h1 className="text-[1.05rem] !mt-0 !mb-0 font-display font-semibold tracking-tight">
                  Localware
                </h1>
              </Link>
            </div>
            <Nav />
            <div className="ml-auto">
              <ThemeToggle />
            </div>
          </div>
        </header>
        <main className="max-w-wide mx-auto px-6 py-8">{children}</main>
        <footer className="border-t border-rule mt-16">
          <div className="max-w-wide mx-auto px-6 py-6 text-tiny text-muted flex flex-wrap gap-x-6 gap-y-1 items-baseline">
            <span className="smallcaps">colophon</span>
            <span className="ml-auto italic">All figures simulated against historical prices.</span>
          </div>
        </footer>
      </body>
    </html>
  );
}
