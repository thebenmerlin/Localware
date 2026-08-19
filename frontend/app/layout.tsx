import type { Metadata } from "next";
import Script from "next/script";
import { Header } from "@/components/Header";
import { ThemeToggle } from "@/components/ThemeToggle";
import { Particles } from "@/components/Particles";
import "./globals.css";

export const metadata: Metadata = {
  title: "Localware",
  description: "Fund status at a glance.",
};

// Applies a stored terminal-mode preference before first paint, so toggling
// it doesn't flash the default theme on reload. Data source is our own
// localStorage write in ThemeToggle — safe to run unguarded.
const THEME_INIT_SCRIPT = `
try {
  if (localStorage.getItem("localware-theme") === "terminal") {
    document.documentElement.dataset.theme = "terminal";
  }
} catch (e) {}
`;

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <Script id="theme-init" strategy="beforeInteractive">
          {THEME_INIT_SCRIPT}
        </Script>
        <Particles />
        <div className="relative z-10 min-h-screen flex flex-col">
          <header className="px-6 md:px-12 pt-12 flex justify-center relative">
            <Header />
            <div className="absolute right-6 md:right-12 top-12">
              <ThemeToggle />
            </div>
          </header>
          <div className="flex-1 px-6 md:px-12 py-12">{children}</div>
        </div>
      </body>
    </html>
  );
}
