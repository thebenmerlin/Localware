import type { Metadata } from "next";
import { Header } from "@/components/Header";
import { Particles } from "@/components/Particles";
import "./globals.css";

export const metadata: Metadata = {
  title: "Localware",
  description: "Fund status at a glance.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <Particles />
        <div className="relative z-10 min-h-screen flex flex-col">
          <header className="px-6 md:px-12 pt-12 flex justify-center">
            <Header />
          </header>
          <div className="flex-1 px-6 md:px-12 py-12">{children}</div>
        </div>
      </body>
    </html>
  );
}
