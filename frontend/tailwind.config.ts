import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  darkMode: ["selector", '[data-theme="dark"]'],
  theme: {
    extend: {
      colors: {
        paper:    "var(--paper)",
        ink:      "var(--ink)",
        muted:    "var(--muted)",
        faint:    "var(--faint)",
        positive: "var(--positive)",
        negative: "var(--negative)",
        accent:   "var(--accent)",
      },
      fontFamily: {
        display: ['"Source Serif 4"', "Georgia", "serif"],
        mono: ['"JetBrains Mono"', '"SF Mono"', "ui-monospace", "monospace"],
      },
      maxWidth: {
        dashboard: "56rem",
        wide: "72rem",
      },
    },
  },
  plugins: [],
};
export default config;
