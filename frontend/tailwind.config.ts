import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  darkMode: ["selector", '[data-theme="dark"]'],
  theme: {
    extend: {
      colors: {
        paper:    "var(--paper)",
        ink:      "var(--ink)",
        rule:     "var(--rule)",
        muted:    "var(--muted)",
        faint:    "var(--faint)",
        sheet:    "var(--sheet)",
        hairline: "var(--hairline)",
        crimson:  "var(--crimson)",
        academic: "var(--academic)",
        positive: "var(--positive)",
        negative: "var(--negative)",
      },
      fontFamily: {
        serif: ['"EB Garamond"', "Georgia", "serif"],
        display: ['"Source Serif 4"', "Georgia", "serif"],
        mono: ['"JetBrains Mono"', '"SF Mono"', "ui-monospace", "monospace"],
      },
      fontSize: {
        body: ["1.0625rem", { lineHeight: "1.65" }],
        small: ["0.875rem", { lineHeight: "1.5" }],
        tiny: ["0.75rem", { lineHeight: "1.4" }],
      },
      maxWidth: {
        column: "42rem",
        wide: "76rem",
      },
    },
  },
  plugins: [],
};
export default config;
