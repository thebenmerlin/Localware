import { PixelArrow } from "./PixelArrow";

type Tone = "positive" | "negative" | "neutral";

export interface BigStatDelta {
  /** Raw numeric direction — mechanical, not judgmental. See tone below. */
  direction: "up" | "down" | "flat";
  /** Semantic color — independent of direction (e.g. a deepening drawdown is a
   *  numeric decrease but should read as "bad", not automatically red-because-down). */
  tone: Tone;
  /** Optional small caption next to the arrow, e.g. "+0.34%". */
  label?: string;
}

const toneClass: Record<Tone, string> = {
  positive: "text-[var(--positive)]",
  negative: "text-[var(--negative)]",
  neutral: "text-[var(--muted)]",
};

export function BigStat({
  label,
  value,
  delta,
  size = "secondary",
}: {
  label: string;
  value: string;
  delta?: BigStatDelta;
  size?: "hero" | "secondary";
}) {
  return (
    <div className="flex flex-col items-start text-left">
      <div className="font-mono text-[0.7rem] tracking-[0.18em] uppercase text-[var(--muted)]">
        {label}
      </div>
      <div
        className={`tabular font-display font-semibold leading-none ${
          size === "hero" ? "mt-3 text-5xl md:text-6xl lg:text-7xl" : "mt-2 text-3xl md:text-4xl"
        }`}
      >
        {value}
      </div>
      {delta && (
        <div className={`mt-2 flex items-center gap-1.5 ${toneClass[delta.tone]}`}>
          <PixelArrow direction={delta.direction} className="h-3 w-3" />
          {delta.label && <span className="tabular font-mono text-sm">{delta.label}</span>}
        </div>
      )}
    </div>
  );
}
