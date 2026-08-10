import {
  getLatestNav,
  getPreviousNav,
  getMetrics,
  getRollingSharpe,
  getDrawdownSeries,
} from "@/lib/queries";
import { BigStat, type BigStatDelta } from "@/components/BigStat";
import { PixelArrow } from "@/components/PixelArrow";
import { pct, num, signed } from "@/lib/format";

export const revalidate = 600;

/** Full-precision dollar formatting for the hero digit — money() from lib/format
 *  compresses to "$10.5M" which reads small; here we want every digit. */
function fullMoney(x: number | null | undefined): string {
  if (x === null || x === undefined || Number.isNaN(x)) return "—";
  const sign = x < 0 ? "-" : "";
  return `${sign}$${Math.abs(x).toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
}

function deltaOf(change: number | null, epsilon = 1e-9): "up" | "down" | "flat" {
  if (change === null || Number.isNaN(change)) return "flat";
  if (change > epsilon) return "up";
  if (change < -epsilon) return "down";
  return "flat";
}

export default async function Page() {
  const [nav, prevNav, annMetrics, rollingSharpe, drawdown] = await Promise.all([
    getLatestNav(),
    getPreviousNav(),
    getMetrics("1y"),
    getRollingSharpe(63),
    getDrawdownSeries(),
  ]);

  // --- NAV + Day P&L ---------------------------------------------------
  const navValue = nav?.nav ?? null;
  const navChange = navValue !== null && prevNav ? navValue - prevNav.nav : null;
  const dayReturnPct = nav?.daily_return ?? null;
  const dayDirection = deltaOf(dayReturnPct);
  const dayTone: BigStatDelta["tone"] =
    dayDirection === "up" ? "positive" : dayDirection === "down" ? "negative" : "neutral";

  // --- Sharpe (rolling 63d, delta = last two points of the same series) --
  const sharpeSeries = rollingSharpe.filter((r) => r.sharpe !== null);
  const sharpeCurr = sharpeSeries.at(-1)?.sharpe ?? null;
  const sharpePrev = sharpeSeries.at(-2)?.sharpe ?? null;
  const sharpeChange = sharpeCurr !== null && sharpePrev !== null ? sharpeCurr - sharpePrev : null;
  const sharpeDirection = deltaOf(sharpeChange);
  const sharpeTone: BigStatDelta["tone"] =
    sharpeDirection === "up" ? "positive" : sharpeDirection === "down" ? "negative" : "neutral";

  // --- Drawdown from peak (current, not historical max) ------------------
  const ddCurr = drawdown.at(-1)?.drawdown ?? null;
  const ddPrev = drawdown.at(-2)?.drawdown ?? null;
  const ddChange = ddCurr !== null && ddPrev !== null ? ddCurr - ddPrev : null;
  const ddDirection = deltaOf(ddChange);
  // Mechanical direction only — sentiment is judged separately: a deepening
  // (more negative) drawdown is bad regardless of which way the arrow points.
  const ddTone: BigStatDelta["tone"] =
    ddChange === null || ddDirection === "flat" ? "neutral" : ddChange < 0 ? "negative" : "positive";

  // --- Annualized return (1y) --------------------------------------------
  // No daily series exists for a rolling annualized return yet, so this is a
  // proxy: is today's realized daily return running above or below the pace
  // implied by the trailing annualized figure? A real getRollingAnnReturn()
  // (mirroring getRollingSharpe) is a good fast-follow once this is validated.
  const annReturn = annMetrics?.ann_return ?? null;
  const impliedDailyPace = annReturn !== null ? annReturn / 252 : null;
  const paceGap = dayReturnPct !== null && impliedDailyPace !== null ? dayReturnPct - impliedDailyPace : null;
  const paceDirection = deltaOf(paceGap);
  const paceTone: BigStatDelta["tone"] =
    paceDirection === "up" ? "positive" : paceDirection === "down" ? "negative" : "neutral";

  const asOf = nav?.date ? String(nav.date).slice(0, 10) : null;

  return (
    <div>
      {/* Hero: NAV + Day P&L */}
      <BigStat label="NAV" value={fullMoney(navValue)} size="hero" />
      <div className={`mt-4 flex items-center gap-2 ${dayTone === "positive" ? "text-[var(--positive)]" : dayTone === "negative" ? "text-[var(--negative)]" : "text-[var(--muted)]"}`}>
        <PixelArrow direction={dayDirection} className="h-4 w-4" />
        <span className="tabular font-mono text-lg">
          {signed(navChange, 0) !== "—" ? `${navChange && navChange > 0 ? "+" : ""}${fullMoney(navChange)}` : "—"}
        </span>
        <span className="tabular font-mono text-lg text-[var(--muted)]">
          ({dayReturnPct !== null ? signed(dayReturnPct * 100, 2) + "%" : "—"})
        </span>
      </div>

      {/* Secondary row: Sharpe, Drawdown, Ann. Return */}
      <div className="mt-14 md:mt-16 grid grid-cols-1 md:grid-cols-3 gap-12 md:gap-16 w-full">
        <BigStat
          label="Sharpe (63d)"
          value={num(sharpeCurr)}
          delta={{
            direction: sharpeDirection,
            tone: sharpeTone,
            label: sharpeChange !== null ? signed(sharpeChange, 2) : undefined,
          }}
        />
        <BigStat
          label="Drawdown"
          value={pct(ddCurr, 1)}
          delta={{
            direction: ddDirection,
            tone: ddTone,
            label: ddChange !== null ? signed(ddChange * 100, 1) + "%" : undefined,
          }}
        />
        <BigStat
          label="Ann. Return (1y)"
          value={pct(annReturn, 1)}
          delta={{
            direction: paceDirection,
            tone: paceTone,
          }}
        />
      </div>

      {asOf && (
        <div className="mt-14 font-mono text-xs tracking-wide uppercase text-[var(--faint)]">
          as of {asOf}
        </div>
      )}
    </div>
  );
}
