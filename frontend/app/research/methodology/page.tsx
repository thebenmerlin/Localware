import { Equation, InlineEq } from "@/components/paper/Equation";
import { Abstract } from "@/components/paper/Section";
import { PaperTable } from "@/components/paper/PaperTable";

export default function Page() {
  return (
    <article className="prose-paper max-w-column mx-auto">
      <div className="text-center mb-4">
        <div className="smallcaps">Working Paper · v1.0 · 2026</div>
        <h1 className="font-display !mt-1 leading-tight">
          A Multi-Factor Systematic Equity Portfolio
          <br />
          <span className="text-[1.2rem] font-normal italic text-muted">
            with Volatility Targeting and a Drawdown Overlay
          </span>
        </h1>
        <div className="caption mt-3 not-italic">Localware · Research Note 26-01</div>
      </div>

      <Abstract>
        We document the construction of a fully-automated, in-process equity portfolio combining
        four orthogonal factor sleeves (cross-sectional momentum, quality, low volatility, and
        short-term mean-reversion) under a 12% volatility target with sector and per-name caps and
        a drawdown overlay. The system targets an information ratio of 1.5+ at low realised
        volatility and avoids any external broker dependency by design.
      </Abstract>

      <hr className="rule" />

      <h2>1. Universe and data</h2>
      <p>
        The investable universe is a curated set of <span className="font-mono">≈ 90</span> large-
        and mid-capitalisation US equities spanning every GICS sector. SPY is included as the
        market benchmark for beta and factor regressions but is excluded from signal generation.
        Daily open-high-low-close-volume bars are sourced from the Yahoo Finance public API and
        persisted in Postgres (table <span className="font-mono">prices</span>, primary key
        <span className="font-mono"> (security_id, date)</span>) with idempotent upserts so the
        worker is safe to re-run.
      </p>
      <p>
        Fundamentals are pulled weekly from the same source: trailing P/E, P/B, ROE, debt-to-equity,
        market capitalisation, and earnings growth. Survivorship bias in the universe is
        acknowledged and not corrected — the historical results overstate the true ex-ante hit
        rate by a small margin we estimate at 30–80 bps annualised.
      </p>

      <h2>2. Strategy sleeves</h2>

      <h3>2.1. Cross-sectional momentum (40%)</h3>
      <p>
        Following Jegadeesh and Titman (1993) and Asness, Moskowitz, and Pedersen (2013), we rank
        the universe each rebalance by 12-month price return skipping the most recent month:
      </p>
      <Equation tex={"r^{12,1}_{i,t} = \\frac{P_{i,t-21}}{P_{i,t-252}} - 1"} number="1" />
      <p>
        The portfolio holds the top-decile names long and the bottom-decile short, equal weighted
        within each leg, capped at 30 names per leg. Skipping the most-recent month removes the
        short-horizon reversal effect that contaminates raw 12-month rankings.
      </p>

      <h3>2.2. Quality (25%)</h3>
      <p>
        Quality combines profitability, leverage, and growth into a composite z-score:
      </p>
      <Equation
        tex={"Q_i = z(\\mathrm{ROE}_i) - z(\\mathrm{D/E}_i) + z(\\mathrm{EarningsGrowth}_i)"}
        number="2"
      />
      <p>
        Names with <InlineEq tex={"\\mathrm{ROE} \\ge 15\\%"} />,{" "}
        <InlineEq tex={"\\mathrm{D/E} \\le 1.0"} /> and positive year-on-year earnings growth are
        eligible; the top 30 by composite score are held long with equal weight.
      </p>

      <h3>2.3. Low volatility (20%)</h3>
      <p>
        We compute realised 60-day daily volatility,
      </p>
      <Equation
        tex={"\\hat\\sigma^{60}_{i,t} = \\sqrt{\\frac{1}{59}\\sum_{s=t-59}^{t}(r_{i,s}-\\bar r_i)^2}"}
        number="3"
      />
      <p>
        and hold the bottom-quintile (lowest realised vol) names long, equal weighted, capped at
        50 names. This sleeve harvests the well-documented betting-against-beta premium (Frazzini
        and Pedersen, 2014).
      </p>

      <h3>2.4. Short-term mean reversion (15%)</h3>
      <p>
        A name is held long if its Wilder 14-day RSI is below 30 and its current price is above
        its 200-day simple moving average. Positions are exited after five trading days regardless
        of further mean reversion, ensuring the sleeve does not double-up on momentum-driven
        breakdowns.
      </p>

      <h2>3. Portfolio construction</h2>
      <p>
        Sleeve signals are summed weighted by their static allocation to produce raw target
        weights. We then apply, in order:
      </p>
      <ol className="list-[lower-roman] ml-6 marker:font-mono marker:text-muted">
        <li>Per-name cap of 5%, applied as a hard clamp.</li>
        <li>Per-sector cap of 25%, scaling all positive weights within an over-cap sector pro-rata.</li>
        <li>
          Volatility target: scale the gross weight vector so the ex-ante portfolio volatility
          equals 12%. With sample covariance{" "}
          <InlineEq tex={"\\hat\\Sigma"} /> on the trailing 60 days,
          <Equation
            tex={"\\hat\\sigma_p^{2} = w^{\\top}\\hat\\Sigma\\, w \\cdot 252,\\qquad w \\leftarrow w \\cdot \\frac{0.12}{\\hat\\sigma_p}"}
            number="4"
          />
        </li>
        <li>
          Drawdown overlay: if the rolling drawdown exceeds 8%, multiply gross by 0.5 until the
          drawdown recovers below the threshold.
        </li>
        <li>Cap aggregate gross leverage at 1.5×.</li>
      </ol>

      <h2>4. Execution model</h2>
      <p>
        The simulator fills orders at the close-of-day adjusted price plus a slippage charge of
      </p>
      <Equation
        tex={"\\mathrm{slip}_{\\mathrm{bps}} = 5 + \\min\\!\\left(\\frac{H-L}{P} \\cdot 5000,\\, 20\\right) + \\mathbb{1}\\!\\left(\\frac{|q|P}{\\mathrm{ADV}_{30}} > 1\\%\\right) \\cdot 15"}
        number="5"
      />
      <p>
        where the first term is a fixed half-spread proxy, the second adds a high-low spread
        component capped at 20 bps, and the third applies a 15 bps impact penalty when the order
        size exceeds 1% of trailing 30-day average dollar volume. Commissions are{" "}
        <InlineEq tex={"\\$0.005"} /> per share with a one-dollar minimum.
      </p>

      <h2>5. Risk model</h2>
      <p>
        The 1-day historical Value-at-Risk and Expected Shortfall are
      </p>
      <Equation
        tex={"\\widehat{\\mathrm{VaR}}_\\alpha = \\mathrm{Quantile}_\\alpha(\\{r_t\\}),\\qquad \\widehat{\\mathrm{ES}}_\\alpha = \\mathbb{E}\\!\\left[r_t\\,|\\,r_t \\le \\widehat{\\mathrm{VaR}}_\\alpha\\right]"}
        number="6"
      />
      <p>
        We additionally regress the portfolio return on five factor proxies built from the
        universe: market (SPY), size (small-minus-big by market cap), value (low-minus-high P/B),
        profitability (high-minus-low ROE), and a low-vol proxy in place of CMA (low-vol minus
        high-vol).
      </p>

      <h2>6. Targets and expectations</h2>
      <PaperTable
        number="10"
        title="Design targets versus historical evidence on similar stacks"
        caption={
          <>
            Historical evidence aggregated from Asness et al. (2013, 2014), Frazzini and Pedersen
            (2014), and AQR working papers; figures are typical, not guaranteed.
          </>
        }
      >
        <thead>
          <tr>
            <th>Metric</th>
            <th className="num">Target</th>
            <th className="num">Comparable evidence</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Annualised return</td>
            <td className="num">~ 18%</td>
            <td className="num">15–22%</td>
          </tr>
          <tr>
            <td>Sharpe ratio</td>
            <td className="num">&gt; 1.5</td>
            <td className="num">1.3–1.7</td>
          </tr>
          <tr>
            <td>Maximum drawdown</td>
            <td className="num">&lt; 12%</td>
            <td className="num">8–14%</td>
          </tr>
          <tr>
            <td>Annualised volatility</td>
            <td className="num">~ 12%</td>
            <td className="num">10–13%</td>
          </tr>
        </tbody>
      </PaperTable>

      <h2>7. Implementation notes</h2>
      <p>
        The system is implemented as Python workers writing to a Postgres instance, with a
        Next.js read-only front-end. The scheduler runs the daily pipeline at 16:30 ET on
        weekdays and the fundamentals refresh on Sunday at 02:00. There is no broker API in the
        path: order generation and fills are entirely synthetic, against the most recent recorded
        price. All portfolio state lives in the database — there is no in-memory cache that can
        diverge from the persisted truth.
      </p>

      <h2>References</h2>
      <ul className="list-none ml-0 text-small text-muted">
        <li>Asness, C., Moskowitz, T., Pedersen, L. (2013). <em>Value and Momentum Everywhere</em>. JoF.</li>
        <li>Frazzini, A., Pedersen, L. (2014). <em>Betting Against Beta</em>. JFE.</li>
        <li>Fama, E., French, K. (2015). <em>A Five-Factor Asset Pricing Model</em>. JFE.</li>
        <li>Jegadeesh, N., Titman, S. (1993). <em>Returns to Buying Winners and Selling Losers</em>. JoF.</li>
      </ul>

      <div className="ornament"></div>
    </article>
  );
}
