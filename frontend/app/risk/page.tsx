import { getRiskLatest, getRiskHistory } from "@/lib/queries";
import { Figure } from "@/components/paper/Figure";
import { PaperTable } from "@/components/paper/PaperTable";
import { AcademicLine } from "@/components/charts/AcademicLine";
import { Equation, InlineEq } from "@/components/paper/Equation";
import { pct, num } from "@/lib/format";

export const dynamic = "force-dynamic";

const FACTOR_DESC: Record<string, string> = {
  alpha_daily: "Intercept of daily-frequency factor regression — the unexplained excess return.",
  MKT: "Market beta against SPY total return.",
  SMB: "Small-minus-big size factor (bottom market-cap quintile minus top).",
  HML: "High-minus-low value factor (low P/B minus high).",
  RMW: "Robust-minus-weak profitability factor (high ROE minus low).",
  CMA: "Conservative-minus-aggressive proxy via vol quintiles (low vol minus high).",
};

export default async function Page() {
  const [latest, history] = await Promise.all([getRiskLatest(), getRiskHistory(252)]);
  const histData = history.map((r) => ({
    date: String(r.date).slice(0, 10),
    var95: r.var_95 == null ? null : Number(r.var_95),
    vol: r.realized_vol == null ? null : Number(r.realized_vol),
  }));
  const factors = (latest?.factor_exposures || {}) as Record<string, number>;

  return (
    <div className="space-y-2">
      <div>
        <div className="smallcaps">Section VI</div>
        <h1>Risk Diagnostics</h1>
        <p className="text-muted italic">
          Tail-risk measurement, realised volatility, and factor decomposition of the strategy returns.
        </p>
      </div>

      <hr className="rule" />

      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="kpi-card">
          <div className="label">95% Daily VaR</div>
          <div className="value">{pct(Number(latest?.var_95 ?? 0))}</div>
          <div className="sub">Historical, 1-day</div>
        </div>
        <div className="kpi-card">
          <div className="label">99% Daily VaR</div>
          <div className="value">{pct(Number(latest?.var_99 ?? 0))}</div>
          <div className="sub">Historical, 1-day</div>
        </div>
        <div className="kpi-card">
          <div className="label">Expected shortfall</div>
          <div className="value">{pct(Number(latest?.expected_shortfall ?? 0))}</div>
          <div className="sub">Mean of left tail (5%)</div>
        </div>
        <div className="kpi-card">
          <div className="label">Realised vol</div>
          <div className="value">{pct(Number(latest?.realized_vol ?? 0))}</div>
          <div className="sub">Annualised, ex-post</div>
        </div>
      </div>

      <h3>VaR&nbsp;methodology</h3>
      <p className="max-w-column">
        We use a non-parametric historical VaR: given a daily-return series&nbsp;
        <InlineEq tex="\\{r_t\\}_{t=1}^{T}" />, the&nbsp;<InlineEq tex="\\alpha" />-quantile estimate is
      </p>
      <Equation tex="\\widehat{\\mathrm{VaR}}_\\alpha = \\mathrm{Quantile}_\\alpha\\!\\left(\\{r_t\\}\\right)" number="1" />
      <p className="max-w-column">
        and the expected shortfall (CVaR) is the conditional mean below that quantile,
      </p>
      <Equation tex="\\widehat{\\mathrm{ES}}_\\alpha = \\mathbb{E}\\!\\left[r_t \\mid r_t \\le \\widehat{\\mathrm{VaR}}_\\alpha\\right]." number="2" />

      <Figure
        number="7"
        title="Trailing 1-year 95% VaR and realised volatility"
        caption={
          <>
            VaR is in daily-return units; realised vol is annualised. Spikes in VaR coincide with
            transient realised-vol expansions.
          </>
        }
      >
        <AcademicLine
          data={histData}
          xKey="date"
          yKeys={[
            { key: "var95", label: "VaR 95%", color: "crimson" },
            { key: "vol", label: "Realised vol (ann.)", color: "academic", dash: "3 3" },
          ]}
          height={240}
          yFmt="pct2"
          xFmt="YYYY-MM"
        />
      </Figure>

      <h3>Factor exposures</h3>
      <p className="max-w-column">
        We regress portfolio daily returns&nbsp;<InlineEq tex="r_t^p" /> on five factor proxies — market,
        size, value, profitability, and a low-vol/conservative proxy — and report the OLS coefficients.
      </p>
      <Equation
        tex="r_t^p = \\alpha + \\beta_{\\mathrm{MKT}} \\mathrm{MKT}_t + \\beta_{\\mathrm{SMB}} \\mathrm{SMB}_t + \\beta_{\\mathrm{HML}} \\mathrm{HML}_t + \\beta_{\\mathrm{RMW}} \\mathrm{RMW}_t + \\beta_{\\mathrm{CMA}} \\mathrm{CMA}_t + \\varepsilon_t"
        number="3"
      />

      <PaperTable
        number="8"
        title="Estimated factor loadings"
        caption={
          <>
            Loadings are unitless except <span className="font-mono">α</span>, which is reported as
            a daily-return intercept and is annualised in the methodology section.
          </>
        }
      >
        <thead>
          <tr>
            <th>Factor</th>
            <th className="num">Estimate</th>
            <th>Interpretation</th>
          </tr>
        </thead>
        <tbody>
          {Object.entries(factors).map(([k, v]) => (
            <tr key={k}>
              <td className="font-mono">{k}</td>
              <td className="num">{num(Number(v), 4)}</td>
              <td className="text-small text-muted">{FACTOR_DESC[k] ?? "—"}</td>
            </tr>
          ))}
          {Object.keys(factors).length === 0 && (
            <tr><td colSpan={3} className="caption">No factor exposures yet.</td></tr>
          )}
        </tbody>
      </PaperTable>
    </div>
  );
}
