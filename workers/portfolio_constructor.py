"""Portfolio constructor — blended signal → risk-managed target weights.

Reads:
  derived.signals_daily       blended signal per ticker for `as_of`
  raw.ohlcv_daily             60d panel for portfolio-vol estimation
  securities                  sector mapping
  portfolio_nav (optional)    most recent NAV history for drawdown overlay

Writes:
  portfolio.positions_daily   target weights effective on the NEXT business day

Pipeline (single pass, fully vectorized):
  1. Load blended signals as a pd.Series indexed by ticker.
  2. Estimate annualized portfolio vol from the 60d return covariance.
  3. vol scalar = clip(VOL_TARGET / realized, [0.2, 2.0]).
  4. drawdown overlay: scalar *= 0.5 if current DD > 8%.
  5. Apply scalar.
  6. Per-name cap: clip(|w|) to 5%.
  7. Per-sector cap: proportionally scale down sectors whose long gross
     (or short gross) exceeds 25%.
  8. Gross leverage cap: scale all weights so Σ|w| ≤ 1.5.
  9. Upsert to portfolio.positions_daily with date = next business day.

No per-ticker or per-date loops anywhere in the math path.

CLI:
  python -m workers.portfolio_constructor
  python -m workers.portfolio_constructor --as-of 2026-05-09
"""
from __future__ import annotations

import argparse
import datetime as dt

import numpy as np
import pandas as pd

from .lib import db
from .portfolio_optimizer import solve_ensemble
from .score_ensemble import load_latest_member_weights


# --- spec-driven tunables ---------------------------------------------------

VOL_TARGET = 0.12          # 12% annualized portfolio vol
DD_TRIGGER = 0.08          # halve gross above this drawdown
DD_FACTOR = 0.5
PER_NAME_CAP = 0.05        # 5% per name
PER_SECTOR_CAP = 0.25      # 25% per sector (each side)
MAX_GROSS = 1.5            # 1.5× max gross leverage

VOL_LOOKBACK = 60
VOL_SCALAR_CLAMP = (0.2, 2.0)


# --- I/O --------------------------------------------------------------------

def _load_signals(as_of: dt.date) -> pd.DataFrame:
    rows = db.query(
        """
        SELECT s.ticker, s.id AS security_id, s.sector, sd.blended_signal
        FROM derived.signals_daily sd
        JOIN securities s ON s.id = sd.security_id
        WHERE sd.date = %s;
        """,
        (as_of,),
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["blended_signal"] = pd.to_numeric(df["blended_signal"])
    return df.set_index("ticker")


def _load_price_panel(as_of: dt.date, days: int = 100) -> pd.DataFrame:
    start = as_of - dt.timedelta(days=int(days * 1.7))
    rows = db.query(
        """
        SELECT s.ticker, o.date, o.adj_close
        FROM raw.ohlcv_daily o
        JOIN securities s ON s.id = o.security_id
        WHERE s.active = TRUE AND s.asset_class = 'equity'
          AND o.date BETWEEN %s AND %s;
        """,
        (start, as_of),
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return (
        df.pivot(index="date", columns="ticker", values="adj_close")
          .sort_index()
          .astype(float)
          .ffill(limit=2)
    )


def _load_prev_weights(as_of: dt.date) -> pd.Series:
    """Most recent filled target weights from portfolio.positions_daily on or
    before `as_of`. Indexed by ticker. Empty Series on cold start."""
    rows = db.query(
        """
        SELECT s.ticker, p.target_weight
        FROM portfolio.positions_daily p
        JOIN securities s ON s.id = p.security_id
        WHERE p.date = (
            SELECT MAX(date) FROM portfolio.positions_daily WHERE date <= %s
        );
        """,
        (as_of,),
    )
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series(
        {r["ticker"]: float(r["target_weight"]) for r in rows}, dtype=float,
    )


def _current_drawdown(as_of: dt.date) -> float:
    """Returns drawdown as a positive number (0.10 = 10% below peak)."""
    rows = db.query(
        "SELECT nav FROM portfolio.nav_daily WHERE date <= %s ORDER BY date;",
        (as_of,),
    )
    if not rows or len(rows) < 2:
        return 0.0
    navs = pd.Series([float(r["nav"]) for r in rows])
    peak = navs.cummax()
    dd = (navs - peak) / peak
    return float(-dd.iloc[-1])


# --- vectorized risk math ---------------------------------------------------

def _portfolio_vol(weights: pd.Series, panel: pd.DataFrame, lookback: int) -> float:
    common = weights.index.intersection(panel.columns)
    if len(common) == 0:
        return 0.0
    rets = panel[common].pct_change().iloc[-lookback:].dropna(how="all")
    if rets.empty:
        return 0.0
    rets = rets.fillna(0.0)
    w = weights.loc[common].values.astype(float)
    cov = np.cov(rets.values, rowvar=False) * 252
    if cov.ndim == 0:
        cov = np.array([[float(cov)]])
    var = float(w @ cov @ w)
    return float(np.sqrt(max(var, 0.0)))


def _apply_per_name_cap(weights: pd.Series, cap: float) -> pd.Series:
    return weights.clip(lower=-cap, upper=cap)


def _apply_sector_cap(weights: pd.Series, sectors: pd.Series, cap: float) -> pd.Series:
    """Scale long/short halves separately so per-sector gross ≤ cap."""
    if weights.empty:
        return weights
    sec = sectors.reindex(weights.index).fillna("?")

    long_w = weights.where(weights > 0, 0.0)
    short_w = (-weights).where(weights < 0, 0.0)

    long_sums = long_w.groupby(sec).sum()
    short_sums = short_w.groupby(sec).sum()

    long_scale = (cap / long_sums.replace(0, np.nan)).clip(upper=1.0).fillna(1.0)
    short_scale = (cap / short_sums.replace(0, np.nan)).clip(upper=1.0).fillna(1.0)

    long_factor = sec.map(long_scale).astype(float).fillna(1.0)
    short_factor = sec.map(short_scale).astype(float).fillna(1.0)

    factor = pd.Series(1.0, index=weights.index)
    long_mask = weights > 0
    short_mask = weights < 0
    factor.loc[long_mask] = long_factor.loc[long_mask]
    factor.loc[short_mask] = short_factor.loc[short_mask]
    return weights * factor


def _cap_gross(weights: pd.Series, max_gross: float) -> pd.Series:
    gross = float(weights.abs().sum())
    if gross <= max_gross or gross == 0:
        return weights
    return weights * (max_gross / gross)


# --- pure risk pipeline (used by both single-day & backtest paths) ---------

def apply_overlays(
    blended: pd.Series,
    sectors: pd.Series | dict,
    panel: pd.DataFrame,
    drawdown: float = 0.0,
    vol_target: float = VOL_TARGET,
    dd_trigger: float = DD_TRIGGER,
    dd_factor: float = DD_FACTOR,
    per_name_cap: float = PER_NAME_CAP,
    per_sector_cap: float = PER_SECTOR_CAP,
    max_gross: float = MAX_GROSS,
    vol_lookback: int = VOL_LOOKBACK,
    vol_scalar_clamp: tuple[float, float] = VOL_SCALAR_CLAMP,
) -> pd.Series:
    """vol-target → DD overlay → per-name cap → per-sector cap → gross cap."""
    pre = blended.astype(float)
    if pre.empty:
        return pre
    realized = _portfolio_vol(pre, panel, vol_lookback)
    if realized > 0:
        vol_scalar = float(np.clip(vol_target / realized, *vol_scalar_clamp))
    else:
        vol_scalar = 1.0
    dd_f = dd_factor if drawdown > dd_trigger else 1.0
    scalar = vol_scalar * dd_f

    sec_series = sectors if isinstance(sectors, pd.Series) else pd.Series(sectors)
    w = pre * scalar
    w = _apply_per_name_cap(w, per_name_cap)
    w = _apply_sector_cap(w, sec_series, per_sector_cap)
    w = _cap_gross(w, max_gross)
    return w[w.abs() > 1e-6]


# --- next business day ------------------------------------------------------

def _next_business_day(d: dt.date) -> dt.date:
    return (pd.Timestamp(d) + pd.tseries.offsets.BDay(1)).date()


# --- main -------------------------------------------------------------------

POSITIONS_TABLE = "portfolio.positions_daily"
POSITIONS_COLS = [
    "security_id", "date", "target_weight", "pre_overlay_weight", "sector",
]


def construct(as_of: dt.date | None = None) -> pd.DataFrame:
    if as_of is None:
        rows = db.query("SELECT MAX(date) AS d FROM derived.signals_daily;")
        as_of = rows[0]["d"] if rows else None
        if as_of is None:
            print("No signals in derived.signals_daily — nothing to construct.")
            return pd.DataFrame()

    print(f"Portfolio constructor @ signal_date={as_of}")
    sigs = _load_signals(as_of)
    if sigs.empty:
        print("No signals for this date.")
        return pd.DataFrame()

    alpha_raw = sigs["blended_signal"].astype(float).copy()
    sectors = sigs["sector"].astype("string")
    sids = sigs["security_id"].astype(int)

    # --- Filter out tickers with unknown sectors before optimization.
    # solve_ensemble builds one neutrality constraint row per unique sector
    # label; allowing NaN / "?" creates a spurious "Unknown" bucket that the
    # optimizer would dutifully neutralize against, distorting weights.
    known_mask = sectors.notna() & (sectors.str.strip() != "") & (sectors != "?")
    n_unknown = int((~known_mask).sum())
    if n_unknown:
        print(f"  dropping {n_unknown} tickers with unknown sector before optimization")
    alpha = alpha_raw[known_mask]
    sectors_known = sectors[known_mask]

    panel = _load_price_panel(as_of)
    prev_w = _load_prev_weights(as_of)
    print(f"  prev positions loaded: {len(prev_w)} names "
          f"(gross={float(prev_w.abs().sum()):.4f})")

    # QP ensemble: equal-weighted across (risk, turnover, lookback, neutrality)
    # grid; returns a pre-overlay weight vector that we then feed through the
    # existing vol / DD / cap pipeline.
    member_w = load_latest_member_weights(as_of)
    print(f"  loaded {len(member_w)} member scores (sum={float(member_w.sum()):.2f})")

    pre_opt = solve_ensemble(
        alpha=alpha,
        panel=panel,
        sectors=sectors_known,
        prev_w=prev_w,
        benchmark=None,  # wire up market ETF once it lives in `securities`
        member_weights=member_w if not member_w.empty else None,
    )
    # Re-index to the full signal universe so downstream code (sids lookup,
    # bulk_upsert) still sees every ticker; missing names become 0 weight.
    pre = pre_opt.reindex(alpha_raw.index).fillna(0.0)
    print(f"  ensemble pre-overlay: nonzero={int((pre.abs() > 1e-8).sum())}  "
          f"gross={float(pre.abs().sum()):.4f}  net={float(pre.sum()):+.4f}")

    realized = _portfolio_vol(pre, panel, VOL_LOOKBACK)
    if realized > 0:
        raw_scalar = VOL_TARGET / realized
        vol_scalar = float(np.clip(raw_scalar, *VOL_SCALAR_CLAMP))
    else:
        vol_scalar = 1.0

    dd = _current_drawdown(as_of)
    dd_factor = DD_FACTOR if dd > DD_TRIGGER else 1.0
    scalar = vol_scalar * dd_factor

    print(f"  pre-overlay gross={float(pre.abs().sum()):.4f}  net={float(pre.sum()):+.4f}")
    print(f"  realized_vol={realized:.4f}  vol_scalar={vol_scalar:.3f}  "
          f"dd={dd:.3f}  dd_factor={dd_factor:.2f}  -> scalar={scalar:.3f}")

    w = pre * scalar
    w = _apply_per_name_cap(w, PER_NAME_CAP)
    w = _apply_sector_cap(w, sectors, PER_SECTOR_CAP)
    w = _cap_gross(w, MAX_GROSS)

    # Drop ~zero weights
    w = w[w.abs() > 1e-6]
    if w.empty:
        print("All weights collapsed to zero after caps.")
        return pd.DataFrame()

    target_date = _next_business_day(as_of)
    print(f"  final: {len(w)} positions  gross={float(w.abs().sum()):.4f}  "
          f"net={float(w.sum()):+.4f}  target_date={target_date}")

    out_rows: list[tuple] = []
    for ticker, weight in w.items():
        out_rows.append((
            int(sids.loc[ticker]),
            target_date,
            float(weight),
            float(pre.loc[ticker]),
            sectors.loc[ticker] if pd.notna(sectors.loc[ticker]) else None,
        ))

    n = db.bulk_upsert(
        POSITIONS_TABLE, POSITIONS_COLS, out_rows,
        conflict_cols=["security_id", "date"],
    )
    print(f"  upserted {n} rows into {POSITIONS_TABLE}")

    # Return the same shape as a DataFrame for callers that want to inspect
    out_df = pd.DataFrame(out_rows, columns=POSITIONS_COLS)
    return out_df.sort_values("target_weight", ascending=False).reset_index(drop=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", type=str, default=None,
                    help="Signal date (YYYY-MM-DD); default = latest in derived.signals_daily")
    args = ap.parse_args()
    aod = dt.date.fromisoformat(args.as_of) if args.as_of else None
    df = construct(as_of=aod)
    if df is not None and not df.empty:
        print()
        print(df.head(20).to_string(index=False))
