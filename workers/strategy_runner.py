"""Vectorized alpha engine — produces the blended cross-sectional signal.

Reads from:
  raw.ohlcv_daily              wide price panel
  raw.fundamentals_quarterly   point-in-time (available_at <= as_of)
  securities                   sector / asset_class lookup
  strategies                   per-sleeve allocation weights

Writes to:
  derived.signals_daily        one row per (security, date) with blended signal
                               + per-sleeve attribution (JSONB)

All cross-sectional math is panel-vectorized. No loop iterates over dates or
tickers; the only Python-level iteration is the final marshalling of ~300 rows
into the bulk-upsert payload.

CLI:
  python -m workers.strategy_runner
  python -m workers.strategy_runner --as-of 2026-05-09
"""
from __future__ import annotations

import argparse
import datetime as dt

import numpy as np
import pandas as pd
from psycopg.types.json import Jsonb

from .lib import db, mathx


# --- defaults (overridable via strategies.params, where applicable) --------

DEFAULT_HISTORY_DAYS = 420  # ~13 trading months — enough for the 252+skip window

MOM_PARAMS  = {"lookback": 252, "skip": 21, "longs": 30, "shorts": 30}
QUAL_PARAMS = {"longs": 30, "min_roe": 0.12, "max_de": 1.5}
LV_PARAMS   = {"lookback": 60, "quantile": 0.20}
MR_PARAMS   = {"rsi_period": 14, "rsi_th": 30, "ma_period": 200, "hold_days": 5}

# Fallback if the strategies table is empty (Phase 1 left it seeded, but
# don't trust it).
DEFAULT_ALLOC = {
    "momentum":       0.40,
    "quality":        0.25,
    "low_volatility": 0.20,
    "mean_reversion": 0.15,
}


# --- I/O --------------------------------------------------------------------

def _load_securities() -> pd.DataFrame:
    rows = db.query(
        "SELECT id AS security_id, ticker, sector, asset_class "
        "FROM securities WHERE active = TRUE;"
    )
    return pd.DataFrame(rows).set_index("ticker")


def _load_price_panel(as_of: dt.date, history_days: int) -> pd.DataFrame:
    """Equities-only adj_close panel. Index=date (Timestamp), columns=ticker."""
    start = as_of - dt.timedelta(days=int(history_days * 1.6))  # cushion for weekends/holidays
    rows = db.query(
        """
        SELECT s.ticker, o.date, o.adj_close
        FROM raw.ohlcv_daily o
        JOIN securities s ON s.id = o.security_id
        WHERE s.active = TRUE AND s.asset_class = 'equity'
          AND o.date BETWEEN %s AND %s
        ORDER BY o.date, s.ticker;
        """,
        (start, as_of),
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    panel = df.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    return panel.astype(float).ffill(limit=2)


def _load_fundamentals_ttm(as_of: dt.date) -> pd.DataFrame:
    """Point-in-time TTM aggregates per ticker (available_at <= as_of).

    TTM rule: flow items (revenue, net_income, opcf) sum the last 4 reported
    quarters; stock items (equity, debt, assets) take the most recent value.
    """
    rows = db.query(
        """
        SELECT s.ticker, fq.fiscal_period_end,
               fq.total_revenue, fq.net_income, fq.operating_cashflow,
               fq.total_equity, fq.total_debt, fq.total_assets
        FROM raw.fundamentals_quarterly fq
        JOIN securities s ON s.id = fq.security_id
        WHERE fq.available_at <= %s AND s.asset_class = 'equity'
        ORDER BY s.ticker, fq.fiscal_period_end DESC;
        """,
        (as_of,),
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.sort_values(["ticker", "fiscal_period_end"], ascending=[True, False])
    grp = df.groupby("ticker", as_index=False)
    flow = grp.head(4).groupby("ticker", as_index=False).agg(
        total_revenue_ttm=("total_revenue", "sum"),
        net_income_ttm=("net_income", "sum"),
        opcf_ttm=("operating_cashflow", "sum"),
    )
    stock = grp.head(1)[["ticker", "total_equity", "total_debt", "total_assets"]]
    return flow.merge(stock, on="ticker", how="left").set_index("ticker")


def _allocation_weights() -> dict[str, float]:
    rows = db.query(
        "SELECT name, allocation_weight FROM strategies WHERE enabled = TRUE;"
    )
    w = {r["name"]: float(r["allocation_weight"]) for r in (rows or [])}
    for k, v in DEFAULT_ALLOC.items():
        w.setdefault(k, v)
    return w


def _enabled_sleeves() -> set[str]:
    """Sleeve names the strategies table has enabled; all defaults if empty."""
    rows = db.query("SELECT name FROM strategies WHERE enabled = TRUE;")
    names = {r["name"] for r in (rows or [])}
    return names or set(DEFAULT_ALLOC.keys())


# --- risk-parity sleeve allocation ------------------------------------------

RP_VOL_LOOKBACK = 60


def _sleeve_vol(weights: pd.Series, panel: pd.DataFrame, lookback: int = RP_VOL_LOOKBACK) -> float:
    """Standalone annualized vol of a sleeve's signal vector, via the price cov."""
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
    return float(np.sqrt(max(float(w @ cov @ w), 0.0)))


def _risk_parity_alloc(
    sleeves: dict[str, pd.Series],
    panel: pd.DataFrame,
    enabled: set[str],
    lookback: int = RP_VOL_LOOKBACK,
) -> dict[str, float]:
    """Inverse-volatility weights across enabled, non-empty sleeves.

    Each sleeve is sized so it contributes comparable standalone risk to the
    book (risk parity), replacing the static 40/25/20/15 split. Returns {} when
    no sleeve has a computable vol, so the caller can fall back to static."""
    inv: dict[str, float] = {}
    for name, sig in sleeves.items():
        if name not in enabled or sig is None or sig.empty:
            continue
        v = _sleeve_vol(sig, panel, lookback)
        if v > 0:
            inv[name] = 1.0 / v
    total = sum(inv.values())
    if total <= 0:
        return {}
    return {k: v / total for k, v in inv.items()}


# --- sleeves (vectorized) ---------------------------------------------------
#
# Each sleeve emits a *continuous cross-sectional z-score* (a standardized
# factor view), not a hard ±1/N basket. The blended z-score is the alpha the
# downstream MVO sizes and the overlays cap, so feeding it continuous,
# winsorized, comparably-scaled scores uses far more of each signal than rank
# cliffs did. The MVO + per-name / gross caps control final long/short sizing.

def _standardize(raw: pd.Series) -> pd.Series:
    """Winsorize then cross-sectionally z-score a raw factor metric.

    Returns an empty Series if fewer than 5 valid names (too thin to rank)."""
    s = raw.replace([np.inf, -np.inf], np.nan).dropna()
    if len(s) < 5:
        return pd.Series(dtype=float)
    return mathx.zscore(mathx.winsorize(s))


def compute_momentum(panel: pd.DataFrame, **p) -> pd.Series:
    """12-1 momentum standardized cross-sectionally (long high, short low)."""
    lookback, skip = p["lookback"], p["skip"]
    if panel.empty or len(panel) < lookback + skip + 1:
        return pd.Series(dtype=float)

    p_skip = panel.shift(skip).iloc[-1]
    p_lb = panel.shift(lookback).iloc[-1]
    mom = (p_skip / p_lb) - 1.0
    return _standardize(mom)


def compute_quality(fund_ttm: pd.DataFrame, **p) -> pd.Series:
    """High ROE, low D/E composite, standardized over the eligible set."""
    min_roe, max_de = p["min_roe"], p["max_de"]
    if fund_ttm.empty:
        return pd.Series(dtype=float)

    f = fund_ttm
    eq = f["total_equity"].where(f["total_equity"] > 0)
    roe = (f["net_income_ttm"] / eq).replace([np.inf, -np.inf], np.nan)
    de = (f["total_debt"] / eq).replace([np.inf, -np.inf], np.nan)
    # Some yfinance feeds expose D/E in percent form (e.g. 87 instead of 0.87).
    de = de.where(de <= 10, de / 100)

    elig_mask = roe.notna() & de.notna() & (roe >= min_roe) & (de.between(0, max_de))
    if int(elig_mask.sum()) < 5:
        return pd.Series(dtype=float)

    comp = (mathx.zscore(mathx.winsorize(roe[elig_mask]))
            - mathx.zscore(mathx.winsorize(de[elig_mask])))
    return _standardize(comp)


def compute_low_volatility(panel: pd.DataFrame, **p) -> pd.Series:
    """Negative realized vol, standardized (low vol → high score)."""
    lookback = p["lookback"]
    if panel.empty or len(panel) < lookback + 5:
        return pd.Series(dtype=float)

    rets = panel.pct_change().iloc[-lookback:]
    counts = rets.notna().sum()
    vol = rets.std()
    vol = vol.where(counts >= max(20, lookback // 2))
    vol = vol.replace([np.inf, -np.inf], np.nan).dropna()
    if vol.empty:
        return pd.Series(dtype=float)
    return _standardize(-vol)


def compute_mean_reversion(panel: pd.DataFrame, **p) -> pd.Series:
    """Oversold-in-uptrend: (50 − RSI) for names above their MA, standardized."""
    rsi_p, ma_p = p["rsi_period"], p["ma_period"]
    if panel.empty or len(panel) < ma_p + 5:
        return pd.Series(dtype=float)

    rsi_now = mathx.rsi(panel, period=rsi_p).iloc[-1]
    ma_now = panel.rolling(ma_p, min_periods=ma_p).mean().iloc[-1]
    uptrend = panel.iloc[-1] > ma_now
    raw = (50.0 - rsi_now).where(uptrend)   # higher = more oversold in an uptrend
    return _standardize(raw)


# --- blend ------------------------------------------------------------------

def blend(sleeves: dict[str, pd.Series], alloc: dict[str, float]) -> tuple[pd.Series, pd.DataFrame]:
    """Auto-align sleeves on ticker index; multiply by allocation; sum to blended."""
    contributions = {
        name: (series * alloc.get(name, 0.0))
        for name, series in sleeves.items()
        if not series.empty and alloc.get(name, 0.0) != 0
    }
    if not contributions:
        return pd.Series(dtype=float), pd.DataFrame()
    attribution = pd.DataFrame(contributions).fillna(0.0)
    blended = attribution.sum(axis=1)
    return blended, attribution


# --- main -------------------------------------------------------------------

SIGNALS_TABLE = "derived.signals_daily"
SIGNALS_COLS = ["security_id", "date", "blended_signal", "attribution"]


def run(as_of: dt.date | None = None, history_days: int = DEFAULT_HISTORY_DAYS) -> dict:
    if as_of is None:
        rows = db.query("SELECT MAX(date) AS d FROM raw.ohlcv_daily;")
        as_of = rows[0]["d"] if rows else None
        if as_of is None:
            print("No prices in raw.ohlcv_daily — nothing to do.")
            return {"rows": 0}

    print(f"Strategy runner @ {as_of}  (history_days={history_days})")
    secs = _load_securities()
    panel = _load_price_panel(as_of, history_days)
    if panel.empty:
        print("Empty price panel — aborting.")
        return {"rows": 0}

    fund_ttm = _load_fundamentals_ttm(as_of)
    print(f"  panel: {panel.shape[0]} dates × {panel.shape[1]} tickers; "
          f"fundamentals: {len(fund_ttm)} tickers")

    sleeves: dict[str, pd.Series] = {
        "momentum":       compute_momentum(panel, **MOM_PARAMS),
        "quality":        compute_quality(fund_ttm, **QUAL_PARAMS),
        "low_volatility": compute_low_volatility(panel, **LV_PARAMS),
        "mean_reversion": compute_mean_reversion(panel, **MR_PARAMS),
    }
    for name, s in sleeves.items():
        print(f"  sleeve {name:>15}: n={len(s):>4}  "
              f"sum={float(s.sum()):+.4f}  gross={float(s.abs().sum()):.4f}")

    enabled = _enabled_sleeves()
    alloc = _risk_parity_alloc(sleeves, panel, enabled)
    if alloc:
        print("  risk-parity alloc: "
              + ", ".join(f"{k}={v:.3f}" for k, v in sorted(alloc.items())))
    else:
        # Fall back to the static table/default weights (enabled sleeves only).
        alloc = {k: v for k, v in _allocation_weights().items() if k in enabled}
        print("  risk-parity unavailable — static alloc: "
              + ", ".join(f"{k}={v:.3f}" for k, v in sorted(alloc.items())))

    blended, attribution = blend(sleeves, alloc)
    blended = blended[blended.abs() > 1e-9]
    if blended.empty:
        print("No blended signal produced.")
        return {"rows": 0}

    # Re-standardize the combined score so the alpha handed to the MVO has a
    # stable unit cross-sectional scale regardless of sleeve count / weights.
    bstd = blended.std()
    if bstd and not pd.isna(bstd) and bstd > 0:
        blended = blended / bstd
        attribution = attribution / bstd

    print(f"  blended: {len(blended)} non-zero names  "
          f"gross={float(blended.abs().sum()):.4f}  net={float(blended.sum()):+.4f}")

    ticker_to_sid = secs["security_id"].to_dict()
    attr_dict = attribution.to_dict(orient="index")
    out_rows: list[tuple] = []
    for ticker, sig in blended.items():
        sid = ticker_to_sid.get(ticker)
        if sid is None:
            continue
        contribs = {
            name: float(val)
            for name, val in attr_dict.get(ticker, {}).items()
            if val != 0 and not pd.isna(val)
        }
        out_rows.append((sid, as_of, float(sig), Jsonb(contribs)))

    n = db.bulk_upsert(
        SIGNALS_TABLE, SIGNALS_COLS, out_rows,
        conflict_cols=["security_id", "date"],
    )
    print(f"  upserted {n} rows into {SIGNALS_TABLE}")
    return {
        "rows": n,
        "sleeves": {k: int(len(v)) for k, v in sleeves.items()},
        "blended_gross": float(blended.abs().sum()),
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", type=str, default=None,
                    help="Date (YYYY-MM-DD); default = latest in raw.ohlcv_daily")
    ap.add_argument("--history-days", type=int, default=DEFAULT_HISTORY_DAYS,
                    help="Calendar days of price history to load")
    args = ap.parse_args()
    aod = dt.date.fromisoformat(args.as_of) if args.as_of else None
    run(as_of=aod, history_days=args.history_days)
