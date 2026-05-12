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


# --- sleeves (vectorized) ---------------------------------------------------

def compute_momentum(panel: pd.DataFrame, **p) -> pd.Series:
    """12-1 momentum: return from (-lookback) to (-skip). Long top, short bottom."""
    lookback, skip, longs, shorts = p["lookback"], p["skip"], p["longs"], p["shorts"]
    if panel.empty or len(panel) < lookback + skip + 1:
        return pd.Series(dtype=float)

    p_skip = panel.shift(skip).iloc[-1]
    p_lb = panel.shift(lookback).iloc[-1]
    mom = (p_skip / p_lb) - 1.0
    mom = mom.replace([np.inf, -np.inf], np.nan).dropna()
    if len(mom) < (longs + shorts):
        return pd.Series(dtype=float)

    ranked = mom.sort_values()
    out = pd.Series(0.0, index=mom.index)
    if shorts > 0:
        out.loc[ranked.head(shorts).index] = -1.0 / shorts
    out.loc[ranked.tail(longs).index] = 1.0 / longs
    return out[out != 0]


def compute_quality(fund_ttm: pd.DataFrame, **p) -> pd.Series:
    """High ROE, low D/E. Long-only top N by composite z-score."""
    longs, min_roe, max_de = p["longs"], p["min_roe"], p["max_de"]
    if fund_ttm.empty:
        return pd.Series(dtype=float)

    f = fund_ttm
    eq = f["total_equity"].where(f["total_equity"] > 0)
    roe = (f["net_income_ttm"] / eq).replace([np.inf, -np.inf], np.nan)
    de = (f["total_debt"] / eq).replace([np.inf, -np.inf], np.nan)
    # Some yfinance feeds expose D/E in percent form (e.g. 87 instead of 0.87).
    de = de.where(de <= 10, de / 100)

    elig_mask = roe.notna() & de.notna() & (roe >= min_roe) & (de.between(0, max_de))
    elig = pd.DataFrame({"roe": roe[elig_mask], "de": de[elig_mask]})
    if len(elig) < 5:
        return pd.Series(dtype=float)

    z = mathx.zscore(mathx.winsorize(elig["roe"])) - mathx.zscore(mathx.winsorize(elig["de"]))
    top = z.sort_values(ascending=False).head(longs)
    return pd.Series(1.0 / len(top), index=top.index)


def compute_low_volatility(panel: pd.DataFrame, **p) -> pd.Series:
    """Bottom-quantile 60-day realized vol; equal weight long-only."""
    lookback, q = p["lookback"], p["quantile"]
    if panel.empty or len(panel) < lookback + 5:
        return pd.Series(dtype=float)

    rets = panel.pct_change().iloc[-lookback:]
    counts = rets.notna().sum()
    vol = rets.std()
    vol = vol.where(counts >= max(20, lookback // 2))
    vol = vol.replace([np.inf, -np.inf], np.nan).dropna()
    if vol.empty:
        return pd.Series(dtype=float)

    cutoff = vol.quantile(q)
    basket = vol[vol <= cutoff]
    if basket.empty:
        return pd.Series(dtype=float)
    return pd.Series(1.0 / len(basket), index=basket.index)


def compute_mean_reversion(panel: pd.DataFrame, **p) -> pd.Series:
    """RSI<th AND price>MA. Signal stays active for hold_days after trigger."""
    rsi_p, rsi_th, ma_p, hold = p["rsi_period"], p["rsi_th"], p["ma_period"], p["hold_days"]
    if panel.empty or len(panel) < ma_p + 5:
        return pd.Series(dtype=float)

    rsi_panel = mathx.rsi(panel, period=rsi_p)
    ma_panel = panel.rolling(ma_p, min_periods=ma_p).mean()
    triggered = (rsi_panel < rsi_th) & (panel > ma_panel)
    active = triggered.iloc[-hold:].any(axis=0)
    basket = active[active].index
    if len(basket) == 0:
        return pd.Series(dtype=float)
    return pd.Series(1.0 / len(basket), index=basket)


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

    alloc = _allocation_weights()
    blended, attribution = blend(sleeves, alloc)
    blended = blended[blended.abs() > 1e-9]
    if blended.empty:
        print("No blended signal produced.")
        return {"rows": 0}

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
