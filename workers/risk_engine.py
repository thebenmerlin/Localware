"""Risk engine — VaR, expected shortfall, factor exposures.

Factor model: 5-factor proxies built from the universe rather than fetching
Ken French data:
  MKT  = SPY excess returns over flat rf=0
  SMB  = bottom-quintile market_cap minus top-quintile
  HML  = bottom-quintile P/B (value) minus top-quintile (growth)
  RMW  = top ROE minus bottom ROE
  CMA  = top low-vol minus bottom (low-investment proxy via inverse vol)
"""
from __future__ import annotations

import datetime as dt
import json

import numpy as np
import pandas as pd

from .lib import db, mathx


def _portfolio_returns(start: dt.date, end: dt.date) -> pd.Series:
    rows = db.query(
        "SELECT date, daily_return FROM portfolio_nav WHERE date BETWEEN %s AND %s ORDER BY date;",
        (start, end),
    )
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    s = pd.to_numeric(df["daily_return"], errors="coerce")
    s.index = df["date"]
    return s.dropna()


def _spy_returns(start: dt.date, end: dt.date) -> pd.Series:
    rows = db.query(
        """
        SELECT p.date, p.adj_close
        FROM prices p JOIN securities s ON s.id = p.security_id
        WHERE s.ticker = 'SPY' AND p.date BETWEEN %s AND %s
        ORDER BY p.date;
        """,
        (start, end),
    )
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    s = pd.to_numeric(df["adj_close"]).pct_change()
    s.index = df["date"]
    return s.dropna()


def _factor_returns(start: dt.date, end: dt.date) -> pd.DataFrame:
    """Build SMB/HML/RMW/CMA proxies from current cross-section + price returns."""
    fund = db.query(
        """
        SELECT s.ticker, f.market_cap, f.pb, f.roe FROM fundamentals f
        JOIN securities s ON s.id = f.security_id
        WHERE f.date = (SELECT MAX(date) FROM fundamentals);
        """
    )
    prices = db.query(
        """
        SELECT s.ticker, p.date, p.adj_close FROM prices p
        JOIN securities s ON s.id = p.security_id
        WHERE s.asset_class = 'equity' AND p.date BETWEEN %s AND %s;
        """,
        (start, end),
    )
    if not fund or not prices:
        return pd.DataFrame()
    fdf = pd.DataFrame(fund).set_index("ticker")
    pdf = pd.DataFrame(prices)
    pdf["date"] = pd.to_datetime(pdf["date"])
    pdf["adj_close"] = pd.to_numeric(pdf["adj_close"])
    panel = pdf.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    rets = panel.pct_change()

    fdf = fdf.apply(pd.to_numeric, errors="coerce")
    common = [t for t in rets.columns if t in fdf.index]
    if len(common) < 20:
        return pd.DataFrame()

    def long_short(series, q_long_low: bool):
        s = fdf[series].dropna()
        if len(s) < 10:
            return pd.Series(dtype=float)
        lo = s.quantile(0.2)
        hi = s.quantile(0.8)
        low_set = s[s <= lo].index
        high_set = s[s >= hi].index
        long_set, short_set = (low_set, high_set) if q_long_low else (high_set, low_set)
        long_set = [t for t in long_set if t in rets.columns]
        short_set = [t for t in short_set if t in rets.columns]
        if not long_set or not short_set:
            return pd.Series(dtype=float)
        return rets[long_set].mean(axis=1) - rets[short_set].mean(axis=1)

    vols = rets.tail(60).std()
    vol_df = pd.DataFrame({"vol": vols})
    f_low_vol = long_short("vol", q_long_low=True) if False else None  # placeholder
    # Use vol in place of investment factor (CMA proxy: low vol minus high vol)
    if not vol_df.empty:
        lo = vol_df["vol"].quantile(0.2)
        hi = vol_df["vol"].quantile(0.8)
        long_set = [t for t in vol_df[vol_df["vol"] <= lo].index if t in rets.columns]
        short_set = [t for t in vol_df[vol_df["vol"] >= hi].index if t in rets.columns]
        cma = rets[long_set].mean(axis=1) - rets[short_set].mean(axis=1) if long_set and short_set else pd.Series(dtype=float)
    else:
        cma = pd.Series(dtype=float)

    smb = long_short("market_cap", q_long_low=True)
    hml = long_short("pb", q_long_low=True)
    rmw = long_short("roe", q_long_low=False)

    mkt = _spy_returns(start, end)
    df = pd.concat({"MKT": mkt, "SMB": smb, "HML": hml, "RMW": rmw, "CMA": cma}, axis=1).dropna()
    return df


def factor_exposures(window_days: int = 252, as_of: dt.date | None = None) -> dict:
    if as_of is None:
        rows = db.query("SELECT MAX(date) AS d FROM portfolio_nav;")
        as_of = rows[0]["d"]
        if not as_of:
            return {}
    start = as_of - dt.timedelta(days=int(window_days * 1.6))
    p = _portfolio_returns(start, as_of)
    F = _factor_returns(start, as_of)
    if p.empty or F.empty:
        return {}
    df = pd.concat([p.rename("p"), F], axis=1).dropna()
    if len(df) < 30:
        return {}
    X = df[F.columns].values
    y = df["p"].values
    X1 = np.column_stack([np.ones(len(X)), X])
    try:
        beta, *_ = np.linalg.lstsq(X1, y, rcond=None)
    except Exception:
        return {}
    return {
        "alpha_daily": float(beta[0]),
        **{name: float(b) for name, b in zip(F.columns, beta[1:])},
    }


def run(as_of: dt.date | None = None) -> dict:
    if as_of is None:
        rows = db.query("SELECT MAX(date) AS d FROM portfolio_nav;")
        as_of = rows[0]["d"]
        if not as_of:
            return {}
    start = as_of - dt.timedelta(days=400)
    rets = _portfolio_returns(start, as_of)
    if rets.empty:
        return {}
    var95 = mathx.historical_var(rets, 0.05)
    var99 = mathx.historical_var(rets, 0.01)
    es = mathx.expected_shortfall(rets, 0.05)
    vol = mathx.realized_vol(rets)
    factors = factor_exposures(252, as_of)

    db.execute(
        """
        INSERT INTO risk_metrics (date, var_95, var_99, expected_shortfall, realized_vol, factor_exposures)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (date) DO UPDATE SET
          var_95 = EXCLUDED.var_95,
          var_99 = EXCLUDED.var_99,
          expected_shortfall = EXCLUDED.expected_shortfall,
          realized_vol = EXCLUDED.realized_vol,
          factor_exposures = EXCLUDED.factor_exposures;
        """,
        (as_of, var95, var99, es, vol, json.dumps(factors)),
    )
    return {"date": str(as_of), "var_95": var95, "var_99": var99, "es": es, "vol": vol, "factors": factors}


if __name__ == "__main__":
    print(run())
