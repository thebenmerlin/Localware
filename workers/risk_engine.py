"""Rolling risk metrics → analytics.var_daily.

For every date in portfolio.nav_daily, computes the rolling-1y:
  * historical VaR at 95% and 99% (5th / 1st percentile of returns)
  * expected shortfall (mean of returns ≤ var_95)
  * realized vol (annualized stdev)

All math is rolling-window vectorized. ES is the one expensive piece — it
needs a rolling quantile mean — but the dataset is small enough (~1260 dates)
that .rolling().apply() finishes in under a second.

Writes via a single bulk upsert.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .lib import db


WINDOW = 252
MIN_PERIODS = 60


def _load_returns() -> pd.Series:
    rows = db.query(
        "SELECT date, daily_return FROM portfolio.nav_daily ORDER BY date;"
    )
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    s = pd.to_numeric(df["daily_return"], errors="coerce")
    s.index = df["date"]
    return s


def _rolling_es(rets: pd.Series, alpha: float = 0.05,
                window: int = WINDOW, min_periods: int = MIN_PERIODS) -> pd.Series:
    """Expected shortfall: mean of the worst alpha-quantile within each window."""
    def _es(arr):
        if len(arr) < min_periods:
            return float("nan")
        cutoff = np.quantile(arr, alpha)
        tail = arr[arr <= cutoff]
        return float(tail.mean()) if tail.size else float("nan")
    return rets.rolling(window, min_periods=min_periods).apply(_es, raw=True)


def run() -> dict:
    rets = _load_returns().fillna(0.0)
    if rets.empty or len(rets) < MIN_PERIODS:
        print("risk_engine: not enough NAV history to compute rolling VaR.")
        return {}

    var_95 = rets.rolling(WINDOW, min_periods=MIN_PERIODS).quantile(0.05)
    var_99 = rets.rolling(WINDOW, min_periods=MIN_PERIODS).quantile(0.01)
    es_95 = _rolling_es(rets, 0.05, WINDOW, MIN_PERIODS)
    vol = rets.rolling(WINDOW, min_periods=MIN_PERIODS).std() * np.sqrt(252)

    def _r(s):
        return s.where(s.notna(), None).astype(object)

    rows = list(zip(
        [d.date() for d in rets.index],
        _r(var_95),
        _r(var_99),
        _r(es_95),
        _r(vol),
    ))
    n = db.bulk_upsert(
        "analytics.var_daily",
        ["date", "var_95", "var_99", "expected_shortfall", "realized_vol"],
        rows,
        conflict_cols=["date"],
    )
    print(f"risk_engine: upserted {n} rows into analytics.var_daily")
    return {"rows": n}


if __name__ == "__main__":
    run()
