"""Performance analytics — pre-calculates every chart the Vercel UI needs.

Reads:
  portfolio.nav_daily          full NAV history
  raw.ohlcv_daily              SPY benchmark adj_close

Writes (all idempotent bulk upserts):
  analytics.equity_curve       per-date nav, return, drawdown, benchmark
  analytics.performance_summary  one row per period ('all','ytd','1y','3m','1m')
  analytics.rolling_metrics    per-date rolling 1y sharpe/vol/MDD, 3m return
  analytics.monthly_returns    year × month heatmap
  analytics.drawdown_periods   peak → trough → recovery spans

All math is panel- or series-vectorized. No per-day loops in computation.
Rendering each chart in Vercel is then one SELECT against a single table.
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from .lib import db, mathx


# --- I/O --------------------------------------------------------------------

def _load_nav() -> pd.DataFrame:
    rows = db.query(
        "SELECT date, nav, daily_return FROM portfolio.nav_daily ORDER BY date;"
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df["nav"] = pd.to_numeric(df["nav"]).astype(float)
    df["daily_return"] = pd.to_numeric(df["daily_return"], errors="coerce").astype(float)
    # Backfill daily_return from NAV if missing
    if df["daily_return"].isna().all():
        df["daily_return"] = df["nav"].pct_change()
    return df


def _load_spy() -> pd.Series:
    rows = db.query(
        """
        SELECT o.date, o.adj_close
        FROM raw.ohlcv_daily o JOIN securities s ON s.id = o.security_id
        WHERE s.ticker = 'SPY' ORDER BY o.date;
        """
    )
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    s = pd.to_numeric(df["adj_close"]).astype(float)
    s.index = df["date"]
    return s.sort_index()


# --- equity curve -----------------------------------------------------------

def _write_equity_curve(nav_df: pd.DataFrame, spy: pd.Series) -> int:
    eq = nav_df["nav"]
    rets = nav_df["daily_return"].fillna(0.0)
    peak = eq.cummax()
    drawdown = (eq - peak) / peak
    cum_ret = eq / eq.iloc[0] - 1.0

    if not spy.empty:
        spy_aligned = spy.reindex(eq.index).ffill()
        bench_cum = spy_aligned / spy_aligned.dropna().iloc[0] - 1.0
    else:
        bench_cum = pd.Series(np.nan, index=eq.index)

    rows = list(zip(
        [d.date() for d in eq.index],
        eq.astype(float).round(4),
        rets.astype(float).round(8),
        cum_ret.astype(float).round(8),
        peak.astype(float).round(4),
        drawdown.astype(float).round(8),
        bench_cum.astype(float).round(8).where(bench_cum.notna(), None),
    ))
    return db.bulk_upsert(
        "analytics.equity_curve",
        ["date", "nav", "daily_return", "cumulative_return",
         "peak_nav", "drawdown", "benchmark_cumret"],
        rows,
        conflict_cols=["date"],
    )


# --- rolling metrics --------------------------------------------------------

def _write_rolling_metrics(nav_df: pd.DataFrame) -> int:
    rets = nav_df["daily_return"].fillna(0.0)
    eq = nav_df["nav"]

    roll_252 = rets.rolling(252, min_periods=60)
    rolling_1y_ret = roll_252.mean() * 252
    rolling_1y_vol = roll_252.std() * np.sqrt(252)
    rolling_1y_sharpe = (rolling_1y_ret / rolling_1y_vol).where(rolling_1y_vol > 0)
    rolling_3m_ret = rets.rolling(63, min_periods=20).mean() * 252
    rolling_1y_mdd = mathx.rolling_max_drawdown(eq, window=252, min_periods=60)

    def _r(s):
        return s.where(s.notna(), None).astype(object)

    rows = list(zip(
        [d.date() for d in nav_df.index],
        _r(rolling_1y_ret),
        _r(rolling_1y_vol),
        _r(rolling_1y_sharpe),
        _r(rolling_3m_ret),
        _r(rolling_1y_mdd),
    ))
    return db.bulk_upsert(
        "analytics.rolling_metrics",
        ["date", "rolling_1y_return", "rolling_1y_vol",
         "rolling_1y_sharpe", "rolling_3m_return", "rolling_1y_max_dd"],
        rows,
        conflict_cols=["date"],
    )


# --- monthly heatmap --------------------------------------------------------

def _write_monthly_returns(nav_df: pd.DataFrame) -> int:
    rets = nav_df["daily_return"].fillna(0.0)
    if rets.empty:
        return 0
    monthly = (
        (1.0 + rets)
        .groupby([rets.index.year.rename("year"), rets.index.month.rename("month")])
        .prod()
        - 1.0
    )
    counts = rets.groupby([rets.index.year, rets.index.month]).size()

    rows = []
    for (y, m), r in monthly.items():
        rows.append((int(y), int(m), float(r), int(counts.loc[(y, m)])))
    return db.bulk_upsert(
        "analytics.monthly_returns",
        ["year", "month", "total_return", "trading_days"],
        rows,
        conflict_cols=["year", "month"],
    )


# --- performance summary (per period) ---------------------------------------

def _period_slice(rets: pd.Series, period: str) -> pd.Series:
    if period == "all":
        return rets
    if period == "ytd":
        year = rets.index.max().year
        return rets[rets.index >= pd.Timestamp(year, 1, 1)]
    days = {"1m": 21, "3m": 63, "1y": 252}.get(period)
    return rets.tail(days) if days else rets


def _summary_row(period: str, rets: pd.Series, spy: pd.Series, as_of: dt.date) -> tuple | None:
    if len(rets) < 3:
        return None
    eq = (1.0 + rets).cumprod()
    total_ret = float(eq.iloc[-1] - 1.0)
    ann_ret = mathx.annualized_return(rets)
    ann_vol = mathx.realized_vol(rets)
    sharpe = mathx.sharpe(rets)
    sortino = mathx.sortino(rets)
    mdd = mathx.max_drawdown(eq)
    calmar = mathx.calmar(rets)
    hit = mathx.hit_rate(rets)
    if not spy.empty:
        spy_rets = spy.reindex(rets.index).pct_change().fillna(0.0)
        beta, alpha = mathx.beta_alpha(rets, spy_rets)
    else:
        beta, alpha = 0.0, 0.0
    return (
        period, as_of,
        float(total_ret), float(ann_ret), float(ann_vol),
        float(sharpe), float(sortino), float(mdd), float(calmar), float(hit),
        float(beta), float(alpha),
        float(rets.max()), float(rets.min()),
        int(len(rets)),
    )


def _write_summary(nav_df: pd.DataFrame, spy: pd.Series) -> int:
    rets = nav_df["daily_return"].fillna(0.0)
    as_of = nav_df.index.max().date()
    rows = []
    for period in ("all", "ytd", "1y", "3m", "1m"):
        row = _summary_row(period, _period_slice(rets, period), spy, as_of)
        if row is not None:
            rows.append(row)
    return db.bulk_upsert(
        "analytics.performance_summary",
        ["period", "as_of", "total_return", "ann_return", "ann_vol",
         "sharpe", "sortino", "max_drawdown", "calmar", "hit_rate",
         "beta", "alpha", "best_day", "worst_day", "trading_days"],
        rows,
        conflict_cols=["period"],
    )


# --- drawdown periods -------------------------------------------------------

def _write_drawdown_periods(nav_df: pd.DataFrame, threshold: float = 0.02) -> int:
    """Detect peak→trough→recovery spans where depth >= threshold."""
    eq = nav_df["nav"]
    if len(eq) < 5:
        return 0
    arr = eq.values.astype(float)
    peak_arr = np.maximum.accumulate(arr)
    dd_arr = (arr - peak_arr) / peak_arr
    dates = nav_df.index

    # Detect spans bounded by new peaks. A new peak occurs at index i when arr[i] == peak_arr[i]
    # and arr[i] > peak_arr[i-1] (or i == 0).
    new_peak = arr == peak_arr
    # period_id increments whenever we're at a new peak (so each period is a peak run)
    period_id = np.cumsum(new_peak)
    # Group indices by period; each period is [peak_idx, ..., next_peak_idx - 1]
    df = pd.DataFrame({"period_id": period_id, "dd": dd_arr, "nav": arr}, index=dates)

    # Drop the truly-zero-dd periods (where the peak strictly increases each day)
    # by requiring at least one negative dd inside the period.
    period_min = df.groupby("period_id")["dd"].min()
    interesting = period_min[period_min <= -threshold].index

    # Clear table first (small set; whole-rewrite is simplest & idempotent)
    db.execute("DELETE FROM analytics.drawdown_periods;")
    if len(interesting) == 0:
        return 0

    rows: list[tuple] = []
    last_date = dates.max()
    for pid in interesting:
        sub = df[df["period_id"] == pid]
        # Span starts at the peak; goes until just before the next peak.
        start = sub.index.min()
        trough_idx = sub["dd"].idxmin()
        depth = float(sub["dd"].min())
        # Recovery = first date AFTER trough where nav >= peak_at_start. If never,
        # the span continues to the last date (ongoing).
        peak_val = float(sub["nav"].iloc[0])
        recovery_pool = df.loc[trough_idx:, "nav"]
        recovered_idx = recovery_pool[recovery_pool >= peak_val].index
        if len(recovered_idx) > 0:
            end = recovered_idx.min()
            ongoing = False
        else:
            end = None
            ongoing = True

        duration_days = (
            ((end if end is not None else last_date) - start).days
        )
        recovery_days = (
            (end - trough_idx).days if end is not None else None
        )
        rows.append((
            start.date(), trough_idx.date(),
            end.date() if end is not None else None,
            float(depth),
            int(duration_days) if duration_days is not None else None,
            int(recovery_days) if recovery_days is not None else None,
            bool(ongoing),
        ))

    # Append rows; PK is BIGSERIAL so no conflict
    with db.conn() as c, c.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO analytics.drawdown_periods
              (start_date, trough_date, end_date, depth,
               duration_days, recovery_days, ongoing)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """,
            rows,
        )
    return len(rows)


# --- main -------------------------------------------------------------------

def run() -> dict:
    nav_df = _load_nav()
    if nav_df.empty or len(nav_df) < 2:
        print("performance: portfolio.nav_daily is empty — nothing to compute.")
        return {}
    spy = _load_spy()

    n_eq = _write_equity_curve(nav_df, spy)
    n_roll = _write_rolling_metrics(nav_df)
    n_mo = _write_monthly_returns(nav_df)
    n_sum = _write_summary(nav_df, spy)
    n_dd = _write_drawdown_periods(nav_df)

    print(
        f"performance: equity_curve={n_eq}  rolling={n_roll}  monthly={n_mo}  "
        f"summary_periods={n_sum}  dd_periods={n_dd}"
    )
    return {
        "equity_curve": n_eq,
        "rolling_metrics": n_roll,
        "monthly_returns": n_mo,
        "summary": n_sum,
        "drawdown_periods": n_dd,
    }


if __name__ == "__main__":
    run()
