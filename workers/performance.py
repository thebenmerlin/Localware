"""Performance — daily NAV updates for closed positions and metrics roll-up."""
from __future__ import annotations

import datetime as dt

import pandas as pd

from .lib import db, mathx


def update_nav_returns():
    """Backfill daily_return and cumulative_return on portfolio_nav."""
    rows = db.query("SELECT date, nav FROM portfolio_nav ORDER BY date;")
    if len(rows) < 2:
        return
    df = pd.DataFrame(rows)
    df["nav"] = pd.to_numeric(df["nav"])
    df["daily_return"] = df["nav"].pct_change()
    base = float(df["nav"].iloc[0])
    df["cumulative_return"] = df["nav"] / base - 1
    for _, r in df.iterrows():
        if pd.isna(r["daily_return"]):
            continue
        db.execute(
            "UPDATE portfolio_nav SET daily_return = %s, cumulative_return = %s WHERE date = %s;",
            (float(r["daily_return"]), float(r["cumulative_return"]), r["date"]),
        )


def compute_metrics(as_of: dt.date | None = None) -> dict:
    if as_of is None:
        rows = db.query("SELECT MAX(date) AS d FROM portfolio_nav;")
        as_of = rows[0]["d"]
        if not as_of:
            return {}

    rows = db.query("SELECT date, nav, daily_return FROM portfolio_nav ORDER BY date;")
    if len(rows) < 5:
        return {}
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    df["nav"] = pd.to_numeric(df["nav"])
    df["daily_return"] = pd.to_numeric(df["daily_return"]).fillna(0)
    rets = df["daily_return"]
    eq = df["nav"]

    # SPY benchmark for beta/alpha
    spy = db.query("""
        SELECT p.date, p.adj_close FROM prices p JOIN securities s ON s.id = p.security_id
        WHERE s.ticker = 'SPY' ORDER BY p.date;
    """)
    spy_df = pd.DataFrame(spy)
    if not spy_df.empty:
        spy_df["date"] = pd.to_datetime(spy_df["date"])
        spy_df.set_index("date", inplace=True)
        spy_df["ret"] = pd.to_numeric(spy_df["adj_close"]).pct_change()
        beta, alpha = mathx.beta_alpha(rets, spy_df["ret"])
    else:
        beta, alpha = 0.0, 0.0

    metrics = {
        "total_return": float(eq.iloc[-1] / eq.iloc[0] - 1),
        "ann_return":   mathx.annualized_return(rets),
        "ann_vol":      mathx.realized_vol(rets),
        "sharpe":       mathx.sharpe(rets),
        "sortino":      mathx.sortino(rets),
        "max_drawdown": mathx.max_drawdown(eq),
        "calmar":       mathx.calmar(rets),
        "hit_rate":     mathx.hit_rate(rets),
        "beta":         beta,
        "alpha":        alpha,
    }
    db.execute(
        """
        INSERT INTO performance_metrics
          (period, as_of, total_return, ann_return, ann_vol, sharpe, sortino,
           max_drawdown, calmar, hit_rate, beta, alpha)
        VALUES ('all', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (period, as_of) DO UPDATE SET
          total_return = EXCLUDED.total_return,
          ann_return = EXCLUDED.ann_return,
          ann_vol = EXCLUDED.ann_vol,
          sharpe = EXCLUDED.sharpe,
          sortino = EXCLUDED.sortino,
          max_drawdown = EXCLUDED.max_drawdown,
          calmar = EXCLUDED.calmar,
          hit_rate = EXCLUDED.hit_rate,
          beta = EXCLUDED.beta,
          alpha = EXCLUDED.alpha,
          computed_at = NOW();
        """,
        (
            as_of, metrics["total_return"], metrics["ann_return"], metrics["ann_vol"],
            metrics["sharpe"], metrics["sortino"], metrics["max_drawdown"],
            metrics["calmar"], metrics["hit_rate"], metrics["beta"], metrics["alpha"],
        ),
    )

    # Sub-period metrics
    for period, days in (("1m", 21), ("3m", 63), ("ytd", None), ("1y", 252)):
        if period == "ytd":
            ytd_start = pd.Timestamp(as_of.year, 1, 1)
            sub = rets[rets.index >= ytd_start]
        else:
            sub = rets.tail(days)
        if len(sub) < 3:
            continue
        sub_eq = (1 + sub).cumprod()
        db.execute(
            """
            INSERT INTO performance_metrics
              (period, as_of, total_return, ann_return, ann_vol, sharpe, sortino,
               max_drawdown, calmar, hit_rate, beta, alpha)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (period, as_of) DO UPDATE SET
              total_return = EXCLUDED.total_return,
              ann_return = EXCLUDED.ann_return,
              ann_vol = EXCLUDED.ann_vol,
              sharpe = EXCLUDED.sharpe,
              sortino = EXCLUDED.sortino,
              max_drawdown = EXCLUDED.max_drawdown,
              calmar = EXCLUDED.calmar,
              hit_rate = EXCLUDED.hit_rate,
              computed_at = NOW();
            """,
            (
                period, as_of,
                float(sub_eq.iloc[-1] - 1),
                mathx.annualized_return(sub),
                mathx.realized_vol(sub),
                mathx.sharpe(sub),
                mathx.sortino(sub),
                mathx.max_drawdown(sub_eq),
                mathx.calmar(sub),
                mathx.hit_rate(sub),
                None, None,
            ),
        )

    return metrics


def run() -> dict:
    update_nav_returns()
    return compute_metrics()


if __name__ == "__main__":
    m = run()
    if m:
        print(f"Sharpe {m['sharpe']:.2f}  AnnRet {m['ann_return']:.2%}  "
              f"AnnVol {m['ann_vol']:.2%}  MaxDD {m['max_drawdown']:.2%}  "
              f"Calmar {m['calmar']:.2f}  Beta {m['beta']:.2f}  Alpha {m['alpha']:.2%}")
