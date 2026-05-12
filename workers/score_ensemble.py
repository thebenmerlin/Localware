"""Nightly walk-forward scoring of ensemble members → derived.ensemble_member_scores.

Reads:
  derived.signals_daily   trailing alpha history
  raw.ohlcv_daily         trailing price panel
  securities              sector mapping

Writes:
  derived.ensemble_member_scores  one row per (as_of, member_key)

The portfolio_constructor reads the most recent `as_of` slice of this table
on the next day and passes it as `member_weights` to solve_ensemble, biasing
the ensemble toward parameterizations that have actually paid out OOS.

CLI:
  python -m workers.score_ensemble
  python -m workers.score_ensemble --as-of 2026-05-09 --steps 60 --warmup 10
"""
from __future__ import annotations

import argparse
import datetime as dt

import pandas as pd

from .lib import db
from .portfolio_optimizer import score_members_walk_forward


# Pull enough history to cover the longest cov lookback (252) + warmup + steps.
DEFAULT_STEPS = 60
DEFAULT_WARMUP = 10
HISTORY_BUFFER = 280   # business days of price/alpha history beyond steps+warmup


SCORES_TABLE = "derived.ensemble_member_scores"
SCORES_COLS = ["as_of", "member_key", "sharpe_raw", "sharpe", "n_obs"]


def _load_signals_history(as_of: dt.date, days: int) -> pd.DataFrame:
    start = as_of - dt.timedelta(days=int(days * 1.7))
    rows = db.query(
        """
        SELECT s.ticker, sd.date, sd.blended_signal
        FROM derived.signals_daily sd
        JOIN securities s ON s.id = sd.security_id
        WHERE sd.date BETWEEN %s AND %s;
        """,
        (start, as_of),
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["blended_signal"] = pd.to_numeric(df["blended_signal"])
    return df.pivot(index="date", columns="ticker", values="blended_signal").sort_index()


def _load_price_history(as_of: dt.date, days: int) -> pd.DataFrame:
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


def _load_sectors() -> pd.Series:
    rows = db.query(
        "SELECT ticker, sector FROM securities WHERE active = TRUE AND asset_class = 'equity';"
    )
    if not rows:
        return pd.Series(dtype="string")
    s = pd.Series({r["ticker"]: r["sector"] for r in rows}, dtype="string")
    # Same hygiene as portfolio_constructor: drop unknowns so they don't form
    # a neutralization bucket in the optimizer.
    mask = s.notna() & (s.str.strip() != "") & (s != "?")
    return s[mask]


def score(
    as_of: dt.date | None = None,
    n_steps: int = DEFAULT_STEPS,
    n_warmup: int = DEFAULT_WARMUP,
    cost_bps: float = 10.0,
) -> pd.DataFrame:
    if as_of is None:
        rows = db.query("SELECT MAX(date) AS d FROM derived.signals_daily;")
        as_of = rows[0]["d"] if rows else None
        if as_of is None:
            print("No signals — nothing to score.")
            return pd.DataFrame()

    print(f"Walk-forward scoring @ as_of={as_of}  steps={n_steps}  warmup={n_warmup}")

    history_days = n_steps + n_warmup + HISTORY_BUFFER
    alpha_h = _load_signals_history(as_of, history_days)
    panel = _load_price_history(as_of, history_days)
    sectors = _load_sectors()

    if alpha_h.empty or panel.empty or sectors.empty:
        print(f"  insufficient inputs (alpha={alpha_h.shape}, panel={panel.shape}, "
              f"sectors={len(sectors)}) — abort.")
        return pd.DataFrame()
    print(f"  loaded alpha={alpha_h.shape}  panel={panel.shape}  sectors={len(sectors)}")

    scores = score_members_walk_forward(
        alpha_history=alpha_h,
        panel=panel,
        sectors=sectors,
        benchmark=None,
        n_steps=n_steps,
        n_warmup=n_warmup,
        cost_bps=cost_bps,
    )
    if scores.empty:
        print("  scorer returned empty.")
        return scores

    top = scores.head(5)
    bot = scores.tail(3)
    print(f"  {len(scores)} members scored. Top:")
    for k, r in top.iterrows():
        print(f"    {k:40s}  sharpe={r['sharpe']:+.3f}  raw={r['sharpe_raw']:+.3f}  n={int(r['n_obs'])}")
    print(f"  Bottom:")
    for k, r in bot.iterrows():
        print(f"    {k:40s}  sharpe={r['sharpe']:+.3f}  raw={r['sharpe_raw']:+.3f}  n={int(r['n_obs'])}")

    out_rows = [
        (as_of, k, float(r["sharpe_raw"]), float(r["sharpe"]), int(r["n_obs"]))
        for k, r in scores.iterrows()
    ]
    n = db.bulk_upsert(
        SCORES_TABLE, SCORES_COLS, out_rows,
        conflict_cols=["as_of", "member_key"],
    )
    print(f"  upserted {n} rows into {SCORES_TABLE}")
    return scores


def load_latest_member_weights(as_of: dt.date | None = None) -> pd.Series:
    """Read the most recent member scores for use as solve_ensemble(member_weights=...).

    Returns the `sharpe` column (already clipped at 0 by the scorer) indexed
    by member_key. Empty Series → caller falls back to equal-weighted ensemble.
    """
    if as_of is None:
        rows = db.query(f"SELECT MAX(as_of) AS d FROM {SCORES_TABLE};")
        latest = rows[0]["d"] if rows else None
    else:
        rows = db.query(
            f"SELECT MAX(as_of) AS d FROM {SCORES_TABLE} WHERE as_of <= %s;",
            (as_of,),
        )
        latest = rows[0]["d"] if rows else None
    if latest is None:
        return pd.Series(dtype=float)
    rows = db.query(
        f"SELECT member_key, sharpe FROM {SCORES_TABLE} WHERE as_of = %s;",
        (latest,),
    )
    return pd.Series(
        {r["member_key"]: float(r["sharpe"]) for r in rows}, dtype=float,
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", type=str, default=None,
                    help="Scoring as-of date (YYYY-MM-DD); default = latest signal date")
    ap.add_argument("--steps", type=int, default=DEFAULT_STEPS,
                    help="Walk-forward evaluation steps (default %(default)s)")
    ap.add_argument("--warmup", type=int, default=DEFAULT_WARMUP,
                    help="Warmup steps that update prev_w but don't accrue PnL (default %(default)s)")
    ap.add_argument("--cost-bps", type=float, default=10.0,
                    help="One-way linear cost in bps (default %(default)s)")
    args = ap.parse_args()
    aod = dt.date.fromisoformat(args.as_of) if args.as_of else None
    score(as_of=aod, n_steps=args.steps, n_warmup=args.warmup, cost_bps=args.cost_bps)
