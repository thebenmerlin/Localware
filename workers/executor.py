"""Single-day executor — turns target weights into one NAV row.

Reads:
  portfolio.positions_daily   target weights for `as_of` (and prior day for turnover)
  raw.ohlcv_daily             closing prices for `as_of` and prior day
  portfolio.nav_daily         previous NAV (most recent row before `as_of`)

Writes:
  portfolio.nav_daily         one upserted row for `as_of`

Cost model: flat 10 bps × turnover (turnover = Σ|W[D] - W[D-1]|). No slippage,
no commission — this is the simulation-grade drag mandated by the spec.

Accounting convention:
  positions_daily.date = D means the weights HELD DURING day D (rebalanced at
  close of D-1). Therefore:
    portfolio_return[D] = Σ_i W[D, i] * (P[D, i] / P[D-1, i] - 1)
    turnover[D]         = Σ_i |W[D, i] - W[D-1, i]|
    cost[D]             = turnover[D] * COST_BPS / 10_000
    nav[D]              = nav[D-1] * (1 + portfolio_return[D] - cost[D])

This module is idempotent: re-running for the same `as_of` recomputes and
upserts the same row.

CLI:
  python -m workers.executor                   # latest day with positions
  python -m workers.executor --as-of 2026-05-09
"""
from __future__ import annotations

import argparse
import datetime as dt
import os

import pandas as pd

from .lib import db


# --- tunables ---------------------------------------------------------------

COST_BPS = 10.0   # flat execution drag, applied to turnover
INITIAL_NAV_ENV = "INITIAL_NAV"
DEFAULT_INITIAL_NAV = 10_000_000.0


NAV_TABLE = "portfolio.nav_daily"
NAV_COLS = [
    "date", "nav", "daily_return", "cumulative_return",
    "gross_exposure", "net_exposure", "turnover", "execution_cost_bps",
]


# --- I/O --------------------------------------------------------------------

def _prev_trading_day(as_of: dt.date) -> dt.date | None:
    row = db.query(
        "SELECT MAX(date) AS d FROM raw.ohlcv_daily WHERE date < %s;",
        (as_of,),
    )
    return row[0]["d"] if row and row[0]["d"] else None


def _load_weights(as_of: dt.date) -> pd.Series:
    """ticker → target_weight effective on as_of."""
    rows = db.query(
        """
        SELECT s.ticker, pd.target_weight
        FROM portfolio.positions_daily pd
        JOIN securities s ON s.id = pd.security_id
        WHERE pd.date = %s;
        """,
        (as_of,),
    )
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    return pd.to_numeric(df["target_weight"]).set_axis(df["ticker"]).astype(float)


def _load_prev_weights(as_of: dt.date) -> pd.Series:
    """Most recent positions_daily row strictly before as_of."""
    rows = db.query(
        """
        SELECT s.ticker, pd.target_weight
        FROM portfolio.positions_daily pd
        JOIN securities s ON s.id = pd.security_id
        WHERE pd.date = (
          SELECT MAX(date) FROM portfolio.positions_daily WHERE date < %s
        );
        """,
        (as_of,),
    )
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    return pd.to_numeric(df["target_weight"]).set_axis(df["ticker"]).astype(float)


def _load_prices(as_of: dt.date) -> pd.Series:
    rows = db.query(
        """
        SELECT s.ticker, o.adj_close
        FROM raw.ohlcv_daily o
        JOIN securities s ON s.id = o.security_id
        WHERE o.date = %s;
        """,
        (as_of,),
    )
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows)
    return pd.to_numeric(df["adj_close"]).set_axis(df["ticker"]).astype(float)


def _prev_nav(as_of: dt.date) -> tuple[float, float]:
    """Returns (prev_nav, initial_nav)."""
    initial = float(os.environ.get(INITIAL_NAV_ENV, DEFAULT_INITIAL_NAV))
    rows = db.query(
        "SELECT nav FROM portfolio.nav_daily WHERE date < %s ORDER BY date DESC LIMIT 1;",
        (as_of,),
    )
    if rows:
        return float(rows[0]["nav"]), initial
    return initial, initial


# --- main -------------------------------------------------------------------

def execute(as_of: dt.date | None = None) -> dict:
    if as_of is None:
        row = db.query("SELECT MAX(date) AS d FROM portfolio.positions_daily;")
        as_of = row[0]["d"] if row else None
        if as_of is None:
            print("No positions in portfolio.positions_daily — nothing to execute.")
            return {}

    prev = _prev_trading_day(as_of)
    if prev is None:
        print(f"No prior trading day before {as_of}; treating as t=0.")
    w_today = _load_weights(as_of)
    if w_today.empty:
        print(f"No target weights for {as_of}.")
        return {}

    w_prev = _load_prev_weights(as_of)
    p_today = _load_prices(as_of)
    p_prev = _load_prices(prev) if prev else pd.Series(dtype=float)

    # Vectorized realized return: only tickers present on both days contribute
    if not p_prev.empty:
        common = w_today.index.intersection(p_today.index).intersection(p_prev.index)
        rets = (p_today.loc[common] / p_prev.loc[common] - 1.0).replace(
            [float("inf"), float("-inf")], 0.0
        ).fillna(0.0)
        portfolio_ret = float((w_today.loc[common] * rets).sum())
    else:
        portfolio_ret = 0.0

    # Turnover = Σ|W[D] - W[D-1]| over union of holdings
    universe = w_today.index.union(w_prev.index)
    turnover = float(
        (w_today.reindex(universe).fillna(0.0) - w_prev.reindex(universe).fillna(0.0))
        .abs()
        .sum()
    )

    cost = turnover * COST_BPS / 10_000.0
    net_return = portfolio_ret - cost

    prev_nav_val, initial_nav = _prev_nav(as_of)
    new_nav = prev_nav_val * (1.0 + net_return)
    cumret = new_nav / initial_nav - 1.0

    gross = float(w_today.abs().sum())
    net = float(w_today.sum())

    row = (
        as_of,
        round(new_nav, 4),
        round(net_return, 8),
        round(cumret, 8),
        round(gross, 6),
        round(net, 6),
        round(turnover, 6),
        COST_BPS,
    )
    db.bulk_upsert(NAV_TABLE, NAV_COLS, [row], conflict_cols=["date"])

    print(
        f"[{as_of}] gross_ret={portfolio_ret:+.4%}  turnover={turnover:.3f}  "
        f"cost={cost:.4%}  net_ret={net_return:+.4%}  "
        f"NAV=${new_nav:,.0f}  cum={cumret:+.2%}"
    )
    return {
        "date": as_of,
        "nav": new_nav,
        "daily_return": net_return,
        "turnover": turnover,
        "gross_exposure": gross,
        "net_exposure": net,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of", type=str, default=None, help="YYYY-MM-DD")
    args = ap.parse_args()
    aod = dt.date.fromisoformat(args.as_of) if args.as_of else None
    execute(as_of=aod)
