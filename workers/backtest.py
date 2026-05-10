"""Walk-forward backtest.

Replays the strategies day-by-day from start_date to end_date over the existing
price history. Uses the same strategy_runner / portfolio_constructor / executor
as live, so the backtest equity curve is the live system's equity curve had we
launched on start_date.

Writes a row to `backtests` with results + equity curve. Optionally also writes
the daily NAV / positions to live tables (clears them first).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os

import numpy as np
import pandas as pd

from .lib import db, mathx
from . import strategy_runner, portfolio_constructor, executor


REBALANCE_EVERY_DAYS = 5  # weekly rebalance for liquidity / cost


def _trading_days(start: dt.date, end: dt.date) -> list[dt.date]:
    rows = db.query(
        """
        SELECT DISTINCT p.date FROM prices p JOIN securities s ON s.id = p.security_id
        WHERE s.ticker = 'SPY' AND p.date BETWEEN %s AND %s ORDER BY p.date;
        """,
        (start, end),
    )
    return [r["date"] for r in rows]


def _reset_state():
    """Clear NAV/positions/trades/signals/risk/perf for a fresh run."""
    for sql in [
        "DELETE FROM trades;",
        "DELETE FROM positions;",
        "DELETE FROM signals;",
        "DELETE FROM portfolio_nav;",
        "DELETE FROM performance_metrics;",
        "DELETE FROM risk_metrics;",
        "DELETE FROM strategy_performance;",
    ]:
        db.execute(sql)


def _seed_initial_nav(start: dt.date):
    initial = float(os.environ.get("INITIAL_NAV", "10000000"))
    db.execute(
        """
        INSERT INTO portfolio_nav (date, nav, cash, gross_exposure, net_exposure, leverage)
        VALUES (%s, %s, %s, 0, 0, 0)
        ON CONFLICT (date) DO NOTHING;
        """,
        (start, initial, initial),
    )


def _mark_to_market_no_trade(as_of: dt.date):
    """On non-rebalance days, just MTM positions to today's close."""
    prev_pos = db.query(
        """
        SELECT p.security_id, p.quantity, p.avg_cost, p.peak_price
        FROM positions p
        WHERE p.as_of = (SELECT MAX(as_of) FROM positions WHERE as_of < %s);
        """,
        (as_of,),
    )
    prev_nav_row = db.query(
        "SELECT cash FROM portfolio_nav WHERE date < %s ORDER BY date DESC LIMIT 1;",
        (as_of,),
    )
    if not prev_nav_row:
        return
    cash = float(prev_nav_row[0]["cash"])

    gross = net = 0.0
    for pos in prev_pos:
        sid = pos["security_id"]
        prc = db.query("SELECT close FROM prices WHERE security_id = %s AND date = %s;", (sid, as_of))
        if not prc or not prc[0]["close"]:
            # use most recent price
            prc = db.query(
                "SELECT close FROM prices WHERE security_id = %s AND date <= %s ORDER BY date DESC LIMIT 1;",
                (sid, as_of),
            )
        if not prc:
            continue
        price = float(prc[0]["close"])
        qty = float(pos["quantity"])
        mv = qty * price
        gross += abs(mv)
        net += mv
        peak = max(float(pos["peak_price"] or price), price)
        unrealized = (price - float(pos["avg_cost"])) * qty
        db.execute(
            """
            INSERT INTO positions
              (security_id, quantity, avg_cost, market_value, weight, unrealized_pnl, peak_price, as_of)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (security_id, as_of) DO UPDATE SET
              quantity = EXCLUDED.quantity, avg_cost = EXCLUDED.avg_cost,
              market_value = EXCLUDED.market_value, weight = EXCLUDED.weight,
              unrealized_pnl = EXCLUDED.unrealized_pnl, peak_price = EXCLUDED.peak_price;
            """,
            (sid, qty, float(pos["avg_cost"]), round(mv, 2),
             round(mv / (cash + net) if (cash + net) else 0, 6),
             round(unrealized, 2), peak, as_of),
        )

    nav = cash + net
    db.execute(
        """
        INSERT INTO portfolio_nav (date, nav, cash, gross_exposure, net_exposure, leverage)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
          nav = EXCLUDED.nav, cash = EXCLUDED.cash,
          gross_exposure = EXCLUDED.gross_exposure,
          net_exposure = EXCLUDED.net_exposure,
          leverage = EXCLUDED.leverage;
        """,
        (as_of, round(nav, 2), round(cash, 2), round(gross, 2), round(net, 2),
         round(gross / nav, 4) if nav else 0),
    )


def run(start_date: dt.date, end_date: dt.date, name: str = "main",
        reset: bool = True) -> dict:
    if reset:
        _reset_state()
    _seed_initial_nav(start_date - dt.timedelta(days=1))

    days = _trading_days(start_date, end_date)
    print(f"Backtest {name}: {len(days)} trading days from {start_date} to {end_date}")

    last_rebalance = None
    for i, d in enumerate(days):
        if last_rebalance is None or (d - last_rebalance).days >= REBALANCE_EVERY_DAYS:
            try:
                strategy_runner.run(as_of=d)
                executor.execute(as_of=d)
                last_rebalance = d
            except Exception as e:
                print(f"  {d}: rebalance failed: {e}")
                _mark_to_market_no_trade(d)
        else:
            _mark_to_market_no_trade(d)
        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(days)}] {d}")

    # Final metrics
    from . import performance
    performance.update_nav_returns()
    metrics = performance.compute_metrics(as_of=end_date)

    # Equity curve
    rows = db.query("SELECT date, nav, daily_return FROM portfolio_nav ORDER BY date;")
    eq = [{"date": str(r["date"]), "nav": float(r["nav"]),
           "ret": float(r["daily_return"]) if r["daily_return"] is not None else None}
          for r in rows]

    # Save backtest
    db.execute(
        """
        INSERT INTO backtests (name, strategy_config, start_date, end_date, results, equity_curve, status)
        VALUES (%s, %s::jsonb, %s, %s, %s::jsonb, %s::jsonb, 'completed');
        """,
        (
            name, json.dumps({"rebalance_days": REBALANCE_EVERY_DAYS}),
            start_date, end_date,
            json.dumps(metrics, default=float),
            json.dumps(eq, default=float),
        ),
    )

    print("Backtest complete:")
    print(f"  AnnReturn   : {metrics.get('ann_return', 0):.2%}")
    print(f"  AnnVol      : {metrics.get('ann_vol', 0):.2%}")
    print(f"  Sharpe      : {metrics.get('sharpe', 0):.2f}")
    print(f"  MaxDD       : {metrics.get('max_drawdown', 0):.2%}")
    print(f"  Calmar      : {metrics.get('calmar', 0):.2f}")
    return metrics


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None, help="YYYY-MM-DD")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD")
    ap.add_argument("--name", default="main")
    ap.add_argument("--no-reset", action="store_true")
    args = ap.parse_args()

    rows = db.query("SELECT MIN(date) AS s, MAX(date) AS e FROM prices;")
    s = dt.date.fromisoformat(args.start) if args.start else rows[0]["s"]
    e = dt.date.fromisoformat(args.end) if args.end else rows[0]["e"]
    run(s, e, name=args.name, reset=not args.no_reset)
