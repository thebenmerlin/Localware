"""Executor — turns target weights into simulated trades.

Models:
  - Slippage: 5 bps + half-spread proxy (high-low / mid * 0.5, capped 20bps).
  - Commission: $0.005/share, min $1.
  - Market impact: extra slippage if size > 1% of 30-day ADV.
"""
from __future__ import annotations

import datetime as dt
import os

import pandas as pd

from .lib import db
from . import portfolio_constructor


BASE_SLIPPAGE_BPS = 5.0
COMMISSION_PER_SHARE = 0.005
COMMISSION_MIN = 1.0
ADV_IMPACT_THRESHOLD = 0.01
ADV_IMPACT_BPS = 15.0


def _last_price(security_id: int, as_of: dt.date) -> dict | None:
    rows = db.query(
        """
        SELECT close, adj_close, high, low, volume
        FROM prices WHERE security_id = %s AND date = %s;
        """,
        (security_id, as_of),
    )
    return rows[0] if rows else None


def _avg_dollar_volume(security_id: int, as_of: dt.date, days: int = 30) -> float:
    start = as_of - dt.timedelta(days=int(days * 1.6))
    rows = db.query(
        """
        SELECT close, volume FROM prices
        WHERE security_id = %s AND date BETWEEN %s AND %s;
        """,
        (security_id, start, as_of),
    )
    if not rows:
        return 0.0
    df = pd.DataFrame(rows)
    df["close"] = pd.to_numeric(df["close"])
    df["volume"] = pd.to_numeric(df["volume"])
    return float((df["close"] * df["volume"]).tail(days).mean())


def _ticker_to_id() -> dict[str, int]:
    return {r["ticker"]: r["id"] for r in db.query("SELECT id, ticker FROM securities;")}


def _current_positions(as_of: dt.date) -> dict[int, dict]:
    rows = db.query(
        """
        SELECT security_id, quantity, avg_cost, peak_price
        FROM positions WHERE as_of = (
          SELECT MAX(as_of) FROM positions WHERE as_of <= %s
        );
        """,
        (as_of,),
    )
    return {r["security_id"]: r for r in rows}


def _nav_and_cash(as_of: dt.date) -> tuple[float, float]:
    rows = db.query(
        "SELECT nav, cash FROM portfolio_nav WHERE date <= %s ORDER BY date DESC LIMIT 1;",
        (as_of,),
    )
    if rows:
        return float(rows[0]["nav"]), float(rows[0]["cash"])
    initial = float(os.environ.get("INITIAL_NAV", "10000000"))
    return initial, initial


def _strategy_id_for(ticker: str, as_of: dt.date) -> int | None:
    rows = db.query(
        """
        SELECT strategy_id, ABS(signal) AS s FROM signals sg
        JOIN securities sec ON sec.id = sg.security_id
        WHERE sec.ticker = %s AND sg.date = %s
        ORDER BY s DESC LIMIT 1;
        """,
        (ticker, as_of),
    )
    return rows[0]["strategy_id"] if rows else None


def _slippage_bps(price: float, high: float, low: float, qty: float, adv_dollars: float) -> float:
    if not price or price <= 0:
        return BASE_SLIPPAGE_BPS
    half_spread = max((high - low) / price * 0.5 * 10_000, 0) if high and low else 0
    half_spread = min(half_spread, 20.0)
    impact = 0.0
    notional = abs(qty) * price
    if adv_dollars > 0 and notional / adv_dollars > ADV_IMPACT_THRESHOLD:
        impact = ADV_IMPACT_BPS
    return BASE_SLIPPAGE_BPS + half_spread + impact


INSERT_TRADE = """
INSERT INTO trades
  (security_id, strategy_id, side, quantity, price, slippage_bps, commission, notional, executed_at, reason)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

UPSERT_POSITION = """
INSERT INTO positions
  (security_id, quantity, avg_cost, market_value, weight, unrealized_pnl, peak_price, as_of)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (security_id, as_of) DO UPDATE SET
  quantity = EXCLUDED.quantity,
  avg_cost = EXCLUDED.avg_cost,
  market_value = EXCLUDED.market_value,
  weight = EXCLUDED.weight,
  unrealized_pnl = EXCLUDED.unrealized_pnl,
  peak_price = EXCLUDED.peak_price;
"""


def execute(as_of: dt.date | None = None) -> int:
    if as_of is None:
        rows = db.query("SELECT MAX(date) AS d FROM prices;")
        as_of = rows[0]["d"]
        if as_of is None:
            return 0

    targets = portfolio_constructor.construct(as_of)
    if targets.empty:
        return 0

    nav, cash = _nav_and_cash(as_of)
    tid = _ticker_to_id()
    current = _current_positions(as_of)
    target_by_id: dict[int, float] = {tid[t]: w for t, w in zip(targets["ticker"], targets["target_weight"]) if t in tid}

    trades = 0
    new_positions: dict[int, dict] = {}

    # Sells / closes for names not in target
    for sec_id, pos in current.items():
        if sec_id not in target_by_id and float(pos["quantity"]) != 0:
            target_by_id[sec_id] = 0.0  # close

    for sec_id, target_weight in target_by_id.items():
        ticker_row = db.query("SELECT ticker FROM securities WHERE id = %s;", (sec_id,))
        if not ticker_row:
            continue
        ticker = ticker_row[0]["ticker"]
        prc = _last_price(sec_id, as_of)
        if not prc or not prc["close"]:
            continue
        price = float(prc["close"])
        target_dollars = target_weight * nav
        target_qty = round(target_dollars / price, 4) if price > 0 else 0
        cur_qty = float(current.get(sec_id, {}).get("quantity", 0))
        delta = target_qty - cur_qty
        if abs(delta) * price < 1000:  # skip dust trades < $1k
            if cur_qty != 0:
                avg_cost = float(current.get(sec_id, {}).get("avg_cost", price))
                peak = max(float(current.get(sec_id, {}).get("peak_price") or price), price)
                new_positions[sec_id] = {
                    "qty": cur_qty,
                    "avg_cost": avg_cost,
                    "price": price,
                    "peak_price": peak,
                }
            continue

        side = "BUY" if delta > 0 else "SELL"
        adv = _avg_dollar_volume(sec_id, as_of)
        slip_bps = _slippage_bps(
            price,
            float(prc["high"] or price), float(prc["low"] or price),
            delta, adv,
        )
        slip_factor = 1 + (slip_bps / 10_000) * (1 if side == "BUY" else -1)
        fill_price = round(price * slip_factor, 4)
        commission = max(abs(delta) * COMMISSION_PER_SHARE, COMMISSION_MIN)
        notional = round(abs(delta) * fill_price, 2)

        strat = _strategy_id_for(ticker, as_of)
        db.execute(
            INSERT_TRADE,
            (
                sec_id, strat, side, abs(round(delta, 4)), fill_price,
                round(slip_bps, 2), round(commission, 2), notional,
                dt.datetime.combine(as_of, dt.time(16, 0)),
                f"target={target_weight:.4f}",
            ),
        )
        trades += 1

        # update cash
        if side == "BUY":
            cash -= notional + commission
        else:
            cash += notional - commission

        new_qty = cur_qty + delta
        if cur_qty != 0 and ((cur_qty > 0) == (new_qty > 0)) and side == "BUY":
            old_cost = float(current.get(sec_id, {}).get("avg_cost", fill_price)) * cur_qty
            new_avg = (old_cost + delta * fill_price) / new_qty if new_qty != 0 else fill_price
        elif cur_qty == 0 or (cur_qty > 0) != (new_qty > 0):
            new_avg = fill_price
        else:
            new_avg = float(current.get(sec_id, {}).get("avg_cost", fill_price))

        if abs(new_qty) > 1e-6:
            peak = max(float(current.get(sec_id, {}).get("peak_price") or fill_price), fill_price)
            new_positions[sec_id] = {
                "qty": new_qty,
                "avg_cost": new_avg,
                "price": price,
                "peak_price": peak,
            }

    # Write today's position snapshot (only for non-zero)
    for sec_id, p in new_positions.items():
        mv = p["qty"] * p["price"]
        weight = mv / nav if nav else 0
        unrealized = (p["price"] - p["avg_cost"]) * p["qty"]
        db.execute(UPSERT_POSITION, (
            sec_id, p["qty"], p["avg_cost"], round(mv, 2),
            round(weight, 6), round(unrealized, 2),
            p["peak_price"], as_of,
        ))

    # Update portfolio_nav
    gross = sum(abs(p["qty"] * p["price"]) for p in new_positions.values())
    net = sum(p["qty"] * p["price"] for p in new_positions.values())
    new_nav = cash + net
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
        (
            as_of, round(new_nav, 2), round(cash, 2),
            round(gross, 2), round(net, 2),
            round(gross / new_nav, 4) if new_nav else 0,
        ),
    )

    print(f"[{as_of}] {trades} trades, NAV ${new_nav:,.0f}, gross {gross/new_nav*100:.0f}%")
    return trades


if __name__ == "__main__":
    execute()
