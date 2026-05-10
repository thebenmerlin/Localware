"""Fetch fundamentals snapshot per security via yfinance.info.

yfinance.info is rate-limited and slow; we run weekly. We snapshot today's
trailing PE/PB/ROE/D-E + market cap + earnings growth.
"""
from __future__ import annotations

import datetime as dt
import sys
import time

import yfinance as yf

from .lib import db, universe


UPSERT_SQL = """
INSERT INTO fundamentals
  (security_id, date, pe, pb, roe, debt_to_equity, market_cap, earnings_growth)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (security_id, date) DO UPDATE SET
  pe = EXCLUDED.pe,
  pb = EXCLUDED.pb,
  roe = EXCLUDED.roe,
  debt_to_equity = EXCLUDED.debt_to_equity,
  market_cap = EXCLUDED.market_cap,
  earnings_growth = EXCLUDED.earnings_growth;
"""


def _safe_float(v):
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


def run() -> int:
    secs = universe.get_active(asset_class="equity")
    today = dt.date.today()
    total = 0
    for s in secs:
        try:
            t = yf.Ticker(s["ticker"])
            info = t.info or {}
            row = (
                s["id"], today,
                _safe_float(info.get("trailingPE")),
                _safe_float(info.get("priceToBook")),
                _safe_float(info.get("returnOnEquity")),
                _safe_float(info.get("debtToEquity")),
                _safe_float(info.get("marketCap")),
                _safe_float(info.get("earningsGrowth")),
            )
            db.execute(UPSERT_SQL, row)
            total += 1
        except Exception as e:
            print(f"  {s['ticker']}: FAILED ({e})", file=sys.stderr)
        time.sleep(0.1)
    print(f"Fundamentals updated for {total} securities.")
    return total


if __name__ == "__main__":
    run()
