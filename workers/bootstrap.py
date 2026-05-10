"""One-shot bootstrap: load universe, fetch history, fetch fundamentals,
run a 4-year walk-forward backtest to seed live state.

Designed to be safe on the large (~900 ticker) universe: prices are batched
in fetch_prices, fundamentals are sequential but throttled, and bootstrap
will skip stages that look already populated."""
from __future__ import annotations

import datetime as dt

from . import fetch_fundamentals, backtest, fetch_prices
from .lib import db, universe


def main():
    print("→ Loading universe…")
    n = universe.load_universe()
    print(f"  {n} securities active")

    rows = db.query("SELECT COUNT(*) AS c FROM prices;")
    expected_min_rows = n * 200  # heuristic: < 200 rows/name means very incomplete
    if rows[0]["c"] < expected_min_rows:
        print(f"→ Fetching 5y price history for {n} tickers (this can take ~10 min for ~900 names)…")
        fetch_prices.run(history_years=5)
    else:
        print(f"  Prices already loaded ({rows[0]['c']} rows)")

    rows = db.query("SELECT COUNT(*) AS c FROM fundamentals;")
    if rows[0]["c"] < n // 3:
        print("→ Fetching fundamentals (yfinance.info is rate-limited; ~1 req/sec)…")
        fetch_fundamentals.run()
    else:
        print(f"  Fundamentals already loaded ({rows[0]['c']} rows)")

    print("→ Running 4y walk-forward backtest as initial state…")
    rows = db.query("SELECT MIN(date) AS s, MAX(date) AS e FROM prices;")
    earliest, latest = rows[0]["s"], rows[0]["e"]
    start = max(earliest + dt.timedelta(days=365), latest - dt.timedelta(days=365 * 4))
    backtest.run(start, latest, name="bootstrap", reset=True)
    print("✓ Bootstrap complete.")


if __name__ == "__main__":
    main()
