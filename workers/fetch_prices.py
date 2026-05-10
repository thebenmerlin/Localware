"""Fetch daily OHLCV via yfinance and upsert into prices.

For a universe of ~900 names we batch tickers per yfinance.download call
(yfinance accepts multi-ticker requests and returns a MultiIndex frame). This
collapses 900 sequential HTTP calls into ~20 batched calls while remaining
polite to Yahoo's endpoint.

Run modes:
  python -m workers.fetch_prices --history 5     # 5y of history
  python -m workers.fetch_prices                 # incremental (since latest)
  python -m workers.fetch_prices --batch 40      # tweak batch size
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from typing import Iterable

import pandas as pd
import yfinance as yf

from .lib import db, universe


UPSERT_SQL = """
INSERT INTO prices (security_id, date, open, high, low, close, adj_close, volume)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (security_id, date) DO UPDATE SET
  open = EXCLUDED.open,
  high = EXCLUDED.high,
  low = EXCLUDED.low,
  close = EXCLUDED.close,
  adj_close = EXCLUDED.adj_close,
  volume = EXCLUDED.volume;
"""

DEFAULT_BATCH = 40
THROTTLE_SECONDS = 1.5     # between batches
RETRY_BACKOFF = (3, 8, 20) # seconds


def _latest_date_map(security_ids: list[int]) -> dict[int, dt.date | None]:
    rows = db.query(
        """
        SELECT security_id, MAX(date) AS d
        FROM prices WHERE security_id = ANY(%s) GROUP BY security_id;
        """,
        (security_ids,),
    )
    return {r["security_id"]: r["d"] for r in rows}


def _yf_download_with_retry(tickers: list[str], start: dt.date, end: dt.date) -> pd.DataFrame | None:
    last_exc: Exception | None = None
    for i, backoff in enumerate((0,) + RETRY_BACKOFF):
        if backoff:
            time.sleep(backoff)
        try:
            df = yf.download(
                tickers,
                start=start.isoformat(),
                end=(end + dt.timedelta(days=1)).isoformat(),
                group_by="ticker",
                progress=False,
                auto_adjust=False,
                threads=True,
                timeout=30,
            )
            if df is None or df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            last_exc = e
            if i == len(RETRY_BACKOFF):
                print(f"  batch failed after {i+1} attempts: {e}", file=sys.stderr)
            else:
                print(f"  batch retry {i+1}/{len(RETRY_BACKOFF)}: {e}", file=sys.stderr)
    if last_exc:
        return None
    return None


def _frame_for_ticker(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Slice a multi-ticker yfinance frame for one ticker into a clean DataFrame."""
    if df is None or df.empty:
        return pd.DataFrame()
    sub: pd.DataFrame
    if isinstance(df.columns, pd.MultiIndex):
        if ticker not in df.columns.get_level_values(0):
            return pd.DataFrame()
        sub = df[ticker].copy()
    else:
        sub = df.copy()
    sub = sub.dropna(how="all")
    if sub.empty:
        return pd.DataFrame()
    sub = sub.reset_index()
    sub.columns = [str(c).lower().replace(" ", "_") for c in sub.columns]
    return sub


def _f(x):
    if x is None or pd.isna(x):
        return None
    return float(x)


def _i(x):
    if x is None or pd.isna(x):
        return None
    try:
        return int(x)
    except (ValueError, TypeError):
        return None


def _chunks(seq: list, n: int) -> Iterable[list]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def run(history_years: float | None = None, batch_size: int = DEFAULT_BATCH) -> int:
    secs = universe.get_active(asset_class=None)  # include ETFs (SPY/MDY)
    if not secs:
        print("No securities in universe.")
        return 0

    today = dt.date.today()
    sec_by_ticker = {s["ticker"]: s for s in secs}
    sec_ids = [s["id"] for s in secs]
    latest = _latest_date_map(sec_ids)

    # Group tickers by their fetch start so similar-range tickers batch together
    groups: dict[dt.date, list[str]] = {}
    for s in secs:
        sid = s["id"]
        if history_years:
            start = today - dt.timedelta(days=int(history_years * 365.25))
        else:
            last = latest.get(sid)
            start = (last + dt.timedelta(days=1)) if last else today - dt.timedelta(days=365 * 5)
        if start >= today:
            continue
        groups.setdefault(start, []).append(s["ticker"])

    total_rows = 0
    total_tickers = sum(len(v) for v in groups.values())
    failed: list[str] = []
    n_batches = 0
    done_tickers = 0

    for start, tickers in sorted(groups.items()):
        for chunk in _chunks(tickers, batch_size):
            n_batches += 1
            df = _yf_download_with_retry(chunk, start, today)
            if df is None:
                failed.extend(chunk)
                continue
            if df.empty:
                failed.extend(chunk)
                done_tickers += len(chunk)
                continue
            batch_rows: list[tuple] = []
            for tk in chunk:
                sub = _frame_for_ticker(df, tk)
                if sub.empty:
                    failed.append(tk)
                    continue
                sid = sec_by_ticker[tk]["id"]
                for _, r in sub.iterrows():
                    d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
                    batch_rows.append((
                        sid, d,
                        _f(r.get("open")), _f(r.get("high")), _f(r.get("low")),
                        _f(r.get("close")), _f(r.get("adj_close")),
                        _i(r.get("volume")),
                    ))
            if batch_rows:
                db.executemany(UPSERT_SQL, batch_rows)
                total_rows += len(batch_rows)
            done_tickers += len(chunk)
            print(f"  batch {n_batches:>3}  start={start}  size={len(chunk)}  "
                  f"+{len(batch_rows)} rows  ({done_tickers}/{total_tickers})")
            time.sleep(THROTTLE_SECONDS)

    print(f"\nInserted/updated {total_rows} price rows across {n_batches} batches. "
          f"Failed tickers: {len(failed)}")
    if failed:
        print(f"  examples: {', '.join(failed[:20])}{'...' if len(failed) > 20 else ''}")
    return total_rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", type=float, default=None,
                    help="Years of history to fetch; default = incremental")
    ap.add_argument("--batch", type=int, default=DEFAULT_BATCH,
                    help="Tickers per yfinance.download call")
    args = ap.parse_args()
    run(history_years=args.history, batch_size=args.batch)
