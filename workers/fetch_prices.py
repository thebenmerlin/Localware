"""Fetch daily OHLCV from yfinance into raw.ohlcv_daily.

Design:
  * Incremental: per-ticker latest date is read from raw.ohlcv_daily; only the
    missing trailing days are fetched. Backfill is opt-in via --history.
  * Chunked: tickers grouped by required start date, then batched into
    yfinance.download (one batched HTTP call per chunk).
  * Retries: per-batch exponential backoff for transient errors.
  * Universe-level circuit breaker: N consecutive batches with no usable data
    aborts the run (yfinance likely broken; don't burn the 4.5h window).
  * Per-ticker skip: raw.ticker_health tracks consecutive failures across runs
    and parks persistently broken tickers via skip_until.
  * Idempotent writes: ON CONFLICT DO UPDATE so re-runs are safe.

CLI:
  python -m workers.fetch_prices                    # incremental (default)
  python -m workers.fetch_prices --history 5        # 5y backfill
  python -m workers.fetch_prices --batch 60         # tune batch size
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


# --- tunables ---------------------------------------------------------------

DEFAULT_BATCH = 80                  # tickers per yfinance.download call
THROTTLE_SECONDS = 1.0              # polite gap between successful batches
RETRY_BACKOFF = (3, 8, 20)          # seconds; applied per-batch on failure
CIRCUIT_THRESHOLD = 5               # consecutive no-data batches → abort run
TICKER_FAIL_THRESHOLD = 5           # consecutive run-level failures → skip
TICKER_COOLDOWN_DAYS = 7            # how long to park a failing ticker
DEFAULT_HISTORY_YEARS = 5           # used when no prior rows exist


# --- SQL --------------------------------------------------------------------

OHLCV_TABLE = "raw.ohlcv_daily"
OHLCV_COLS = ["security_id", "date", "open", "high", "low", "close", "adj_close", "volume"]

HEALTH_TABLE = "raw.ticker_health"

LOAD_HEALTH_SQL = """
SELECT security_id, consecutive_failures, skip_until
FROM raw.ticker_health;
"""

MARK_FAIL_SQL = """
INSERT INTO raw.ticker_health
  (security_id, consecutive_failures, last_failure_at, last_failure_reason, skip_until)
VALUES (%s, 1, NOW(), %s, NULL)
ON CONFLICT (security_id) DO UPDATE SET
  consecutive_failures = raw.ticker_health.consecutive_failures + 1,
  last_failure_at = NOW(),
  last_failure_reason = EXCLUDED.last_failure_reason,
  skip_until = CASE
    WHEN raw.ticker_health.consecutive_failures + 1 >= %s
    THEN CURRENT_DATE + (%s::INT) * INTERVAL '1 day'
    ELSE NULL
  END;
"""

MARK_SUCCESS_SQL = """
INSERT INTO raw.ticker_health
  (security_id, consecutive_failures, last_success_at, skip_until)
VALUES (%s, 0, NOW(), NULL)
ON CONFLICT (security_id) DO UPDATE SET
  consecutive_failures = 0,
  last_success_at = NOW(),
  skip_until = NULL;
"""


# --- helpers ----------------------------------------------------------------

def _chunks(seq: list, n: int) -> Iterable[list]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


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


def _yf_download_with_retry(tickers: list[str], start: dt.date, end: dt.date) -> pd.DataFrame | None:
    """Returns a DataFrame, empty DataFrame (no data but call succeeded), or None (call failed)."""
    last_exc: Exception | None = None
    attempts = (0,) + RETRY_BACKOFF
    for i, backoff in enumerate(attempts):
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
            if df is None:
                return pd.DataFrame()
            return df
        except Exception as e:
            last_exc = e
            if i < len(RETRY_BACKOFF):
                print(f"  batch retry {i+1}/{len(RETRY_BACKOFF)}: {e}", file=sys.stderr)
            else:
                print(f"  batch failed after {len(attempts)} attempts: {e}", file=sys.stderr)
    _ = last_exc
    return None


def _frame_for_ticker(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Slice a multi-ticker yfinance frame for one ticker into a clean DataFrame."""
    if df is None or df.empty:
        return pd.DataFrame()
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


def _load_health() -> dict[int, dict]:
    rows = db.query(LOAD_HEALTH_SQL)
    return {r["security_id"]: r for r in rows}


def _mark_failure(security_id: int, reason: str) -> None:
    db.execute(MARK_FAIL_SQL, (security_id, reason[:500], TICKER_FAIL_THRESHOLD, TICKER_COOLDOWN_DAYS))


def _mark_success(security_id: int) -> None:
    db.execute(MARK_SUCCESS_SQL, (security_id,))


# --- main -------------------------------------------------------------------

def run(history_years: float | None = None, batch_size: int = DEFAULT_BATCH) -> int:
    secs = universe.get_active(asset_class=None)  # equities + index ETFs (SPY/MDY)
    if not secs:
        print("No securities in universe.")
        return 0

    today = dt.date.today()
    health = _load_health()
    sec_by_ticker = {s["ticker"]: s for s in secs}

    # Filter out persistently failing tickers under cooldown
    skipped_for_cooldown: list[str] = []
    eligible: list[dict] = []
    for s in secs:
        h = health.get(s["id"])
        if h and h["skip_until"] and h["skip_until"] > today:
            skipped_for_cooldown.append(s["ticker"])
            continue
        eligible.append(s)

    if not eligible:
        print("All securities under cooldown. Nothing to do.")
        return 0

    sec_ids = [s["id"] for s in eligible]
    latest = db.latest_dates(OHLCV_TABLE)
    latest = {sid: latest.get(sid) for sid in sec_ids}

    # Group tickers by their start date so each batch fetches a uniform window
    groups: dict[dt.date, list[str]] = {}
    for s in eligible:
        sid = s["id"]
        if history_years:
            start = today - dt.timedelta(days=int(history_years * 365.25))
        else:
            last = latest.get(sid)
            start = (
                last + dt.timedelta(days=1)
                if last
                else today - dt.timedelta(days=int(DEFAULT_HISTORY_YEARS * 365.25))
            )
        if start >= today:
            continue
        groups.setdefault(start, []).append(s["ticker"])

    total_tickers = sum(len(v) for v in groups.values())
    if not total_tickers:
        print(f"Universe up-to-date as of {today}. ({len(skipped_for_cooldown)} on cooldown)")
        return 0

    print(
        f"Fetching prices: {total_tickers} tickers across {len(groups)} start-windows, "
        f"batch={batch_size}, cooldown_skipped={len(skipped_for_cooldown)}"
    )

    total_rows = 0
    n_batches = 0
    done_tickers = 0
    failed_run: list[str] = []
    consecutive_empty_batches = 0

    for start, tickers in sorted(groups.items()):
        for chunk in _chunks(tickers, batch_size):
            n_batches += 1
            df = _yf_download_with_retry(chunk, start, today)

            # Universe-level circuit breaker
            if df is None or df.empty:
                consecutive_empty_batches += 1
                reason = "yfinance returned None" if df is None else "yfinance returned empty frame"
                for tk in chunk:
                    sid = sec_by_ticker[tk]["id"]
                    _mark_failure(sid, reason)
                    failed_run.append(tk)
                done_tickers += len(chunk)
                print(
                    f"  batch {n_batches:>3}  start={start}  size={len(chunk)}  "
                    f"NO DATA ({done_tickers}/{total_tickers})  empty_streak={consecutive_empty_batches}"
                )
                if consecutive_empty_batches >= CIRCUIT_THRESHOLD:
                    print(
                        f"\nCircuit breaker tripped: {consecutive_empty_batches} consecutive empty batches. "
                        f"Aborting run to preserve the runtime budget.",
                        file=sys.stderr,
                    )
                    return total_rows
                continue

            consecutive_empty_batches = 0

            # Materialize this batch's rows for bulk_upsert
            batch_rows: list[tuple] = []
            per_chunk_failed: list[str] = []
            per_chunk_succeeded: list[int] = []
            for tk in chunk:
                sub = _frame_for_ticker(df, tk)
                if sub.empty:
                    per_chunk_failed.append(tk)
                    continue
                sid = sec_by_ticker[tk]["id"]
                per_chunk_succeeded.append(sid)
                for _, r in sub.iterrows():
                    d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
                    batch_rows.append((
                        sid, d,
                        _f(r.get("open")), _f(r.get("high")), _f(r.get("low")),
                        _f(r.get("close")), _f(r.get("adj_close")),
                        _i(r.get("volume")),
                    ))

            if batch_rows:
                db.bulk_upsert(
                    OHLCV_TABLE,
                    OHLCV_COLS,
                    batch_rows,
                    conflict_cols=["security_id", "date"],
                )
                total_rows += len(batch_rows)

            for sid in per_chunk_succeeded:
                _mark_success(sid)
            for tk in per_chunk_failed:
                sid = sec_by_ticker[tk]["id"]
                _mark_failure(sid, "no data in batched response")
                failed_run.append(tk)

            done_tickers += len(chunk)
            print(
                f"  batch {n_batches:>3}  start={start}  size={len(chunk)}  "
                f"+{len(batch_rows)} rows  fail={len(per_chunk_failed)}  "
                f"({done_tickers}/{total_tickers})"
            )
            time.sleep(THROTTLE_SECONDS)

    print(
        f"\nInserted/updated {total_rows} OHLCV rows across {n_batches} batches. "
        f"Per-ticker failures this run: {len(failed_run)}; on cooldown: {len(skipped_for_cooldown)}."
    )
    if failed_run:
        sample = ", ".join(failed_run[:20])
        more = "..." if len(failed_run) > 20 else ""
        print(f"  examples: {sample}{more}")
    return total_rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", type=float, default=None,
                    help="Years of history to fetch; default = incremental from latest stored date")
    ap.add_argument("--batch", type=int, default=DEFAULT_BATCH,
                    help="Tickers per yfinance.download call")
    args = ap.parse_args()
    run(history_years=args.history, batch_size=args.batch)
