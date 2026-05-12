"""Fetch fundamentals from yfinance into raw.fundamentals_quarterly and
raw.fundamentals_snapshot.

What we store:
  raw.fundamentals_quarterly  — quarterly time series. Each row carries
    `available_at = fiscal_period_end + 90 days`, the 90-day reporting lag we
    use as a defense against look-ahead bias. Downstream queries do
    `WHERE available_at <= as_of`.
  raw.fundamentals_snapshot   — one row per ticker, slow-changing
    (market cap, trailing PE, beta). Overwritten on each fetch.

Limitations to know about (yfinance, not us):
  * Quarterly statements typically return 4–5 quarters, NOT 5 years. Multi-year
    historical fundamentals require a paid source.
  * `info.beta` and `info.marketCap` are *current* values. Treat as snapshot,
    not time series.
  * Per-ticker calls are slow (~0.5–1s each); we throttle accordingly. This is
    a 10–20 min job for ~1000 tickers; run it weekly, not daily.

CLI:
  python -m workers.fetch_fundamentals                  # all securities
  python -m workers.fetch_fundamentals --limit 50       # smoke test
  python -m workers.fetch_fundamentals --no-snapshot    # quarterlies only
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
import time

import pandas as pd
import yfinance as yf

from .lib import db, universe


# --- tunables ---------------------------------------------------------------

THROTTLE_SECONDS = 0.4              # per-ticker pacing (yfinance is rate-limited)
RETRY_BACKOFF = (3, 8)              # per-ticker retries on transient failure
REPORTING_LAG_DAYS = 90             # neutralizes most look-ahead bias
CIRCUIT_THRESHOLD = 20              # consecutive ticker failures → abort run
COMMIT_EVERY = 50                   # tickers between intermediate writes


# --- SQL --------------------------------------------------------------------

Q_TABLE = "raw.fundamentals_quarterly"
Q_COLS = [
    "security_id", "fiscal_period_end", "available_at",
    "total_revenue", "net_income", "total_assets", "total_equity",
    "total_debt", "operating_cashflow", "eps_diluted", "shares_diluted",
]

SNAP_TABLE = "raw.fundamentals_snapshot"
SNAP_COLS = [
    "security_id", "market_cap", "shares_outstanding",
    "trailing_pe", "forward_pe", "price_to_book",
    "trailing_eps", "beta",
]


# Line-item aliases — yfinance names have drifted across versions, so we try
# several. First match wins; missing items are stored as NULL.
FIN_ALIASES: dict[str, list[str]] = {
    "total_revenue":      ["Total Revenue", "TotalRevenue", "Revenue"],
    "net_income":         ["Net Income", "NetIncome", "Net Income Common Stockholders"],
    "eps_diluted":        ["Diluted EPS", "DilutedEPS", "Basic EPS"],
    "shares_diluted":     ["Diluted Average Shares", "DilutedAverageShares",
                           "Basic Average Shares"],
}
BAL_ALIASES: dict[str, list[str]] = {
    "total_assets":       ["Total Assets", "TotalAssets"],
    "total_equity":       ["Total Equity Gross Minority Interest",
                           "Stockholders Equity",
                           "Total Stockholder Equity",
                           "TotalStockholderEquity"],
    "total_debt":         ["Total Debt", "TotalDebt",
                           "Long Term Debt And Capital Lease Obligation"],
}
CF_ALIASES: dict[str, list[str]] = {
    "operating_cashflow": ["Operating Cash Flow", "OperatingCashFlow",
                           "Cash Flow From Continuing Operating Activities"],
}


# --- helpers ----------------------------------------------------------------

def _f(v) -> float | None:
    try:
        if v is None:
            return None
        f = float(v)
        if f != f:  # NaN
            return None
        return f
    except (TypeError, ValueError):
        return None


def _pick(frame: pd.DataFrame | None, aliases: list[str], col) -> float | None:
    """Try each alias as a row label in `frame` and return the value at `col`."""
    if frame is None or frame.empty:
        return None
    for name in aliases:
        if name in frame.index:
            try:
                return _f(frame.loc[name, col])
            except (KeyError, ValueError):
                continue
    return None


def _as_date(c) -> dt.date | None:
    try:
        if hasattr(c, "date"):
            return c.date()
        return pd.Timestamp(c).date()
    except Exception:
        return None


def _fetch_one(ticker: str) -> tuple[list[tuple], tuple | None, str | None]:
    """
    Returns (quarterly_rows, snapshot_row_or_None, error_message_or_None).
    Quarterly rows have 11 elements aligned to Q_COLS (less security_id).
    Snapshot row has 7 elements aligned to SNAP_COLS (less security_id).
    """
    attempts = (0,) + RETRY_BACKOFF
    qf = qb = qc = None
    info: dict = {}
    for i, backoff in enumerate(attempts):
        if backoff:
            time.sleep(backoff)
        try:
            t = yf.Ticker(ticker)
            qf = t.quarterly_financials
            qb = t.quarterly_balance_sheet
            qc = t.quarterly_cashflow
            info = t.info or {}
            break
        except Exception as e:
            if i == len(attempts) - 1:
                return [], None, f"{type(e).__name__}: {e}"

    # Quarterly columns are quarter-end timestamps; union across the three frames.
    cols: set = set()
    for frame in (qf, qb, qc):
        if frame is not None and not frame.empty:
            cols.update(frame.columns)

    quarterly: list[tuple] = []
    for col in sorted(cols, reverse=True):
        fpe = _as_date(col)
        if fpe is None:
            continue
        avail = fpe + dt.timedelta(days=REPORTING_LAG_DAYS)
        row = (
            fpe, avail,
            _pick(qf, FIN_ALIASES["total_revenue"], col),
            _pick(qf, FIN_ALIASES["net_income"], col),
            _pick(qb, BAL_ALIASES["total_assets"], col),
            _pick(qb, BAL_ALIASES["total_equity"], col),
            _pick(qb, BAL_ALIASES["total_debt"], col),
            _pick(qc, CF_ALIASES["operating_cashflow"], col),
            _pick(qf, FIN_ALIASES["eps_diluted"], col),
            _pick(qf, FIN_ALIASES["shares_diluted"], col),
        )
        # Skip rows that are entirely empty after fiscal_period_end / available_at
        if all(v is None for v in row[2:]):
            continue
        quarterly.append(row)

    snapshot = (
        _f(info.get("marketCap")),
        _f(info.get("sharesOutstanding")),
        _f(info.get("trailingPE")),
        _f(info.get("forwardPE")),
        _f(info.get("priceToBook")),
        _f(info.get("trailingEps")),
        _f(info.get("beta")),
    )
    if all(v is None for v in snapshot):
        snapshot = None  # type: ignore[assignment]

    return quarterly, snapshot, None


# --- main -------------------------------------------------------------------

def run(limit: int | None = None, include_snapshot: bool = True) -> dict:
    secs = universe.get_active(asset_class="equity")
    if limit:
        secs = secs[:limit]
    if not secs:
        print("No equities in universe.")
        return {"quarterlies": 0, "snapshots": 0, "failed": 0}

    today = dt.date.today()

    # Skip securities currently on health cooldown
    health = {r["security_id"]: r for r in db.query(
        "SELECT security_id, skip_until FROM raw.ticker_health;"
    )}

    def _on_cooldown(sid: int) -> bool:
        h = health.get(sid)
        return bool(h and h["skip_until"] and h["skip_until"] > today)

    eligible = [s for s in secs if not _on_cooldown(s["id"])]

    print(
        f"Fetching fundamentals for {len(eligible)} securities "
        f"(cooldown_skipped={len(secs) - len(eligible)})"
    )

    q_buf: list[tuple] = []
    s_buf: list[tuple] = []
    failed: list[str] = []
    consecutive_failures = 0
    total_q = 0
    total_s = 0

    for idx, s in enumerate(eligible, start=1):
        sid = s["id"]
        ticker = s["ticker"]
        quarterly, snapshot, err = _fetch_one(ticker)

        if err:
            failed.append(ticker)
            consecutive_failures += 1
            db.execute(
                """
                INSERT INTO raw.ticker_health
                  (security_id, consecutive_failures, last_failure_at, last_failure_reason)
                VALUES (%s, 1, NOW(), %s)
                ON CONFLICT (security_id) DO UPDATE SET
                  consecutive_failures = raw.ticker_health.consecutive_failures + 1,
                  last_failure_at = NOW(),
                  last_failure_reason = EXCLUDED.last_failure_reason;
                """,
                (sid, err[:500]),
            )
            print(f"  [{idx}/{len(eligible)}] {ticker}: FAILED ({err})", file=sys.stderr)
            if consecutive_failures >= CIRCUIT_THRESHOLD:
                print(
                    f"\nCircuit breaker tripped: {consecutive_failures} consecutive ticker "
                    f"failures. Aborting run.",
                    file=sys.stderr,
                )
                break
            time.sleep(THROTTLE_SECONDS)
            continue

        consecutive_failures = 0
        for q in quarterly:
            q_buf.append((sid, *q))
        if include_snapshot and snapshot:
            s_buf.append((sid, *snapshot))
        db.execute(
            """
            INSERT INTO raw.ticker_health (security_id, consecutive_failures, last_success_at, skip_until)
            VALUES (%s, 0, NOW(), NULL)
            ON CONFLICT (security_id) DO UPDATE SET
              consecutive_failures = 0,
              last_success_at = NOW(),
              skip_until = NULL;
            """,
            (sid,),
        )

        if idx % COMMIT_EVERY == 0:
            if q_buf:
                total_q += db.bulk_upsert(
                    Q_TABLE, Q_COLS, q_buf,
                    conflict_cols=["security_id", "fiscal_period_end"],
                )
                q_buf = []
            if s_buf:
                total_s += db.bulk_upsert(
                    SNAP_TABLE, SNAP_COLS, s_buf,
                    conflict_cols=["security_id"],
                )
                s_buf = []
            print(f"  ... checkpoint at {idx}/{len(eligible)}  q_rows={total_q}  snaps={total_s}")

        time.sleep(THROTTLE_SECONDS)

    # Final flush
    if q_buf:
        total_q += db.bulk_upsert(
            Q_TABLE, Q_COLS, q_buf,
            conflict_cols=["security_id", "fiscal_period_end"],
        )
    if s_buf:
        total_s += db.bulk_upsert(
            SNAP_TABLE, SNAP_COLS, s_buf,
            conflict_cols=["security_id"],
        )

    print(
        f"\nFundamentals: {total_q} quarterly rows, {total_s} snapshot rows, "
        f"{len(failed)} ticker failures."
    )
    if failed:
        sample = ", ".join(failed[:20])
        more = "..." if len(failed) > 20 else ""
        print(f"  examples: {sample}{more}")
    return {"quarterlies": total_q, "snapshots": total_s, "failed": len(failed)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap the number of securities (smoke test).")
    ap.add_argument("--no-snapshot", action="store_true",
                    help="Skip the per-ticker snapshot (info) write.")
    args = ap.parse_args()
    run(limit=args.limit, include_snapshot=not args.no_snapshot)
