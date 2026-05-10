"""Portfolio constructor — combines strategy signals into target weights.

Algorithm:
  1. For each enabled strategy, sum its signals (weighted by allocation_weight).
  2. Apply per-name cap (5%), per-sector cap (25%).
  3. Volatility-target the portfolio: scale gross to hit 12% annualized vol.
  4. Drawdown overlay: if rolling DD > 8%, halve gross exposure.
  5. Output target weights as a snapshot; executor will diff against current
     positions and place trades.
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from .lib import db, mathx


PER_NAME_CAP = 0.05
PER_SECTOR_CAP = 0.30
VOL_TARGET = 0.13   # 13% ann
DD_OVERLAY_TRIGGER = 0.09
DD_OVERLAY_FACTOR = 0.6
MAX_GROSS = 1.7


def _price_panel(as_of: dt.date, lookback: int = 90) -> pd.DataFrame:
    start = as_of - dt.timedelta(days=int(lookback * 1.6))
    rows = db.query(
        """
        SELECT s.ticker, p.date, p.adj_close
        FROM prices p JOIN securities s ON s.id = p.security_id
        WHERE s.active = TRUE AND s.asset_class = 'equity'
          AND p.date BETWEEN %s AND %s;
        """,
        (start, as_of),
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["adj_close"] = pd.to_numeric(df["adj_close"])
    return df.pivot(index="date", columns="ticker", values="adj_close").sort_index().ffill()


def _signals(as_of: dt.date) -> pd.DataFrame:
    rows = db.query(
        """
        SELECT st.name AS strategy, st.allocation_weight, s.ticker, sec.sector,
               sg.signal
        FROM signals sg
        JOIN strategies st ON st.id = sg.strategy_id
        JOIN securities sec ON sec.id = sg.security_id
        JOIN securities s ON s.id = sg.security_id
        WHERE sg.date = %s AND st.enabled = TRUE;
        """,
        (as_of,),
    )
    return pd.DataFrame(rows)


def _current_dd(as_of: dt.date) -> float:
    """Drawdown right now: (latest NAV - running peak NAV) / running peak NAV."""
    rows = db.query(
        "SELECT date, nav FROM portfolio_nav WHERE date <= %s ORDER BY date;",
        (as_of,),
    )
    if not rows:
        return 0.0
    s = pd.Series([float(r["nav"]) for r in rows])
    if len(s) < 2:
        return 0.0
    peak = float(s.cummax().iloc[-1])
    cur = float(s.iloc[-1])
    if peak <= 0:
        return 0.0
    return max(0.0, (peak - cur) / peak)


def _portfolio_vol(weights: dict[str, float], panel: pd.DataFrame) -> float:
    if not weights:
        return 0.0
    rets = panel.pct_change().dropna(how="all").tail(60)
    common = [t for t in weights if t in rets.columns]
    if not common:
        return 0.0
    w = np.array([weights[t] for t in common])
    R = rets[common].fillna(0).values
    cov = np.cov(R.T) * 252
    if cov.ndim == 0:
        cov = np.array([[float(cov)]])
    var = float(w @ cov @ w)
    return float(np.sqrt(max(var, 0)))


def _apply_caps(target: dict[str, float], sectors: dict[str, str]) -> dict[str, float]:
    # per-name
    for t in list(target.keys()):
        target[t] = float(np.clip(target[t], -PER_NAME_CAP, PER_NAME_CAP))
    # per-sector long only (caps positive sums)
    by_sector: dict[str, float] = {}
    for t, w in target.items():
        if w > 0:
            by_sector.setdefault(sectors.get(t, "?"), 0.0)
            by_sector[sectors.get(t, "?")] += w
    for sec, total in by_sector.items():
        if total > PER_SECTOR_CAP:
            scale = PER_SECTOR_CAP / total
            for t, w in list(target.items()):
                if w > 0 and sectors.get(t) == sec:
                    target[t] = w * scale
    return target


def _target_table(as_of: dt.date) -> pd.DataFrame:
    sigs = _signals(as_of)
    if sigs.empty:
        return pd.DataFrame()
    sigs["signal"] = pd.to_numeric(sigs["signal"])
    sigs["allocation_weight"] = pd.to_numeric(sigs["allocation_weight"])
    sigs["weighted"] = sigs["signal"] * sigs["allocation_weight"]
    agg = sigs.groupby("ticker", as_index=False).agg(
        weight=("weighted", "sum"),
        sector=("sector", "first"),
    )
    return agg


def construct(as_of: dt.date | None = None) -> pd.DataFrame:
    if as_of is None:
        rows = db.query("SELECT MAX(date) AS d FROM signals;")
        as_of = rows[0]["d"]
        if as_of is None:
            return pd.DataFrame()

    table = _target_table(as_of)
    if table.empty:
        return table

    sectors = dict(zip(table["ticker"], table["sector"]))
    weights = dict(zip(table["ticker"], table["weight"]))

    weights = _apply_caps(weights, sectors)

    panel = _price_panel(as_of)
    realized = _portfolio_vol(weights, panel)
    if realized > 0:
        scale = VOL_TARGET / realized
        scale = float(np.clip(scale, 0.2, 2.0))
        weights = {t: w * scale for t, w in weights.items()}

    # DD overlay
    dd = _current_dd(as_of)
    if dd > DD_OVERLAY_TRIGGER:
        weights = {t: w * DD_OVERLAY_FACTOR for t, w in weights.items()}

    # Cap gross leverage
    gross = sum(abs(w) for w in weights.values())
    if gross > MAX_GROSS:
        scale = MAX_GROSS / gross
        weights = {t: w * scale for t, w in weights.items()}

    out = pd.DataFrame([
        {"ticker": t, "target_weight": w, "sector": sectors.get(t)}
        for t, w in weights.items() if abs(w) > 1e-5
    ])
    out["as_of"] = as_of
    return out.sort_values("target_weight", ascending=False).reset_index(drop=True)


if __name__ == "__main__":
    df = construct()
    if df.empty:
        print("No targets produced.")
    else:
        print(df.head(20).to_string(index=False))
        print(f"\nTotal positions: {len(df)}, gross: {df['target_weight'].abs().sum():.2%}, "
              f"net: {df['target_weight'].sum():.2%}")
