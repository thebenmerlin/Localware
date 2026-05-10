"""Strategy runner — generates daily signals for each enabled strategy.

Each strategy returns a DataFrame with columns: ticker, score, signal.
- score: raw factor value (for inspection)
- signal: target weight contribution within the strategy sleeve, in [-1, 1]

Sleeve weights are scaled later by portfolio_constructor.
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from .lib import db, mathx, universe


# ---- helpers ---------------------------------------------------------------

def _price_panel(end: dt.date, lookback_days: int = 400) -> pd.DataFrame:
    """Wide panel of adj_close indexed by date, columns=ticker (equities only)."""
    start = end - dt.timedelta(days=lookback_days)
    rows = db.query(
        """
        SELECT s.ticker, p.date, p.adj_close
        FROM prices p
        JOIN securities s ON s.id = p.security_id
        WHERE s.active = TRUE AND s.asset_class = 'equity'
          AND p.date BETWEEN %s AND %s
        ORDER BY p.date, s.ticker
        """,
        (start, end),
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    panel = df.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    return panel.ffill(limit=2)


def _ticker_to_id() -> dict[str, int]:
    rows = db.query("SELECT id, ticker FROM securities WHERE active = TRUE;")
    return {r["ticker"]: r["id"] for r in rows}


def _fundamentals() -> pd.DataFrame:
    rows = db.query(
        """
        SELECT s.ticker, f.pe, f.pb, f.roe, f.debt_to_equity, f.earnings_growth, f.market_cap
        FROM fundamentals f
        JOIN securities s ON s.id = f.security_id
        WHERE f.date = (SELECT MAX(date) FROM fundamentals);
        """
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for c in ("pe", "pb", "roe", "debt_to_equity", "earnings_growth", "market_cap"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.set_index("ticker")


# ---- strategies ------------------------------------------------------------

def momentum(panel: pd.DataFrame, longs: int = 30, shorts: int = 30,
             lookback: int = 252, skip: int = 21) -> pd.DataFrame:
    if panel.empty or len(panel) < lookback + 5:
        return pd.DataFrame()
    p = panel.iloc[-1]
    p_skip = panel.iloc[-skip - 1] if len(panel) > skip + 1 else panel.iloc[-1]
    p_lb = panel.iloc[-lookback - 1] if len(panel) > lookback + 1 else panel.iloc[0]
    ret_12_1 = (p_skip / p_lb) - 1
    ret_12_1 = ret_12_1.replace([np.inf, -np.inf], np.nan).dropna()
    if len(ret_12_1) < (longs + shorts):
        return pd.DataFrame()
    ranked = ret_12_1.sort_values()
    bot = ranked.head(shorts).index
    top = ranked.tail(longs).index
    out = pd.DataFrame({"ticker": ret_12_1.index, "score": ret_12_1.values})
    out["signal"] = 0.0
    out.loc[out["ticker"].isin(top), "signal"] = 1.0 / longs
    out.loc[out["ticker"].isin(bot), "signal"] = -1.0 / shorts
    return out[out["signal"] != 0]


def quality(fund: pd.DataFrame, longs: int = 30,
            min_roe: float = 0.15, max_de: float = 1.0) -> pd.DataFrame:
    if fund.empty:
        return pd.DataFrame()
    f = fund.copy()
    f["de"] = f["debt_to_equity"].fillna(999)
    # yfinance reports D/E as percentage in many cases; normalize
    f.loc[f["de"] > 10, "de"] = f.loc[f["de"] > 10, "de"] / 100
    f["eps_g"] = f["earnings_growth"].fillna(-999)
    elig = f[(f["roe"].fillna(0) >= min_roe) & (f["de"] <= max_de) & (f["eps_g"] > 0)].copy()
    if len(elig) < 5:
        # relax if too few
        elig = f[(f["roe"].fillna(0) >= min_roe / 2) & (f["de"] <= max_de * 2)].copy()
    if elig.empty:
        return pd.DataFrame()
    elig["z"] = (
        mathx.zscore(elig["roe"].fillna(0))
        - mathx.zscore(elig["de"])
        + mathx.zscore(elig["eps_g"].clip(lower=-1, upper=2))
    )
    top = elig.sort_values("z", ascending=False).head(longs)
    out = pd.DataFrame({
        "ticker": top.index,
        "score": top["z"].values,
        "signal": 1.0 / len(top),
    })
    return out


def low_volatility(panel: pd.DataFrame, longs: int = 50, lookback: int = 60) -> pd.DataFrame:
    if panel.empty or len(panel) < lookback + 5:
        return pd.DataFrame()
    rets = panel.pct_change().tail(lookback)
    vols = rets.std().dropna()
    if vols.empty:
        return pd.DataFrame()
    bottom = vols.sort_values().head(longs)
    out = pd.DataFrame({
        "ticker": bottom.index,
        "score": bottom.values,
        "signal": 1.0 / len(bottom),
    })
    return out


def mean_reversion(panel: pd.DataFrame, rsi_period: int = 14, rsi_th: int = 30,
                   ma_period: int = 200) -> pd.DataFrame:
    if panel.empty or len(panel) < ma_period + 5:
        return pd.DataFrame()
    last = panel.iloc[-1]
    ma = panel.tail(ma_period).mean()
    longs = []
    scores = []
    for t in panel.columns:
        s = panel[t].dropna()
        if len(s) < ma_period + rsi_period:
            continue
        r = mathx.rsi(s, period=rsi_period).iloc[-1]
        if r < rsi_th and last[t] > ma[t]:
            longs.append(t)
            scores.append(float(r))
    if not longs:
        return pd.DataFrame()
    out = pd.DataFrame({
        "ticker": longs,
        "score": scores,
        "signal": 1.0 / len(longs),
    })
    return out


# ---- runner ----------------------------------------------------------------

INSERT_SQL = """
INSERT INTO signals (strategy_id, security_id, date, signal, score, metadata)
VALUES (%s, %s, %s, %s, %s, %s::jsonb)
ON CONFLICT (strategy_id, security_id, date) DO UPDATE SET
  signal = EXCLUDED.signal,
  score = EXCLUDED.score,
  metadata = EXCLUDED.metadata;
"""


def _strategy_row(name: str) -> dict | None:
    rows = db.query("SELECT id, params, allocation_weight FROM strategies WHERE name = %s AND enabled = TRUE", (name,))
    return rows[0] if rows else None


def run(as_of: dt.date | None = None) -> dict[str, int]:
    if as_of is None:
        rows = db.query("SELECT MAX(date) AS d FROM prices;")
        as_of = rows[0]["d"]
        if as_of is None:
            print("No prices yet — skipping strategy runner.")
            return {}

    panel = _price_panel(as_of)
    fund = _fundamentals()
    tid = _ticker_to_id()

    counts: dict[str, int] = {}

    handlers = {
        "momentum":      lambda p: momentum(panel, **{k: v for k, v in (p or {}).items() if k in ("longs", "shorts", "lookback", "skip") or k.startswith("lookback")}),
        "quality":       lambda p: quality(fund, **{k: v for k, v in (p or {}).items() if k in ("longs", "min_roe", "max_de")}),
        "low_volatility":lambda p: low_volatility(panel, **{k: v for k, v in (p or {}).items() if k in ("longs", "lookback")}),
        "mean_reversion":lambda p: mean_reversion(panel, **{k: v for k, v in (p or {}).items() if k in ("rsi_period", "rsi_th", "ma_period")}),
    }

    for name in handlers:
        srow = _strategy_row(name)
        if not srow:
            continue
        params = srow["params"] or {}
        # Adapt some param names (rsi_threshold -> rsi_th, lookback_days -> lookback, etc.)
        params = dict(params)
        if "rsi_threshold" in params:
            params["rsi_th"] = params.pop("rsi_threshold")
        if "lookback_days" in params and name != "momentum":
            params["lookback"] = params.pop("lookback_days")
        if "lookback_days" in params and name == "momentum":
            params["lookback"] = params.pop("lookback_days")
        if "skip_days" in params:
            params["skip"] = params.pop("skip_days")
        try:
            sig = handlers[name](params)
        except Exception as e:
            print(f"  {name}: FAILED ({e})")
            continue
        if sig is None or sig.empty:
            counts[name] = 0
            continue
        rows_to_insert = []
        for _, r in sig.iterrows():
            tk = r["ticker"]
            if tk not in tid:
                continue
            rows_to_insert.append((
                srow["id"], tid[tk], as_of,
                float(r["signal"]), float(r["score"]),
                "{}",
            ))
        if rows_to_insert:
            db.executemany(INSERT_SQL, rows_to_insert)
        counts[name] = len(rows_to_insert)
        print(f"  {name}: {len(rows_to_insert)} signals")

    return counts


if __name__ == "__main__":
    run()
