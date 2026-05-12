"""Walk-forward backtest — fully vectorized PnL, panel-vectorized signals.

The fast path (the part you'd run for 5y of history):
  1. Load price panel + full fundamentals history in two queries.
  2. For each REBALANCE date (every `rebalance_days` trading days), call the
     pure sleeve functions from strategy_runner with sliced in-memory panels;
     call portfolio_constructor.apply_overlays with the resulting blended
     signal. Accumulate rows for derived.signals_daily and
     portfolio.positions_daily. No DB writes inside the loop.
  3. After the loop, bulk-upsert the accumulated signal & position rows.
  4. Build the positions panel by forward-filling target weights between
     rebalances. Multiply element-wise with the return panel → daily portfolio
     return SERIES (one shot — no per-day loop in the math path). Apply flat
     10bps × turnover cost. Cumprod → NAV curve.
  5. Bulk upsert portfolio.nav_daily.
  6. Call performance.run() + risk_engine.run() to refresh analytics.*.

CLI:
  python -m workers.backtest --start 2021-01-04 --end 2026-05-09
  python -m workers.backtest --rebalance 21 --no-reset
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os

import numpy as np
import pandas as pd
from psycopg.types.json import Jsonb

from .lib import db
from . import strategy_runner, portfolio_constructor, executor, performance, risk_engine


# --- tunables ---------------------------------------------------------------

DEFAULT_REBALANCE_DAYS = 21         # ~monthly
DEFAULT_HISTORY_BUFFER = 420        # extra calendar days loaded before `start` so
                                    # momentum's 252+skip window is filled on day 1
COST_BPS = executor.COST_BPS        # 10bps (kept in sync with single-day executor)


# --- data loading -----------------------------------------------------------

def _load_securities() -> pd.DataFrame:
    rows = db.query(
        "SELECT id AS security_id, ticker, sector, asset_class "
        "FROM securities WHERE active = TRUE;"
    )
    return pd.DataFrame(rows).set_index("ticker")


def _load_price_panel(start: dt.date, end: dt.date) -> pd.DataFrame:
    """Equity adj_close panel covering [start, end]. Index=Timestamp, columns=ticker."""
    rows = db.query(
        """
        SELECT s.ticker, o.date, o.adj_close
        FROM raw.ohlcv_daily o
        JOIN securities s ON s.id = o.security_id
        WHERE s.active = TRUE AND s.asset_class = 'equity'
          AND o.date BETWEEN %s AND %s
        ORDER BY o.date, s.ticker;
        """,
        (start, end),
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    panel = df.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    return panel.astype(float).ffill(limit=2)


def _load_fundamentals_history(end: dt.date) -> pd.DataFrame:
    """All point-in-time fundamentals up through `end`. Long format."""
    rows = db.query(
        """
        SELECT s.ticker, fq.fiscal_period_end, fq.available_at,
               fq.total_revenue, fq.net_income, fq.operating_cashflow,
               fq.total_equity, fq.total_debt, fq.total_assets
        FROM raw.fundamentals_quarterly fq
        JOIN securities s ON s.id = fq.security_id
        WHERE fq.available_at <= %s AND s.asset_class = 'equity';
        """,
        (end,),
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["fiscal_period_end"] = pd.to_datetime(df["fiscal_period_end"])
    df["available_at"] = pd.to_datetime(df["available_at"])
    return df


def _ttm_at_date(fund_history: pd.DataFrame, as_of: dt.date) -> pd.DataFrame:
    """Filter fund_history to rows available at as_of, then aggregate to TTM."""
    if fund_history.empty:
        return pd.DataFrame()
    snap = fund_history[fund_history["available_at"] <= pd.Timestamp(as_of)]
    if snap.empty:
        return pd.DataFrame()
    snap = snap.sort_values(["ticker", "fiscal_period_end"], ascending=[True, False])
    grp = snap.groupby("ticker", as_index=False)
    flow = grp.head(4).groupby("ticker", as_index=False).agg(
        total_revenue_ttm=("total_revenue", "sum"),
        net_income_ttm=("net_income", "sum"),
        opcf_ttm=("operating_cashflow", "sum"),
    )
    stock = grp.head(1)[["ticker", "total_equity", "total_debt", "total_assets"]]
    return flow.merge(stock, on="ticker", how="left").set_index("ticker")


# --- per-rebalance signal computation (in-memory; reuses Phase 2 functions) -

def _generate_rebalance(
    as_of: dt.date,
    panel_slice: pd.DataFrame,
    fund_history: pd.DataFrame,
    secs: pd.DataFrame,
    alloc: dict[str, float],
    drawdown: float,
) -> tuple[pd.Series, pd.DataFrame, pd.Series]:
    """Returns (blended_signal, attribution, final_weights)."""
    fund_ttm = _ttm_at_date(fund_history, as_of)
    sleeves = {
        "momentum":       strategy_runner.compute_momentum(panel_slice, **strategy_runner.MOM_PARAMS),
        "quality":        strategy_runner.compute_quality(fund_ttm, **strategy_runner.QUAL_PARAMS),
        "low_volatility": strategy_runner.compute_low_volatility(panel_slice, **strategy_runner.LV_PARAMS),
        "mean_reversion": strategy_runner.compute_mean_reversion(panel_slice, **strategy_runner.MR_PARAMS),
    }
    blended, attribution = strategy_runner.blend(sleeves, alloc)
    blended = blended[blended.abs() > 1e-9]
    if blended.empty:
        return pd.Series(dtype=float), pd.DataFrame(), pd.Series(dtype=float)

    sectors = secs.reindex(blended.index)["sector"]
    final = portfolio_constructor.apply_overlays(
        blended,
        sectors=sectors,
        panel=panel_slice,
        drawdown=drawdown,
    )
    return blended, attribution, final


# --- vectorized NAV simulation ----------------------------------------------

def _simulate_nav(
    weights_panel: pd.DataFrame,
    price_panel: pd.DataFrame,
    initial_nav: float,
    cost_bps: float = COST_BPS,
) -> pd.DataFrame:
    """One vectorized pass: weight panel × return panel → NAV curve.

    `weights_panel` must already be aligned to `price_panel`'s index and
    forward-filled between rebalances (zeros where no position).
    """
    cols = weights_panel.columns.intersection(price_panel.columns)
    W = weights_panel[cols].astype(float).fillna(0.0)
    P = price_panel[cols].astype(float)
    R = P.pct_change().fillna(0.0)
    R = R.replace([np.inf, -np.inf], 0.0)

    # Held during day t = W[t] (set at close of t-1)
    portfolio_ret = (W * R).sum(axis=1)
    turnover = (W - W.shift(1)).abs().sum(axis=1)
    # Day-0 turnover = entering from cash; capture initial gross
    turnover.iloc[0] = float(W.iloc[0].abs().sum())
    # Day-0 has no PnL — only the entry cost matters
    portfolio_ret.iloc[0] = 0.0

    costs = turnover * (cost_bps / 10_000.0)
    net_ret = portfolio_ret - costs
    nav = initial_nav * (1.0 + net_ret).cumprod()

    gross = W.abs().sum(axis=1)
    net = W.sum(axis=1)
    cumret = nav / initial_nav - 1.0

    return pd.DataFrame({
        "nav": nav,
        "daily_return": net_ret,
        "cumulative_return": cumret,
        "gross_exposure": gross,
        "net_exposure": net,
        "turnover": turnover,
        "execution_cost_bps": cost_bps,
    })


# --- state reset ------------------------------------------------------------

def _reset_state(start: dt.date, end: dt.date) -> None:
    """Clear derived signals, target positions, NAV, and analytics for the range."""
    for sql in [
        "DELETE FROM derived.signals_daily WHERE date BETWEEN %s AND %s;",
        "DELETE FROM portfolio.positions_daily WHERE date BETWEEN %s AND %s;",
        "DELETE FROM portfolio.nav_daily WHERE date BETWEEN %s AND %s;",
        "DELETE FROM analytics.equity_curve WHERE date BETWEEN %s AND %s;",
        "DELETE FROM analytics.rolling_metrics WHERE date BETWEEN %s AND %s;",
        "DELETE FROM analytics.var_daily WHERE date BETWEEN %s AND %s;",
    ]:
        db.execute(sql, (start, end))
    db.execute("DELETE FROM analytics.monthly_returns;")
    db.execute("DELETE FROM analytics.performance_summary;")
    db.execute("DELETE FROM analytics.drawdown_periods;")


# --- orchestrator -----------------------------------------------------------

def run(
    start_date: dt.date,
    end_date: dt.date,
    rebalance_days: int = DEFAULT_REBALANCE_DAYS,
    history_buffer_days: int = DEFAULT_HISTORY_BUFFER,
    initial_nav: float | None = None,
    reset: bool = True,
) -> dict:
    if initial_nav is None:
        initial_nav = float(os.environ.get("INITIAL_NAV", "10000000"))

    print(
        f"Backtest {start_date} → {end_date}  "
        f"rebalance={rebalance_days}d  initial_nav=${initial_nav:,.0f}"
    )

    if reset:
        print("  resetting derived/portfolio/analytics tables for the range...")
        _reset_state(start_date, end_date)

    # Load everything once
    load_from = start_date - dt.timedelta(days=history_buffer_days)
    print(f"  loading data from {load_from}...")
    secs = _load_securities()
    panel = _load_price_panel(load_from, end_date)
    if panel.empty:
        print("  empty price panel — aborting.")
        return {}
    fund_history = _load_fundamentals_history(end_date)
    print(f"  panel: {panel.shape[0]} dates × {panel.shape[1]} tickers; "
          f"fundamentals rows: {len(fund_history)}")

    alloc = strategy_runner._allocation_weights()
    ticker_to_sid = secs["security_id"].to_dict()
    sector_map = secs["sector"]

    # Trading days in-range and rebalance schedule
    trading_days = panel.index[(panel.index >= pd.Timestamp(start_date)) &
                               (panel.index <= pd.Timestamp(end_date))]
    if len(trading_days) == 0:
        print("  no trading days in range.")
        return {}
    rebalance_idx = list(range(0, len(trading_days), rebalance_days))
    rebalance_dates = [trading_days[i].date() for i in rebalance_idx]
    print(f"  {len(trading_days)} trading days; {len(rebalance_dates)} rebalances")

    # Walk forward
    signal_rows: list[tuple] = []
    position_rows: list[tuple] = []
    weights_history: dict[dt.date, pd.Series] = {}
    running_dd = 0.0  # we approximate DD overlay using the running NAV
    fake_nav = pd.Series(dtype=float)

    for i, as_of in enumerate(rebalance_dates):
        panel_slice = panel.loc[:pd.Timestamp(as_of)]
        blended, attribution, final = _generate_rebalance(
            as_of, panel_slice, fund_history, secs, alloc, running_dd,
        )
        if final.empty:
            continue
        weights_history[as_of] = final

        # signals_daily rows
        attr_dict = attribution.to_dict(orient="index") if not attribution.empty else {}
        for ticker, sig in blended.items():
            sid = ticker_to_sid.get(ticker)
            if sid is None:
                continue
            contribs = {
                name: float(v) for name, v in attr_dict.get(ticker, {}).items()
                if v != 0 and not pd.isna(v)
            }
            signal_rows.append((sid, as_of, float(sig), Jsonb(contribs)))

        # positions_daily rows (effective next business day)
        target_date = portfolio_constructor._next_business_day(as_of)
        for ticker, w in final.items():
            sid = ticker_to_sid.get(ticker)
            if sid is None:
                continue
            sec = sector_map.get(ticker)
            position_rows.append((
                sid, target_date, float(w), float(blended.get(ticker, 0.0)),
                sec if pd.notna(sec) else None,
            ))

        if (i + 1) % 5 == 0 or i == len(rebalance_dates) - 1:
            print(f"  rebalance {i+1}/{len(rebalance_dates)} @ {as_of}  "
                  f"longs={int((final>0).sum())} shorts={int((final<0).sum())} "
                  f"gross={float(final.abs().sum()):.3f}")

    # Bulk-write signals + positions
    if signal_rows:
        n_sig = db.bulk_upsert(
            "derived.signals_daily",
            ["security_id", "date", "blended_signal", "attribution"],
            signal_rows,
            conflict_cols=["security_id", "date"],
        )
        print(f"  upserted {n_sig} rows into derived.signals_daily")
    if position_rows:
        n_pos = db.bulk_upsert(
            "portfolio.positions_daily",
            ["security_id", "date", "target_weight", "pre_overlay_weight", "sector"],
            position_rows,
            conflict_cols=["security_id", "date"],
        )
        print(f"  upserted {n_pos} rows into portfolio.positions_daily")

    # Build weights_panel aligned to trading_days, forward-filled between rebalances
    all_tickers = sorted(set().union(*(w.index for w in weights_history.values())))
    if not all_tickers:
        print("  no positions produced — nothing to simulate.")
        return {}

    weights_panel = pd.DataFrame(0.0, index=trading_days, columns=all_tickers)
    for as_of, w in weights_history.items():
        # positions are EFFECTIVE on next business day, so apply from target_date forward
        target_date = portfolio_constructor._next_business_day(as_of)
        target_ts = pd.Timestamp(target_date)
        if target_ts > trading_days.max():
            continue
        # Set the row at target_date (or the next available trading day)
        eff_ts = trading_days[trading_days >= target_ts]
        if len(eff_ts) == 0:
            continue
        weights_panel.loc[eff_ts[0], w.index] = w.values
        # Mark this as a rebalance row — between rebalances we forward-fill below.
    # Replace zeros with NaN so ffill correctly carries the last set of weights forward
    # (zero is a valid target too, so we explicitly track "set" rows by storing a marker).
    # Simpler approach: any row where any ticker is nonzero is a rebalance day; others ffill.
    nonzero_rows = (weights_panel != 0).any(axis=1)
    weights_panel = weights_panel.where(nonzero_rows, other=np.nan).ffill().fillna(0.0)

    # Vectorized NAV
    print(f"  simulating NAV over {len(trading_days)} days (vectorized)...")
    nav_df = _simulate_nav(weights_panel, panel.reindex(trading_days), initial_nav, COST_BPS)

    # Bulk write nav_daily
    nav_rows = []
    for ts, row in nav_df.iterrows():
        nav_rows.append((
            ts.date(),
            float(row["nav"]),
            float(row["daily_return"]),
            float(row["cumulative_return"]),
            float(row["gross_exposure"]),
            float(row["net_exposure"]),
            float(row["turnover"]),
            float(row["execution_cost_bps"]),
        ))
    n_nav = db.bulk_upsert(
        "portfolio.nav_daily",
        ["date", "nav", "daily_return", "cumulative_return",
         "gross_exposure", "net_exposure", "turnover", "execution_cost_bps"],
        nav_rows,
        conflict_cols=["date"],
    )
    print(f"  upserted {n_nav} rows into portfolio.nav_daily")

    # Analytics refresh
    print("  refreshing analytics.*...")
    perf = performance.run()
    risk = risk_engine.run()

    # Headline summary
    final_nav = float(nav_df["nav"].iloc[-1])
    total_ret = final_nav / initial_nav - 1.0
    eq = nav_df["nav"]
    peak = eq.cummax()
    mdd = float(((eq - peak) / peak).min())
    rets = nav_df["daily_return"].fillna(0.0)
    ann_vol = float(rets.std() * np.sqrt(252))
    ann_ret = float((1 + total_ret) ** (252 / max(1, len(rets))) - 1)
    sharpe = float(ann_ret / ann_vol) if ann_vol > 0 else 0.0

    # Persist a row in `backtests` so the Next.js /backtest pages can list it.
    summary_rows = db.query(
        "SELECT * FROM analytics.performance_summary WHERE period = 'all' LIMIT 1;"
    )
    if summary_rows:
        s = summary_rows[0]
        results_payload = {
            k: (float(s[k]) if s.get(k) is not None else None)
            for k in (
                "total_return", "ann_return", "ann_vol", "sharpe", "sortino",
                "max_drawdown", "calmar", "hit_rate", "beta", "alpha",
            )
        }
    else:
        results_payload = {
            "total_return": total_ret, "ann_return": ann_ret, "ann_vol": ann_vol,
            "sharpe": sharpe, "sortino": 0.0, "max_drawdown": mdd,
            "calmar": 0.0, "hit_rate": 0.0, "beta": 0.0, "alpha": 0.0,
        }
    equity_payload = [
        {
            "date": ts.date().isoformat(),
            "nav": float(nav_val),
            "ret": (None if pd.isna(ret_val) else float(ret_val)),
        }
        for ts, nav_val, ret_val in zip(
            nav_df.index, nav_df["nav"], nav_df["daily_return"]
        )
    ]
    config_payload = {
        "rebalance_days": rebalance_days,
        "initial_nav": initial_nav,
        "cost_bps": COST_BPS,
        "history_buffer_days": history_buffer_days,
    }
    db.execute(
        """
        INSERT INTO backtests
          (name, strategy_config, start_date, end_date, results, equity_curve, status)
        VALUES (%s, %s::jsonb, %s, %s, %s::jsonb, %s::jsonb, 'completed');
        """,
        (
            f"run_{dt.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}",
            json.dumps(config_payload),
            start_date, end_date,
            json.dumps(results_payload, default=float),
            json.dumps(equity_payload, default=float),
        ),
    )

    print()
    print(f"Backtest complete.  final NAV ${final_nav:,.0f}  total {total_ret:+.2%}")
    print(f"  ann_return = {ann_ret:+.2%}")
    print(f"  ann_vol    = {ann_vol:.2%}")
    print(f"  sharpe     = {sharpe:.2f}")
    print(f"  max_dd     = {mdd:.2%}")

    return {
        "trading_days": len(trading_days),
        "rebalances": len(rebalance_dates),
        "final_nav": final_nav,
        "total_return": total_ret,
        "ann_return": ann_ret,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": mdd,
        "performance": perf,
        "risk": risk,
    }


# --- CLI --------------------------------------------------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default=None, help="YYYY-MM-DD")
    ap.add_argument("--end", type=str, default=None, help="YYYY-MM-DD")
    ap.add_argument("--rebalance", type=int, default=DEFAULT_REBALANCE_DAYS,
                    help="Trading days between rebalances (default 21 = monthly)")
    ap.add_argument("--initial-nav", type=float, default=None)
    ap.add_argument("--no-reset", action="store_true",
                    help="Don't clear existing derived / portfolio / analytics rows")
    args = ap.parse_args()

    bounds = db.query("SELECT MIN(date) AS s, MAX(date) AS e FROM raw.ohlcv_daily;")
    if not bounds or not bounds[0]["s"]:
        raise SystemExit("raw.ohlcv_daily is empty — fetch prices first.")
    default_start = bounds[0]["s"]
    default_end = bounds[0]["e"]

    s = dt.date.fromisoformat(args.start) if args.start else default_start
    e = dt.date.fromisoformat(args.end) if args.end else default_end
    run(
        s, e,
        rebalance_days=args.rebalance,
        initial_nav=args.initial_nav,
        reset=not args.no_reset,
    )
