"""QP ensemble portfolio construction — sits in front of the overlay pipeline.

Replaces `pre * vol_scalar` in portfolio_constructor with a proper
mean-variance solve, ensembled across a grid of risk-aversion and
turnover-penalty parameters, with sector- and beta-neutral projection.

Inputs (all in-memory, no DB writes):
  alpha       : pd.Series of blended_signal indexed by ticker (the "view")
  panel       : pd.DataFrame of adj_close, dates × tickers (for cov & beta)
  sectors     : pd.Series sector by ticker
  prev_w      : pd.Series previous day's target_weight (for turnover penalty)
  benchmark   : pd.Series of market returns (for beta neutrality), optional

Output:
  pd.Series of weights — *unconstrained by name/sector/gross*. Feed into
  portfolio_constructor.apply_overlays to enforce caps.

Math summary per ensemble member, given shrunk covariance Σ̂:
  w* = (Σ̂ + λ_t · I)^-1 · (α + λ_t · w_prev) / λ_r
  then project onto {Aw = 0} for sector / beta neutrality:
  w  = w* - A^T (A A^T)^-1 A w*

The ensemble averages w across all members. Optionally weight members by
their trailing walk-forward Sharpe (stub provided).

CPU budget: ~200 members × <1s/member × ~250 walk-forward steps =
hours of compute, all numpy BLAS. RAM peak: one (1000×1000) cov + one
(k×k) projection inverse where k = #sectors+1.
"""
from __future__ import annotations

import datetime as dt
import itertools
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# 1. Ledoit-Wolf shrinkage toward constant-correlation target.
#    Closed form, no sklearn dependency.
# ---------------------------------------------------------------------------

def shrunk_covariance(returns: np.ndarray) -> np.ndarray:
    """Ledoit-Wolf shrinkage toward the constant-correlation target.

    returns: (T, N) ndarray of demeaned-or-not daily returns. We demean here.
    """
    T, N = returns.shape
    X = returns - returns.mean(axis=0, keepdims=True)
    S = (X.T @ X) / T                          # sample cov

    # constant-correlation target F
    var = np.diag(S)
    std = np.sqrt(np.maximum(var, 1e-12))
    corr = S / np.outer(std, std)
    iu = np.triu_indices(N, k=1)
    r_bar = corr[iu].mean()
    F = r_bar * np.outer(std, std)
    np.fill_diagonal(F, var)

    # optimal shrinkage intensity (Ledoit-Wolf 2004, eq 14)
    X2 = X ** 2
    pi_mat = (X2.T @ X2) / T - S ** 2
    pi_hat = pi_mat.sum()

    # rho: diagonal terms + off-diagonal asymptotic covariance terms
    rho_diag = np.diag(pi_mat).sum()
    term = ((X ** 3).T @ X) / T - var[:, None] * S
    rho_off = (r_bar * (np.outer(1.0 / std, std) * term
                        + np.outer(std, 1.0 / std) * term.T) / 2)
    np.fill_diagonal(rho_off, 0.0)
    rho_hat = rho_diag + rho_off.sum()

    gamma_hat = ((F - S) ** 2).sum()
    kappa = (pi_hat - rho_hat) / max(gamma_hat, 1e-12)
    delta = float(np.clip(kappa / T, 0.0, 1.0))
    return delta * F + (1.0 - delta) * S


# ---------------------------------------------------------------------------
# 2. Closed-form L2-turnover-penalized mean-variance solve.
# ---------------------------------------------------------------------------

def mvo_l2_turnover(
    alpha: np.ndarray,
    sigma: np.ndarray,
    w_prev: np.ndarray,
    risk_aversion: float,
    turnover_penalty: float,
) -> np.ndarray:
    """Solve  min  ½ w'Σw·λ_r − α'w + ½ λ_t ||w − w_prev||²

    Closed-form: w = (λ_r·Σ + λ_t·I)^-1 (α + λ_t·w_prev)
    """
    N = sigma.shape[0]
    A = risk_aversion * sigma + turnover_penalty * np.eye(N)
    b = alpha + turnover_penalty * w_prev
    return np.linalg.solve(A, b)


# ---------------------------------------------------------------------------
# 3. Project weights onto linear neutrality constraints A w = 0.
#    Used for sector neutrality and beta neutrality.
# ---------------------------------------------------------------------------

def project_to_neutral(w: np.ndarray, A: np.ndarray) -> np.ndarray:
    """w_proj = w − A^T (A A^T)^-1 A w.  A is (k × N), assumed full row rank."""
    if A.size == 0:
        return w
    M = A @ A.T
    return w - A.T @ np.linalg.solve(M, A @ w)


def _build_sector_matrix(tickers: pd.Index, sectors: pd.Series) -> np.ndarray:
    """One row per sector — each row sums weights of names in that sector."""
    sec = sectors.reindex(tickers).fillna("?")
    uniq = sorted(sec.unique())
    rows = [(sec.values == s).astype(float) for s in uniq]
    return np.vstack(rows) if rows else np.zeros((0, len(tickers)))


def _beta_vector(panel: pd.DataFrame, benchmark: pd.Series, lookback: int) -> pd.Series:
    """Per-ticker beta to benchmark over last `lookback` days."""
    rets = panel.pct_change().iloc[-lookback:]
    bmk = benchmark.reindex(rets.index).pct_change().fillna(0.0)
    bvar = float(bmk.var())
    if bvar <= 0:
        return pd.Series(0.0, index=panel.columns)
    cov = rets.apply(lambda c: c.cov(bmk))
    return (cov / bvar).fillna(0.0)


# ---------------------------------------------------------------------------
# 4. Ensemble loop. CPU budget lives here.
# ---------------------------------------------------------------------------

# Reasonable defaults — tune empirically with walk-forward.
RISK_AVERSION_GRID = [1.0, 3.0, 10.0, 30.0]
TURNOVER_GRID      = [0.0, 0.5, 2.0, 8.0]
COV_LOOKBACKS      = [60, 126, 252]                 # 3m, 6m, 12m
NEUTRALITY_MODES   = ["none", "sector", "sector+beta"]


def solve_ensemble(
    alpha: pd.Series,
    panel: pd.DataFrame,
    sectors: pd.Series,
    prev_w: pd.Series,
    benchmark: pd.Series | None = None,
    *,
    risk_grid: list[float] = RISK_AVERSION_GRID,
    turnover_grid: list[float] = TURNOVER_GRID,
    cov_lookbacks: list[int] = COV_LOOKBACKS,
    neutrality_modes: list[str] = NEUTRALITY_MODES,
    member_weights: pd.Series | None = None,
) -> pd.Series:
    """Run the ensemble and return the *averaged* pre-overlay weight vector."""

    # 1. Align inputs to a common ticker set.
    common = alpha.index.intersection(panel.columns)
    if len(common) == 0:
        return pd.Series(dtype=float)
    tickers = pd.Index(sorted(common))
    a = alpha.reindex(tickers).fillna(0.0).values.astype(float)
    prev = prev_w.reindex(tickers).fillna(0.0).values.astype(float)
    px = panel[tickers]

    # 2. Pre-compute covariance for each lookback (most expensive step).
    cov_cache: dict[int, np.ndarray] = {}
    for L in cov_lookbacks:
        rets = px.pct_change().iloc[-L:].dropna(how="all").fillna(0.0)
        if rets.shape[0] < 20:
            continue
        cov_cache[L] = shrunk_covariance(rets.values) * 252.0  # annualized

    if not cov_cache:
        return pd.Series(0.0, index=tickers)

    # 3. Pre-compute neutrality matrices.
    A_sector = _build_sector_matrix(tickers, sectors)
    if benchmark is not None and len(benchmark) > 30:
        beta = _beta_vector(px, benchmark, max(cov_lookbacks)).reindex(tickers).fillna(0.0).values
        A_beta = beta.reshape(1, -1)
    else:
        A_beta = np.zeros((0, len(tickers)))

    A_by_mode = {
        "none":         np.zeros((0, len(tickers))),
        "sector":       A_sector,
        "sector+beta":  np.vstack([A_sector, A_beta]) if A_beta.size else A_sector,
    }

    # 4. Loop. Each iteration is a matrix solve + a small projection.
    members: list[tuple[str, np.ndarray]] = []
    for L, lr, lt, mode in itertools.product(
        cov_cache.keys(), risk_grid, turnover_grid, neutrality_modes,
    ):
        sigma = cov_cache[L]
        w = mvo_l2_turnover(a, sigma, prev, lr, lt)
        w = project_to_neutral(w, A_by_mode[mode])
        key = f"L{L}_r{lr}_t{lt}_{mode}"
        members.append((key, w))

    # 5. Combine. Equal-weight unless member_weights tells us otherwise.
    keys = [k for k, _ in members]
    W = np.vstack([w for _, w in members])
    if member_weights is None:
        coef = np.ones(len(members)) / len(members)
    else:
        s = member_weights.reindex(keys).fillna(0.0).values
        coef = s / max(s.sum(), 1e-12)

    avg = coef @ W
    return pd.Series(avg, index=tickers).loc[lambda s: s.abs() > 1e-8]


# ---------------------------------------------------------------------------
# 5. Walk-forward scoring stub — soaks remaining CPU, persists tiny JSON.
#    Run once per day; feed `member_weights` back into solve_ensemble next run.
# ---------------------------------------------------------------------------

def _enumerate_members(
    risk_grid: list[float],
    turnover_grid: list[float],
    cov_lookbacks: list[int],
    neutrality_modes: list[str],
) -> list[tuple[str, int, float, float, str]]:
    """Same iteration order as solve_ensemble — keep these in sync so a
    `member_weights` Series produced by scoring is consumable downstream."""
    out = []
    for L, lr, lt, mode in itertools.product(
        cov_lookbacks, risk_grid, turnover_grid, neutrality_modes,
    ):
        key = f"L{L}_r{lr}_t{lt}_{mode}"
        out.append((key, L, lr, lt, mode))
    return out


def score_members_walk_forward(
    alpha_history: pd.DataFrame,   # date × ticker (signals_daily)
    panel: pd.DataFrame,           # date × ticker adj_close
    sectors: pd.Series,            # ticker → sector (already filtered)
    benchmark: pd.Series | None = None,
    *,
    risk_grid: list[float] = RISK_AVERSION_GRID,
    turnover_grid: list[float] = TURNOVER_GRID,
    cov_lookbacks: list[int] = COV_LOOKBACKS,
    neutrality_modes: list[str] = NEUTRALITY_MODES,
    n_steps: int = 60,
    n_warmup: int = 10,
    cost_bps: float = 10.0,
    deflate: bool = True,
) -> pd.DataFrame:
    """Sequential walk-forward backtest scoring each ensemble member in
    isolation. Returns a DataFrame indexed by member_key with columns:
      sharpe_raw  : annualized Sharpe over the eval window
      sharpe      : deflated Sharpe (clipped to ≥0; suitable as a combiner
                    coefficient passed back into solve_ensemble)
      n_obs       : eval steps used (= n_steps when data is complete)

    Memory: holds (cov_cache : L×N²) + (prev_w_by_member : M×N) at peak.
    For N=1000, M≈144, L=3 that is ~30MB. No member×date tensor is ever
    materialized — PnL is accumulated incrementally as scalars per member.

    Costs: linear, charged at rebalance against |Δw|. Warmup steps update
    prev_w but do not contribute to PnL (kills the cold-start turnover spike).
    """
    # 1. Common tickers + date alignment.
    tickers = pd.Index(sorted(
        set(alpha_history.columns) & set(panel.columns) & set(sectors.index)
    ))
    if len(tickers) == 0 or alpha_history.empty:
        return pd.DataFrame(columns=["sharpe_raw", "sharpe", "n_obs"])

    alpha_h = alpha_history[tickers].sort_index()
    px = panel[tickers].sort_index()
    daily_ret = px.pct_change().fillna(0.0)

    # Use only dates present in both, and where a next-day return exists.
    common_dates = alpha_h.index.intersection(daily_ret.index)
    if len(common_dates) < 2:
        return pd.DataFrame(columns=["sharpe_raw", "sharpe", "n_obs"])
    eval_dates = common_dates[:-1]                     # need r_{d+1}
    eval_dates = eval_dates[-(n_steps + n_warmup):]
    if len(eval_dates) == 0:
        return pd.DataFrame(columns=["sharpe_raw", "sharpe", "n_obs"])

    members = _enumerate_members(risk_grid, turnover_grid, cov_lookbacks, neutrality_modes)
    M, N = len(members), len(tickers)

    # 2. Pre-build the (mostly-static) sector matrix.  Beta gets recomputed
    #    per step from the trailing window — cheap (O(N·T)) and avoids
    #    using end-of-window info for early steps.
    A_sector = _build_sector_matrix(tickers, sectors)
    has_beta = benchmark is not None and len(benchmark) > 30

    # 3. Per-member state. Start each member from a zero book; warmup absorbs
    #    the first-rebalance turnover so it doesn't poison the Sharpe.
    prev_w_by_member: dict[str, np.ndarray] = {k: np.zeros(N) for k, *_ in members}
    pnl_by_member:    dict[str, list[float]] = {k: [] for k, *_ in members}

    cost_per_unit = cost_bps / 1e4

    # 4. Walk forward.
    for i, d in enumerate(eval_dates):
        is_warmup = i < n_warmup

        # alpha at d (drop names with NaN signal today)
        a_row = alpha_h.loc[d].astype(float)
        if a_row.isna().all():
            continue
        a = a_row.reindex(tickers).fillna(0.0).values

        # Realized next-day return for PnL — find the next date in daily_ret.
        d_pos = daily_ret.index.get_indexer([d])[0]
        if d_pos < 0 or d_pos + 1 >= len(daily_ret.index):
            continue
        r_next = daily_ret.iloc[d_pos + 1].reindex(tickers).fillna(0.0).values

        # 4a. Shrunk covariance per lookback, on returns strictly up to d.
        cov_cache: dict[int, np.ndarray] = {}
        ret_window = daily_ret.iloc[max(0, d_pos - max(cov_lookbacks) - 5):d_pos + 1]
        for L in cov_lookbacks:
            sub = ret_window.iloc[-L:]
            if sub.shape[0] < 20:
                continue
            cov_cache[L] = shrunk_covariance(sub.values) * 252.0
        if not cov_cache:
            continue

        # 4b. Neutrality matrices for this step.
        if has_beta:
            beta = _beta_vector(
                px.iloc[max(0, d_pos - max(cov_lookbacks)):d_pos + 1],
                benchmark, max(cov_lookbacks),
            ).reindex(tickers).fillna(0.0).values
            A_beta = beta.reshape(1, -1)
        else:
            A_beta = np.zeros((0, N))
        A_by_mode = {
            "none":         np.zeros((0, N)),
            "sector":       A_sector,
            "sector+beta":  np.vstack([A_sector, A_beta]) if A_beta.size else A_sector,
        }

        # 4c. Per-member solve.  cov is shared across (lr, lt, mode) at this
        #     lookback; the linear solve is the only per-member cost.
        for key, L, lr, lt, mode in members:
            if L not in cov_cache:
                continue
            prev = prev_w_by_member[key]
            w = mvo_l2_turnover(a, cov_cache[L], prev, lr, lt)
            w = project_to_neutral(w, A_by_mode[mode])

            if not is_warmup:
                turnover = float(np.abs(w - prev).sum())
                pnl = float(w @ r_next) - cost_per_unit * turnover
                pnl_by_member[key].append(pnl)

            prev_w_by_member[key] = w

        # Free the per-step cov immediately — covers RAM-safety even if GC
        # would do the same.
        del cov_cache

    # 5. Sharpe per member.
    rows = []
    M_eff = sum(1 for k in pnl_by_member if len(pnl_by_member[k]) > 1)
    for key, *_ in members:
        s = np.asarray(pnl_by_member[key], dtype=float)
        if s.size < 2 or s.std(ddof=1) == 0:
            rows.append((key, 0.0, 0.0, int(s.size)))
            continue
        sr = float(s.mean() / s.std(ddof=1) * np.sqrt(252))
        if deflate and M_eff > 1:
            # Bonferroni-flavored multiple-testing penalty.  CPCV-deflated
            # Sharpe is the principled upgrade; this is the right shape and
            # stays dependency-light.  M_eff ≈ number of trials; s.size ≈ T.
            penalty = float(np.sqrt(2.0 * np.log(M_eff) / s.size))
            sr_def = sr - penalty
        else:
            sr_def = sr
        rows.append((key, sr, max(sr_def, 0.0), int(s.size)))

    return (
        pd.DataFrame(rows, columns=["member_key", "sharpe_raw", "sharpe", "n_obs"])
          .set_index("member_key")
          .sort_values("sharpe", ascending=False)
    )
