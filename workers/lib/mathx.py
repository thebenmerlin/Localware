"""Numerical helpers — returns, vol, Sharpe, drawdown, RSI, etc."""
from __future__ import annotations

import numpy as np
import pandas as pd

TRADING_DAYS = 252


def returns(prices: pd.Series) -> pd.Series:
    return prices.pct_change()


def log_returns(prices: pd.Series) -> pd.Series:
    return np.log(prices / prices.shift(1))


def realized_vol(rets: pd.Series, annualize: bool = True) -> float:
    s = rets.dropna().std()
    if pd.isna(s):
        return 0.0
    return float(s * np.sqrt(TRADING_DAYS) if annualize else s)


def annualized_return(rets: pd.Series) -> float:
    r = rets.dropna()
    if len(r) == 0:
        return 0.0
    cum = (1 + r).prod()
    if cum <= 0:
        return -1.0
    return float(cum ** (TRADING_DAYS / len(r)) - 1)


def sharpe(rets: pd.Series, rf: float = 0.0) -> float:
    r = rets.dropna()
    if len(r) < 2:
        return 0.0
    excess = r - rf / TRADING_DAYS
    sd = excess.std()
    if sd == 0 or pd.isna(sd):
        return 0.0
    return float(excess.mean() / sd * np.sqrt(TRADING_DAYS))


def sortino(rets: pd.Series, rf: float = 0.0) -> float:
    r = rets.dropna()
    if len(r) < 2:
        return 0.0
    excess = r - rf / TRADING_DAYS
    downside = excess[excess < 0]
    sd = downside.std()
    if sd == 0 or pd.isna(sd) or len(downside) == 0:
        return 0.0
    return float(excess.mean() / sd * np.sqrt(TRADING_DAYS))


def max_drawdown(equity: pd.Series) -> float:
    """Returns drawdown as a negative number (e.g., -0.15 for 15% DD)."""
    e = equity.dropna()
    if len(e) == 0:
        return 0.0
    peaks = e.cummax()
    dd = (e - peaks) / peaks
    return float(dd.min())


def calmar(rets: pd.Series) -> float:
    if len(rets.dropna()) == 0:
        return 0.0
    eq = (1 + rets.fillna(0)).cumprod()
    mdd = abs(max_drawdown(eq))
    if mdd == 0:
        return 0.0
    return annualized_return(rets) / mdd


def hit_rate(rets: pd.Series) -> float:
    r = rets.dropna()
    if len(r) == 0:
        return 0.0
    return float((r > 0).mean())


def rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    delta = prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    out = 100 - 100 / (1 + rs)
    return out.fillna(50)


def beta_alpha(rets: pd.Series, mkt: pd.Series) -> tuple[float, float]:
    """OLS beta/alpha against a market series. Both daily."""
    df = pd.concat([rets, mkt], axis=1).dropna()
    if len(df) < 30:
        return 0.0, 0.0
    x = df.iloc[:, 1].values
    y = df.iloc[:, 0].values
    cov = np.cov(x, y)[0, 1]
    var = np.var(x)
    if var == 0:
        return 0.0, 0.0
    b = cov / var
    a = float(np.mean(y) - b * np.mean(x)) * TRADING_DAYS
    return float(b), a


def historical_var(rets: pd.Series, alpha: float = 0.05) -> float:
    r = rets.dropna()
    if len(r) < 30:
        return 0.0
    return float(np.percentile(r, alpha * 100))


def expected_shortfall(rets: pd.Series, alpha: float = 0.05) -> float:
    r = rets.dropna()
    if len(r) < 30:
        return 0.0
    cutoff = np.percentile(r, alpha * 100)
    tail = r[r <= cutoff]
    return float(tail.mean()) if len(tail) else 0.0


def zscore(s: pd.Series) -> pd.Series:
    sd = s.std()
    if sd == 0 or pd.isna(sd):
        return s * 0
    return (s - s.mean()) / sd


def winsorize(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    qlo, qhi = s.quantile(lo), s.quantile(hi)
    return s.clip(lower=qlo, upper=qhi)
