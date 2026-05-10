# Localware Capital — Automated Hedge Fund

A fully self-contained, automated, multi-factor systematic equity portfolio.

- **Postgres** owns every fact: prices, signals, positions, trades, NAV, risk metrics.
- **Python workers** are pure compute: fetch market data (yfinance), run strategies, simulate execution, compute risk and performance, and write to Postgres.
- **Next.js** is read-only: it serves both the API routes and the research-paper-styled UI.
- **No broker, no third-party trading API.** All execution is simulated against historical close prices with explicit slippage, commission, and impact models.

## Stack

```
Python 3.12+        →  pandas, numpy, scipy, statsmodels, yfinance,
                       psycopg, APScheduler
Postgres 16+        →  single source of truth
Next.js 15          →  TypeScript, Tailwind, Recharts, KaTeX
                       Drizzle-style raw SQL via postgres.js
```

## Strategy stack (designed for ~18% return, Sharpe > 1.5, DD < 12%)

| Sleeve              | Allocation | Logic                                                           |
| ------------------- | ---------- | --------------------------------------------------------------- |
| Cross-sectional momentum | 40%   | 12-1 month return, top 30 long / bottom 30 short, monthly       |
| Quality factor      | 25%        | High ROE, low D/E, positive EPS growth — long-only top 30       |
| Low volatility      | 20%        | Bottom-quintile 60-day realized vol, equal weight               |
| Mean reversion      | 15%        | RSI<30 above 200-DMA, 5-day hold                                |

**Risk overlays:** 12% portfolio vol target, gross exposure halved when drawdown > 8%, 25% sector cap, 5% per-name cap, 1.5× max gross leverage.

## Layout

```
Localware/
├── docker-compose.yml           # optional (we use system Postgres by default)
├── .env                         # DATABASE_URL etc.
├── db/migrations/0001_init.sql  # full schema
├── workers/                     # python workers
│   ├── lib/{db.py, universe.py, mathx.py}
│   ├── fetch_prices.py
│   ├── fetch_fundamentals.py
│   ├── strategy_runner.py
│   ├── portfolio_constructor.py
│   ├── executor.py
│   ├── risk_engine.py
│   ├── performance.py
│   ├── backtest.py
│   ├── scheduler.py            # APScheduler glue
│   └── bootstrap.py            # one-shot init
└── frontend/                    # Next.js 15 app
    ├── app/                    # pages + API routes
    │   ├── api/                # /api/portfolio, /api/performance, ...
    │   ├── page.tsx            # Overview
    │   ├── performance/        # /performance
    │   ├── positions/          # /positions
    │   ├── strategies/         # /strategies
    │   ├── trades/             # /trades
    │   ├── risk/               # /risk
    │   ├── backtest/           # /backtest, /backtest/[id]
    │   └── research/methodology/
    ├── components/paper/       # Figure, PaperTable, Equation, Sidenote, KPI
    └── components/charts/      # AcademicLine, BarChart, Heatmap
```

## Setup

### 1. Postgres

If you already have Postgres running locally:

```bash
createdb localware_fund
psql -d localware_fund -f db/migrations/0001_init.sql
```

Or via Docker:

```bash
docker compose up -d
# (the migration auto-runs from /docker-entrypoint-initdb.d)
```

Edit `.env` so `DATABASE_URL` points at your DB. Default for local Postgres on macOS:

```
DATABASE_URL=postgresql://$USER@localhost:5432/localware_fund
```

### 2. Python workers

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r workers/requirements.txt
```

### 3. Bootstrap (load universe, 5y prices, fundamentals, run 4y backtest)

```bash
python -m workers.bootstrap
```

This will:

1. Insert ~90 securities into `securities` and `universe`.
2. Pull 5 years of daily OHLCV from yfinance (~2 minutes).
3. Pull current fundamentals snapshot (~10 minutes — yfinance.info is slow).
4. Run a 4-year walk-forward backtest, populating `portfolio_nav`, `positions`, `trades`, `signals`, `performance_metrics`, `risk_metrics`, and a row in `backtests`.

### 4. Frontend

```bash
cd frontend
npm install
npm run dev
```

Open <http://localhost:3000>.

### 5. Daily scheduler (optional)

```bash
source .venv/bin/activate
python -m workers.scheduler
```

This runs the daily pipeline (fetch_prices → strategy_runner → executor → risk_engine → performance) at 16:30 ET, Mon–Fri. Fundamentals refresh runs Sundays at 02:00.

## Running individual workers

```bash
python -m workers.fetch_prices                # incremental
python -m workers.fetch_prices --history 5    # 5 years of history
python -m workers.fetch_fundamentals
python -m workers.strategy_runner
python -m workers.executor
python -m workers.risk_engine
python -m workers.performance
python -m workers.backtest --start 2024-01-01 --end 2026-05-01 --name custom
```

## Pages

| Path                       | What it shows                                               |
| -------------------------- | ----------------------------------------------------------- |
| `/`                        | Overview: KPIs, NAV curve, top positions, recent trades     |
| `/performance`             | Equity curve, drawdown waterfall, monthly heatmap, rolling Sharpe |
| `/positions`               | Full position ledger, sector exposure                       |
| `/strategies`              | Per-sleeve descriptions, formulas, latest signals           |
| `/trades`                  | Trade ledger with filters                                   |
| `/risk`                    | VaR, ES, factor exposures (Fama-French 5-style)             |
| `/backtest`                | List of historical backtests                                |
| `/backtest/[id]`           | Individual backtest equity curve and stats                  |
| `/research/methodology`    | Full methodology paper                                      |

## API

All routes return JSON.

```
GET /api/portfolio         { nav, positions, sectors }
GET /api/performance       { equity, metrics, drawdown, monthly, rollingSharpe }
GET /api/positions         { positions, sectors }
GET /api/trades?limit=200  { trades }
GET /api/strategies        { strategies, contribution, signals }
GET /api/risk              { latest, history }
GET /api/backtest          { backtests }
GET /api/backtest?id=N     { backtest }
```

## Design

The UI is intentionally aesthetically aligned with academic finance journals: EB Garamond body type, Source Serif 4 headings, JetBrains Mono numerics, KaTeX equations, off-white paper background, numbered figures and tables, sidenotes. All numerics use tabular nums; all charts use thin 1px strokes with serif axis labels.

## License

Internal research code. Not investment advice. Targets are design goals; realised performance depends on the period and is not guaranteed.
# Localware
