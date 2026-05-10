"""APScheduler glue. Runs the daily pipeline at 16:30 ET.

  fetch_prices → strategy_runner → executor → risk_engine → performance
"""
from __future__ import annotations

import logging
import os
import time

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from . import fetch_prices, fetch_fundamentals, strategy_runner, executor, risk_engine, performance


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
log = logging.getLogger("localware.scheduler")


def daily_pipeline():
    log.info("=== daily pipeline start ===")
    try:
        fetch_prices.run()
    except Exception as e:
        log.error(f"fetch_prices failed: {e}")
    try:
        strategy_runner.run()
    except Exception as e:
        log.error(f"strategy_runner failed: {e}")
    try:
        executor.execute()
    except Exception as e:
        log.error(f"executor failed: {e}")
    try:
        risk_engine.run()
    except Exception as e:
        log.error(f"risk_engine failed: {e}")
    try:
        performance.run()
    except Exception as e:
        log.error(f"performance failed: {e}")
    log.info("=== daily pipeline complete ===")


def weekly_fundamentals():
    log.info("=== weekly fundamentals ===")
    try:
        fetch_fundamentals.run()
    except Exception as e:
        log.error(f"fetch_fundamentals failed: {e}")


def main():
    tz = os.environ.get("TZ", "America/New_York")
    sched = BlockingScheduler(timezone=tz)
    # Daily at 16:30 ET, Mon-Fri
    sched.add_job(
        daily_pipeline, CronTrigger(day_of_week="mon-fri", hour=16, minute=30),
        id="daily_pipeline",
    )
    # Weekly Sunday 02:00 ET
    sched.add_job(
        weekly_fundamentals, CronTrigger(day_of_week="sun", hour=2, minute=0),
        id="weekly_fundamentals",
    )
    log.info("Scheduler started. Daily pipeline at 16:30 ET, weekly fundamentals Sun 02:00.")
    sched.start()


if __name__ == "__main__":
    main()
