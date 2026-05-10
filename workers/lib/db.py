"""Database access. One connection function. No ORM."""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import psycopg
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    f"postgresql://{os.environ.get('USER', 'postgres')}@localhost:5432/localware_fund",
)


@contextmanager
def conn():
    """psycopg3 connection with autocommit off; commit on success."""
    c = psycopg.connect(DATABASE_URL)
    try:
        yield c
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()


def execute(sql: str, params: tuple | list | dict | None = None):
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, params)
        if cur.description:
            return cur.fetchall()
    return None


def executemany(sql: str, rows):
    with conn() as c, c.cursor() as cur:
        cur.executemany(sql, rows)


def query(sql: str, params: tuple | list | dict | None = None):
    """Return list of dict rows."""
    with conn() as c, c.cursor() as cur:
        cur.execute(sql, params)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
