"""Database access. One connection function. No ORM."""
from __future__ import annotations

import os
import re
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

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@contextmanager
def conn(autocommit: bool = False):
    """psycopg3 connection. Commit on success unless autocommit=True."""
    c = psycopg.connect(DATABASE_URL, autocommit=autocommit)
    try:
        yield c
        if not autocommit:
            c.commit()
    except Exception:
        if not autocommit:
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


# ---------------------------------------------------------------------------
# Bulk upsert helper
# ---------------------------------------------------------------------------

def _check_ident(*idents: str) -> None:
    for i in idents:
        # allow schema-qualified table names like "raw.ohlcv_daily"
        parts = i.split(".")
        for p in parts:
            if not _IDENT_RE.match(p):
                raise ValueError(f"Invalid SQL identifier: {i!r}")


def bulk_upsert(
    table: str,
    columns: list[str],
    rows: list[tuple],
    conflict_cols: list[str],
    update_cols: list[str] | None = None,
    page_size: int = 1000,
) -> int:
    """
    Bulk INSERT ... ON CONFLICT DO UPDATE.

    - `columns` defines the insert column order; `rows` must match.
    - `conflict_cols` is the unique constraint we're upserting against.
    - `update_cols` defaults to (columns - conflict_cols); pass [] to DO NOTHING.
    - Commits every `page_size` rows so a mid-batch failure keeps partial progress.

    Returns the number of rows submitted (not necessarily the number changed).
    """
    if not rows:
        return 0
    _check_ident(table, *columns, *conflict_cols)
    if update_cols is None:
        update_cols = [c for c in columns if c not in conflict_cols]
    else:
        _check_ident(*update_cols)

    cols_sql = ", ".join(columns)
    placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
    conflict_sql = ", ".join(conflict_cols)
    if update_cols:
        update_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        on_conflict = f"DO UPDATE SET {update_sql}"
    else:
        on_conflict = "DO NOTHING"
    sql = (
        f"INSERT INTO {table} ({cols_sql}) VALUES {placeholders} "
        f"ON CONFLICT ({conflict_sql}) {on_conflict};"
    )

    submitted = 0
    with conn(autocommit=False) as c, c.cursor() as cur:
        for i in range(0, len(rows), page_size):
            chunk = rows[i:i + page_size]
            cur.executemany(sql, chunk)
            c.commit()
            submitted += len(chunk)
    return submitted


# ---------------------------------------------------------------------------
# Convenience: latest-date map for incremental fetches
# ---------------------------------------------------------------------------

def latest_dates(table: str, date_col: str = "date", id_col: str = "security_id"):
    """Returns {security_id: max(date_col)} for the given table."""
    _check_ident(table, date_col, id_col)
    sql = f"SELECT {id_col} AS sid, MAX({date_col}) AS d FROM {table} GROUP BY {id_col};"
    return {r["sid"]: r["d"] for r in query(sql)}
