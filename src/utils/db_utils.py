"""
db_utils.py – Database connection and helper utilities for DE PoC.
Compatible with SQLAlchemy 1.4.x (required by Airflow 2.8.1).
"""
import os
from contextlib import contextmanager
from typing import Optional, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.utils.logger import get_logger

logger = get_logger("db_utils")


def get_connection_string() -> str:
    """Build PostgreSQL connection string from environment variables."""
    host     = os.getenv("POSTGRES_HOST",     "localhost")
    port     = os.getenv("POSTGRES_PORT",     "5432")
    db       = os.getenv("POSTGRES_DB",       "de_poc")
    user     = os.getenv("POSTGRES_USER",     "de_user")
    password = os.getenv("POSTGRES_PASSWORD", "de_password123")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def get_engine(pool_size: int = 5) -> Engine:
    """Create and return a SQLAlchemy 1.4-compatible engine."""
    conn_str = get_connection_string()
    engine = create_engine(
        conn_str,
        pool_size=pool_size,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False,
        future=False,   # SQLAlchemy 1.4 legacy mode
    )
    logger.debug("Engine created for: %s", conn_str.replace(
        os.getenv("POSTGRES_PASSWORD", "de_password123"), "***"
    ))
    return engine


@contextmanager
def get_connection(engine: Optional[Engine] = None):
    """Context manager that yields a DB connection."""
    _engine = engine or get_engine()
    with _engine.connect() as conn:
        yield conn


def table_exists(table: str, schema: str, engine: Optional[Engine] = None) -> bool:
    """Check if a table exists in the given schema."""
    _engine = engine or get_engine()
    with _engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_name = :table)"
            ),
            {"schema": schema, "table": table},
        )
        return bool(result.scalar())


def execute_sql(sql: str, engine: Optional[Engine] = None, params: dict = None) -> None:
    """Execute a SQL statement (DDL or DML)."""
    _engine = engine or get_engine()
    with _engine.begin() as conn:
        conn.execute(text(sql), params or {})


def read_sql(query: str, engine: Optional[Engine] = None, params: dict = None) -> pd.DataFrame:
    """Execute a SELECT and return a DataFrame (SQLAlchemy 1.4 compatible)."""
    _engine = engine or get_engine()
    with _engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        rows   = result.fetchall()
        cols   = result.keys()
        return pd.DataFrame(rows, columns=list(cols))


def upsert_dataframe(
    df: pd.DataFrame,
    table: str,
    schema: str,
    conflict_columns: List[str],
    update_columns: List[str],
    engine: Optional[Engine] = None,
) -> int:
    """INSERT … ON CONFLICT DO UPDATE (upsert) for a DataFrame."""
    if df.empty:
        return 0

    _engine      = engine or get_engine()
    cols         = list(df.columns)
    col_names    = ", ".join(cols)
    placeholders = ", ".join([f":{c}" for c in cols])
    conflict_str = ", ".join(conflict_columns)
    update_str   = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_columns])

    sql = (
        f"INSERT INTO {schema}.{table} ({col_names}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_str}) "
        f"DO UPDATE SET {update_str}, updated_at = NOW()"
    )
    with _engine.begin() as conn:
        conn.execute(text(sql), df.to_dict(orient="records"))

    logger.debug("Upserted %d rows into %s.%s", len(df), schema, table)
    return len(df)


def bulk_insert(
    df: pd.DataFrame,
    table: str,
    schema: str,
    engine: Optional[Engine] = None,
    if_exists: str = "append",
) -> int:
    """Bulk insert a DataFrame into a table using pandas to_sql."""
    if df.empty:
        return 0
    _engine = engine or get_engine()
    df.to_sql(
        table, _engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=500,
    )
    logger.debug("Inserted %d rows into %s.%s", len(df), schema, table)
    return len(df)
