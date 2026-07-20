"""
postgres_reader.py – Read data from a PostgreSQL source table into a DataFrame.
Supports full load and incremental load via watermark column.
"""
import os
from typing import Optional

import pandas as pd
from sqlalchemy import text

from src.utils.db_utils import get_engine
from src.utils.logger import get_logger

logger = get_logger("postgres_reader")


def read_postgres_source(
    table: str,
    schema: str = "public",
    columns: Optional[list] = None,
    where_clause: Optional[str] = None,
    watermark_col: Optional[str] = None,
    watermark_value: Optional[str] = None,
    limit: Optional[int] = None,
    engine=None,
) -> pd.DataFrame:
    """
    Read a table (or filtered subset) from PostgreSQL into a DataFrame.

    Args:
        table:           Source table name
        schema:          Source schema
        columns:         List of columns to select (default: all)
        where_clause:    Optional additional WHERE filter
        watermark_col:   Column for incremental load
        watermark_value: Threshold for incremental load
        limit:           Row limit
        engine:          SQLAlchemy engine (optional)

    Returns:
        DataFrame with source data
    """
    _engine = engine or get_engine()
    col_expr = ", ".join(columns) if columns else "*"

    conditions = []
    if watermark_col and watermark_value:
        conditions.append(f"{watermark_col} > '{watermark_value}'")
    if where_clause:
        conditions.append(where_clause)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    limit_expr = f"LIMIT {limit}" if limit else ""

    query = f"SELECT {col_expr} FROM {schema}.{table} {where} ORDER BY 1 {limit_expr}"
    logger.info(f"Reading PostgreSQL: {schema}.{table} | {where or 'full load'}")

    with _engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    logger.info(f"  Loaded {len(df):,} rows from {schema}.{table}")
    return df
