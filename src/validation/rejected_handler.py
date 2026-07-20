"""
rejected_handler.py – Handle and persist rejected records from validation.
"""
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

from src.utils.db_utils import get_engine
from src.utils.logger import get_logger

logger = get_logger("rejected_handler")

REJECTED_PATH = Path(os.getenv("DATA_REJECTED_PATH", "data/rejected"))


def save_rejected_records(
    rejected_df: pd.DataFrame,
    source_table: str,
    batch_id: str,
    engine=None,
) -> int:
    """
    Persist rejected records to:
    1. bronze.rejected_records table
    2. CSV file in data/rejected/ for audit

    Returns: number of rejected records saved
    """
    if rejected_df.empty:
        return 0

    _engine = engine or get_engine()
    REJECTED_PATH.mkdir(parents=True, exist_ok=True)

    # 1. Save to DB
    rows_to_insert = []
    for _, row in rejected_df.iterrows():
        rows_to_insert.append({
            "source_table":  source_table,
            "batch_id":      batch_id,
            "raw_data":      row.to_json(),
            "reject_reason": row.get("reject_reason", "Unknown"),
        })

    sql = """
        INSERT INTO bronze.rejected_records (source_table, batch_id, raw_data, reject_reason)
        VALUES (:source_table, :batch_id, :raw_data, :reject_reason)
    """
    with _engine.begin() as conn:
        conn.execute(text(sql), rows_to_insert)

    # 2. Save to CSV file
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = REJECTED_PATH / f"{source_table}_{batch_id}_{ts}_rejected.csv"
    rejected_df.to_csv(csv_path, index=False, encoding="utf-8")

    logger.warning(
        f"  ⚠ {len(rejected_df)} rejected records | "
        f"table={source_table} | saved to {csv_path.name}"
    )
    return len(rejected_df)
