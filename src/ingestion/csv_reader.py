"""
csv_reader.py – Read CSV source files into a pandas DataFrame.
Supports full load and incremental load (based on watermark).
"""
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("csv_reader")


def read_csv_source(
    filename: str,
    raw_path: Optional[str] = None,
    dtype: Optional[dict] = None,
    encoding: str = "utf-8",
    incremental: bool = False,
    watermark_col: Optional[str] = None,
    watermark_value: Optional[str] = None,
) -> tuple[pd.DataFrame, str]:
    """
    Read a CSV file from the raw data path.

    Returns:
        (DataFrame, source_file_path)
    """
    base_path = Path(raw_path or os.getenv("DATA_RAW_PATH", "data/raw"))
    file_path = base_path / filename

    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    logger.info(f"Reading CSV: {file_path}")
    df = pd.read_csv(
        file_path,
        dtype=dtype or str,   # Read all as string for bronze (no type casting)
        encoding=encoding,
        low_memory=False,
    )

    logger.info(f"  Loaded {len(df):,} rows from {filename}")

    # Incremental filter
    if incremental and watermark_col and watermark_value and watermark_col in df.columns:
        df[watermark_col] = pd.to_datetime(df[watermark_col], errors="coerce")
        wm = pd.to_datetime(watermark_value)
        before = len(df)
        df = df[df[watermark_col] > wm]
        logger.info(f"  Incremental filter: {before:,} → {len(df):,} rows (watermark={watermark_value})")

    return df, str(file_path)
