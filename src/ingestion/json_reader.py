"""
json_reader.py – Read JSON source files into a pandas DataFrame.
Supports nested JSON normalization.
"""
import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger("json_reader")


def read_json_source(
    filename: str,
    raw_path: Optional[str] = None,
    record_path: Optional[str] = None,
    normalize: bool = True,
    encoding: str = "utf-8",
) -> tuple[pd.DataFrame, str]:
    """
    Read a JSON file from the raw data path.
    Supports flat arrays and nested structures (via json_normalize).

    Returns:
        (DataFrame, source_file_path)
    """
    base_path = Path(raw_path or os.getenv("DATA_RAW_PATH", "data/raw"))
    file_path = base_path / filename

    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    logger.info(f"Reading JSON: {file_path}")

    with open(file_path, "r", encoding=encoding) as f:
        raw = json.load(f)

    if isinstance(raw, list):
        if normalize:
            df = pd.json_normalize(raw, record_path=record_path)
        else:
            df = pd.DataFrame(raw)
    elif isinstance(raw, dict):
        if record_path and record_path in raw:
            df = pd.json_normalize(raw[record_path])
        else:
            df = pd.json_normalize([raw])
    else:
        raise ValueError(f"Unsupported JSON structure in {filename}")

    # Convert all columns to string for bronze layer
    df = df.astype(str).replace("None", None).replace("nan", None)

    logger.info(f"  Loaded {len(df):,} rows from {filename}")
    return df, str(file_path)
