"""
xml_reader.py – Read XML source files into a pandas DataFrame.
Parses element-based XML (one child element = one record).
"""
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import xml.etree.ElementTree as ET

from src.utils.logger import get_logger

logger = get_logger("xml_reader")


def read_xml_source(
    filename: str,
    raw_path: Optional[str] = None,
    record_tag: Optional[str] = None,
    encoding: str = "utf-8",
) -> tuple[pd.DataFrame, str]:
    """
    Read an XML file from the raw data path.
    Each child element of the root (or matching record_tag) becomes a row.

    Returns:
        (DataFrame, source_file_path)
    """
    base_path = Path(raw_path or os.getenv("DATA_RAW_PATH", "data/raw"))
    file_path = base_path / filename

    if not file_path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    logger.info(f"Reading XML: {file_path}")

    tree = ET.parse(str(file_path))
    root = tree.getroot()

    records = []
    children = root.findall(record_tag) if record_tag else list(root)

    for child in children:
        row = {}
        for elem in child:
            row[elem.tag] = elem.text
        if row:
            records.append(row)

    if not records:
        logger.warning(f"No records found in {filename}")
        return pd.DataFrame(), str(file_path)

    df = pd.DataFrame(records).astype(str).replace("None", None)

    logger.info(f"  Loaded {len(df):,} rows from {filename}")
    return df, str(file_path)
