"""
schema_validator.py – Data quality validation for Bronze layer records.
Implements: null checks, type checks, PK uniqueness, range checks, duplicate detection.
Results are persisted to metadata.dq_results.
"""
from typing import Optional

import pandas as pd

from src.utils.logger import get_logger
from src.metadata.metadata_tracker import log_dq_result

logger = get_logger("schema_validator")


class DataValidator:
    """
    Run a suite of DQ checks on a DataFrame.

    Usage:
        v = DataValidator(df, "bronze.customers", batch_id)
        v.check_nulls(["customer_id", "state"])
        v.check_unique(["customer_id"])
        v.check_no_duplicates(["customer_id"])
        ok, rejected = v.finalize()
    """

    def __init__(self, df: pd.DataFrame, table_name: str, batch_id: str, engine=None):
        self.df          = df.copy()
        self.table_name  = table_name
        self.batch_id    = batch_id
        self.engine      = engine
        self._reject_mask = pd.Series([False] * len(df), index=df.index)
        self._reject_reasons: pd.Series = pd.Series([""] * len(df), index=df.index)
        self.passed_checks = 0
        self.failed_checks = 0

    # ─── Null Check ──────────────────────────────────────────
    def check_nulls(self, columns: list, severity: str = "ERROR") -> "DataValidator":
        for col in columns:
            if col not in self.df.columns:
                logger.warning(f"  [DQ] Column '{col}' not in DataFrame – skipping null check")
                continue
            null_mask = self.df[col].isnull() | (self.df[col].astype(str).str.strip() == "")
            null_count = null_mask.sum()
            passed = null_count == 0

            self._record_check(
                check_name=f"null_check_{col}",
                check_type="null_check",
                column_name=col,
                passed=passed,
                failed_count=int(null_count),
                observed_value=f"{null_count} nulls",
                expected_value="0 nulls",
                severity=severity,
            )

            if not passed and severity == "ERROR":
                self._reject_mask |= null_mask
                self._reject_reasons[null_mask] += f"NULL in {col}; "

        return self

    # ─── Uniqueness Check ────────────────────────────────────
    def check_unique(self, columns: list, severity: str = "ERROR") -> "DataValidator":
        dup_mask = self.df.duplicated(subset=columns, keep="first")
        dup_count = dup_mask.sum()
        passed = dup_count == 0
        col_label = ", ".join(columns)

        self._record_check(
            check_name=f"unique_check_{col_label}",
            check_type="pk_check",
            column_name=col_label,
            passed=passed,
            failed_count=int(dup_count),
            observed_value=f"{dup_count} duplicates",
            expected_value="0 duplicates",
            severity=severity,
        )

        if not passed and severity == "ERROR":
            self._reject_mask |= dup_mask
            self._reject_reasons[dup_mask] += f"Duplicate on {col_label}; "

        return self

    # ─── Data Type Check ─────────────────────────────────────
    def check_numeric(self, columns: list, severity: str = "WARNING") -> "DataValidator":
        for col in columns:
            if col not in self.df.columns:
                continue
            numeric_mask = pd.to_numeric(self.df[col], errors="coerce").isnull() & self.df[col].notnull()
            bad_count = numeric_mask.sum()
            passed = bad_count == 0

            self._record_check(
                check_name=f"type_check_{col}",
                check_type="type_check",
                column_name=col,
                passed=passed,
                failed_count=int(bad_count),
                observed_value=f"{bad_count} non-numeric values",
                expected_value="all numeric",
                severity=severity,
            )

        return self

    # ─── Range Check ─────────────────────────────────────────
    def check_value_in_set(self, column: str, valid_values: list, severity: str = "WARNING") -> "DataValidator":
        if column not in self.df.columns:
            return self
        invalid_mask = ~self.df[column].isin(valid_values) & self.df[column].notnull()
        bad_count = invalid_mask.sum()
        passed = bad_count == 0

        self._record_check(
            check_name=f"domain_check_{column}",
            check_type="domain_check",
            column_name=column,
            passed=passed,
            failed_count=int(bad_count),
            observed_value=f"{bad_count} out-of-domain values",
            expected_value=f"values in {valid_values[:5]}...",
            severity=severity,
        )

        return self

    # ─── Row Count Check ─────────────────────────────────────
    def check_min_rows(self, min_rows: int, severity: str = "ERROR") -> "DataValidator":
        actual = len(self.df)
        passed = actual >= min_rows

        self._record_check(
            check_name="min_row_count",
            check_type="count_check",
            column_name="*",
            passed=passed,
            failed_count=0 if passed else 1,
            observed_value=str(actual),
            expected_value=f">= {min_rows}",
            severity=severity,
        )

        return self

    # ─── Internal helpers ────────────────────────────────────
    def _record_check(self, check_name, check_type, column_name,
                      passed, failed_count, observed_value, expected_value, severity):
        icon = "✔" if passed else "✗"
        level = logger.info if passed else logger.warning
        level(f"  [{icon}] {check_name:<40} failed={failed_count}")

        if passed:
            self.passed_checks += 1
        else:
            self.failed_checks += 1

        try:
            log_dq_result(
                batch_id=self.batch_id,
                table_name=self.table_name,
                check_name=check_name,
                check_type=check_type,
                column_name=column_name,
                passed=passed,
                total_records=len(self.df),
                failed_records=failed_count,
                observed_value=observed_value,
                expected_value=expected_value,
                severity=severity,
                engine=self.engine,
            )
        except Exception as e:
            logger.debug(f"DQ logging failed (non-critical): {e}")

    def finalize(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split DataFrame into valid and rejected records.
        Returns:
            (valid_df, rejected_df)
        """
        valid    = self.df[~self._reject_mask].copy()
        rejected = self.df[self._reject_mask].copy()

        if not rejected.empty:
            rejected["reject_reason"] = self._reject_reasons[self._reject_mask].values

        logger.info(
            f"  DQ Summary | passed_checks={self.passed_checks} | "
            f"failed_checks={self.failed_checks} | "
            f"valid_rows={len(valid)} | rejected_rows={len(rejected)}"
        )
        return valid, rejected
