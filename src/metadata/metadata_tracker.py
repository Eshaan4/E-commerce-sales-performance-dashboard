"""
metadata_tracker.py – Pipeline run metadata, audit, and watermark management.
"""
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import text

from src.utils.db_utils import get_engine
from src.utils.logger import get_logger

logger = get_logger("metadata_tracker")


def generate_batch_id(pipeline_name: str) -> str:
    """Generate a unique batch ID for a pipeline run."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    uid = str(uuid.uuid4())[:8].upper()
    return f"{pipeline_name.upper()}_{ts}_{uid}"


class PipelineRun:
    """
    Context manager that tracks a pipeline run in metadata.pipeline_runs.

    Usage:
        with PipelineRun("bronze_customers", layer="bronze", ...) as run:
            # do work
            run.records_inserted = 500
        # On exit, status is set to SUCCESS/FAILED automatically
    """

    def __init__(
        self,
        pipeline_name: str,
        layer: str = "bronze",
        source_system: str = "",
        source_table: str = "",
        target_table: str = "",
        run_mode: str = "FULL",
        triggered_by: str = "manual",
    ):
        self.pipeline_name  = pipeline_name
        self.layer          = layer
        self.source_system  = source_system
        self.source_table   = source_table
        self.target_table   = target_table
        self.run_mode       = run_mode
        self.triggered_by   = triggered_by
        self.batch_id       = generate_batch_id(pipeline_name)
        self.start_time     = datetime.now()
        self.end_time       = None
        self.status         = "RUNNING"

        self.total_records_read = 0
        self.records_inserted   = 0
        self.records_updated    = 0
        self.records_rejected   = 0
        self.records_skipped    = 0
        self.error_message      = None

        self._engine = get_engine()
        self._run_id = None

    def __enter__(self):
        self._insert_run()
        logger.info(f"▶ Pipeline started  | batch_id={self.batch_id}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()

        if exc_type is not None:
            self.status        = "FAILED"
            self.error_message = str(exc_val)
            log_error(self.batch_id, self.pipeline_name, type(exc_val).__name__, str(exc_val), self._engine)
            logger.error(f"✗ Pipeline FAILED   | batch_id={self.batch_id} | error={exc_val}")
        else:
            self.status = "SUCCESS" if self.records_rejected == 0 else "PARTIAL"
            logger.info(
                f"✔ Pipeline {self.status:<8} | batch_id={self.batch_id} "
                f"| inserted={self.records_inserted} | updated={self.records_updated} "
                f"| rejected={self.records_rejected} | duration={duration:.1f}s"
            )

        self._update_run(duration)
        return False   # Don't suppress exceptions

    def _insert_run(self):
        sql = """
            INSERT INTO metadata.pipeline_runs
                (batch_id, pipeline_name, layer, source_system, source_table,
                 target_table, status, start_time, run_mode, triggered_by)
            VALUES
                (:batch_id, :pipeline_name, :layer, :source_system, :source_table,
                 :target_table, :status, :start_time, :run_mode, :triggered_by)
            RETURNING run_id
        """
        with self._engine.begin() as conn:
            result = conn.execute(text(sql), {
                "batch_id":      self.batch_id,
                "pipeline_name": self.pipeline_name,
                "layer":         self.layer,
                "source_system": self.source_system,
                "source_table":  self.source_table,
                "target_table":  self.target_table,
                "status":        self.status,
                "start_time":    self.start_time,
                "run_mode":      self.run_mode,
                "triggered_by":  self.triggered_by,
            })
            self._run_id = result.scalar()

    def _update_run(self, duration: float):
        sql = """
            UPDATE metadata.pipeline_runs SET
                status              = :status,
                end_time            = :end_time,
                duration_seconds    = :duration,
                total_records_read  = :total,
                records_inserted    = :inserted,
                records_updated     = :updated,
                records_rejected    = :rejected,
                records_skipped     = :skipped,
                error_message       = :error
            WHERE batch_id = :batch_id
        """
        with self._engine.begin() as conn:
            conn.execute(text(sql), {
                "status":   self.status,
                "end_time": self.end_time,
                "duration": duration,
                "total":    self.total_records_read,
                "inserted": self.records_inserted,
                "updated":  self.records_updated,
                "rejected": self.records_rejected,
                "skipped":  self.records_skipped,
                "error":    self.error_message,
                "batch_id": self.batch_id,
            })


def log_dq_result(
    batch_id: str,
    table_name: str,
    check_name: str,
    check_type: str,
    column_name: str,
    passed: bool,
    total_records: int,
    failed_records: int,
    observed_value: str = "",
    expected_value: str = "",
    severity: str = "ERROR",
    engine=None,
):
    """Log a data quality check result."""
    _engine = engine or get_engine()
    success_pct = round((total_records - failed_records) / max(total_records, 1) * 100, 3)
    sql = """
        INSERT INTO metadata.dq_results
            (batch_id, table_name, check_name, check_type, column_name,
             passed, total_records, failed_records, success_pct,
             observed_value, expected_value, severity)
        VALUES
            (:batch_id, :table_name, :check_name, :check_type, :column_name,
             :passed, :total, :failed, :success_pct,
             :observed, :expected, :severity)
    """
    with _engine.begin() as conn:
        conn.execute(text(sql), {
            "batch_id":    batch_id,
            "table_name":  table_name,
            "check_name":  check_name,
            "check_type":  check_type,
            "column_name": column_name,
            "passed":      passed,
            "total":       total_records,
            "failed":      failed_records,
            "success_pct": success_pct,
            "observed":    str(observed_value),
            "expected":    str(expected_value),
            "severity":    severity,
        })


def log_error(batch_id: str, pipeline_name: str, error_type: str, error_message: str, engine=None):
    """Log a pipeline error."""
    _engine = engine or get_engine()
    sql = """
        INSERT INTO metadata.error_log (batch_id, pipeline_name, error_type, error_message)
        VALUES (:batch_id, :pipeline_name, :error_type, :error_message)
    """
    with _engine.begin() as conn:
        conn.execute(text(sql), {
            "batch_id":      batch_id,
            "pipeline_name": pipeline_name,
            "error_type":    error_type,
            "error_message": error_message,
        })


def get_watermark(source_table: str, engine=None) -> str:
    """Get the last loaded watermark value for a source table."""
    _engine = engine or get_engine()
    sql = "SELECT last_loaded_value FROM metadata.watermarks WHERE source_table = :table"
    with _engine.connect() as conn:
        result = conn.execute(text(sql), {"table": source_table})
        row = result.fetchone()
        return row[0] if row else "1970-01-01 00:00:00"


def update_watermark(source_table: str, new_value: str, engine=None):
    """Update the watermark for a source table."""
    _engine = engine or get_engine()
    sql = """
        INSERT INTO metadata.watermarks (source_table, last_loaded_value, last_updated)
        VALUES (:table, :value, NOW())
        ON CONFLICT (source_table)
        DO UPDATE SET last_loaded_value = EXCLUDED.last_loaded_value, last_updated = NOW()
    """
    with _engine.begin() as conn:
        conn.execute(text(sql), {"table": source_table, "value": new_value})
    logger.info(f"Watermark updated | {source_table} → {new_value}")
