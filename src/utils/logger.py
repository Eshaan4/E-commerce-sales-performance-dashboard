"""
logger.py – Structured, coloured logging utility for the DE PoC pipeline.
"""
import logging
import sys
from datetime import datetime

try:
    import colorlog
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Return a configured logger with colour support if available."""
    logger = logging.getLogger(name)

    if logger.handlers:          # Avoid duplicate handlers
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    fmt = "[%(asctime)s] [%(levelname)-8s] [%(name)s] %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"

    if HAS_COLOR:
        handler = colorlog.StreamHandler(sys.stdout)
        handler.setFormatter(colorlog.ColoredFormatter(
            "%(log_color)s" + fmt,
            datefmt=date_fmt,
            log_colors={
                "DEBUG":    "cyan",
                "INFO":     "green",
                "WARNING":  "yellow",
                "ERROR":    "red",
                "CRITICAL": "bold_red",
            }
        ))
    else:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(fmt, datefmt=date_fmt))

    logger.addHandler(handler)
    return logger


def log_separator(logger: logging.Logger, title: str = "") -> None:
    """Print a visual separator line."""
    line = "─" * 70
    if title:
        logger.info(f"{line}")
        logger.info(f"  {title}")
    logger.info(f"{line}")
