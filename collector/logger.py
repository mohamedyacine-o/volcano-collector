"""
Structured logging setup for the Volcano Artifact Collector.

Provides a single ``setup_logging()`` function that configures the root
logger with a consistent format, optional file output, and a configurable
log level.
"""

import logging
import sys
from pathlib import Path


_LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
)
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def setup_logging(level: str = "INFO", log_file: str = "") -> logging.Logger:
    """
    Configure and return the application root logger.

    Parameters
    ----------
    level : str
        Python log level name (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    log_file : str
        Optional path to a log file.  Leave empty for stdout only.

    Returns
    -------
    logging.Logger
        The configured root logger (named ``"collector"``).
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    root = logging.getLogger("collector")
    root.setLevel(numeric_level)

    # Avoid adding duplicate handlers on repeated calls
    if root.handlers:
        return root

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(numeric_level)
    console.setFormatter(formatter)
    root.addHandler(console)

    # Optional file handler
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    return root
