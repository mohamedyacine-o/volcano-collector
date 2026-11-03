"""
Database connection helper with retry and exponential back‑off.

Provides ``connect_with_retry()`` which returns a ``psycopg2`` connection
object, retrying on transient errors up to ``max_attempts`` times.
"""

from __future__ import annotations

import logging
import time

import psycopg2

from collector.config import Config

log = logging.getLogger("collector.db")


class DatabaseConnectionError(Exception):
    """Raised when all retry attempts are exhausted."""


def connect_with_retry(cfg: Config) -> psycopg2.extensions.connection:
    """
    Open a PostgreSQL connection using the settings in *cfg*.

    Retries up to ``cfg.retry_max_attempts`` times with exponential back‑off
    (base delay = ``cfg.retry_base_delay`` seconds).

    Parameters
    ----------
    cfg : Config
        Resolved application configuration.

    Returns
    -------
    psycopg2.extensions.connection
        A live database connection.

    Raises
    ------
    DatabaseConnectionError
        If all attempts fail.
    """
    last_err: Exception | None = None

    for attempt in range(1, cfg.retry_max_attempts + 1):
        try:
            conn = psycopg2.connect(
                host=cfg.db_host,
                port=cfg.db_port,
                dbname=cfg.db_name,
                user=cfg.db_user,
                password=cfg.db_pass,
                sslmode=cfg.db_sslmode,
                connect_timeout=10,
            )
            log.info(
                "Connected to %s:%s/%s (attempt %d/%d)",
                cfg.db_host, cfg.db_port, cfg.db_name,
                attempt, cfg.retry_max_attempts,
            )
            return conn

        except psycopg2.OperationalError as exc:
            last_err = exc
            delay = cfg.retry_base_delay * (2 ** (attempt - 1))
            log.warning(
                "DB connection attempt %d/%d failed: %s – retrying in %.1fs",
                attempt, cfg.retry_max_attempts, exc, delay,
            )
            time.sleep(delay)

    raise DatabaseConnectionError(
        f"Failed to connect after {cfg.retry_max_attempts} attempts: {last_err}"
    )


def close_connection(conn: psycopg2.extensions.connection) -> None:
    """Close a database connection safely, logging any errors."""
    if conn is None or conn.closed:
        return
    try:
        conn.close()
        log.info("Database connection closed.")
    except Exception as exc:
        log.error("Error closing database connection: %s", exc)
