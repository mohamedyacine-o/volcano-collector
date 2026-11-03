"""
Core extraction logic – streams artifact rows from the Volcano DB.

The extractor uses a **server‑side cursor** to keep memory usage low,
even when the result set spans millions of rows across 143+ tables.

Two entry‑points:

- ``extract_by_analysis_id()`` – fetch all rows for a single analysis.
- ``extract_all_incremental()`` – fetch new rows since the last checkpoint.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from collector.checkpoint import Checkpoint
from collector.config import Config
from collector.skip_tracker import SkipTracker
from collector.writer import RotatingFileWriter

log = logging.getLogger("collector.extractor")

# The DB function that returns all artifacts for a given analysis_id
_QUERY_BY_ANALYSIS = (
    "SELECT table_name, artifact_data "
    "FROM artifacts.get_all_analyses_artifacts(%s::uuid);"
)


def extract_by_analysis_id(
    conn: psycopg2.extensions.connection,
    analysis_id: str,
    writers: list[RotatingFileWriter],
    cfg: Config,
    skip_tracker: SkipTracker,
    investigation_id: Optional[str] = None,
) -> int:
    """
    Stream all artifact rows for a single *analysis_id* and write them
    to every active writer.

    Parameters
    ----------
    conn : connection
        A live psycopg2 connection.
    analysis_id : str
        UUID of the analysis to extract.
    writers : list[RotatingFileWriter]
        One or more rotating file writers.
    cfg : Config
        Resolved application config.
    skip_tracker : SkipTracker
        Tracker for empty / skipped analyses.
    investigation_id : str | None
        Optional investigation UUID (included in each output line).

    Returns
    -------
    int
        Total number of rows written.
    """
    total_rows = 0
    log.info("Starting extraction for analysis_id=%s", analysis_id)

    cursor_name = f"artifact_stream_{uuid.uuid4().hex[:8]}"
    try:
        with conn.cursor(
            name=cursor_name, cursor_factory=RealDictCursor
        ) as cursor:
            cursor.itersize = cfg.db_fetch_size
            cursor.execute(_QUERY_BY_ANALYSIS, (analysis_id,))

            while True:
                rows = cursor.fetchmany(cfg.db_fetch_size)
                if not rows:
                    break

                for row in rows:
                    line = _build_line(row, analysis_id, investigation_id)
                    _write_to_all(writers, line)
                    total_rows += 1

                log.debug(
                    "Fetched batch of %d rows (running total: %d)",
                    len(rows), total_rows,
                )

    except psycopg2.Error as exc:
        log.error(
            "Database error during extraction for analysis_id=%s: %s",
            analysis_id, exc,
        )
        raise

    if total_rows == 0:
        skip_tracker.record_skip(analysis_id)
        log.warning(
            "SKIPPED – analysis_id=%s returned 0 rows", analysis_id,
        )
    else:
        log.info(
            "Completed extraction for analysis_id=%s – %d rows written",
            analysis_id, total_rows,
        )

    return total_rows


def extract_all_incremental(
    conn: psycopg2.extensions.connection,
    writers: list[RotatingFileWriter],
    cfg: Config,
    checkpoint: Checkpoint,
    skip_tracker: SkipTracker,
) -> int:
    """
    Incrementally extract new rows from **all** artifact tables.

    Uses the checkpoint to determine the last‑seen ``id`` per table
    and only fetches rows with ``id > last_seen_id``.

    Parameters
    ----------
    conn : connection
        A live psycopg2 connection.
    writers : list[RotatingFileWriter]
        Active file writers.
    cfg : Config
        Application config.
    checkpoint : Checkpoint
        Checkpoint state for incremental tracking.
    skip_tracker : SkipTracker
        Tracker for empty results.

    Returns
    -------
    int
        Total number of new rows written.
    """
    total_rows = 0
    tables = _discover_tables(conn)

    if not tables:
        log.warning("No artifact tables found in the 'artifacts' schema.")
        return 0

    log.info("Discovered %d artifact tables for incremental extraction.", len(tables))

    for table_name in tables:
        last_id = checkpoint.get_last_id(table_name)
        rows_written, max_id = _extract_table_incremental(
            conn, table_name, last_id, writers, cfg,
        )
        total_rows += rows_written

        if max_id > last_id:
            checkpoint.update(table_name, max_id)

        if rows_written > 0:
            log.info(
                "Table '%s': %d new rows (last_id: %d → %d)",
                table_name, rows_written, last_id, max_id,
            )

    checkpoint.save()
    log.info("Incremental extraction complete – %d total new rows.", total_rows)
    return total_rows


# ── private helpers ─────────────────────────────────────────────────────────

def _discover_tables(conn: psycopg2.extensions.connection) -> list[str]:
    """
    Query ``information_schema`` to find all tables in the ``artifacts``
    schema that have an ``analyses_id`` column.
    """
    query = """
        SELECT DISTINCT c.table_name
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON c.table_schema = t.table_schema
         AND c.table_name = t.table_name
        WHERE c.table_schema = 'artifacts'
          AND t.table_type = 'BASE TABLE'
          AND c.column_name = 'analyses_id'
        ORDER BY c.table_name;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]
    except psycopg2.Error as exc:
        log.error("Failed to discover artifact tables: %s", exc)
        return []


def _extract_table_incremental(
    conn: psycopg2.extensions.connection,
    table_name: str,
    last_id: int,
    writers: list[RotatingFileWriter],
    cfg: Config,
) -> tuple[int, int]:
    """
    Fetch rows from a single table where ``id > last_id``.

    Returns ``(rows_written, max_id_seen)``.
    """
    # Use %s for the parameter and format the table name safely
    query = (
        f"SELECT id, analyses_id, to_jsonb(t.*) AS artifact_data "
        f"FROM artifacts.{_safe_ident(table_name)} t "
        f"WHERE t.id > %s ORDER BY t.id"
    )

    rows_written = 0
    max_id = last_id

    try:
        with conn.cursor(
            name=f"incr_{table_name}", cursor_factory=RealDictCursor
        ) as cursor:
            cursor.itersize = cfg.db_fetch_size
            cursor.execute(query, (last_id,))

            while True:
                rows = cursor.fetchmany(cfg.db_fetch_size)
                if not rows:
                    break

                for row in rows:
                    row_id = row.get("id", 0)
                    if row_id > max_id:
                        max_id = row_id

                    analysis_id = str(row.get("analyses_id", ""))
                    line = _build_line(
                        {"table_name": table_name, "artifact_data": row["artifact_data"]},
                        analysis_id,
                        investigation_id=None,
                    )
                    _write_to_all(writers, line)
                    rows_written += 1

    except psycopg2.Error as exc:
        log.error(
            "Error extracting table '%s' (last_id=%d): %s",
            table_name, last_id, exc,
        )

    return rows_written, max_id


def _build_line(
    row: dict,
    analysis_id: str,
    investigation_id: Optional[str] = None,
) -> str:
    """
    Build a single NDJSON line from a DB row.

    Output format:
    ``{"artifact_table": "...", "analysis_id": "...", "investigation_id": "...", "artifact_data": {...}}``
    """
    event = {
        "artifact_table": row["table_name"],
        "analysis_id": analysis_id,
        "investigation_id": investigation_id or "",
        "artifact_data": row["artifact_data"],
    }
    return json.dumps(event, default=str)


def _write_to_all(writers: list[RotatingFileWriter], line: str) -> None:
    """Write a line to every active writer."""
    for writer in writers:
        try:
            writer.write_line(line)
        except OSError as exc:
            log.error("Failed to write to '%s': %s", writer.current_path, exc)


def _safe_ident(name: str) -> str:
    """
    Sanitise a SQL identifier (table name) to prevent injection.

    Only allows alphanumeric characters and underscores.
    """
    cleaned = "".join(c for c in name if c.isalnum() or c == "_")
    if cleaned != name:
        log.warning(
            "Table name sanitised: '%s' → '%s'", name, cleaned,
        )
    return cleaned


def discover_analyses(
    conn: psycopg2.extensions.connection,
    cfg: Config,
) -> list[dict[str, str]]:
    """
    Query the analyses table (default: investigations.analyses) to discover all
    analyses, returning a list of dicts: ``{"analysis_id": "...", "investigation_id": "..."}``.

    Dynamically inspects columns to handle both 'analyses_id' and 'id', as well as
    'investigation_id' and 'investigations_id'.
    """
    schema = _safe_ident(cfg.analyses_schema)
    table = _safe_ident(cfg.analyses_table)

    # 1. Query column metadata from information_schema
    col_query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
    """
    cols: set[str] = set()
    try:
        with conn.cursor() as cur:
            cur.execute(col_query, (cfg.analyses_schema, cfg.analyses_table))
            cols = {row[0].lower() for row in cur.fetchall()}
    except Exception as exc:
        log.debug("Column auto-detection via information_schema failed: %s", exc)

    # 2. Determine column names
    if "analyses_id" in cols:
        id_col = "analyses_id"
    elif "id" in cols:
        id_col = "id"
    else:
        id_col = "analyses_id"

    if "investigation_id" in cols:
        inv_col = "investigation_id"
    elif "investigations_id" in cols:
        inv_col = "investigations_id"
    else:
        inv_col = None

    # 3. Build query
    select_parts = [f"{id_col} AS analysis_id"]
    if inv_col:
        select_parts.append(f"{inv_col} AS investigation_id")
    else:
        select_parts.append("'' AS investigation_id")

    query = f"SELECT {', '.join(select_parts)} FROM {schema}.{table}"
    params: list[str] = []

    if "status" in cols and cfg.analyses_status_filter:
        query += " WHERE status = %s"
        params.append(cfg.analyses_status_filter)

    if "create_time" in cols:
        query += " ORDER BY create_time ASC"
    elif "created_at" in cols:
        query += " ORDER BY created_at ASC"
    else:
        query += f" ORDER BY {id_col} ASC"

    log.debug("Executing analyses discovery query: %s (params: %s)", query, params)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            results = []
            for r in rows:
                aid = str(r["analysis_id"]).strip() if r.get("analysis_id") else ""
                if aid:
                    results.append({
                        "analysis_id": aid,
                        "investigation_id": str(r.get("investigation_id") or "").strip(),
                    })
            log.info("Discovered %d analyses from %s.%s", len(results), schema, table)
            return results
    except psycopg2.Error as exc:
        log.error(
            "Failed to discover analyses from %s.%s: %s (ensure user has USAGE on schema '%s' and SELECT on '%s.%s')",
            schema, table, exc, schema, schema, table,
        )
        return []
