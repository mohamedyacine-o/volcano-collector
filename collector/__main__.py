"""
Volcano Artifact Collector – CLI entry point.

Run with::

    python -m collector --mode <service|once|http> [options]

See ``python -m collector --help`` for the full list of arguments.
"""

from __future__ import annotations

import logging
import signal
import sys
import time

import psycopg2

from collector.checkpoint import Checkpoint
from collector.config import Config, load_config
from collector.db import (
    DatabaseConnectionError,
    close_connection,
    connect_with_retry,
)
from collector.extractor import (
    discover_analyses,
    extract_all_incremental,
    extract_by_analysis_id,
)
from collector.http_server import start_http_server
from collector.logger import setup_logging
from collector.queue_tracker import AnalysisQueueTracker
from collector.skip_tracker import SkipTracker
from collector.writer import build_writers

log: logging.Logger  # populated in main()

# ── graceful shutdown ───────────────────────────────────────────────────────

_shutdown_requested = False


def _handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    log.info("Shutdown signal received (signal=%d). Finishing current cycle…", signum)


# ── mode runners ────────────────────────────────────────────────────────────

def _run_service(cfg: Config) -> int:
    """
    Mode 1 – Continuous service.

    Polls the DB every ``cfg.poll_interval`` seconds and incrementally
    exports new rows.  Runs until interrupted (SIGINT / SIGTERM).
    """
    checkpoint = Checkpoint(cfg.output_dir)
    skip_tracker = SkipTracker(cfg.output_dir)
    writers = build_writers(
        cfg.output_dir,
        [cfg.prefix_1],
        cfg.max_file_size_mb,
        cfg.rotate_daily,
    )

    log.info(
        "Service mode started – polling every %ds. Press Ctrl+C to stop.",
        cfg.poll_interval,
    )

    cycle = 0
    try:
        while not _shutdown_requested:
            cycle += 1
            log.info("── Cycle %d ──", cycle)

            conn = None
            try:
                conn = connect_with_retry(cfg)
                total = extract_all_incremental(
                    conn, writers, cfg, checkpoint, skip_tracker,
                )
                log.info("Cycle %d complete – %d new rows.", cycle, total)

            except DatabaseConnectionError as exc:
                log.error("Cycle %d – DB connection failed: %s", cycle, exc)

            except Exception as exc:
                log.error("Cycle %d – unexpected error: %s", cycle, exc, exc_info=True)

            finally:
                close_connection(conn)

            # Sleep in small increments so we can react to shutdown signals
            if cfg.poll_interval <= 0:
                log.info("Poll interval is 0 – running once and exiting.")
                break

            _interruptible_sleep(cfg.poll_interval)

    finally:
        for w in writers:
            w.close()
        log.info("Service mode stopped. Total skipped: %d", skip_tracker.total_skipped)

    return 0


def _run_once(cfg: Config) -> int:
    """
    Mode 2 – One‑shot export for a single ``analysis_id``.
    """
    if not cfg.analysis_id:
        log.error("Mode 'once' requires --analysis-id (-a).")
        return 2

    skip_tracker = SkipTracker(cfg.output_dir)
    writers = build_writers(
        cfg.output_dir,
        [cfg.prefix_2],
        cfg.max_file_size_mb,
        cfg.rotate_daily,
    )

    conn = None
    try:
        conn = connect_with_retry(cfg)
        total = extract_by_analysis_id(
            conn,
            cfg.analysis_id,
            writers,
            cfg,
            skip_tracker,
            cfg.investigation_id,
        )

        if total == 0:
            log.warning(
                "Analysis %s returned 0 rows – marked as SKIPPED.",
                cfg.analysis_id,
            )
            return 0
        else:
            log.info("Export complete – %d rows written.", total)
            return 0

    except DatabaseConnectionError as exc:
        log.error("DB connection failed: %s", exc)
        return 1

    except Exception as exc:
        log.error("Export failed: %s", exc, exc_info=True)
        return 1

    finally:
        close_connection(conn)
        for w in writers:
            w.close()


def _run_http(cfg: Config) -> int:
    """
    Mode 3 – HTTP listener for on‑demand exports.
    """
    skip_tracker = SkipTracker(cfg.output_dir)

    try:
        start_http_server(cfg, skip_tracker)
    except Exception as exc:
        log.error("HTTP server error: %s", exc, exc_info=True)
        return 1

    return 0


def _run_queue(cfg: Config) -> int:
    """
    Mode 4 – Analysis-Centric Queue Worker.

    Phase 1 (Day 0 Backfill): Discovers all historical analyses from
    investigations.analyses, saves them to a local JSON queue file, and
    extracts them 1-by-1 with throttling to protect Logstash memory.

    Phase 2 (Recurring Polling): Every cfg.poll_interval seconds, polls
    investigations.analyses for newly added analyses, enqueues them, and
    processes them 1-by-1.
    """
    queue_tracker = AnalysisQueueTracker(cfg.output_dir)
    skip_tracker = SkipTracker(cfg.output_dir)
    writers = build_writers(
        cfg.output_dir,
        [cfg.prefix_1],
        cfg.max_file_size_mb,
        cfg.rotate_daily,
        cfg.flush_interval,
    )

    log.info(
        "Queue mode started – schema=%s, table=%s, poll_interval=%ds, throttle=%.1fs. Press Ctrl+C to stop.",
        cfg.analyses_schema,
        cfg.analyses_table,
        cfg.poll_interval,
        cfg.analyses_throttle_seconds,
    )
    log.info(
        "Current Queue State: %d pending, %d completed, %d failed",
        queue_tracker.pending_count,
        queue_tracker.completed_count,
        queue_tracker.failed_count,
    )

    conn = None
    last_discovery_time = 0.0

    try:
        while not _shutdown_requested:
            now = time.time()

            # Ensure DB connection is active
            if conn is None or getattr(conn, "closed", True):
                try:
                    conn = connect_with_retry(cfg)
                except DatabaseConnectionError as exc:
                    log.error(
                        "DB connection failed: %s (retrying in %ds)",
                        exc, min(10, cfg.poll_interval),
                    )
                    _interruptible_sleep(min(10, cfg.poll_interval))
                    continue

            # Check if discovery should run
            if queue_tracker.pending_count == 0 or (now - last_discovery_time) >= cfg.poll_interval:
                log.info("Running analyses discovery from %s.%s...", cfg.analyses_schema, cfg.analyses_table)
                try:
                    discovered = discover_analyses(conn, cfg)
                    enqueued = queue_tracker.enqueue_batch(discovered)
                    last_discovery_time = time.time()
                    if enqueued > 0:
                        log.info(
                            "Discovered and enqueued %d new analyses (Total pending: %d)",
                            enqueued, queue_tracker.pending_count,
                        )
                    else:
                        log.info("Discovery complete – no new analyses (Total pending: %d)", queue_tracker.pending_count)
                except Exception as exc:
                    log.error("Analyses discovery failed: %s", exc, exc_info=True)

            # Process next item from queue 1-by-1
            item = queue_tracker.pop_next()
            if item is not None:
                aid = item["analysis_id"]
                iid = item.get("investigation_id") or ""
                log.info(
                    "Processing analysis [%d remaining in queue]: analysis_id=%s, investigation_id=%s",
                    queue_tracker.pending_count, aid, iid or "N/A",
                )

                try:
                    total_rows = extract_by_analysis_id(
                        conn, aid, writers, cfg, skip_tracker, iid,
                    )
                    queue_tracker.mark_completed(aid, rows_exported=total_rows)
                    if total_rows > 0:
                        log.info("Successfully exported analysis_id=%s (%d rows written)", aid, total_rows)
                    else:
                        log.warning("Analysis_id=%s returned 0 rows (recorded as skipped)", aid)
                except (psycopg2.OperationalError, DatabaseConnectionError) as exc:
                    log.error("DB connection dropped during analysis_id=%s: %s (will reconnect)", aid, exc)
                    close_connection(conn)
                    conn = None
                    queue_tracker.enqueue(aid, iid)
                except Exception as exc:
                    log.error("Failed to extract analysis_id=%s: %s", aid, exc, exc_info=True)
                    queue_tracker.mark_failed(aid, str(exc))

                # Throttle delay between analyses to protect Logstash heap and DB resources
                if cfg.analyses_throttle_seconds > 0 and not _shutdown_requested:
                    time.sleep(cfg.analyses_throttle_seconds)

            else:
                # Queue is empty; wait for next discovery cycle
                log.info(
                    "Queue is empty. Sleeping for %ds before next discovery cycle...",
                    cfg.poll_interval,
                )
                _interruptible_sleep(cfg.poll_interval)

    finally:
        close_connection(conn)
        for w in writers:
            w.close()
        log.info(
            "Queue mode stopped. Summary: %d completed, %d failed, %d pending",
            queue_tracker.completed_count,
            queue_tracker.failed_count,
            queue_tracker.pending_count,
        )

    return 0


# ── helpers ─────────────────────────────────────────────────────────────────

def _interruptible_sleep(seconds: int) -> None:
    """Sleep in 1‑second increments to allow clean shutdown."""
    for _ in range(seconds):
        if _shutdown_requested:
            break
        time.sleep(1)


def _validate_config(cfg: Config) -> bool:
    """
    Run basic config validation.  Returns ``True`` if OK.
    """
    from pathlib import Path

    output = Path(cfg.output_dir)
    if not output.exists():
        try:
            output.mkdir(parents=True, exist_ok=True)
            log.info("Created output directory: %s", output)
        except OSError as exc:
            log.error("Cannot create output directory %s: %s", output, exc)
            return False

    if cfg.mode == "once" and not cfg.analysis_id:
        log.error("Mode 'once' requires --analysis-id (-a).")
        return False

    return True


# ── main ────────────────────────────────────────────────────────────────────

def main() -> int:
    """Parse config, set up logging, and dispatch to the chosen mode."""
    global log

    cfg = load_config()
    log = setup_logging(cfg.log_level, cfg.log_file)

    log.info("Volcano Artifact Collector v1.0.0")
    log.info("Mode: %s", cfg.mode)
    log.info("DB:   %s@%s:%s/%s", cfg.db_user, cfg.db_host, cfg.db_port, cfg.db_name)
    log.info("Output: %s", cfg.output_dir)

    if cfg.dry_run:
        log.info("Dry run – validating configuration only.")
        ok = _validate_config(cfg)
        return 0 if ok else 2

    if not _validate_config(cfg):
        return 2

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    mode_map = {
        "service": _run_service,
        "once": _run_once,
        "http": _run_http,
        "queue": _run_queue,
    }

    runner = mode_map.get(cfg.mode)
    if runner is None:
        log.error("Unknown mode: '%s'", cfg.mode)
        return 2

    return runner(cfg)


if __name__ == "__main__":
    sys.exit(main())
