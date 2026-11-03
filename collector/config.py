"""
Configuration loader for the Volcano Artifact Collector.

Merges settings from three sources (lowest to highest priority):

1. ``config.yaml`` (or the file pointed to by ``--config``)
2. Environment variables (loaded from ``.env`` via *python-dotenv*)
3. CLI arguments

Every public attribute on the resulting ``Config`` object is guaranteed
to have a usable default so the tool can run with zero external files
in a pinch.
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(dotenv_path: str | Path | None = None) -> bool:
        """Lightweight zero-dependency fallback when python-dotenv is not installed."""
        target = Path(dotenv_path) if dotenv_path else Path(".env")
        if not target.is_file():
            return False
        try:
            with open(target, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip()
                        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                            v = v[1:-1]
                        os.environ.setdefault(k, v)
            return True
        except Exception:
            return False

log = logging.getLogger("collector.config")


# ── defaults ────────────────────────────────────────────────────────────────

_DEFAULTS = {
    "db_host": "localhost",
    "db_port": 5432,
    "db_name": "volcano",
    "db_user": "logstash_ro",
    "db_pass": "postgres",
    "db_sslmode": "prefer",
    "output_dir": "/tmp/volcano_artifacts",
    "prefix_1": "volcano_artifacts",
    "prefix_2": "volcano_one_shot",
    "prefix_3": "volcano_http",
    "max_file_size_mb": 100,
    "rotate_daily": True,
    "poll_interval": 300,
    "http_host": "0.0.0.0",
    "http_port": 8080,
    "retry_max_attempts": 5,
    "retry_base_delay": 1.0,
    "log_level": "INFO",
    "log_file": "",
    "db_fetch_size": 1000,
    "flush_interval": 1000,
    "api_key": "",
    "analyses_schema": "investigations",
    "analyses_table": "analyses",
    "analyses_status_filter": "completed",
    "analyses_throttle_seconds": 1.0,
}


# ── dataclass ───────────────────────────────────────────────────────────────

@dataclass
class Config:
    """Immutable bag of every setting the collector needs."""

    # Execution mode
    mode: str = "service"

    # Database
    db_host: str = _DEFAULTS["db_host"]
    db_port: int = _DEFAULTS["db_port"]
    db_name: str = _DEFAULTS["db_name"]
    db_user: str = _DEFAULTS["db_user"]
    db_pass: str = _DEFAULTS["db_pass"]
    db_sslmode: str = _DEFAULTS["db_sslmode"]

    # Output
    output_dir: str = _DEFAULTS["output_dir"]
    prefix_1: str = _DEFAULTS["prefix_1"]
    prefix_2: str = _DEFAULTS["prefix_2"]
    prefix_3: str = _DEFAULTS["prefix_3"]

    # Rotation
    max_file_size_mb: int = _DEFAULTS["max_file_size_mb"]
    rotate_daily: bool = _DEFAULTS["rotate_daily"]

    # Scheduler
    poll_interval: int = _DEFAULTS["poll_interval"]

    # HTTP
    http_host: str = _DEFAULTS["http_host"]
    http_port: int = _DEFAULTS["http_port"]

    # Retry
    retry_max_attempts: int = _DEFAULTS["retry_max_attempts"]
    retry_base_delay: float = _DEFAULTS["retry_base_delay"]

    # Logging
    log_level: str = _DEFAULTS["log_level"]
    log_file: str = _DEFAULTS["log_file"]

    # Performance
    db_fetch_size: int = _DEFAULTS["db_fetch_size"]
    flush_interval: int = _DEFAULTS["flush_interval"]

    # Security (optional)
    api_key: str = _DEFAULTS["api_key"]

    # Queue mode settings
    analyses_schema: str = _DEFAULTS["analyses_schema"]
    analyses_table: str = _DEFAULTS["analyses_table"]
    analyses_status_filter: str = _DEFAULTS["analyses_status_filter"]
    analyses_throttle_seconds: float = _DEFAULTS["analyses_throttle_seconds"]

    # Optional one‑shot / HTTP identifiers
    analysis_id: Optional[str] = None
    investigation_id: Optional[str] = None

    # Dry run
    dry_run: bool = False


# ── YAML loader ─────────────────────────────────────────────────────────────

def _load_yaml(path: str) -> dict:
    """
    Read a YAML config file and return a flat dict of settings.

    Nested keys are flattened: ``database.host`` → ``db_host``.
    """
    result: dict = {}
    config_path = Path(path)
    if not config_path.is_file():
        return result

    if yaml is None:
        log.warning("PyYAML is not installed; skipping YAML config '%s'", path)
        return result

    with open(config_path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    # Flatten known sections
    db = raw.get("database", {})
    if db:
        result["db_host"] = db.get("host", _DEFAULTS["db_host"])
        result["db_port"] = int(db.get("port", _DEFAULTS["db_port"]))
        result["db_name"] = db.get("name", _DEFAULTS["db_name"])
        result["db_user"] = db.get("user", _DEFAULTS["db_user"])
        result["db_pass"] = db.get("password", _DEFAULTS["db_pass"])
        result["db_sslmode"] = db.get("sslmode", _DEFAULTS["db_sslmode"])

    out = raw.get("output", {})
    if out:
        result["output_dir"] = out.get("directory", _DEFAULTS["output_dir"])
        pfx = out.get("prefixes", {})
        result["prefix_1"] = pfx.get("service", _DEFAULTS["prefix_1"])
        result["prefix_2"] = pfx.get("one_shot", _DEFAULTS["prefix_2"])
        result["prefix_3"] = pfx.get("http", _DEFAULTS["prefix_3"])

    rot = raw.get("rotation", {})
    if rot:
        result["max_file_size_mb"] = int(rot.get("max_file_size_mb", _DEFAULTS["max_file_size_mb"]))
        result["rotate_daily"] = rot.get("daily", _DEFAULTS["rotate_daily"])

    sched = raw.get("scheduler", {})
    if sched:
        result["poll_interval"] = int(sched.get("poll_interval_seconds", _DEFAULTS["poll_interval"]))

    http = raw.get("http_server", {})
    if http:
        result["http_host"] = http.get("host", _DEFAULTS["http_host"])
        result["http_port"] = int(http.get("port", _DEFAULTS["http_port"]))
        api_key = http.get("api_key", _DEFAULTS["api_key"])
        if api_key:
            result["api_key"] = api_key

    retry = raw.get("retry", {})
    if retry:
        result["retry_max_attempts"] = int(retry.get("max_attempts", _DEFAULTS["retry_max_attempts"]))
        result["retry_base_delay"] = float(retry.get("base_delay_seconds", _DEFAULTS["retry_base_delay"]))

    log_cfg = raw.get("logging", {})
    if log_cfg:
        result["log_level"] = log_cfg.get("level", _DEFAULTS["log_level"])
        result["log_file"] = log_cfg.get("file", _DEFAULTS["log_file"])

    perf = raw.get("performance", {})
    if perf:
        result["db_fetch_size"] = int(perf.get("db_fetch_size", _DEFAULTS["db_fetch_size"]))
        result["flush_interval"] = int(perf.get("flush_interval", _DEFAULTS["flush_interval"]))

    queue = raw.get("queue", {})
    if queue:
        result["analyses_schema"] = queue.get("schema", _DEFAULTS["analyses_schema"])
        result["analyses_table"] = queue.get("table", _DEFAULTS["analyses_table"])
        result["analyses_status_filter"] = queue.get("status_filter", _DEFAULTS["analyses_status_filter"])
        if "throttle_seconds" in queue:
            result["analyses_throttle_seconds"] = float(queue["throttle_seconds"])

    return result


# ── ENV loader ──────────────────────────────────────────────────────────────

def _load_env() -> dict:
    """
    Read environment variables (including ``.env``) and return a dict
    of the settings that were explicitly set.
    """
    load_dotenv()
    result: dict = {}

    _map = {
        "VOLCANO_DB_HOST": ("db_host", str),
        "VOLCANO_DB_PORT": ("db_port", int),
        "VOLCANO_DB_NAME": ("db_name", str),
        "VOLCANO_DB_USER": ("db_user", str),
        "VOLCANO_DB_PASS": ("db_pass", str),
        "ARTIFACTS_OUTPUT_DIR": ("output_dir", str),
        "OUTPUT_PREFIX_1": ("prefix_1", str),
        "OUTPUT_PREFIX_2": ("prefix_2", str),
        "OUTPUT_PREFIX_3": ("prefix_3", str),
        "MAX_FILE_SIZE_MB": ("max_file_size_mb", int),
        "ROTATE_DAILY": ("rotate_daily", lambda v: v.lower() in ("true", "1", "yes")),
        "POLL_INTERVAL_SECONDS": ("poll_interval", int),
        "HTTP_LISTEN_HOST": ("http_host", str),
        "HTTP_LISTEN_PORT": ("http_port", int),
        "RETRY_MAX_ATTEMPTS": ("retry_max_attempts", int),
        "RETRY_BASE_DELAY": ("retry_base_delay", float),
        "LOG_LEVEL": ("log_level", str),
        "LOG_FILE": ("log_file", str),
        "DB_FETCH_SIZE": ("db_fetch_size", int),
        "FLUSH_INTERVAL": ("flush_interval", int),
        "HTTP_API_KEY": ("api_key", str),
        "ANALYSES_SCHEMA": ("analyses_schema", str),
        "ANALYSES_TABLE": ("analyses_table", str),
        "ANALYSES_STATUS_FILTER": ("analyses_status_filter", str),
        "ANALYSES_THROTTLE_SECONDS": ("analyses_throttle_seconds", float),
    }

    for env_key, (cfg_key, cast) in _map.items():
        val = os.getenv(env_key)
        if val is not None:
            try:
                result[cfg_key] = cast(val)
            except (ValueError, TypeError):
                log.warning("Invalid value for %s=%r, using default", env_key, val)

    return result


# ── CLI parser ──────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with every overridable option."""
    p = argparse.ArgumentParser(
        prog="volcano-collector",
        description=(
            "Volcano Artifact Collector – Extracts DFIR artifact data from "
            "the Volcano PostgreSQL database and writes rotating NDJSON files."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Continuous service (default settings)
  python -m collector --mode service

  # One-shot export for a specific analysis
  python -m collector --mode once --analysis-id <uuid>

  # HTTP listener on a custom port
  python -m collector --mode http --http-port 9090

  # Override DB host and output directory
  python -m collector --mode service --db-host 10.0.0.5 --output-dir /data/exports

Configuration priority: CLI > Environment (.env) > config.yaml > defaults
""",
    )

    # Required
    p.add_argument(
        "-m", "--mode",
        choices=["service", "once", "http", "queue"],
        default=None,
        help="Execution mode: 'service' (table-incremental), 'once' (one-shot), 'http' (listener), 'queue' (analyses queue).",
    )

    # Database
    db = p.add_argument_group("Database connection")
    db.add_argument("--db-host", default=None, help="PostgreSQL host (env: VOLCANO_DB_HOST).")
    db.add_argument("--db-port", type=int, default=None, help="PostgreSQL port (env: VOLCANO_DB_PORT).")
    db.add_argument("--db-name", default=None, help="Database name (env: VOLCANO_DB_NAME).")
    db.add_argument("--db-user", default=None, help="Database user (env: VOLCANO_DB_USER).")
    db.add_argument("--db-pass", default=None, help="Database password (env: VOLCANO_DB_PASS).")

    # Output
    out = p.add_argument_group("Output settings")
    out.add_argument("--output-dir", default=None, help="Output directory for NDJSON files (env: ARTIFACTS_OUTPUT_DIR).")
    out.add_argument("--prefix-1", default=None, help="File prefix for Mode 1/service (env: OUTPUT_PREFIX_1).")
    out.add_argument("--prefix-2", default=None, help="File prefix for Mode 2/one-shot (env: OUTPUT_PREFIX_2).")
    out.add_argument("--prefix-3", default=None, help="File prefix for Mode 3/http (env: OUTPUT_PREFIX_3).")

    # Rotation
    rot = p.add_argument_group("File rotation")
    rot.add_argument("--max-size", type=int, default=None, dest="max_file_size_mb", help="Max file size in MiB before rotation (env: MAX_FILE_SIZE_MB).")
    rot.add_argument("--no-daily-rotate", action="store_true", default=False, help="Disable daily rotation (env: ROTATE_DAILY=false).")

    # Scheduler
    sched = p.add_argument_group("Scheduler (Mode 1)")
    sched.add_argument("--poll-interval", type=int, default=None, help="Seconds between DB polls (env: POLL_INTERVAL_SECONDS).")

    # HTTP
    http = p.add_argument_group("HTTP listener (Mode 3)")
    http.add_argument("--http-port", type=int, default=None, help="HTTP listener port (env: HTTP_LISTEN_PORT).")
    http.add_argument("--api-key", default=None, help="API key for HTTP auth (env: HTTP_API_KEY). Leave empty to disable auth.")

    # Retry
    retry = p.add_argument_group("Connection retry")
    retry.add_argument("--retry-attempts", type=int, default=None, help="Max retry attempts (env: RETRY_MAX_ATTEMPTS).")
    retry.add_argument("--retry-delay", type=float, default=None, help="Base back-off delay in seconds (env: RETRY_BASE_DELAY).")

    # Identifiers
    ids = p.add_argument_group("Identifiers (Mode 2 / HTTP)")
    ids.add_argument("-a", "--analysis-id", default=None, help="UUID of the analysis to export.")
    ids.add_argument("-i", "--investigation-id", default=None, help="UUID of the investigation (optional metadata).")

    # Logging
    lg = p.add_argument_group("Logging")
    lg.add_argument("--log-level", default=None, choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Log level (env: LOG_LEVEL).")
    lg.add_argument("--log-file", default=None, help="Optional log file path (env: LOG_FILE).")

    # Performance
    perf = p.add_argument_group("Performance")
    perf.add_argument("--fetch-size", type=int, default=None, dest="db_fetch_size", help="Rows per cursor fetch (env: DB_FETCH_SIZE).")
    perf.add_argument("--flush-interval", type=int, default=None, dest="flush_interval", help="Flush output file every N lines (env: FLUSH_INTERVAL). Default: 1000.")

    # Queue mode
    q_grp = p.add_argument_group("Queue Mode (Mode 'queue')")
    q_grp.add_argument("--analyses-schema", default=None, help="Schema containing analyses table (env: ANALYSES_SCHEMA, default: investigations).")
    q_grp.add_argument("--analyses-table", default=None, help="Table containing analyses (env: ANALYSES_TABLE, default: analyses).")
    q_grp.add_argument("--status-filter", default=None, dest="analyses_status_filter", help="Analysis status filter (env: ANALYSES_STATUS_FILTER, default: completed). Set to '' to disable.")
    q_grp.add_argument("--throttle", type=float, default=None, dest="analyses_throttle_seconds", help="Delay in seconds between analyses (env: ANALYSES_THROTTLE_SECONDS, default: 1.0).")

    # Config file
    p.add_argument("--config", default="config.yaml", help="Path to YAML config file (default: config.yaml).")

    # Misc
    p.add_argument("--dry-run", action="store_true", default=False, help="Validate configuration and exit without connecting.")

    return p


# ── public API ──────────────────────────────────────────────────────────────

def load_config(argv: list[str] | None = None) -> Config:
    """
    Build a ``Config`` object by merging YAML, env, and CLI sources.

    Parameters
    ----------
    argv : list[str] | None
        Command‑line arguments.  ``None`` uses ``sys.argv[1:]``.

    Returns
    -------
    Config
        Fully resolved configuration.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    # Layer 1: defaults (already in the dataclass)
    merged: dict = dict(_DEFAULTS)

    # Layer 2: YAML config file
    yaml_cfg = _load_yaml(args.config)
    merged.update({k: v for k, v in yaml_cfg.items() if v is not None})

    # Layer 3: Environment variables
    env_cfg = _load_env()
    merged.update({k: v for k, v in env_cfg.items() if v is not None})

    # Layer 4: CLI arguments (highest priority)
    cli_map = {
        "mode": args.mode,
        "db_host": args.db_host,
        "db_port": args.db_port,
        "db_name": args.db_name,
        "db_user": args.db_user,
        "db_pass": args.db_pass,
        "output_dir": args.output_dir,
        "prefix_1": args.prefix_1,
        "prefix_2": args.prefix_2,
        "prefix_3": args.prefix_3,
        "max_file_size_mb": args.max_file_size_mb,
        "poll_interval": args.poll_interval,
        "http_port": args.http_port,
        "retry_max_attempts": args.retry_attempts,
        "retry_base_delay": args.retry_delay,
        "log_level": args.log_level,
        "log_file": args.log_file,
        "db_fetch_size": args.db_fetch_size,
        "flush_interval": args.flush_interval,
        "api_key": args.api_key,
        "analyses_schema": args.analyses_schema,
        "analyses_table": args.analyses_table,
        "analyses_status_filter": args.analyses_status_filter,
        "analyses_throttle_seconds": args.analyses_throttle_seconds,
        "analysis_id": args.analysis_id,
        "investigation_id": args.investigation_id,
        "dry_run": args.dry_run,
    }
    merged.update({k: v for k, v in cli_map.items() if v is not None})

    # Handle --no-daily-rotate flag
    if args.no_daily_rotate:
        merged["rotate_daily"] = False

    # Default mode if not specified anywhere
    if "mode" not in merged or merged["mode"] is None:
        merged["mode"] = "service"

    # Build Config object (only pass known fields)
    known_fields = {f.name for f in Config.__dataclass_fields__.values()}
    filtered = {k: v for k, v in merged.items() if k in known_fields}
    cfg = Config(**filtered)

    return cfg
