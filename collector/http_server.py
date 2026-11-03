"""
HTTP listener for on‑demand artifact exports (Mode 3).

Exposes a single endpoint:

    POST /export
    Content-Type: application/json

    {
      "analysis_id": "<uuid>",
      "investigation_id": "<uuid>",   // optional
      "prefix": "custom_prefix"       // optional override
    }

The server runs in a thread‑based ``HTTPServer`` for simplicity.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from functools import partial
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg2

from collector.config import Config
from collector.db import connect_with_retry, close_connection
from collector.extractor import extract_by_analysis_id
from collector.skip_tracker import SkipTracker
from collector.writer import RotatingFileWriter, build_writers

log = logging.getLogger("collector.http_server")


class _RequestHandler(BaseHTTPRequestHandler):
    """Handle POST /export requests."""

    # Set by the factory so we can access shared state
    cfg: Config
    skip_tracker: SkipTracker

    def do_POST(self):  # noqa: N802  (HTTP handler naming convention)
        if self.path != "/export":
            self._respond(HTTPStatus.NOT_FOUND, {"error": "Not found. Use POST /export."})
            return

        # Optional API key authentication (only enforced if api_key is configured)
        if self.cfg.api_key:
            header_key = self.headers.get("X-API-Key", "")
            auth_header = self.headers.get("Authorization", "")
            bearer_key = auth_header[7:].strip() if auth_header.startswith("Bearer ") else ""
            if header_key != self.cfg.api_key and bearer_key != self.cfg.api_key:
                self._respond(
                    HTTPStatus.UNAUTHORIZED,
                    {"error": "Unauthorized: invalid or missing API key. Provide via X-API-Key or Bearer token."},
                )
                return

        try:
            body = self._read_body()
        except ValueError as exc:
            self._respond(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        analysis_id = body.get("analysis_id")
        if not analysis_id:
            self._respond(HTTPStatus.BAD_REQUEST, {"error": "Missing 'analysis_id'."})
            return

        # Validate UUID format
        try:
            uuid.UUID(str(analysis_id))
        except (ValueError, TypeError):
            self._respond(
                HTTPStatus.BAD_REQUEST,
                {"error": f"Invalid UUID format for 'analysis_id': {analysis_id}"},
            )
            return

        investigation_id = body.get("investigation_id")
        prefix_override = body.get("prefix")

        # Determine prefix
        prefix = prefix_override or self.cfg.prefix_3

        log.info(
            "HTTP export request: analysis_id=%s, investigation_id=%s, prefix=%s",
            analysis_id, investigation_id, prefix,
        )

        # Build a writer for this request
        writers = build_writers(
            self.cfg.output_dir, [prefix],
            self.cfg.max_file_size_mb, self.cfg.rotate_daily,
            self.cfg.flush_interval,
        )

        conn = None
        try:
            conn = connect_with_retry(self.cfg)
            total = extract_by_analysis_id(
                conn, analysis_id, writers, self.cfg,
                self.skip_tracker, investigation_id,
            )

            status = "completed" if total > 0 else "skipped"
            self._respond(HTTPStatus.OK, {
                "status": status,
                "analysis_id": analysis_id,
                "rows_exported": total,
            })

        except Exception as exc:
            log.error("Export failed for analysis_id=%s: %s", analysis_id, exc)
            self._respond(HTTPStatus.INTERNAL_SERVER_ERROR, {
                "error": f"Export failed: {exc}",
            })

        finally:
            close_connection(conn)
            for w in writers:
                w.close()

    def do_GET(self):  # noqa: N802
        """Health check endpoint."""
        if self.path == "/health":
            self._respond(HTTPStatus.OK, {"status": "ok"})
        else:
            self._respond(HTTPStatus.NOT_FOUND, {"error": "Not found."})

    # ── helpers ─────────────────────────────────────────────────────────

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            raise ValueError("Empty request body.")
        raw = self.rfile.read(length)
        return json.loads(raw)

    def _respond(self, status: HTTPStatus, payload: dict) -> None:
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def log_message(self, format, *args):  # noqa: A002
        """Route HTTP server logs through our logger."""
        log.debug(format, *args)


def start_http_server(cfg: Config, skip_tracker: SkipTracker) -> None:
    """
    Start the HTTP listener and block until interrupted.

    Parameters
    ----------
    cfg : Config
        Application configuration.
    skip_tracker : SkipTracker
        Shared skip tracker.
    """
    handler_class = _create_handler_class(cfg, skip_tracker)
    server = ThreadingHTTPServer((cfg.http_host, cfg.http_port), handler_class)

    log.info("HTTP listener started on %s:%d (multi-threaded)", cfg.http_host, cfg.http_port)
    log.info("  POST /export  – trigger an on-demand export")
    log.info("  GET  /health  – health check")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("HTTP listener shutting down.")
    finally:
        server.server_close()


def _create_handler_class(cfg: Config, skip_tracker: SkipTracker) -> type[_RequestHandler]:
    """Create a handler class with cfg and skip_tracker bound as class attributes."""
    class BoundHandler(_RequestHandler):
        pass

    BoundHandler.cfg = cfg
    BoundHandler.skip_tracker = skip_tracker
    return BoundHandler
