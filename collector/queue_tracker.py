"""
Analysis Queue Persistence for Volcano Artifact Collector.

Manages the queue of analyses to be exported:
- Tracks pending analyses, completed analyses, and failed attempts.
- Persists state to a JSON file (atomic write via rename) for crash resilience.
- Enables resuming Day 0 backfills without duplicating already extracted analyses.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger("collector.queue_tracker")

_DEFAULT_QUEUE_FILENAME = "analyses_queue.json"


class AnalysisQueueTracker:
    """
    Persistent queue for tracking and processing analyses 1-by-1.

    Parameters
    ----------
    directory : str
        Directory where the queue file lives.
    filename : str
        Name of the queue JSON file.
    max_retries : int
        Max retry attempts before an analysis is permanently marked failed.
    """

    def __init__(
        self,
        directory: str,
        filename: str = _DEFAULT_QUEUE_FILENAME,
        max_retries: int = 3,
    ) -> None:
        self._dir = Path(directory)
        self._path = self._dir / filename
        self._max_retries = max(1, max_retries)

        self._pending: list[dict[str, Any]] = []
        self._pending_ids: set[str] = set()
        self._completed: dict[str, dict[str, Any]] = {}
        self._failed: dict[str, dict[str, Any]] = {}
        self._total_discovered: int = 0
        self._last_sync_time: str = ""

        self._load()

    # ── public methods ──────────────────────────────────────────────────

    @property
    def pending_count(self) -> int:
        """Return the number of analyses waiting in queue."""
        return len(self._pending)

    @property
    def completed_count(self) -> int:
        """Return the total number of successfully exported analyses."""
        return len(self._completed)

    @property
    def failed_count(self) -> int:
        """Return the number of analyses that permanently failed."""
        return len(self._failed)

    @property
    def total_discovered(self) -> int:
        """Return the total count of discovered analyses."""
        return self._total_discovered

    def enqueue(
        self,
        analysis_id: str,
        investigation_id: Optional[str] = None,
    ) -> bool:
        """
        Add a single analysis to the pending queue if not already known.

        Returns True if newly added, False if duplicate or already completed.
        """
        aid = str(analysis_id).strip()
        if not aid:
            return False

        if aid in self._completed or aid in self._pending_ids or aid in self._failed:
            return False

        item = {
            "analysis_id": aid,
            "investigation_id": str(investigation_id).strip() if investigation_id else "",
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        }
        self._pending.append(item)
        self._pending_ids.add(aid)
        self._total_discovered += 1
        return True

    def enqueue_batch(self, items: list[dict[str, Any]]) -> int:
        """
        Enqueue multiple analyses in bulk and persist state to disk.

        Returns the number of newly added items.
        """
        added = 0
        for it in items:
            aid = it.get("analysis_id", "")
            iid = it.get("investigation_id")
            if self.enqueue(aid, iid):
                added += 1

        if added > 0:
            self._last_sync_time = datetime.now(timezone.utc).isoformat()
            self.save()

        return added

    def pop_next(self) -> Optional[dict[str, Any]]:
        """
        Retrieve and remove the next pending analysis item (FIFO).

        Returns None if queue is empty.
        """
        if not self._pending:
            return None
        item = self._pending.pop(0)
        return item

    def mark_completed(self, analysis_id: str, rows_exported: int) -> None:
        """
        Mark an analysis as completed and remove from pending tracking.
        """
        aid = str(analysis_id).strip()
        self._pending_ids.discard(aid)
        self._completed[aid] = {
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "rows_exported": rows_exported,
        }
        self.save()
        log.debug("Queue: marked analysis_id=%s as COMPLETED (%d rows)", aid, rows_exported)

    def mark_failed(self, analysis_id: str, error_message: str) -> None:
        """
        Handle a failed analysis extraction.

        If retry count < max_retries, re-enqueues at the end of the pending queue.
        Otherwise, moves to the failed registry.
        """
        aid = str(analysis_id).strip()
        record = self._failed.get(aid, {"retries": 0, "errors": []})
        record["retries"] += 1
        record["errors"].append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(error_message),
        })

        if record["retries"] < self._max_retries:
            log.warning(
                "Queue: analysis_id=%s failed (attempt %d/%d). Re-enqueueing for retry.",
                aid, record["retries"], self._max_retries,
            )
            # Re-enqueue item at the back
            self._pending.append({
                "analysis_id": aid,
                "investigation_id": "",
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            })
            self._failed[aid] = record
        else:
            log.error(
                "Queue: analysis_id=%s permanently FAILED after %d attempts: %s",
                aid, record["retries"], error_message,
            )
            self._pending_ids.discard(aid)
            self._failed[aid] = record

        self.save()

    def save(self) -> None:
        """
        Atomically persist queue state to disk using a temporary file.
        """
        self._dir.mkdir(parents=True, exist_ok=True)
        data = {
            "total_discovered": self._total_discovered,
            "last_sync_time": self._last_sync_time,
            "pending": self._pending,
            "completed": self._completed,
            "failed": self._failed,
        }

        tmp_path = self._path.with_suffix(".tmp")
        try:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
            os.replace(tmp_path, self._path)
            log.debug("Queue saved to %s (pending: %d, completed: %d)", self._path, len(self._pending), len(self._completed))
        except OSError as exc:
            log.error("Failed to persist queue to %s: %s", self._path, exc)
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    # ── private methods ─────────────────────────────────────────────────

    def _load(self) -> None:
        """Load queue from disk or initialize empty state."""
        if not self._path.is_file():
            log.info("No existing queue at %s – starting fresh queue.", self._path)
            return

        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                data = json.load(fh)

            if isinstance(data, dict):
                self._total_discovered = int(data.get("total_discovered", 0))
                self._last_sync_time = str(data.get("last_sync_time", ""))
                self._pending = list(data.get("pending", []))
                self._pending_ids = {it["analysis_id"] for it in self._pending if "analysis_id" in it}
                self._completed = dict(data.get("completed", {}))
                self._failed = dict(data.get("failed", {}))
                log.info(
                    "Loaded queue from %s: %d pending, %d completed, %d failed",
                    self._path, len(self._pending), len(self._completed), len(self._failed),
                )
            else:
                log.warning("Invalid queue format in %s – starting fresh.", self._path)
        except Exception as exc:
            log.warning("Could not read queue file %s: %s – starting fresh.", self._path, exc)
