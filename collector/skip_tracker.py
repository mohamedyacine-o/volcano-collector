"""
Skip tracker – records analyses that returned zero artifact rows.

Every skipped ``analysis_id`` is appended to a dedicated log file
(``skipped_analyses.log``) so operators can audit which analyses
contained no data.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("collector.skip_tracker")

_DEFAULT_FILENAME = "skipped_analyses.log"


class SkipTracker:
    """
    Append‑only log of analyses that were skipped (no data).

    Parameters
    ----------
    directory : str
        Directory where the skip log lives.
    filename : str
        Name of the skip log file.
    """

    def __init__(self, directory: str, filename: str = _DEFAULT_FILENAME) -> None:
        self._path = Path(directory) / filename
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._count: int = 0

    def record_skip(self, analysis_id: str, reason: str = "no artifact data found") -> None:
        """
        Append a skip entry to the log file and emit a log warning.

        Parameters
        ----------
        analysis_id : str
            The UUID of the analysis that returned no rows.
        reason : str
            Human‑readable reason for the skip.
        """
        ts = datetime.now(timezone.utc).isoformat()
        entry = f"{ts} | {analysis_id} | SKIPPED | {reason}\n"

        try:
            with open(self._path, "a", encoding="utf-8") as fh:
                fh.write(entry)
            self._count += 1
            log.warning("SKIPPED analysis_id=%s – %s", analysis_id, reason)
        except OSError as exc:
            log.error("Failed to write skip entry: %s", exc)

    @property
    def total_skipped(self) -> int:
        """Return the number of skips recorded in this session."""
        return self._count

    @property
    def path(self) -> Path:
        """Return the path to the skip log file."""
        return self._path
