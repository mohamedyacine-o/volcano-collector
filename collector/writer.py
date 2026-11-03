"""
Rotating file writer for NDJSON output.

Handles:
- Size‑based rotation (``max_file_size_mb``).
- Daily rotation (at midnight UTC).
- Sequential file naming: ``{prefix}_{YYYYMMDD}_{seq}.jsonl``.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("collector.writer")


class RotatingFileWriter:
    """
    Writes lines to a rotating NDJSON file.

    Parameters
    ----------
    output_dir : str
        Directory where files are created.
    prefix : str
        File name prefix (e.g., ``"volcano_artifacts"``).
    max_size_mb : int
        Maximum file size in MiB before rotation.  ``0`` disables.
    rotate_daily : bool
        If ``True``, start a new file at the first write after midnight UTC.
    """

    def __init__(
        self,
        output_dir: str,
        prefix: str,
        max_size_mb: int = 100,
        rotate_daily: bool = True,
        flush_interval: int = 1000,
    ) -> None:
        self._output_dir = Path(output_dir)
        self._prefix = prefix
        self._max_size_bytes = max_size_mb * 1024 * 1024 if max_size_mb > 0 else 0
        self._rotate_daily = rotate_daily
        self._flush_interval = max(1, flush_interval)

        self._current_date: str = ""
        self._seq: int = 0
        self._fh = None
        self._current_size: int = 0
        self._total_lines: int = 0
        self._lines_since_flush: int = 0

        # Ensure the output directory exists
        self._output_dir.mkdir(parents=True, exist_ok=True)

    # ── public ──────────────────────────────────────────────────────────

    def write_line(self, line: str) -> None:
        """
        Write a single line (without trailing newline) to the current file.

        Triggers rotation if needed before writing.
        """
        today = datetime.now(timezone.utc).strftime("%Y%m%d")

        if self._should_rotate(today):
            self._rotate(today)

        data = line + "\n"
        encoded = data.encode("utf-8")
        self._fh.write(data)
        self._current_size += len(encoded)
        self._total_lines += 1
        self._lines_since_flush += 1

        # Flush every N lines instead of every line (throughput optimisation)
        if self._lines_since_flush >= self._flush_interval:
            self._fh.flush()
            self._lines_since_flush = 0

    def close(self) -> None:
        """Flush and close the current file handle."""
        if self._fh and not self._fh.closed:
            self._fh.flush()
            self._lines_since_flush = 0
            self._fh.close()
            log.info(
                "Closed file for prefix '%s' (total lines written: %d)",
                self._prefix, self._total_lines,
            )

    @property
    def total_lines(self) -> int:
        """Return the total number of lines written across all rotations."""
        return self._total_lines

    @property
    def current_path(self) -> Path | None:
        """Return the path of the currently open file, or ``None``."""
        if self._fh and not self._fh.closed:
            return Path(self._fh.name)
        return None

    # ── private ─────────────────────────────────────────────────────────

    def _should_rotate(self, today: str) -> bool:
        """Decide whether a rotation is needed."""
        # No file open yet
        if self._fh is None or self._fh.closed:
            return True

        # Daily rotation
        if self._rotate_daily and today != self._current_date:
            return True

        # Size rotation
        if self._max_size_bytes and self._current_size >= self._max_size_bytes:
            return True

        return False

    def _rotate(self, today: str) -> None:
        """Close the current file (if any) and open a new one."""
        self.close()

        # Reset sequence counter on a new day
        if today != self._current_date:
            self._seq = 0
            self._current_date = today

        self._seq += 1
        filename = f"{self._prefix}_{self._current_date}_{self._seq:03d}.jsonl"
        filepath = self._output_dir / filename

        self._fh = open(filepath, "a", encoding="utf-8")
        self._fh.seek(0, 2)  # seek to end
        self._current_size = self._fh.tell()
        self._lines_since_flush = 0

        log.info("Opened new output file: %s", filepath)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def build_writers(
    output_dir: str,
    prefixes: list[str],
    max_size_mb: int,
    rotate_daily: bool,
    flush_interval: int = 1000,
) -> list[RotatingFileWriter]:
    """
    Create one ``RotatingFileWriter`` per *unique* prefix.

    Parameters
    ----------
    output_dir : str
        Directory for output files.
    prefixes : list[str]
        List of prefixes (duplicates are collapsed).
    max_size_mb : int
        Max file size in MiB.
    rotate_daily : bool
        Enable daily rotation.

    Returns
    -------
    list[RotatingFileWriter]
        One writer per unique prefix.
    """
    unique = list(dict.fromkeys(p.strip() for p in prefixes if p.strip()))
    if not unique:
        unique = ["volcano_artifacts"]
        log.warning("No valid prefixes provided – falling back to 'volcano_artifacts'.")

    writers = []
    for pfx in unique:
        w = RotatingFileWriter(output_dir, pfx, max_size_mb, rotate_daily, flush_interval)
        writers.append(w)
        log.info("Writer created: prefix='%s', dir='%s'", pfx, output_dir)

    if len(unique) == 1:
        log.info("All prefixes resolved to a single stream: '%s'", unique[0])
    else:
        log.info("Using %d distinct output streams: %s", len(unique), unique)

    return writers
