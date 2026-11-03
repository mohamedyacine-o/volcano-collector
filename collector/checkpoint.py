"""
Checkpoint persistence for incremental artifact extraction.

Stores the last-seen primary‑key (``id``) per artifact table in a JSON
file so that subsequent runs only fetch new rows.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger("collector.checkpoint")

_DEFAULT_FILENAME = "artifact_state.json"


class Checkpoint:
    """
    Read / write a JSON checkpoint mapping ``table_name → last_seen_id``.

    Parameters
    ----------
    directory : str
        Directory where the checkpoint file lives.
    filename : str
        Name of the checkpoint JSON file.
    """

    def __init__(self, directory: str, filename: str = _DEFAULT_FILENAME) -> None:
        self._path = Path(directory) / filename
        self._state: dict[str, int] = {}
        self._load()

    # ── public ──────────────────────────────────────────────────────────

    def get_last_id(self, table_name: str) -> int:
        """Return the last‑seen ID for *table_name*, or ``0`` if unknown."""
        return self._state.get(table_name, 0)

    def update(self, table_name: str, last_id: int) -> None:
        """Update the last‑seen ID for *table_name* (does **not** flush)."""
        self._state[table_name] = last_id

    def save(self) -> None:
        """Persist the current state to disk."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._path, "w", encoding="utf-8") as fh:
                json.dump(self._state, fh, indent=2, sort_keys=True)
            log.debug("Checkpoint saved to %s", self._path)
        except OSError as exc:
            log.error("Failed to save checkpoint: %s", exc)

    def reset(self) -> None:
        """Clear all checkpoint data (in memory; call ``save()`` to persist)."""
        self._state.clear()
        log.info("Checkpoint state cleared.")

    @property
    def state(self) -> dict[str, int]:
        """Return a copy of the current state dict."""
        return dict(self._state)

    # ── private ─────────────────────────────────────────────────────────

    def _load(self) -> None:
        """Load existing checkpoint from disk, or start fresh."""
        if not self._path.is_file():
            log.info("No existing checkpoint at %s – starting fresh.", self._path)
            return

        try:
            with open(self._path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                self._state = {k: int(v) for k, v in data.items()}
                log.info(
                    "Loaded checkpoint with %d table(s) from %s",
                    len(self._state), self._path,
                )
            else:
                log.warning("Invalid checkpoint format in %s – starting fresh.", self._path)
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("Could not read checkpoint %s: %s – starting fresh.", self._path, exc)
