"""File tracking — content hash watermarking for change detection.

Tracks which YAML files have been processed and their content hashes,
so the daily pipeline only processes files that have actually changed.
"""

from __future__ import annotations

import hashlib
from typing import Optional


def compute_content_hash(content: str) -> str:
    """Compute a SHA-256 hash of the file content.

    Args:
        content: Raw file content as string.

    Returns:
        Hex-encoded SHA-256 hash.
    """
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


class FileTracker:
    """Tracks file content hashes to detect changes between pipeline runs.

    Usage:
        tracker = FileTracker.from_state(previous_state)  # load from DB
        if tracker.has_changed("file.yml", current_hash):
            process(file)
            tracker.mark_applied("file.yml", current_hash)
        save_to_db(tracker.get_state())  # persist for next run
    """

    def __init__(self):
        self._applied: dict[str, str] = {}

    @classmethod
    def from_state(cls, state: dict[str, str]) -> FileTracker:
        """Initialize from a previously saved state dict.

        Args:
            state: Dict mapping file_path → last_applied_content_hash.
        """
        tracker = cls()
        tracker._applied = dict(state)
        return tracker

    def has_changed(self, file_path: str, content_hash: str) -> bool:
        """Check if a file's content has changed since last applied.

        Args:
            file_path: Relative path to the YAML file.
            content_hash: Current SHA-256 hash of the file content.

        Returns:
            True if the file is new or its content has changed.
        """
        return self._applied.get(file_path) != content_hash

    def mark_applied(self, file_path: str, content_hash: str) -> None:
        """Record that a file has been successfully processed.

        Args:
            file_path: Relative path to the YAML file.
            content_hash: SHA-256 hash of the processed content.
        """
        self._applied[file_path] = content_hash

    def get_state(self) -> dict[str, str]:
        """Export current state for persistence.

        Returns:
            Dict mapping file_path → last_applied_content_hash.
        """
        return dict(self._applied)
