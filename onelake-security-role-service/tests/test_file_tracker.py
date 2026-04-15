"""Tests for file tracking — content hash watermarking for change detection."""

import pytest
from onelake_security.file_tracker import compute_content_hash, FileTracker


class TestComputeContentHash:
    """SHA-256 content hashing for change detection."""

    def test_same_content_same_hash(self):
        h1 = compute_content_hash("hello world")
        h2 = compute_content_hash("hello world")
        assert h1 == h2

    def test_different_content_different_hash(self):
        h1 = compute_content_hash("hello")
        h2 = compute_content_hash("world")
        assert h1 != h2

    def test_empty_string_has_hash(self):
        h = compute_content_hash("")
        assert isinstance(h, str)
        assert len(h) > 0

    def test_whitespace_matters(self):
        h1 = compute_content_hash("hello")
        h2 = compute_content_hash("hello ")
        assert h1 != h2

    def test_returns_hex_string(self):
        h = compute_content_hash("test")
        assert all(c in "0123456789abcdef" for c in h)


class TestFileTracker:
    """Track which files have changed since last processing."""

    def test_new_file_is_changed(self):
        tracker = FileTracker()
        assert tracker.has_changed("role-defs/neuro.yml", "abc123") is True

    def test_same_hash_not_changed(self):
        tracker = FileTracker()
        tracker.mark_applied("role-defs/neuro.yml", "abc123")
        assert tracker.has_changed("role-defs/neuro.yml", "abc123") is False

    def test_different_hash_is_changed(self):
        tracker = FileTracker()
        tracker.mark_applied("role-defs/neuro.yml", "abc123")
        assert tracker.has_changed("role-defs/neuro.yml", "xyz789") is True

    def test_tracks_multiple_files(self):
        tracker = FileTracker()
        tracker.mark_applied("file1.yml", "hash1")
        tracker.mark_applied("file2.yml", "hash2")
        assert tracker.has_changed("file1.yml", "hash1") is False
        assert tracker.has_changed("file2.yml", "hash2") is False
        assert tracker.has_changed("file3.yml", "hash3") is True

    def test_get_state_returns_all_tracked(self):
        tracker = FileTracker()
        tracker.mark_applied("f1.yml", "h1")
        tracker.mark_applied("f2.yml", "h2")
        state = tracker.get_state()
        assert len(state) == 2
        assert state["f1.yml"] == "h1"

    def test_load_from_state(self):
        """Can initialize from a previously saved state dict."""
        state = {"f1.yml": "h1", "f2.yml": "h2"}
        tracker = FileTracker.from_state(state)
        assert tracker.has_changed("f1.yml", "h1") is False
        assert tracker.has_changed("f1.yml", "new") is True

    def test_update_overwrites_previous_hash(self):
        tracker = FileTracker()
        tracker.mark_applied("f1.yml", "old")
        tracker.mark_applied("f1.yml", "new")
        assert tracker.has_changed("f1.yml", "new") is False
        assert tracker.has_changed("f1.yml", "old") is True
