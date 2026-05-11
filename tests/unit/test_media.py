"""Unit tests for arachnite.media.MediaStore."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from arachnite.exceptions import PathTraversalError
from arachnite.media import MediaStore


@pytest.fixture
def store(tmp_path: Path) -> MediaStore:
    return MediaStore(base_dir=tmp_path / "media")


class TestMediaStoreStore:
    def test_store_returns_absolute_path(self, store: MediaStore) -> None:
        path = store.store(b"frame data", kind="camera", source="cam1")
        assert path.is_absolute()
        assert path.exists()

    def test_store_writes_correct_bytes(self, store: MediaStore) -> None:
        data = b"\x89PNG\r\n\x1a\n fake image"
        path = store.store(data, kind="image", source="cam1")
        assert path.read_bytes() == data

    def test_store_uses_kind_subdirectory(self, store: MediaStore) -> None:
        path = store.store(b"audio", kind="audio", source="mic")
        assert "audio" in path.parts

    def test_store_includes_tick_in_filename(self, store: MediaStore) -> None:
        path = store.store(b"data", kind="camera", source="cam1", tick=42)
        assert "tick42" in path.name

    def test_store_default_extension(self, store: MediaStore) -> None:
        path = store.store(b"frame", kind="camera", source="cam1")
        assert path.suffix == ".jpg"

    def test_store_custom_extension(self, store: MediaStore) -> None:
        path = store.store(b"frame", kind="camera", source="cam1",
                           extension=".png")
        assert path.suffix == ".png"

    def test_store_unknown_kind_defaults_to_bin(self, store: MediaStore) -> None:
        path = store.store(b"data", kind="unknown_sensor", source="s1")
        assert path.suffix == ".bin"

    def test_store_non_bytes_converted(self, store: MediaStore) -> None:
        path = store.store(42.5, kind="thermal", source="temp1")
        assert path.read_bytes() == b"42.5"

    def test_store_multiple_files_unique_names(self, store: MediaStore) -> None:
        p1 = store.store(b"a", kind="camera", source="cam1", tick=1)
        p2 = store.store(b"b", kind="camera", source="cam1", tick=2)
        assert p1 != p2
        assert p1.exists() and p2.exists()


class TestMediaStoreLoad:
    def test_load_roundtrip(self, store: MediaStore) -> None:
        data = b"binary payload \x00\xff"
        path = store.store(data, kind="camera", source="cam1")
        assert store.load(path) == data

    def test_load_accepts_string_path(self, store: MediaStore) -> None:
        path = store.store(b"data", kind="audio", source="mic")
        assert store.load(str(path)) == b"data"

    def test_load_nonexistent_raises(self, store: MediaStore) -> None:
        with pytest.raises(FileNotFoundError):
            store.load("/nonexistent/path.bin")


class TestMediaStoreCleanup:
    def test_cleanup_removes_old_files(self, store: MediaStore) -> None:
        path = store.store(b"old", kind="camera", source="cam1")
        # Backdate the file's mtime
        import os
        old_time = time.time() - 3600
        os.utime(path, (old_time, old_time))
        removed = store.cleanup(max_age_s=60)
        assert removed == 1
        assert not path.exists()

    def test_cleanup_keeps_recent_files(self, store: MediaStore) -> None:
        store.store(b"new", kind="camera", source="cam1")
        removed = store.cleanup(max_age_s=60)
        assert removed == 0
        assert store.file_count() == 1

    def test_clear_removes_everything(self, store: MediaStore) -> None:
        store.store(b"a", kind="camera", source="cam1")
        store.store(b"b", kind="audio", source="mic1")
        store.clear()
        assert store.file_count() == 0
        assert store.base_dir.exists()  # directory itself still exists


class TestMediaStoreIntrospection:
    def test_file_count_empty(self, store: MediaStore) -> None:
        assert store.file_count() == 0

    def test_file_count_after_stores(self, store: MediaStore) -> None:
        store.store(b"a", kind="camera", source="cam1")
        store.store(b"b", kind="audio", source="mic1")
        assert store.file_count() == 2

    def test_repr(self, store: MediaStore) -> None:
        r = repr(store)
        assert "MediaStore" in r
        assert "files=" in r

    def test_base_dir_property(self, store: MediaStore) -> None:
        assert store.base_dir.exists()


class TestMediaStoreCustomExtensions:
    def test_custom_extension_at_init(self, tmp_path: Path) -> None:
        store = MediaStore(
            base_dir=tmp_path / "media",
            extensions={"radar": ".radar"},
        )
        path = store.store(b"blip", kind="radar", source="radar1")
        assert path.suffix == ".radar"

    def test_custom_overrides_default(self, tmp_path: Path) -> None:
        store = MediaStore(
            base_dir=tmp_path / "media",
            extensions={"camera": ".tiff"},
        )
        path = store.store(b"frame", kind="camera", source="cam1")
        assert path.suffix == ".tiff"


class TestMediaStorePathTraversal:
    """B-10: Path traversal prevention in MediaStore.store()."""

    def test_valid_kind_and_source_succeed(self, store: MediaStore) -> None:
        path = store.store(b"frame data", kind="camera", source="CamSense")
        assert path.is_absolute()
        assert path.exists()

    def test_traversal_in_kind_rejected(self, store: MediaStore) -> None:
        with pytest.raises(PathTraversalError, match="Unsafe kind"):
            store.store(b"x", kind="../../etc", source="cam1")

    def test_traversal_in_source_rejected(self, store: MediaStore) -> None:
        with pytest.raises(PathTraversalError, match="Unsafe source"):
            store.store(b"x", kind="camera", source="../../../root")

    def test_slash_in_kind_rejected(self, store: MediaStore) -> None:
        with pytest.raises(PathTraversalError, match="Unsafe kind"):
            store.store(b"x", kind="foo/bar", source="cam1")

    def test_backslash_in_source_rejected(self, store: MediaStore) -> None:
        with pytest.raises(PathTraversalError, match="Unsafe source"):
            store.store(b"x", kind="camera", source="foo\\bar")

    def test_empty_kind_rejected(self, store: MediaStore) -> None:
        with pytest.raises(PathTraversalError, match="Unsafe kind"):
            store.store(b"x", kind="", source="cam1")

    def test_dots_only_rejected(self, store: MediaStore) -> None:
        with pytest.raises(PathTraversalError, match="Unsafe kind"):
            store.store(b"x", kind="..", source="cam1")

    def test_valid_names_with_dots_hyphens_underscores(
        self, store: MediaStore,
    ) -> None:
        path = store.store(b"depth data", kind="depth_v2", source="cam-01")
        assert path.is_absolute()
        assert path.exists()
