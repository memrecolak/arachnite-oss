"""
arachnite.media
~~~~~~~~~~~~~~~
MediaStore: lightweight on-disk storage for large signal payloads
(images, audio, video) so that only file paths travel through the
Signal → Instinct → Proposal → Decision → Action pipeline.

Typical usage inside a SenseNode::

    class CameraSense(BaseSenseNode):
        signal_kind = "camera"

        def __init__(self, bus, media: MediaStore, **kw):
            super().__init__(bus, **kw)
            self._media = media

        async def read(self) -> Signal:
            frame = await asyncio.to_thread(self._capture)
            path  = self._media.store(frame, kind=self.signal_kind,
                                      source=self.node_id)
            return Signal(source=self.node_id, kind=self.signal_kind,
                          value=str(path), confidence=1.0,
                          timestamp=time.monotonic(),
                          metadata={"media_path": str(path)})

Then in an InstinctNode::

    async def evaluate(self, ctx) -> Proposal | None:
        for sig in ctx.signals:
            if sig.kind == "camera":
                path = sig.metadata.get("media_path")
                summary = self._analyze(path)
                return Proposal(..., evidence={
                    "camera_path": path,
                    "camera_summary": summary,
                })
        return None

Spec reference: extends Section 3.1 (Signal payload patterns).
"""

from __future__ import annotations

import re
import shutil
import time
from pathlib import Path
from typing import Any

from arachnite.exceptions import PathTraversalError

_SAFE_NAME = re.compile(r"^[a-zA-Z0-9_][a-zA-Z0-9_.\-]*$")


def _validate_path_component(value: str, param_name: str) -> None:
    """Reject path components that could escape the media base directory"""
    if not _SAFE_NAME.match(value):
        raise PathTraversalError(
            f"Unsafe {param_name}: {value!r}. "
            f"Must match [a-zA-Z0-9_][a-zA-Z0-9_.\\-]*"
        )


class MediaStore:
    """
    Persist large binary payloads to disk and return stable file paths.

    Files are organised as ``base_dir / kind / source_tickN_timestamp.ext``
    so they are easy to browse and correlate with the tick that produced them.

    The store is not a database — it is a simple directory tree.  Cleanup
    is explicit via ``cleanup()`` or ``clear()``.
    """

    #: Default file extensions per signal kind.  Override or extend via
    #: the ``extensions`` constructor parameter.
    DEFAULT_EXTENSIONS: dict[str, str] = {
        "camera":     ".jpg",
        "visual":     ".jpg",
        "image":      ".png",
        "audio":      ".wav",
        "microphone": ".wav",
        "video":      ".mp4",
        "lidar":      ".bin",
        "depth":      ".bin",
    }

    def __init__(
        self,
        base_dir: str | Path = "arachnite_media",
        extensions: dict[str, str] | None = None,
    ) -> None:
        self._base = Path(base_dir)
        self._extensions: dict[str, str] = {
            **self.DEFAULT_EXTENSIONS,
            **(extensions or {}),
        }
        self._base.mkdir(parents=True, exist_ok=True)

    # ── Storage ──────────────────────────────────────────────────────────────

    def store(
        self,
        data: bytes | Any,
        kind: str,
        source: str,
        tick: int | None = None,
        extension: str | None = None,
    ) -> Path:
        """
        Write *data* to disk and return the absolute path.

        Parameters
        ----------
        data
            Raw bytes to write.  If not ``bytes``, the value is converted
            via ``str(data).encode()``.
        kind
            Signal kind (e.g. ``"camera"``).  Used as the subdirectory
            name and to look up the default file extension.
        source
            node_id of the producing SenseNode.
        tick
            Current tick number (optional, included in the filename for
            easy correlation).
        extension
            File extension override.  If ``None`` the store looks up
            ``kind`` in the extensions dict, falling back to ``.bin``.
        """
        _validate_path_component(kind, "kind")
        _validate_path_component(source, "source")

        if not isinstance(data, bytes):
            data = str(data).encode()

        ext = extension or self._extensions.get(kind, ".bin")
        kind_dir = self._base / kind
        kind_dir.mkdir(parents=True, exist_ok=True)

        tick_part = f"_tick{tick}" if tick is not None else ""
        ts = f"{time.monotonic():.4f}".replace(".", "_")
        filename = f"{source}{tick_part}_{ts}{ext}"
        path = kind_dir / filename

        # Defense-in-depth: verify resolved path stays under base directory
        resolved = path.resolve()
        base_resolved = self._base.resolve()
        if not str(resolved).startswith(str(base_resolved)):
            raise PathTraversalError(
                f"Resolved path {resolved} escapes base directory {base_resolved}"
            )

        path.write_bytes(data)
        return resolved

    def load(self, path: str | Path) -> bytes:
        """Read raw bytes from a previously stored file."""
        return Path(path).read_bytes()

    # ── Cleanup ──────────────────────────────────────────────────────────────

    def cleanup(self, max_age_s: float) -> int:
        """
        Remove files older than *max_age_s* seconds.
        Returns the number of files removed.
        """
        cutoff = time.time() - max_age_s
        removed = 0
        for p in self._base.rglob("*"):
            if p.is_file() and p.stat().st_mtime < cutoff:
                p.unlink(missing_ok=True)
                removed += 1
        # Remove empty subdirectories
        for d in sorted(self._base.rglob("*"), reverse=True):
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()
        return removed

    def clear(self) -> None:
        """Remove all stored files and subdirectories."""
        if self._base.exists():
            shutil.rmtree(self._base)
            self._base.mkdir(parents=True, exist_ok=True)

    # ── Introspection ────────────────────────────────────────────────────────

    @property
    def base_dir(self) -> Path:
        return self._base

    def file_count(self) -> int:
        """Return total number of stored files."""
        return sum(1 for p in self._base.rglob("*") if p.is_file())

    def __repr__(self) -> str:
        return f"MediaStore(base_dir={self._base!r}, files={self.file_count()})"
