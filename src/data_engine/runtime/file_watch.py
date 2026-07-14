"""Filesystem discovery and polling services used by the flow runtime."""

from __future__ import annotations

from pathlib import Path
from time import monotonic
from typing import Iterable, Protocol, runtime_checkable

from data_engine.core.model import FlowValidationError
from data_engine.platform.paths import normalized_path_text, path_sort_key


def _normalized_name(value: str) -> str:
    """Normalize a filename for case-insensitive temporary-file checks."""
    return normalized_path_text(value).casefold()


def _queue_key(path: Path) -> str:
    """Return a stable sort key for a filesystem path."""
    return path_sort_key(path)


def _normalize_extensions(extensions: tuple[str, ...] | None) -> tuple[str, ...] | None:
    """Normalize extension filters to lowercase dotted forms."""
    if extensions is None:
        return None
    normalized: list[str] = []
    for ext in extensions:
        value = str(ext).strip().lower()
        if not value:
            raise FlowValidationError("Empty extension is not allowed.")
        if not value.startswith("."):
            value = f".{value}"
        normalized.append(value)
    return tuple(normalized)


def is_temporary_file_path(path: Path) -> bool:
    """Return whether a path looks like a transient temp or download file."""
    name = _normalized_name(path.name)
    if name.startswith(".~lock.") and name.endswith("#"):
        return True
    if name.startswith(".") or name.startswith("~$") or name.startswith("._"):
        return True
    if name.endswith("~"):
        return True
    if any(name.endswith(suffix) for suffix in (".tmp", ".temp", ".part", ".partial", ".crdownload", ".download", ".swp")):
        return True
    return False


def iter_candidate_paths(
    input_root: Path,
    *,
    extensions: tuple[str, ...] | None = None,
    recursive: bool = True,
    allow_missing: bool = False,
) -> Iterable[Path]:
    """Yield candidate files from one root using optional extension filters."""
    if not input_root.exists():
        if allow_missing:
            return
        raise FlowValidationError(f"Input root not found: {input_root}")

    normalized_extensions = _normalize_extensions(extensions)
    if input_root.is_file():
        candidates: Iterable[Path] = (input_root,)
    else:
        globber = input_root.rglob("*") if recursive else input_root.glob("*")
        candidates = sorted(globber, key=_queue_key)

    for path in candidates:
        if not path.is_file():
            continue
        if normalized_extensions is not None and path.suffix.lower() not in normalized_extensions:
            continue
        if is_temporary_file_path(path):
            continue
        yield path


@runtime_checkable
class IFileWatcher(Protocol):
    """Common interface for runtime file watchers."""

    def start(self) -> None:
        """Begin watching for filesystem changes."""

    def stop(self) -> None:
        """Stop watching for filesystem changes."""

    def drain_events(self) -> list[Path]:
        """Return any queued filesystem events observed since the last drain."""


class PollingWatcher:
    """Filesystem polling watcher for one file or directory root."""

    def __init__(
        self,
        input_root: Path,
        *,
        recursive: bool = True,
        extensions: tuple[str, ...] | None = None,
        settle: int = 1,
    ) -> None:
        if settle < 0:
            raise FlowValidationError("settle must be zero or greater.")
        self.input_root = input_root
        self.recursive = recursive
        self.extensions = _normalize_extensions(extensions)
        self.settle = settle
        self._seen: dict[Path, tuple[int, int, int]] = {}
        self._pending_since: dict[Path, float] = {}
        self._emitted: dict[Path, tuple[int, int, int]] = {}
        self._running = False

    def start(self) -> None:
        """Capture an initial filesystem snapshot and begin watching."""
        self._seen = self._snapshot()
        self._pending_since = {}
        self._emitted = dict(self._seen)
        self._running = True

    def stop(self) -> None:
        """Stop watching for new filesystem events."""
        self._running = False

    def drain_events(self) -> list[Path]:
        """Return newly stable files observed since the last poll."""
        if not self._running:
            return []

        current = self._snapshot()
        now = monotonic()
        events: list[Path] = []
        pending_since: dict[Path, float] = {}

        for path, signature in current.items():
            if self._emitted.get(path) == signature:
                continue

            first_observed_at = self._pending_since.get(path)
            if self._seen.get(path) != signature or first_observed_at is None:
                first_observed_at = now
            if now - first_observed_at < self.settle:
                pending_since[path] = first_observed_at
                continue
            events.append(path)
            self._emitted[path] = signature

        self._pending_since = pending_since
        self._seen = current
        return events

    def _snapshot(self) -> dict[Path, tuple[int, int, int]]:
        """Capture the current file signatures for all candidate paths."""
        result: dict[Path, tuple[int, int, int]] = {}
        for path in iter_candidate_paths(self.input_root, extensions=self.extensions, recursive=self.recursive, allow_missing=True):
            try:
                stat = path.stat()
                result[path] = (
                    stat.st_mtime_ns,
                    stat.st_size,
                    getattr(stat, "st_ctime_ns", 0),
                )
            except FileNotFoundError:
                continue

        self._prune_removed_paths(result)
        return result

    def _prune_removed_paths(self, current: dict[Path, tuple[int, int, int]]) -> None:
        """Drop removed paths from retained emitted-signature state."""
        removed_paths = [path for path in self._emitted if path not in current]
        for path in removed_paths:
            del self._emitted[path]


__all__ = ["IFileWatcher", "PollingWatcher", "iter_candidate_paths", "is_temporary_file_path"]
