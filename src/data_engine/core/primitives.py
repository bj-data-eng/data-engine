"""Core flow specs, contexts, and small containers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
import tomllib
from typing import Callable, Generic, Iterator, TypeVar

from data_engine.core.helpers import _normalize_extensions, _resolve_flow_path
from data_engine.core.model import FlowValidationError
from data_engine.platform.workspace_models import WORKSPACE_CONFIG_DIR_NAME, WORKSPACE_DATABASES_DIR_NAME

T = TypeVar("T")


@dataclass(frozen=True)
class WatchSpec:
    """Normalized runtime watch configuration."""

    mode: str
    run_as: str
    source: Path | None = None
    interval: str | None = None
    interval_seconds: float | None = None
    time: str | tuple[str, ...] | None = None
    times: tuple[str, ...] = ()
    time_slots: tuple[tuple[int, int], ...] = ()
    extensions: tuple[str, ...] | None = None
    settle: int = 1


@dataclass(frozen=True)
class MirrorSpec:
    """Static flow-level mirror binding."""

    root: Path


@dataclass(frozen=True)
class StepSpec:
    """One generic callable step in a flow."""

    fn: Callable[..., object]
    use: str | None
    save_as: str | None
    label: str
    function_name: str


@dataclass(frozen=True)
class SourceMetadata:
    """Resolved filesystem metadata for the current source file."""

    path: Path
    name: str
    size_bytes: int
    modified_at_utc: datetime


@dataclass
class WorkspaceConfigContext:
    """Lazy read-only access to workspace-local TOML config files."""

    workspace_root: Path | None = None
    _cache: dict[str, dict[str, object]] = field(default_factory=dict)
    _names: tuple[str, ...] | None = None

    @property
    def config_dir(self) -> Path | None:
        """Return the conventional config directory for the authored workspace."""
        if self.workspace_root is None:
            return None
        return self.workspace_root / WORKSPACE_CONFIG_DIR_NAME

    def names(self) -> tuple[str, ...]:
        """Return available config file stems beneath config/."""
        if self._names is not None:
            return self._names
        config_dir = self.config_dir
        if config_dir is None or not config_dir.is_dir():
            self._names = ()
            return self._names
        self._names = tuple(
            path.stem
            for path in sorted(config_dir.glob("*.toml"))
            if path.is_file() and not path.name.startswith(".")
        )
        return self._names

    def get(self, name: str) -> dict[str, object] | None:
        """Return one parsed config mapping when available."""
        normalized_name = str(name).strip()
        if not normalized_name:
            raise FlowValidationError("config.get() name must be non-empty.")
        if normalized_name in self._cache:
            return dict(self._cache[normalized_name])
        config_dir = self.config_dir
        if config_dir is None:
            return None
        config_path = config_dir / f"{normalized_name}.toml"
        if not config_path.is_file():
            return None
        try:
            with config_path.open("rb") as handle:
                parsed = tomllib.load(handle)
        except tomllib.TOMLDecodeError as exc:
            raise FlowValidationError(f"Config file {config_path} is not valid TOML: {exc}") from exc
        self._cache[normalized_name] = parsed
        return dict(parsed)

    def require(self, name: str) -> dict[str, object]:
        """Return one parsed config mapping or fail loudly when missing."""
        parsed = self.get(name)
        if parsed is not None:
            return parsed
        config_dir = self.config_dir
        if config_dir is None:
            raise FlowValidationError("config.require() is only available for authored workspace flows.")
        raise FlowValidationError(f"Required config file was not found: {config_dir / f'{str(name).strip()}.toml'}")

    def all(self) -> dict[str, dict[str, object]]:
        """Return all parsed config mappings keyed by file stem."""
        return {name: self.require(name) for name in self.names()}


@dataclass(frozen=True)
class MirrorContext:
    """Write-ready mirrored output namespace for one runtime source."""

    root: Path
    source_path: Path | None = None
    relative_path: Path | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root).resolve())
        if self.source_path is not None:
            object.__setattr__(self, "source_path", Path(self.source_path).resolve())
        if self.relative_path is not None:
            object.__setattr__(self, "relative_path", Path(self.relative_path))

    def _prepare(self, path: Path) -> Path:
        resolved = path.resolve()
        resolved.parent.mkdir(parents=True, exist_ok=True)
        return resolved

    @property
    def dir(self) -> Path:
        """Return a write-ready namespace directory for derived files."""
        if self.source_path is None or self.relative_path is None:
            self.root.mkdir(parents=True, exist_ok=True)
            return self.root
        directory = self.root / self.relative_path.with_suffix("")
        directory.mkdir(parents=True, exist_ok=True)
        return directory.resolve()

    @property
    def folder(self) -> Path:
        """Return the mirrored parent folder for the current source file."""
        if self.relative_path is None:
            self.root.mkdir(parents=True, exist_ok=True)
            return self.root
        directory = self.root / self.relative_path.parent
        directory.mkdir(parents=True, exist_ok=True)
        return directory.resolve()

    def with_suffix(self, suffix: str) -> Path:
        """Return the canonical mirrored source path with a replaced suffix."""
        if self.source_path is None or self.relative_path is None:
            raise FlowValidationError("mirror.with_suffix() requires a concrete source file.")
        normalized_suffix = _normalize_extensions((suffix,))[0]
        return self._prepare((self.root / self.relative_path).with_suffix(normalized_suffix))

    def with_extension(self, suffix: str) -> Path:
        """Return the canonical mirrored source path with a replaced extension."""
        return self.with_suffix(suffix)

    def file(self, name: str | Path) -> Path:
        """Return a write-ready file path in the mirrored source folder."""
        candidate = Path(name)
        if candidate.is_absolute():
            raise FlowValidationError("mirror.file() name must be relative.")
        if not str(candidate).strip():
            raise FlowValidationError("mirror.file() name must be non-empty.")
        return self._prepare(self.folder / candidate)

    def namespaced_file(self, name: str | Path) -> Path:
        """Return a write-ready derived file path inside the mirrored source namespace."""
        candidate = Path(name)
        if candidate.is_absolute():
            raise FlowValidationError("mirror.namespaced_file() name must be relative.")
        if not str(candidate).strip():
            raise FlowValidationError("mirror.namespaced_file() name must be non-empty.")
        return self._prepare(self.dir / candidate)

    def root_file(self, name: str | Path) -> Path:
        """Return a write-ready file path directly beneath the mirror root."""
        candidate = Path(name)
        if candidate.is_absolute():
            raise FlowValidationError("mirror.root_file() name must be relative.")
        if not str(candidate).strip():
            raise FlowValidationError("mirror.root_file() name must be non-empty.")
        return self._prepare(self.root / candidate)


@dataclass(frozen=True)
class SourceContext:
    """Resolved source namespace for one runtime source."""

    root: Path
    path: Path | None = None
    relative_path: Path | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root).resolve())
        if self.path is not None:
            object.__setattr__(self, "path", Path(self.path).resolve())
        if self.relative_path is not None:
            object.__setattr__(self, "relative_path", Path(self.relative_path))

    @property
    def dir(self) -> Path:
        """Return the namespace directory for files derived from the active source."""
        if self.path is None or self.relative_path is None:
            return self.root
        return (self.root / self.relative_path.with_suffix("")).resolve()

    @property
    def folder(self) -> Path:
        """Return the parent folder for the active source file."""
        if self.relative_path is None:
            return self.root
        return (self.root / self.relative_path.parent).resolve()

    def with_suffix(self, suffix: str) -> Path:
        """Return the source path with a replaced suffix."""
        if self.path is None or self.relative_path is None:
            raise FlowValidationError("source.with_suffix() requires a concrete source file.")
        normalized_suffix = _normalize_extensions((suffix,))[0]
        return (self.root / self.relative_path).with_suffix(normalized_suffix).resolve()

    def with_extension(self, suffix: str) -> Path:
        """Return the source path with a replaced extension."""
        return self.with_suffix(suffix)

    def file(self, name: str | Path) -> Path:
        """Return a derived file path in the active source folder."""
        candidate = Path(name)
        if candidate.is_absolute():
            raise FlowValidationError("source.file() name must be relative.")
        if not str(candidate).strip():
            raise FlowValidationError("source.file() name must be non-empty.")
        return (self.folder / candidate).resolve()

    def namespaced_file(self, name: str | Path) -> Path:
        """Return a derived file path inside the active source namespace."""
        candidate = Path(name)
        if candidate.is_absolute():
            raise FlowValidationError("source.namespaced_file() name must be relative.")
        if not str(candidate).strip():
            raise FlowValidationError("source.namespaced_file() name must be non-empty.")
        if self.path is None or self.relative_path is None:
            raise FlowValidationError("source.namespaced_file() requires a concrete source file.")
        return (self.dir / candidate).resolve()

    def root_file(self, name: str | Path) -> Path:
        """Return a file path directly beneath the source root."""
        candidate = Path(name)
        if candidate.is_absolute():
            raise FlowValidationError("source.root_file() name must be relative.")
        if not str(candidate).strip():
            raise FlowValidationError("source.root_file() name must be non-empty.")
        return (self.root / candidate).resolve()


@dataclass
class FlowContext:
    """Mutable runtime state shared across steps during one flow execution."""

    flow_name: str
    group: str
    source: SourceContext | None = None
    mirror: MirrorContext | None = None
    current: object | None = None
    objects: dict[str, object] = field(default_factory=dict)
    metadata: dict[str, object] = field(default_factory=dict)
    config: WorkspaceConfigContext = field(default_factory=WorkspaceConfigContext)

    def source_metadata(self) -> SourceMetadata | None:
        """Return filesystem metadata for the current source file when available."""
        source_path = self.source.path if self.source is not None else None
        if source_path is None:
            return None
        stat = source_path.stat()
        return SourceMetadata(
            path=source_path,
            name=source_path.name,
            size_bytes=stat.st_size,
            modified_at_utc=datetime.fromtimestamp(stat.st_mtime, timezone.utc),
        )

    def database(self, name: str | Path) -> Path:
        """Return a write-ready path beneath the workspace databases directory."""
        if self.config.workspace_root is None:
            raise FlowValidationError("context.database() is only available for authored workspace flows.")
        candidate = Path(name)
        if candidate.is_absolute():
            raise FlowValidationError("context.database() name must be relative.")
        if not str(candidate).strip():
            raise FlowValidationError("context.database() name must be non-empty.")
        path = (self.config.workspace_root / WORKSPACE_DATABASES_DIR_NAME / candidate).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path


@dataclass(frozen=True)
class FileRef:
    """Thin runtime wrapper for one filesystem path in a batch-oriented flow."""

    path: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", Path(self.path).resolve())

    @property
    def name(self) -> str:
        """Return the file name including extension."""
        return self.path.name

    @property
    def stem(self) -> str:
        """Return the file name without extension."""
        return self.path.stem

    @property
    def suffix(self) -> str:
        """Return the file extension."""
        return self.path.suffix

    @property
    def parent(self) -> Path:
        """Return the parent directory."""
        return self.path.parent

    def exists(self) -> bool:
        """Return whether the referenced path currently exists."""
        return self.path.exists()

    def __fspath__(self) -> str:
        return str(self.path)

    def __str__(self) -> str:
        return str(self.path)


@dataclass(frozen=True)
class Batch(Generic[T]):
    """Small iterable runtime container used instead of exposing raw lists by default."""

    items: tuple[T, ...]

    def __iter__(self) -> Iterator[T]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index: int) -> T:
        return self.items[index]

    def names(self) -> tuple[str, ...]:
        """Return each item name when all items expose a string name."""
        names: list[str] = []
        for item in self.items:
            value = getattr(item, "name", None)
            if callable(value):
                value = value()
            if not isinstance(value, str):
                raise FlowValidationError("Batch item does not expose a usable name.")
            names.append(value)
        return tuple(names)

    def paths(self) -> tuple[Path, ...]:
        """Return each item path when all items expose a Path-valued path."""
        paths: list[Path] = []
        for item in self.items:
            value = getattr(item, "path", None)
            if not isinstance(value, Path):
                raise FlowValidationError("Batch item does not expose a usable path.")
            paths.append(value)
        return tuple(paths)


def collect_files(
    extensions: tuple[str, ...] | list[str] | set[str],
    *,
    root: str | Path | None = None,
    recursive: bool = False,
) -> Callable[[FlowContext], Batch[FileRef]]:
    """Return a step callable that collects matching files into a Batch of FileRef items."""
    normalized_extensions = _normalize_extensions(extensions)
    assert normalized_extensions is not None
    resolved_root = _resolve_flow_path(root) if root is not None else None

    def _collect(context: FlowContext) -> Batch[FileRef]:
        base = resolved_root
        if base is None and context.source is not None:
            base = context.source.root
        if base is None:
            raise FlowValidationError("collect_files() requires an explicit root or a flow context with source.")
        if not base.exists():
            return Batch(())
        matcher = base.rglob if recursive else base.glob
        items = tuple(
            FileRef(path)
            for path in sorted(matcher("*"))
            if path.is_file() and path.suffix.lower() in normalized_extensions
        )
        return Batch(items)

    return _collect


__all__ = [
    "Batch",
    "FileRef",
    "FlowContext",
    "MirrorContext",
    "MirrorSpec",
    "SourceContext",
    "SourceMetadata",
    "StepSpec",
    "WatchSpec",
    "WorkspaceConfigContext",
    "collect_files",
]
