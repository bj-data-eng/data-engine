"""Domain models for documentation support state."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path


@dataclass(frozen=True)
class DocumentationSessionState:
    """Built documentation session state for one operator surface."""

    build_running: bool = False
    root_dir: Path | None = None

    @classmethod
    def empty(cls) -> "DocumentationSessionState":
        """Return the idle documentation state."""
        return cls()

    @property
    def available(self) -> bool:
        """Return whether built documentation is available."""
        return self.root_dir is not None

    def with_build_running(self, running: bool) -> "DocumentationSessionState":
        """Return a copy with the build-running flag replaced."""
        return replace(self, build_running=bool(running))

    def with_root_dir(self, root_dir: Path | None) -> "DocumentationSessionState":
        """Return a copy with the built-docs root replaced."""
        return replace(self, root_dir=root_dir)


@dataclass(frozen=True)
class WorkspaceSupportState:
    """Combined support state for one operator surface."""

    documentation: DocumentationSessionState

    @classmethod
    def empty(cls) -> "WorkspaceSupportState":
        """Return the idle workspace-support state."""
        return cls(documentation=DocumentationSessionState.empty())

    def with_documentation(self, documentation: DocumentationSessionState) -> "WorkspaceSupportState":
        """Return a copy with documentation state replaced."""
        return replace(self, documentation=documentation)


__all__ = [
    "DocumentationSessionState",
    "WorkspaceSupportState",
]
