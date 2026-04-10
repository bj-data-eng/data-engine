"""Domain models for workspace selection and collection-root state."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Iterable
from data_engine.platform.workspace_models import path_display

if TYPE_CHECKING:
    from data_engine.platform.workspace_models import WorkspacePaths


@dataclass(frozen=True)
class WorkspaceRootState:
    """Workspace collection-root state for one operator surface."""

    effective_root: Path | None
    configured: bool = True
    override_root: Path | None = None

    @classmethod
    def from_paths(
        cls,
        workspace_paths: "WorkspacePaths",
        *,
        override_root: Path | None = None,
    ) -> "WorkspaceRootState":
        """Build one root-state value from resolved workspace paths and an override."""
        configured = bool(getattr(workspace_paths, "workspace_configured", True))
        return cls(
            effective_root=workspace_paths.workspace_collection_root if configured else None,
            configured=configured,
            override_root=override_root.resolve() if override_root is not None else None,
        )

    @property
    def using_override(self) -> bool:
        """Return whether a machine-local collection-root override is active."""
        return self.override_root is not None

    @property
    def input_text(self) -> str:
        """Return the text that should populate workspace-root controls."""
        if self.override_root is not None:
            return str(self.override_root)
        return path_display(self.effective_root, empty="")

    @property
    def status_text(self) -> str:
        """Return plain-language root-source status text for operator surfaces."""
        if not self.configured:
            return "Workspace folder is not configured."
        if self.override_root is not None:
            return f"Workspace folder: {self.override_root}"
        return f"Workspace folder: {path_display(self.effective_root, empty='')}"

    def with_override_root(self, override_root: Path | None) -> "WorkspaceRootState":
        """Return a copy with the override root replaced."""
        return replace(self, override_root=override_root.resolve() if override_root is not None else None)


@dataclass(frozen=True)
class WorkspaceSelectionState:
    """Workspace selection/discovery state for one operator surface."""

    current_workspace_id: str
    discovered_workspace_ids: tuple[str, ...] = ()

    @classmethod
    def from_paths(
        cls,
        workspace_paths: "WorkspacePaths",
        *,
        discovered_workspace_ids: Iterable[str] = (),
    ) -> "WorkspaceSelectionState":
        """Build one selection state from resolved paths and discovered ids."""
        return cls(
            current_workspace_id=workspace_paths.workspace_id,
            discovered_workspace_ids=tuple(discovered_workspace_ids),
        )

    @property
    def selector_enabled(self) -> bool:
        """Return whether the workspace selector should be interactive."""
        return bool(self.discovered_workspace_ids)

    @property
    def selector_options(self) -> tuple[str, ...]:
        """Return selector option ids in display order."""
        return self.discovered_workspace_ids

    def with_discovered_workspace_ids(self, workspace_ids: Iterable[str]) -> "WorkspaceSelectionState":
        """Return a copy with the discovered workspace ids replaced."""
        return replace(self, discovered_workspace_ids=tuple(workspace_ids))

    def with_current_workspace_id(self, workspace_id: str) -> "WorkspaceSelectionState":
        """Return a copy with the current selected workspace id replaced."""
        return replace(self, current_workspace_id=workspace_id)


@dataclass(frozen=True)
class WorkspaceSessionState:
    """Combined workspace selection and collection-root state."""

    root: WorkspaceRootState
    selection: WorkspaceSelectionState

    @classmethod
    def from_paths(
        cls,
        workspace_paths: "WorkspacePaths",
        *,
        override_root: Path | None = None,
        discovered_workspace_ids: Iterable[str] = (),
    ) -> "WorkspaceSessionState":
        """Build one workspace-session value from resolved paths and discovery state."""
        return cls(
            root=WorkspaceRootState.from_paths(workspace_paths, override_root=override_root),
            selection=WorkspaceSelectionState.from_paths(
                workspace_paths,
                discovered_workspace_ids=discovered_workspace_ids,
            ),
        )

    @property
    def workspace_collection_root_override(self) -> Path | None:
        """Return the active machine-local collection-root override, if any."""
        return self.root.override_root

    @property
    def discovered_workspace_ids(self) -> tuple[str, ...]:
        """Return discovered workspace ids for selector-like surfaces."""
        return self.selection.discovered_workspace_ids

    @property
    def current_workspace_id(self) -> str:
        """Return the currently selected workspace id."""
        return self.selection.current_workspace_id

    def with_paths(self, workspace_paths: "WorkspacePaths") -> "WorkspaceSessionState":
        """Return a copy rebound to a new resolved workspace path set."""
        return type(self).from_paths(
            workspace_paths,
            override_root=self.root.override_root,
            discovered_workspace_ids=self.selection.discovered_workspace_ids,
        )

    def with_override_root(self, override_root: Path | None) -> "WorkspaceSessionState":
        """Return a copy with the override root replaced."""
        return replace(self, root=self.root.with_override_root(override_root))

    def with_discovered_workspace_ids(self, workspace_ids: Iterable[str]) -> "WorkspaceSessionState":
        """Return a copy with the discovered workspace ids replaced."""
        return replace(self, selection=self.selection.with_discovered_workspace_ids(workspace_ids))

    def with_current_workspace_id(self, workspace_id: str) -> "WorkspaceSessionState":
        """Return a copy with the current workspace id replaced."""
        return replace(self, selection=self.selection.with_current_workspace_id(workspace_id))


__all__ = ["WorkspaceRootState", "WorkspaceSelectionState", "WorkspaceSessionState"]
