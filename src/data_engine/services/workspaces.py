"""Workspace path and discovery services."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from data_engine.platform.workspace_models import DiscoveredWorkspace, WorkspacePaths
from data_engine.platform.workspace_policy import AppStatePolicy, RuntimeLayoutPolicy, WorkspaceDiscoveryPolicy


class WorkspaceService:
    """Own workspace discovery and path resolution through explicit collaborators."""

    def __init__(
        self,
        *,
        app_state_policy: AppStatePolicy | None = None,
        discovery_policy: WorkspaceDiscoveryPolicy | None = None,
        runtime_layout_policy: RuntimeLayoutPolicy | None = None,
        discover_workspaces_func: Callable[..., tuple[DiscoveredWorkspace, ...]] | None = None,
        resolve_workspace_paths_func: Callable[..., WorkspacePaths] | None = None,
    ) -> None:
        self._app_state_policy = app_state_policy or AppStatePolicy()
        self._discovery_policy = discovery_policy or WorkspaceDiscoveryPolicy(app_state_policy=self._app_state_policy)
        self._runtime_layout_policy = runtime_layout_policy or RuntimeLayoutPolicy(
            app_state_policy=self._app_state_policy,
            discovery_policy=self._discovery_policy,
        )
        self._discover_workspaces = discover_workspaces_func
        self._resolve_workspace_paths = resolve_workspace_paths_func

    def discover(
        self,
        *,
        app_root: Path | None = None,
        workspace_collection_root: Path | None = None,
    ) -> tuple[DiscoveredWorkspace, ...]:
        """Return discoverable workspaces for the current app and collection roots."""
        if self._discover_workspaces is None:
            return self._discovery_policy.discover(
                app_root=app_root,
                workspace_collection_root=workspace_collection_root,
            )
        return self._discover_workspaces(
            app_root=app_root,
            workspace_collection_root=workspace_collection_root,
        )

    def resolve_paths(
        self,
        *,
        workspace_id: str | None = None,
        workspace_root: Path | None = None,
        data_root: Path | None = None,
        workspace_collection_root: Path | None = None,
    ) -> WorkspacePaths:
        """Resolve one workspace path set with the current override-aware rules."""
        if self._resolve_workspace_paths is None:
            return self._runtime_layout_policy.resolve_paths(
                workspace_id=workspace_id,
                workspace_root=workspace_root,
                data_root=data_root,
                workspace_collection_root=workspace_collection_root,
            )
        return self._resolve_workspace_paths(
            workspace_id=workspace_id,
            workspace_root=workspace_root,
            data_root=data_root,
            workspace_collection_root=workspace_collection_root,
        )


__all__ = ["WorkspaceService"]
