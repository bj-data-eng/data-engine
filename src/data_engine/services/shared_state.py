"""Shared workspace lease metadata and runtime snapshot services."""

from __future__ import annotations

from typing import Any

from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.shared_state import (
    RuntimeSnapshotStore,
    WorkspaceUnavailableForResetError,
)
from data_engine.services.workspace_io import WorkspaceIoLayer, default_workspace_io_layer


class SharedStateService:
    """Own lease-based shared snapshot hydration for operator surfaces."""

    def __init__(self, *, workspace_io: WorkspaceIoLayer | None = None) -> None:
        self.workspace_io = workspace_io or default_workspace_io_layer()

    def hydrate_local_runtime(self, paths: WorkspacePaths, ledger: RuntimeSnapshotStore) -> bool:
        """Replace one local runtime ledger from the shared workspace snapshots."""
        return self.workspace_io.hydrate_local_runtime(paths, ledger)

    def read_lease_metadata(self, paths: WorkspacePaths) -> dict[str, Any] | None:
        """Return current workspace lease metadata, if present."""
        return self.workspace_io.read_lease_metadata(paths)

    def lease_is_stale(
        self,
        paths: WorkspacePaths,
        *,
        lease_token: str,
        stale_after_seconds: float,
    ) -> bool:
        """Return whether current workspace lease metadata is stale."""
        return self.workspace_io.lease_is_stale(
            paths,
            lease_token=lease_token,
            stale_after_seconds=stale_after_seconds,
        )

    def reset_flow_state(self, paths: WorkspacePaths, *, lease_token: str, flow_name: str) -> None:
        """Delete one flow's shared snapshot history and freshness state."""
        self.workspace_io.reset_flow_state(paths, lease_token=lease_token, flow_name=flow_name)

    def reset_workspace_state(self, paths: WorkspacePaths, *, lease_token: str) -> None:
        """Delete all shared coordination and snapshot state for one workspace."""
        self.workspace_io.reset_workspace_state(paths, lease_token=lease_token)

    def acquire_maintenance_lease(self, paths: WorkspacePaths) -> str:
        """Claim an available workspace exclusively for destructive maintenance."""
        lease_token = self.workspace_io.claim_workspace(paths)
        if lease_token is None:
            raise WorkspaceUnavailableForResetError(
                f"Workspace {paths.workspace_id!r} must be stopped and available before reset."
            )
        return lease_token

    def release_maintenance_lease(self, paths: WorkspacePaths, *, lease_token: str) -> None:
        """Release one exact maintenance lease after reset completes."""
        self.workspace_io.release_workspace(paths, lease_token=lease_token)

    def workspace_lease_operation(self, paths: WorkspacePaths, *, lease_token: str):
        """Return an exclusive guard for destructive maintenance under one token."""
        return self.workspace_io.workspace_lease_operation(paths, lease_token=lease_token)


__all__ = ["SharedStateService"]
