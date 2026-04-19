"""Host-owned adapter over workspace coordination and runtime snapshot operations."""

from __future__ import annotations

from typing import Any

from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.shared_state import RuntimeSnapshotStore
from data_engine.services.workspace_io import WorkspaceIoLayer, default_workspace_io_layer


class DaemonSharedStateAdapter:
    """Own host-facing access to shared lease, control-request, and snapshot state."""

    def __init__(
        self,
        *,
        workspace_io: WorkspaceIoLayer | None = None,
    ) -> None:
        self.workspace_io = workspace_io or default_workspace_io_layer()

    def initialize_workspace(self, paths: WorkspacePaths) -> None:
        self.workspace_io.initialize_workspace(paths)

    def claim_workspace(self, paths: WorkspacePaths) -> bool:
        return self.workspace_io.claim_workspace(paths)

    def release_workspace(self, paths: WorkspacePaths) -> None:
        self.workspace_io.release_workspace(paths)

    def recover_stale_workspace(
        self,
        paths: WorkspacePaths,
        *,
        machine_id: str,
        stale_after_seconds: float,
        reclaim: bool = True,
    ) -> bool:
        return self.workspace_io.recover_stale_workspace(
            paths,
            machine_id=machine_id,
            stale_after_seconds=stale_after_seconds,
            reclaim=reclaim,
        )

    def lease_is_stale(self, paths: WorkspacePaths, *, stale_after_seconds: float) -> bool:
        return self.workspace_io.lease_is_stale(paths, stale_after_seconds=stale_after_seconds)

    def hydrate_local_runtime(self, paths: WorkspacePaths, ledger: RuntimeSnapshotStore) -> None:
        self.workspace_io.hydrate_local_runtime(paths, ledger)

    def checkpoint_workspace_state(
        self,
        paths: WorkspacePaths,
        ledger: RuntimeSnapshotStore,
        *,
        workspace_id: str,
        machine_id: str,
        daemon_id: str,
        pid: int,
        status: str,
        started_at_utc: str,
        last_checkpoint_at_utc: str,
        app_version: str | None,
    ) -> None:
        self.workspace_io.checkpoint_workspace_state(
            paths,
            ledger,
            workspace_id=workspace_id,
            machine_id=machine_id,
            daemon_id=daemon_id,
            pid=pid,
            status=status,
            started_at_utc=started_at_utc,
            last_checkpoint_at_utc=last_checkpoint_at_utc,
            app_version=app_version,
        )

    def read_lease_metadata(self, paths: WorkspacePaths) -> dict[str, Any] | None:
        return self.workspace_io.read_lease_metadata(paths)

    def write_lease_metadata(
        self,
        paths: WorkspacePaths,
        *,
        workspace_id: str,
        machine_id: str,
        daemon_id: str,
        pid: int,
        status: str,
        started_at_utc: str,
        last_checkpoint_at_utc: str,
        app_version: str | None,
    ) -> None:
        self.workspace_io.write_lease_metadata(
            paths,
            workspace_id=workspace_id,
            machine_id=machine_id,
            daemon_id=daemon_id,
            pid=pid,
            status=status,
            started_at_utc=started_at_utc,
            last_checkpoint_at_utc=last_checkpoint_at_utc,
            app_version=app_version,
        )

    def remove_lease_metadata(self, paths: WorkspacePaths) -> None:
        self.workspace_io.remove_lease_metadata(paths)

    def read_control_request(self, paths: WorkspacePaths) -> dict[str, Any] | None:
        return self.workspace_io.read_control_request(paths)

    def write_control_request(
        self,
        paths: WorkspacePaths,
        *,
        workspace_id: str,
        requester_machine_id: str,
        requester_host_name: str,
        requester_pid: int,
        requester_client_kind: str,
        requested_at_utc: str,
    ) -> None:
        self.workspace_io.write_control_request(
            paths,
            workspace_id=workspace_id,
            requester_machine_id=requester_machine_id,
            requester_host_name=requester_host_name,
            requester_pid=requester_pid,
            requester_client_kind=requester_client_kind,
            requested_at_utc=requested_at_utc,
        )

    def remove_control_request(self, paths: WorkspacePaths) -> None:
        self.workspace_io.remove_control_request(paths)


__all__ = ["DaemonSharedStateAdapter"]
