"""Host-owned adapter over shared workspace lease and snapshot operations."""

from __future__ import annotations

from typing import Any

from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.runtime_db import RuntimeCacheLedger
from data_engine.runtime.shared_state import (
    checkpoint_workspace_state as checkpoint_runtime_workspace_state,
    claim_workspace as claim_runtime_workspace,
    hydrate_local_runtime_state,
    initialize_workspace_state,
    lease_is_stale,
    read_control_request,
    read_lease_metadata,
    recover_stale_workspace,
    release_workspace,
    remove_control_request,
    remove_lease_metadata,
    write_control_request,
    write_lease_metadata,
)


class DaemonSharedStateAdapter:
    """Own host-facing access to shared lease, control-request, and snapshot state."""

    def initialize_workspace(self, paths: WorkspacePaths) -> None:
        initialize_workspace_state(paths)

    def claim_workspace(self, paths: WorkspacePaths) -> bool:
        return claim_runtime_workspace(paths)

    def release_workspace(self, paths: WorkspacePaths) -> None:
        release_workspace(paths)

    def recover_stale_workspace(
        self,
        paths: WorkspacePaths,
        *,
        machine_id: str,
        stale_after_seconds: float,
        reclaim: bool = True,
        ) -> bool:
        return recover_stale_workspace(
            paths,
            machine_id=machine_id,
            stale_after_seconds=stale_after_seconds,
            reclaim=reclaim,
        )

    def lease_is_stale(self, paths: WorkspacePaths, *, stale_after_seconds: float) -> bool:
        return lease_is_stale(paths, stale_after_seconds=stale_after_seconds)

    def hydrate_local_runtime(self, paths: WorkspacePaths, ledger: RuntimeCacheLedger) -> None:
        hydrate_local_runtime_state(paths, ledger)

    def checkpoint_workspace_state(
        self,
        paths: WorkspacePaths,
        ledger: RuntimeCacheLedger,
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
        checkpoint_runtime_workspace_state(
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
        metadata = read_lease_metadata(paths)
        return metadata if isinstance(metadata, dict) else None

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
        write_lease_metadata(
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
        remove_lease_metadata(paths)

    def read_control_request(self, paths: WorkspacePaths) -> dict[str, Any] | None:
        metadata = read_control_request(paths)
        return metadata if isinstance(metadata, dict) else None

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
        write_control_request(
            paths,
            workspace_id=workspace_id,
            requester_machine_id=requester_machine_id,
            requester_host_name=requester_host_name,
            requester_pid=requester_pid,
            requester_client_kind=requester_client_kind,
            requested_at_utc=requested_at_utc,
        )

    def remove_control_request(self, paths: WorkspacePaths) -> None:
        remove_control_request(paths)


__all__ = ["DaemonSharedStateAdapter"]
