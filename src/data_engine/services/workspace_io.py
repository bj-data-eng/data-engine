"""Process-wide shared workspace IO layer for shared-state reads and writes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import RLock
from typing import Any

from data_engine.platform.processes import ProcessIdentity
from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.shared_state import (
    RuntimeSnapshotStore,
    assert_workspace_lease,
    checkpoint_workspace_state as checkpoint_runtime_workspace_state,
    claim_daemon_workspace as claim_runtime_daemon_workspace,
    claim_workspace as claim_runtime_workspace,
    hydrate_local_runtime_state,
    initialize_workspace_state,
    lease_is_stale,
    read_control_request,
    read_lease_metadata,
    read_runtime_snapshot_generation,
    recover_stale_workspace,
    release_workspace,
    remove_control_request,
    remove_lease_metadata,
    resolve_workspace_bundle,
    reset_flow_state,
    reset_workspace_state,
    write_control_request,
    write_lease_metadata,
    workspace_lease_operation as runtime_workspace_lease_operation,
)


@dataclass(frozen=True)
class _CachedRow:
    expires_at: datetime
    file_signature: object
    row: dict[str, Any] | None


@dataclass(frozen=True)
class _HydrationState:
    last_hydrated_at: datetime
    ledger_incarnation: str
    snapshot_generation_id: str | None
    manifest_signature: object


@dataclass(frozen=True)
class _CheckpointState:
    change_token: object
    snapshot_generation_id: str


class WorkspaceIoLayer:
    """Own shared workspace parquet reads, writes, cache invalidation, and hydration cadence."""

    def __init__(
        self,
        *,
        read_interval_seconds: float = 1.0,
        hydrate_interval_seconds: float = 1.0,
    ) -> None:
        self.read_interval_seconds = max(float(read_interval_seconds), 0.0)
        self.hydrate_interval_seconds = max(float(hydrate_interval_seconds), 0.0)
        self._lock = RLock()
        self._lease_cache: dict[str, _CachedRow] = {}
        self._control_cache: dict[str, _CachedRow] = {}
        self._hydration_state: dict[str, _HydrationState] = {}
        self._checkpoint_state: dict[str, _CheckpointState] = {}

    @staticmethod
    def _file_signature(path) -> tuple[bool, int | None]:
        try:
            stat = path.stat()
        except FileNotFoundError:
            return (False, None)
        return (True, stat.st_mtime_ns)

    def _cache_read(
        self,
        *,
        cache: dict[str, _CachedRow],
        cache_key: str,
        signature,
        reader,
    ) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        file_signature = signature()
        with self._lock:
            cached = cache.get(cache_key)
            if cached is not None and cached.expires_at >= now and cached.file_signature == file_signature:
                return None if cached.row is None else dict(cached.row)
        row = reader()
        normalized = row if isinstance(row, dict) else None
        with self._lock:
            cache[cache_key] = _CachedRow(
                expires_at=now + timedelta(seconds=self.read_interval_seconds),
                file_signature=file_signature,
                row=None if normalized is None else dict(normalized),
            )
        return None if normalized is None else dict(normalized)

    def _invalidate_workspace(self, paths: WorkspacePaths, *, preserve_checkpoint: bool = False) -> None:
        lease_key = str(paths.workspace_root)
        control_key = str(paths.control_request_path)
        workspace_key = str(paths.workspace_root)
        with self._lock:
            self._lease_cache.pop(lease_key, None)
            self._control_cache.pop(control_key, None)
            self._hydration_state.pop(workspace_key, None)
            if not preserve_checkpoint:
                self._checkpoint_state.pop(workspace_key, None)

    def initialize_workspace(self, paths: WorkspacePaths) -> None:
        initialize_workspace_state(paths)

    def claim_workspace(self, paths: WorkspacePaths) -> str | None:
        lease_token = claim_runtime_workspace(paths)
        if lease_token is not None:
            self._invalidate_workspace(paths)
        return lease_token

    def claim_daemon_workspace(
        self,
        paths: WorkspacePaths,
        *,
        workspace_id: str,
        machine_id: str,
        host_name: str,
        daemon_id: str,
        pid: int,
        process_identity: ProcessIdentity,
        containment_nonce: str,
        status: str,
        started_at_utc: str,
        last_checkpoint_at_utc: str,
        app_version: str | None,
    ) -> str | None:
        """Atomically claim with the complete daemon containment identity."""
        lease_token = claim_runtime_daemon_workspace(
            paths,
            workspace_id=workspace_id,
            machine_id=machine_id,
            host_name=host_name,
            daemon_id=daemon_id,
            pid=pid,
            process_identity=process_identity,
            containment_nonce=containment_nonce,
            status=status,
            started_at_utc=started_at_utc,
            last_checkpoint_at_utc=last_checkpoint_at_utc,
            app_version=app_version,
        )
        if lease_token is not None:
            self._invalidate_workspace(paths)
        return lease_token

    def release_workspace(self, paths: WorkspacePaths, *, lease_token: str) -> None:
        release_workspace(paths, lease_token=lease_token)
        self._invalidate_workspace(paths)

    def assert_workspace_lease(self, paths: WorkspacePaths, *, lease_token: str) -> None:
        """Raise when one immutable lease token is no longer current."""
        assert_workspace_lease(paths, lease_token)

    def workspace_lease_operation(self, paths: WorkspacePaths, *, lease_token: str):
        """Return an exclusive guard for one rare destructive owner operation."""
        return runtime_workspace_lease_operation(paths, lease_token=lease_token)

    def recover_stale_workspace(
        self,
        paths: WorkspacePaths,
        *,
        lease_token: str,
        machine_id: str,
        stale_after_seconds: float,
    ) -> bool:
        recovered = recover_stale_workspace(
            paths,
            lease_token=lease_token,
            machine_id=machine_id,
            stale_after_seconds=stale_after_seconds,
        )
        if recovered:
            self._invalidate_workspace(paths)
        return recovered

    def hydrate_local_runtime(self, paths: WorkspacePaths, ledger: RuntimeSnapshotStore) -> bool:
        workspace_key = str(paths.workspace_root)
        now = datetime.now(UTC)
        ledger_incarnation = ledger.snapshot_incarnation()
        manifest_signature = self._snapshot_manifest_signature(paths)
        snapshot_generation_id = read_runtime_snapshot_generation(paths)
        with self._lock:
            state = self._hydration_state.get(workspace_key)
            if (
                state is not None
                and state.ledger_incarnation == ledger_incarnation
                and state.manifest_signature == manifest_signature
                and state.snapshot_generation_id == snapshot_generation_id
                and (now - state.last_hydrated_at) < timedelta(seconds=self.hydrate_interval_seconds)
            ):
                return False
        hydrated = hydrate_local_runtime_state(paths, ledger)
        with self._lock:
            self._hydration_state[workspace_key] = _HydrationState(
                last_hydrated_at=now,
                ledger_incarnation=ledger_incarnation,
                snapshot_generation_id=snapshot_generation_id,
                manifest_signature=manifest_signature,
            )
        return hydrated

    def checkpoint_workspace_state(
        self,
        paths: WorkspacePaths,
        ledger: RuntimeSnapshotStore,
        *,
        lease_token: str,
        workspace_id: str,
        machine_id: str,
        host_name: str,
        daemon_id: str,
        pid: int,
        process_identity: ProcessIdentity,
        containment_nonce: str,
        status: str,
        started_at_utc: str,
        last_checkpoint_at_utc: str,
        app_version: str | None,
        heartbeat_interval_seconds: float | None = None,
    ) -> None:
        workspace_key = str(paths.workspace_root)
        change_token = ledger.snapshot_change_token()
        committed_generation_id = read_runtime_snapshot_generation(paths, lease_token=lease_token)
        with self._lock:
            checkpoint_state = self._checkpoint_state.get(workspace_key)
        if (
            checkpoint_state is not None
            and checkpoint_state.change_token == change_token
            and checkpoint_state.snapshot_generation_id == committed_generation_id
        ):
            write_lease_metadata(
                paths,
                lease_token=lease_token,
                workspace_id=workspace_id,
                machine_id=machine_id,
                host_name=host_name,
                daemon_id=daemon_id,
                pid=pid,
                process_identity=process_identity,
                containment_nonce=containment_nonce,
                status=status,
                started_at_utc=started_at_utc,
                last_checkpoint_at_utc=last_checkpoint_at_utc,
                app_version=app_version,
            )
        else:
            committed_generation_id = checkpoint_runtime_workspace_state(
                paths,
                ledger,
                lease_token=lease_token,
                workspace_id=workspace_id,
                machine_id=machine_id,
                host_name=host_name,
                daemon_id=daemon_id,
                pid=pid,
                process_identity=process_identity,
                containment_nonce=containment_nonce,
                status=status,
                started_at_utc=started_at_utc,
                last_checkpoint_at_utc=last_checkpoint_at_utc,
                app_version=app_version,
                heartbeat_interval_seconds=heartbeat_interval_seconds,
            )
            with self._lock:
                self._checkpoint_state[workspace_key] = _CheckpointState(
                    change_token=change_token,
                    snapshot_generation_id=committed_generation_id,
                )
        self._invalidate_workspace(paths, preserve_checkpoint=True)

    def read_lease_metadata(self, paths: WorkspacePaths) -> dict[str, Any] | None:
        return self._cache_read(
            cache=self._lease_cache,
            cache_key=str(paths.workspace_root),
            signature=lambda: self._lease_metadata_signature(paths),
            reader=lambda: read_lease_metadata(paths),
        )

    def write_lease_metadata(
        self,
        paths: WorkspacePaths,
        *,
        lease_token: str,
        workspace_id: str,
        machine_id: str,
        host_name: str,
        daemon_id: str,
        pid: int,
        process_identity: ProcessIdentity,
        containment_nonce: str,
        status: str,
        started_at_utc: str,
        last_checkpoint_at_utc: str,
        app_version: str | None,
    ) -> None:
        write_lease_metadata(
            paths,
            lease_token=lease_token,
            workspace_id=workspace_id,
            machine_id=machine_id,
            host_name=host_name,
            daemon_id=daemon_id,
            pid=pid,
            process_identity=process_identity,
            containment_nonce=containment_nonce,
            status=status,
            started_at_utc=started_at_utc,
            last_checkpoint_at_utc=last_checkpoint_at_utc,
            app_version=app_version,
        )
        self._invalidate_workspace(paths)

    def remove_lease_metadata(self, paths: WorkspacePaths, *, lease_token: str) -> None:
        remove_lease_metadata(paths, lease_token=lease_token)
        self._invalidate_workspace(paths)

    def lease_is_stale(
        self,
        paths: WorkspacePaths,
        *,
        lease_token: str,
        stale_after_seconds: float,
    ) -> bool:
        return lease_is_stale(
            paths,
            lease_token=lease_token,
            stale_after_seconds=stale_after_seconds,
        )

    def read_control_request(self, paths: WorkspacePaths) -> dict[str, Any] | None:
        return self._cache_read(
            cache=self._control_cache,
            cache_key=str(paths.control_request_path),
            signature=lambda: self._file_signature(paths.control_request_path),
            reader=lambda: read_control_request(paths),
        )

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
        self._invalidate_workspace(paths)

    def remove_control_request(self, paths: WorkspacePaths) -> None:
        remove_control_request(paths)
        self._invalidate_workspace(paths)

    def reset_flow_state(self, paths: WorkspacePaths, *, lease_token: str, flow_name: str) -> None:
        reset_flow_state(paths, lease_token=lease_token, flow_name=flow_name)
        self._invalidate_workspace(paths)

    def reset_workspace_state(self, paths: WorkspacePaths, *, lease_token: str) -> None:
        reset_workspace_state(paths, lease_token=lease_token)
        self._invalidate_workspace(paths)

    def _lease_metadata_signature(self, paths: WorkspacePaths) -> object:
        bundle = resolve_workspace_bundle(paths)
        if bundle is None:
            return None
        return (*bundle.topology_signature, self._file_signature(bundle.lease_metadata_path))

    def _snapshot_manifest_signature(self, paths: WorkspacePaths) -> object:
        bundle = resolve_workspace_bundle(paths)
        if bundle is None:
            return None
        return (*bundle.topology_signature, self._file_signature(bundle.snapshot_manifest_path))


_DEFAULT_WORKSPACE_IO_LAYER = WorkspaceIoLayer()


def default_workspace_io_layer() -> WorkspaceIoLayer:
    """Return the process-wide workspace IO layer."""
    return _DEFAULT_WORKSPACE_IO_LAYER


__all__ = ["WorkspaceIoLayer", "default_workspace_io_layer"]
