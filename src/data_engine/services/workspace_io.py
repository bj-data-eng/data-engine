"""Process-wide shared workspace IO layer for shared-state reads and writes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import RLock
from typing import Any

from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.runtime_db import parse_utc_text
from data_engine.runtime.shared_state import (
    RuntimeSnapshotStore,
    checkpoint_workspace_state as checkpoint_runtime_workspace_state,
    claim_workspace as claim_runtime_workspace,
    hydrate_local_runtime_state,
    initialize_workspace_state,
    read_control_request,
    read_lease_metadata,
    recover_stale_workspace,
    release_workspace,
    remove_control_request,
    remove_lease_metadata,
    reset_flow_state,
    reset_workspace_state,
    write_control_request,
    write_lease_metadata,
)


@dataclass(frozen=True)
class _CachedRow:
    expires_at: datetime
    file_signature: tuple[bool, int | None]
    row: dict[str, Any] | None


@dataclass(frozen=True)
class _HydrationState:
    last_hydrated_at: datetime
    snapshot_generation_id: str | None
    lease_signature: tuple[bool, int | None]


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
        path,
        reader,
    ) -> dict[str, Any] | None:
        now = datetime.now(UTC)
        file_signature = self._file_signature(path)
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

    def _invalidate_workspace(self, paths: WorkspacePaths) -> None:
        lease_key = str(paths.lease_metadata_path)
        control_key = str(paths.control_request_path)
        workspace_key = str(paths.workspace_root)
        with self._lock:
            self._lease_cache.pop(lease_key, None)
            self._control_cache.pop(control_key, None)
            self._hydration_state.pop(workspace_key, None)

    def initialize_workspace(self, paths: WorkspacePaths) -> None:
        initialize_workspace_state(paths)

    def claim_workspace(self, paths: WorkspacePaths) -> bool:
        claimed = claim_runtime_workspace(paths)
        if claimed:
            self._invalidate_workspace(paths)
        return claimed

    def release_workspace(self, paths: WorkspacePaths) -> None:
        release_workspace(paths)
        self._invalidate_workspace(paths)

    def recover_stale_workspace(
        self,
        paths: WorkspacePaths,
        *,
        machine_id: str,
        stale_after_seconds: float,
        reclaim: bool = True,
    ) -> bool:
        recovered = recover_stale_workspace(
            paths,
            machine_id=machine_id,
            stale_after_seconds=stale_after_seconds,
            reclaim=reclaim,
        )
        if recovered:
            self._invalidate_workspace(paths)
        return recovered

    def hydrate_local_runtime(self, paths: WorkspacePaths, ledger: RuntimeSnapshotStore) -> bool:
        workspace_key = str(paths.workspace_root)
        now = datetime.now(UTC)
        lease_signature = self._file_signature(paths.lease_metadata_path)
        metadata = self.read_lease_metadata(paths)
        snapshot_generation_id = (
            str(metadata.get("snapshot_generation_id")).strip()
            if isinstance(metadata, dict) and isinstance(metadata.get("snapshot_generation_id"), str)
            else None
        )
        with self._lock:
            state = self._hydration_state.get(workspace_key)
            if (
                state is not None
                and state.lease_signature == lease_signature
                and state.snapshot_generation_id == snapshot_generation_id
                and (now - state.last_hydrated_at) < timedelta(seconds=self.hydrate_interval_seconds)
            ):
                return False
        hydrate_local_runtime_state(paths, ledger)
        with self._lock:
            self._hydration_state[workspace_key] = _HydrationState(
                last_hydrated_at=now,
                snapshot_generation_id=snapshot_generation_id,
                lease_signature=lease_signature,
            )
        return True

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
        self._invalidate_workspace(paths)

    def read_lease_metadata(self, paths: WorkspacePaths) -> dict[str, Any] | None:
        return self._cache_read(
            cache=self._lease_cache,
            cache_key=str(paths.lease_metadata_path),
            path=paths.lease_metadata_path,
            reader=lambda: read_lease_metadata(paths),
        )

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
        self._invalidate_workspace(paths)

    def remove_lease_metadata(self, paths: WorkspacePaths) -> None:
        remove_lease_metadata(paths)
        self._invalidate_workspace(paths)

    def lease_is_stale(self, paths: WorkspacePaths, *, stale_after_seconds: float) -> bool:
        metadata = self.read_lease_metadata(paths)
        if metadata is None:
            return True
        parsed = parse_utc_text(str(metadata.get("last_checkpoint_at_utc")))
        if parsed is None:
            return True
        return datetime.now(UTC) - parsed > timedelta(seconds=max(stale_after_seconds, 0.0))

    def read_control_request(self, paths: WorkspacePaths) -> dict[str, Any] | None:
        return self._cache_read(
            cache=self._control_cache,
            cache_key=str(paths.control_request_path),
            path=paths.control_request_path,
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

    def reset_flow_state(self, paths: WorkspacePaths, *, flow_name: str) -> None:
        reset_flow_state(paths, flow_name=flow_name)
        self._invalidate_workspace(paths)

    def reset_workspace_state(self, paths: WorkspacePaths) -> None:
        reset_workspace_state(paths)
        self._invalidate_workspace(paths)


_DEFAULT_WORKSPACE_IO_LAYER = WorkspaceIoLayer()


def default_workspace_io_layer() -> WorkspaceIoLayer:
    """Return the process-wide workspace IO layer."""
    return _DEFAULT_WORKSPACE_IO_LAYER


__all__ = ["WorkspaceIoLayer", "default_workspace_io_layer"]
