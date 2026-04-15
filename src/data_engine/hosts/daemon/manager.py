"""Client-side daemon state management for Data Engine workspaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import os

from data_engine.domain.time import parse_utc_text
from data_engine.hosts.daemon.app import DaemonClientError, daemon_request, is_daemon_live
from data_engine.hosts.daemon.shared_state import DaemonSharedStateAdapter
from data_engine.domain import WorkspaceControlState
from data_engine.platform.processes import process_is_running as _pid_is_live
from data_engine.platform.workspace_models import WorkspacePaths, machine_id_text

def _lease_pid_is_live(metadata: dict[str, object] | None) -> bool:
    """Return whether the recorded lease owner pid is still alive."""
    if not isinstance(metadata, dict):
        return False
    pid_value = metadata.get("pid")
    try:
        pid = int(pid_value)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    return _pid_is_live(pid)


@dataclass(frozen=True)
class WorkspaceDaemonSnapshot:
    """Normalized client view of one workspace daemon state."""

    live: bool
    workspace_owned: bool
    leased_by_machine_id: str | None
    runtime_active: bool
    runtime_stopping: bool
    manual_runs: tuple[str, ...]
    last_checkpoint_at_utc: str | None
    source: str


class WorkspaceDaemonManager:
    """Track daemon liveness and normalize fallback state for one workspace."""

    def __init__(
        self,
        paths: WorkspacePaths,
        *,
        max_sync_misses: int = 3,
        shared_state_adapter: DaemonSharedStateAdapter | None = None,
    ) -> None:
        self.paths = paths
        self.max_sync_misses = max(max_sync_misses, 1)
        self.shared_state_adapter = shared_state_adapter or DaemonSharedStateAdapter()
        self.workspace_configured = bool(getattr(paths, "workspace_configured", True))
        self._daemon_live = False
        self._sync_misses = 0
        self._last_snapshot: WorkspaceDaemonSnapshot | None = None

    @property
    def daemon_live(self) -> bool:
        """Return the most recent daemon liveness result."""
        return self._daemon_live

    def sync(self) -> WorkspaceDaemonSnapshot:
        """Return the latest normalized daemon snapshot for one workspace."""
        if not self.workspace_configured:
            self._daemon_live = False
            snapshot = WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            )
            self._last_snapshot = snapshot
            return snapshot
        try:
            live = is_daemon_live(self.paths)
        except Exception:
            live = False
        self._daemon_live = live
        if not live:
            self._sync_misses += 1
            if self._sync_misses < self.max_sync_misses and self._last_snapshot is not None:
                return WorkspaceDaemonSnapshot(
                    live=False,
                    workspace_owned=self._last_snapshot.workspace_owned,
                    leased_by_machine_id=self._last_snapshot.leased_by_machine_id,
                    runtime_active=self._last_snapshot.runtime_active,
                    runtime_stopping=self._last_snapshot.runtime_stopping,
                    manual_runs=self._last_snapshot.manual_runs,
                    last_checkpoint_at_utc=self._last_snapshot.last_checkpoint_at_utc,
                    source="cached",
                )
            snapshot = self._lease_snapshot()
            self._last_snapshot = snapshot
            return snapshot
        try:
            response = daemon_request(self.paths, {"command": "daemon_status"}, timeout=2.0)
        except DaemonClientError:
            self._sync_misses += 1
            if self._last_snapshot is not None:
                return WorkspaceDaemonSnapshot(
                    live=False,
                    workspace_owned=self._last_snapshot.workspace_owned,
                    leased_by_machine_id=self._last_snapshot.leased_by_machine_id,
                    runtime_active=self._last_snapshot.runtime_active,
                    runtime_stopping=self._last_snapshot.runtime_stopping,
                    manual_runs=self._last_snapshot.manual_runs,
                    last_checkpoint_at_utc=self._last_snapshot.last_checkpoint_at_utc,
                    source="cached",
                )
            snapshot = self._lease_snapshot()
            self._last_snapshot = snapshot
            return snapshot
        status = response.get("status") if response.get("ok") else None
        if not isinstance(status, dict):
            snapshot = self._lease_snapshot()
            self._last_snapshot = snapshot
            return snapshot
        self._sync_misses = 0
        manual_runs = tuple(name for name in status.get("manual_runs", []) if isinstance(name, str))
        leased_by = status.get("leased_by_machine_id")
        checkpoint = status.get("last_checkpoint_at_utc")
        snapshot = WorkspaceDaemonSnapshot(
            live=True,
            workspace_owned=bool(status.get("workspace_owned", True)),
            leased_by_machine_id=str(leased_by) if isinstance(leased_by, str) and leased_by.strip() else None,
            runtime_active=bool(status.get("engine_active")),
            runtime_stopping=bool(status.get("engine_stopping")),
            manual_runs=manual_runs,
            last_checkpoint_at_utc=str(checkpoint) if isinstance(checkpoint, str) and checkpoint.strip() else None,
            source="daemon",
        )
        self._last_snapshot = snapshot
        return snapshot

    def _lease_snapshot(self) -> WorkspaceDaemonSnapshot:
        metadata = self.shared_state_adapter.read_lease_metadata(self.paths)
        local_machine_id = machine_id_text()
        if (
            isinstance(metadata, dict)
            and str(metadata.get("machine_id", "")).strip() == local_machine_id
            and not _lease_pid_is_live(metadata)
        ):
            recovered = self.shared_state_adapter.recover_stale_workspace(
                self.paths,
                machine_id=local_machine_id,
                stale_after_seconds=0.0,
            )
            if recovered:
                metadata = self.shared_state_adapter.read_lease_metadata(self.paths)
        owner = metadata.get("machine_id") if isinstance(metadata, dict) else None
        checkpoint = metadata.get("last_checkpoint_at_utc") if isinstance(metadata, dict) else None
        checkpoint_text = str(checkpoint) if isinstance(checkpoint, str) and checkpoint.strip() else None
        if checkpoint_text is not None and parse_utc_text(checkpoint_text) is None:
            checkpoint_text = None
        return WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=metadata is None,
            leased_by_machine_id=str(owner) if isinstance(owner, str) and owner.strip() else None,
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=checkpoint_text,
            source="lease" if metadata is not None else "none",
        )

    def control_status_text(
        self,
        snapshot: WorkspaceDaemonSnapshot,
        *,
        daemon_startup_in_progress: bool = False,
    ) -> str | None:
        """Return plain-language control status text for UI/TUI display."""
        return self.control_state(
            snapshot,
            daemon_startup_in_progress=daemon_startup_in_progress,
        ).control_status_text

    def leased_elsewhere_status_text(self, snapshot: WorkspaceDaemonSnapshot) -> str:
        """Return plain-language action-blocked status for another owner."""
        return self.control_state(snapshot).blocked_status_text

    def control_state(
        self,
        snapshot: WorkspaceDaemonSnapshot,
        *,
        daemon_startup_in_progress: bool = False,
    ) -> WorkspaceControlState:
        """Return the structured workspace control state for one snapshot."""
        if not self.workspace_configured:
            return WorkspaceControlState.empty()
        return WorkspaceControlState.from_snapshot(
            snapshot,
            daemon_live=self.daemon_live,
            local_machine_id=machine_id_text(),
            control_request=self.shared_state_adapter.read_control_request(self.paths),
            daemon_startup_in_progress=daemon_startup_in_progress,
        )

    def request_control(self) -> str:
        """Record one control-transfer request for the current workstation."""
        if not self.workspace_configured:
            return "Choose a workspace folder first."
        if self._daemon_live:
            snapshot = self.sync()
            if snapshot.workspace_owned:
                return "This workstation already has control."
        metadata = self.shared_state_adapter.read_lease_metadata(self.paths)
        owner = (
            str(metadata.get("machine_id")).strip()
            if isinstance(metadata, dict) and isinstance(metadata.get("machine_id"), str)
            else ""
        )
        local_machine_id = machine_id_text()
        if owner == local_machine_id and not self._daemon_live:
            if not _lease_pid_is_live(metadata):
                recovered = self.shared_state_adapter.recover_stale_workspace(
                    self.paths,
                    machine_id=local_machine_id,
                    stale_after_seconds=0.0,
                )
                if recovered:
                    return "Recovered local control."
        self.shared_state_adapter.write_control_request(
            self.paths,
            workspace_id=self.paths.workspace_id,
            requester_machine_id=local_machine_id,
            requester_host_name=local_machine_id,
            requester_pid=os.getpid(),
            requester_client_kind="ui",
            requested_at_utc=datetime.now(UTC).isoformat(),
        )
        return "Control request sent."


__all__ = ["WorkspaceDaemonManager", "WorkspaceDaemonSnapshot"]
