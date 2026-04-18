"""Client-side daemon state management for Data Engine workspaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import os

from data_engine.domain import ActiveRunState, FlowActivityState, WorkspaceControlState
from data_engine.domain.time import parse_utc_text
from data_engine.hosts.daemon.app import DaemonClientError, daemon_request, is_daemon_live
from data_engine.hosts.daemon.shared_state import DaemonSharedStateAdapter
from data_engine.platform.instrumentation import new_request_id, timed_operation
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
    transport_mode: str = "heartbeat"
    engine_starting: bool = False
    daemon_id: str | None = None
    projection_version: int = 0
    active_engine_flow_names: tuple[str, ...] = ()
    active_runs: tuple[ActiveRunState, ...] = ()
    flow_activity: tuple[FlowActivityState, ...] = ()


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
        with timed_operation(
            self._timing_log_path(),
            scope="client.daemon",
            event="sync_snapshot",
            fields={"workspace": self.paths.workspace_id},
        ):
            if not self.workspace_configured:
                self._daemon_live = False
                snapshot = WorkspaceDaemonSnapshot(
                    live=False,
                    workspace_owned=True,
                    leased_by_machine_id=None,
                    runtime_active=False,
                    runtime_stopping=False,
                    active_engine_flow_names=(),
                    transport_mode="disconnected",
                    engine_starting=False,
                    manual_runs=(),
                    last_checkpoint_at_utc=None,
                    source="none",
                    daemon_id=None,
                    projection_version=0,
                    active_runs=(),
                    flow_activity=(),
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
                        active_engine_flow_names=self._last_snapshot.active_engine_flow_names,
                        transport_mode="disconnected",
                        engine_starting=self._last_snapshot.engine_starting,
                        manual_runs=self._last_snapshot.manual_runs,
                        last_checkpoint_at_utc=self._last_snapshot.last_checkpoint_at_utc,
                        source="cached",
                        daemon_id=self._last_snapshot.daemon_id,
                        projection_version=self._last_snapshot.projection_version,
                        active_runs=self._last_snapshot.active_runs,
                        flow_activity=self._last_snapshot.flow_activity,
                    )
                snapshot = self._lease_snapshot()
                self._last_snapshot = snapshot
                return snapshot
            request_id = new_request_id("status")
            request_payload: dict[str, object] = {"command": "daemon_status", "request_id": request_id}
            if self._last_snapshot is not None and self._last_snapshot.projection_version > 0:
                request_payload["since_version"] = self._last_snapshot.projection_version
            try:
                response = daemon_request(
                    self.paths,
                    request_payload,
                    timeout=2.0,
                )
            except DaemonClientError:
                self._sync_misses += 1
                if self._last_snapshot is not None:
                    return WorkspaceDaemonSnapshot(
                        live=False,
                        workspace_owned=self._last_snapshot.workspace_owned,
                        leased_by_machine_id=self._last_snapshot.leased_by_machine_id,
                        runtime_active=self._last_snapshot.runtime_active,
                        runtime_stopping=self._last_snapshot.runtime_stopping,
                        active_engine_flow_names=self._last_snapshot.active_engine_flow_names,
                        engine_starting=self._last_snapshot.engine_starting,
                        manual_runs=self._last_snapshot.manual_runs,
                        last_checkpoint_at_utc=self._last_snapshot.last_checkpoint_at_utc,
                        source="cached",
                        daemon_id=self._last_snapshot.daemon_id,
                        projection_version=self._last_snapshot.projection_version,
                        active_runs=self._last_snapshot.active_runs,
                        flow_activity=self._last_snapshot.flow_activity,
                    )
                snapshot = self._lease_snapshot()
                self._last_snapshot = snapshot
                return snapshot
            status = response.get("status") if response.get("ok") else None
            return self._snapshot_from_status_dict(status, assume_live=True, transport_mode="heartbeat")

    def wait_for_update(self, *, timeout_seconds: float = 5.0) -> WorkspaceDaemonSnapshot:
        """Wait for one daemon projection update, reusing the last known version when available."""
        with timed_operation(
            self._timing_log_path(),
            scope="client.daemon",
            event="wait_for_update",
            fields={"workspace": self.paths.workspace_id},
        ):
            if not self.workspace_configured:
                return self.sync()
            try:
                live = is_daemon_live(self.paths)
            except Exception:
                live = False
            self._daemon_live = live
            if not live:
                return self.sync()
            request_id = new_request_id("wait-status")
            since_version = self._last_snapshot.projection_version if self._last_snapshot is not None else 0
            try:
                response = daemon_request(
                    self.paths,
                    {
                        "command": "wait_for_daemon_status",
                        "request_id": request_id,
                        "since_version": since_version,
                        "timeout_ms": max(int(timeout_seconds * 1000.0), 0),
                    },
                    timeout=max(timeout_seconds + 1.0, 2.0),
                )
            except DaemonClientError:
                return self.sync()
            status = response.get("status") if response.get("ok") else None
            return self._snapshot_from_status_dict(status, assume_live=True, transport_mode="subscription")

    def _timing_log_path(self):
        if not self.workspace_configured:
            return None
        return self.paths.runtime_state_dir / "client_timing.log"

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
            active_engine_flow_names=(),
            engine_starting=False,
            manual_runs=(),
            last_checkpoint_at_utc=checkpoint_text,
            source="lease" if metadata is not None else "none",
            transport_mode="disconnected",
            daemon_id=None,
            projection_version=0,
            active_runs=(),
            flow_activity=(),
        )

    def _snapshot_from_status_dict(
        self,
        status: object,
        *,
        assume_live: bool,
        transport_mode: str,
    ) -> WorkspaceDaemonSnapshot:
        """Normalize one raw daemon status payload into a client snapshot."""
        if not isinstance(status, dict):
            snapshot = self._lease_snapshot()
            self._last_snapshot = snapshot
            return snapshot
        if bool(status.get("unchanged")) and self._last_snapshot is not None:
            self._sync_misses = 0
            snapshot = WorkspaceDaemonSnapshot(
                live=assume_live,
                workspace_owned=self._last_snapshot.workspace_owned,
                leased_by_machine_id=self._last_snapshot.leased_by_machine_id,
                runtime_active=self._last_snapshot.runtime_active,
                runtime_stopping=self._last_snapshot.runtime_stopping,
                manual_runs=self._last_snapshot.manual_runs,
                last_checkpoint_at_utc=self._last_snapshot.last_checkpoint_at_utc,
                source="daemon",
                transport_mode=transport_mode,
                daemon_id=(
                    str(status.get("daemon_id")).strip()
                    if isinstance(status.get("daemon_id"), str) and str(status.get("daemon_id")).strip()
                    else self._last_snapshot.daemon_id
                ),
                engine_starting=self._last_snapshot.engine_starting,
                projection_version=int(status.get("projection_version", self._last_snapshot.projection_version) or self._last_snapshot.projection_version),
                active_engine_flow_names=self._last_snapshot.active_engine_flow_names,
                active_runs=self._last_snapshot.active_runs,
                flow_activity=self._last_snapshot.flow_activity,
            )
            self._last_snapshot = snapshot
            return snapshot
        self._sync_misses = 0
        active_engine_flow_names = tuple(name for name in status.get("active_engine_flow_names", []) if isinstance(name, str))
        active_runs = _coerce_active_runs(status.get("active_runs"))
        flow_activity = _coerce_flow_activity(status.get("flow_activity"))
        manual_runs = tuple(name for name in status.get("manual_runs", []) if isinstance(name, str))
        leased_by = status.get("leased_by_machine_id")
        checkpoint = status.get("last_checkpoint_at_utc")
        snapshot = WorkspaceDaemonSnapshot(
            live=assume_live,
            workspace_owned=bool(status.get("workspace_owned", True)),
            leased_by_machine_id=str(leased_by) if isinstance(leased_by, str) and leased_by.strip() else None,
            runtime_active=bool(status.get("engine_active")),
            runtime_stopping=bool(status.get("engine_stopping")),
            active_engine_flow_names=active_engine_flow_names,
            transport_mode=transport_mode,
            engine_starting=bool(status.get("engine_starting")),
            daemon_id=str(status.get("daemon_id")).strip() if isinstance(status.get("daemon_id"), str) and str(status.get("daemon_id")).strip() else None,
            manual_runs=manual_runs,
            last_checkpoint_at_utc=str(checkpoint) if isinstance(checkpoint, str) and checkpoint.strip() else None,
            source="daemon",
            projection_version=int(status.get("projection_version", 0) or 0),
            active_runs=active_runs,
            flow_activity=flow_activity,
        )
        self._last_snapshot = snapshot
        return snapshot

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


def _coerce_active_runs(value: object) -> tuple[ActiveRunState, ...]:
    if not isinstance(value, list):
        return ()
    coerced: list[ActiveRunState] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        run_id = item.get("run_id")
        flow_name = item.get("flow_name")
        group_name = item.get("group_name")
        state = item.get("state")
        if not all(isinstance(field, str) and field.strip() for field in (run_id, flow_name, group_name, state)):
            continue
        elapsed_raw = item.get("elapsed_seconds")
        coerced.append(
            ActiveRunState(
                run_id=run_id,
                flow_name=flow_name,
                group_name=group_name,
                source_path=item.get("source_path") if isinstance(item.get("source_path"), str) else None,
                state=state,
                current_step_name=item.get("current_step_name") if isinstance(item.get("current_step_name"), str) else None,
                current_step_started_at_utc=item.get("current_step_started_at_utc") if isinstance(item.get("current_step_started_at_utc"), str) else None,
                started_at_utc=item.get("started_at_utc") if isinstance(item.get("started_at_utc"), str) else None,
                finished_at_utc=item.get("finished_at_utc") if isinstance(item.get("finished_at_utc"), str) else None,
                elapsed_seconds=float(elapsed_raw) if isinstance(elapsed_raw, int | float) else None,
                error_text=item.get("error_text") if isinstance(item.get("error_text"), str) else None,
            )
        )
    return tuple(coerced)


def _coerce_flow_activity(value: object) -> tuple[FlowActivityState, ...]:
    if not isinstance(value, list):
        return ()
    coerced: list[FlowActivityState] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        flow_name = item.get("flow_name")
        if not isinstance(flow_name, str) or not flow_name.strip():
            continue
        active_raw = item.get("active_run_count", 0)
        queued_raw = item.get("queued_run_count", 0)
        engine_raw = item.get("engine_run_count", 0)
        manual_raw = item.get("manual_run_count", 0)
        stopping_raw = item.get("stopping_run_count", 0)
        running_step_counts_raw = item.get("running_step_counts")
        running_step_counts = (
            {
                str(step_name): int(count)
                for step_name, count in running_step_counts_raw.items()
                if isinstance(step_name, str)
                and step_name.strip()
                and isinstance(count, int | float)
            }
            if isinstance(running_step_counts_raw, dict)
            else {}
        )
        coerced.append(
            FlowActivityState(
                flow_name=flow_name,
                active_run_count=int(active_raw) if isinstance(active_raw, int | float) else 0,
                queued_run_count=int(queued_raw) if isinstance(queued_raw, int | float) else 0,
                engine_run_count=int(engine_raw) if isinstance(engine_raw, int | float) else 0,
                manual_run_count=int(manual_raw) if isinstance(manual_raw, int | float) else 0,
                stopping_run_count=int(stopping_raw) if isinstance(stopping_raw, int | float) else 0,
                running_step_counts=running_step_counts,
            )
        )
    return tuple(coerced)


__all__ = ["WorkspaceDaemonManager", "WorkspaceDaemonSnapshot"]
