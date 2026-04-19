"""Daemon host object."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import threading
from typing import TYPE_CHECKING, Any

from data_engine.domain import DaemonLifecyclePolicy
from data_engine.domain.time import parse_utc_text, utcnow_text
from data_engine.hosts.daemon.client import (
    DaemonClientError,
    force_shutdown_daemon_process,
    WorkspaceLeaseError,
    _remove_stale_unix_endpoint,
    daemon_request,
    is_daemon_live,
    spawn_daemon_process,
)
from data_engine.hosts.daemon.composition import (
    DaemonHostDependencies,
    DaemonHostFacade,
    DaemonHostIdentity,
    DaemonHostState,
)
from data_engine.hosts.daemon.bootstrap import initialize_service
from data_engine.hosts.daemon.entrypoints import (
    default_workspace_service_factory,
    main as run_daemon_module,
    serve_workspace_daemon as serve_daemon_entrypoint,
)
from data_engine.hosts.daemon.commands import (
    DaemonCommandHandler,
)
from data_engine.hosts.daemon.constants import (
    CHECKPOINT_INTERVAL_SECONDS,
    STALE_AFTER_SECONDS,
)
from data_engine.hosts.daemon.runtime_events import (
    DaemonRuntimeEvent,
    DaemonRuntimeEventBus,
    DaemonRuntimeProjector,
)
from data_engine.hosts.daemon.lifecycle import (
    checkpoint_loop,
    relinquish_workspace_after_checkpoint_failures,
    relinquish_workspace_for_control_request,
    relinquish_workspace_for_missing_root,
    shutdown_for_requested_idle_disconnect,
    shutdown,
    shutdown_if_unowned_and_idle,
)
from data_engine.hosts.daemon.server import serve_forever as serve_daemon_forever
from data_engine.platform.workspace_models import (
    WorkspacePaths,
    authored_workspace_is_available,
)
from data_engine.hosts.daemon.runtime_ledger import DaemonRuntimeCacheProxy
from data_engine.platform.instrumentation import (
    append_timing_line,
    maybe_start_viztracer,
    timed_operation,
)
from data_engine.views.models import QtFlowCard, load_qt_flow_cards

if TYPE_CHECKING:
    pass


DAEMON_LOG_RETENTION_DAYS = 30

class DataEngineDaemonService:
    """Own one workspace daemon instance and its runtime state."""

    def __init__(
        self,
        paths: WorkspacePaths,
        *,
        dependencies: DaemonHostDependencies | None = None,
        identity: DaemonHostIdentity | None = None,
        lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT,
    ) -> None:
        self.paths = paths
        dependencies = dependencies or DaemonHostDependencies.build_default(paths)
        daemon_identity = identity or DaemonHostIdentity.current_process()
        self.lifecycle_policy = DaemonLifecyclePolicy.coerce(lifecycle_policy)
        self.started_at_utc = utcnow_text()
        self.state = DaemonHostState.build(started_at_utc=self.started_at_utc)
        self.host = DaemonHostFacade(self.state)

        self.runtime_cache_ledger = dependencies.runtime_cache_ledger
        self.runtime_execution_ledger = DaemonRuntimeCacheProxy(
            self.runtime_cache_ledger,
            publish_event=self._publish_runtime_event,
        )
        self.runtime_control_ledger = dependencies.runtime_control_ledger
        self.flow_catalog_service = dependencies.flow_catalog_service
        self.flow_execution_service = dependencies.flow_execution_service
        self.runtime_execution_service = dependencies.runtime_execution_service
        self.shared_state_adapter = dependencies.shared_state_adapter
        self.machine_id = daemon_identity.machine_id
        self.daemon_id = daemon_identity.daemon_id
        self.pid = daemon_identity.pid
        self._state_lock = threading.RLock()
        self._cached_flow_cards: tuple[QtFlowCard, ...] | None = None
        self._timing_log_path = self.paths.runtime_state_dir / "daemon_timing.log"
        self.runtime_event_bus = DaemonRuntimeEventBus()
        self.runtime_projector = DaemonRuntimeProjector(
            workspace_id=self.paths.workspace_id,
            initial_state=self._runtime_state_payload(),
        )
        self.runtime_event_bus.subscribe(self.runtime_projector.handle)
        maybe_start_viztracer(
            self.paths.runtime_state_dir / "daemon_viztrace.json",
            process_name=f"daemon:{self.paths.workspace_id}",
        )
        self.command_handler = DaemonCommandHandler(self)
        self._publish_runtime_event("daemon.initialized")

    def _workspace_root_is_available(self) -> bool:
        """Return whether the authored workspace still exists at the configured root."""
        return authored_workspace_is_available(self.paths)

    def _retained_daemon_log_lines(self, lines: list[str], *, now: datetime | None = None) -> list[str]:
        """Return daemon-log lines that still fall within the retention window."""
        cutoff = (now or datetime.now(UTC)) - timedelta(days=DAEMON_LOG_RETENTION_DAYS)
        retained: list[str] = []
        for line in lines:
            timestamp_text = line.split(" ", 1)[0].strip()
            try:
                parsed = parse_utc_text(timestamp_text)
            except Exception:
                parsed = None
            if parsed is None or parsed >= cutoff:
                retained.append(line)
        return retained

    def _debug_log(self, message: str) -> None:
        """Append one daemon diagnostic line and keep only the last retention window."""
        try:
            self.paths.runtime_state_dir.mkdir(parents=True, exist_ok=True)
            existing_lines: list[str] = []
            if self.paths.daemon_log_path.exists():
                existing_lines = self.paths.daemon_log_path.read_text(encoding="utf-8").splitlines(keepends=True)
            retained_lines = self._retained_daemon_log_lines(existing_lines)
            retained_lines.append(f"{utcnow_text()} pid={self.pid} workspace={self.paths.workspace_id} {message}\n")
            self.paths.daemon_log_path.write_text("".join(retained_lines), encoding="utf-8")
        except Exception:
            pass

    def _instrument(self, scope: str, event: str, *, phase: str = "mark", elapsed_ms: float | None = None, fields: dict[str, object] | None = None) -> None:
        """Append one dev-only structured timing line for daemon diagnostics."""
        append_timing_line(
            self._timing_log_path,
            scope=scope,
            event=event,
            phase=phase,
            elapsed_ms=elapsed_ms,
            fields=fields,
        )

    def _timed_operation(self, scope: str, event: str, *, fields: dict[str, object] | None = None):
        """Return one dev-only timing context manager for daemon work."""
        return timed_operation(self._timing_log_path, scope=scope, event=event, fields=fields)

    def _runtime_state_payload(self) -> dict[str, object]:
        """Return the current daemon-owned runtime state payload."""
        active_runs = self._active_runs_payload()
        flow_activity = self._flow_activity_payload(active_runs)
        with self._state_lock:
            return {
                "status": self.state.status,
                "workspace_owned": self.state.workspace_owned,
                "leased_by_machine_id": self.state.leased_by_machine_id,
                "runtime_active": self.state.runtime_active,
                "runtime_stopping": self.state.runtime_stopping,
                "engine_starting": self.state.engine_starting,
                "active_engine_flow_names": self.state.active_engine_flow_names,
                "active_runs": active_runs,
                "flow_activity": flow_activity,
                "manual_runs": tuple(sorted(self.state.manual_run_threads)),
                "last_checkpoint_at_utc": self.state.last_checkpoint_at_utc,
            }

    def _active_runs_payload(self) -> tuple[dict[str, object], ...]:
        """Return active run rows derived from the runtime cache ledger."""
        active_runs = self.runtime_cache_ledger.runs.list_active()
        active_steps = self.runtime_cache_ledger.step_outputs.list_active()
        latest_step_by_run: dict[str, object] = {}
        for step in active_steps:
            latest_step_by_run[step.run_id] = step
        return tuple(
            {
                "run_id": run.run_id,
                "flow_name": run.flow_name,
                "group_name": run.group_name,
                "source_path": run.source_path,
                "state": "stopping" if self.state.runtime_stopping else ("running" if run.status == "started" else "starting"),
                "current_step_name": latest_step_by_run[run.run_id].step_label if run.run_id in latest_step_by_run else None,
                "current_step_started_at_utc": latest_step_by_run[run.run_id].started_at_utc if run.run_id in latest_step_by_run else None,
                "started_at_utc": run.started_at_utc,
                "finished_at_utc": run.finished_at_utc,
                "elapsed_seconds": run.elapsed_seconds,
                "error_text": run.error_text,
            }
            for run in active_runs
        )

    def _flow_activity_payload(self, active_runs: tuple[dict[str, object], ...]) -> tuple[dict[str, object], ...]:
        """Return daemon-native per-flow activity counts."""
        active_counts: dict[str, int] = {}
        engine_counts: dict[str, int] = {}
        manual_counts: dict[str, int] = {}
        stopping_counts: dict[str, int] = {}
        running_step_counts: dict[str, dict[str, int]] = {}
        with self._state_lock:
            active_engine_flow_names = set(self.state.active_engine_flow_names)
            pending_manual_run_names = set(self.state.pending_manual_run_names)
        for run in active_runs:
            flow_name = run.get("flow_name")
            if isinstance(flow_name, str) and flow_name.strip():
                active_counts[flow_name] = active_counts.get(flow_name, 0) + 1
                if flow_name in active_engine_flow_names:
                    engine_counts[flow_name] = engine_counts.get(flow_name, 0) + 1
                else:
                    manual_counts[flow_name] = manual_counts.get(flow_name, 0) + 1
                if run.get("state") == "stopping":
                    stopping_counts[flow_name] = stopping_counts.get(flow_name, 0) + 1
                step_name = run.get("current_step_name")
                if isinstance(step_name, str) and step_name.strip():
                    counts = running_step_counts.setdefault(flow_name, {})
                    counts[step_name] = counts.get(step_name, 0) + 1
        queued_counts = {flow_name: 1 for flow_name in pending_manual_run_names}
        flow_names = tuple(
            sorted(
                set(active_counts)
                | set(queued_counts)
                | set(engine_counts)
                | set(manual_counts)
                | set(stopping_counts)
                | set(running_step_counts)
            )
        )
        return tuple(
            {
                "flow_name": flow_name,
                "active_run_count": active_counts.get(flow_name, 0),
                "queued_run_count": queued_counts.get(flow_name, 0),
                "engine_run_count": engine_counts.get(flow_name, 0),
                "manual_run_count": manual_counts.get(flow_name, 0),
                "stopping_run_count": stopping_counts.get(flow_name, 0),
                "running_step_counts": dict(sorted(running_step_counts.get(flow_name, {}).items())),
            }
            for flow_name in flow_names
        )

    def _publish_runtime_event(
        self,
        event_type: str,
        *,
        correlation_id: str | None = None,
        payload: dict[str, object] | None = None,
    ) -> None:
        """Publish one daemon runtime event with the latest full state payload."""
        event_payload = {"state": self._runtime_state_payload()}
        if payload:
            event_payload.update(payload)
        self.runtime_event_bus.publish(
            DaemonRuntimeEvent(
                workspace_id=self.paths.workspace_id,
                event_type=event_type,
                timestamp_utc=utcnow_text(),
                correlation_id=correlation_id,
                payload=event_payload,
            )
        )

    def initialize(self) -> None:
        initialize_service(self)

    def serve_forever(self) -> None:
        serve_daemon_forever(self)

    def _handle_command(self, payload: Any) -> dict[str, Any]:
        return self.command_handler.handle(payload)

    def _load_flow_cards(self, *, force: bool = False) -> tuple[QtFlowCard, ...]:
        with self._timed_operation(
            "daemon.catalog",
            "load_flow_cards",
            fields={"workspace": self.paths.workspace_id, "force": force},
        ):
            if not force and self._cached_flow_cards is not None:
                return self._cached_flow_cards
            cards = load_qt_flow_cards(self.flow_catalog_service, workspace_root=self.paths.workspace_root)
            self._cached_flow_cards = cards
            return cards

    def _checkpoint_loop(self) -> None:
        checkpoint_loop(self)

    def _checkpoint_once(self, *, status: str) -> None:
        self.command_handler.checkpoint_once(status=status)

    def _refresh_observer_snapshot(self) -> None:
        self.command_handler.refresh_observer_snapshot()

    def _update_daemon_state(self, *, status: str) -> None:
        self.command_handler.update_daemon_state(status=status)

    def _relinquish_workspace_after_checkpoint_failures(self) -> None:
        relinquish_workspace_after_checkpoint_failures(self)

    def _relinquish_workspace_for_control_request(self, requester_machine_id: str) -> None:
        relinquish_workspace_for_control_request(self, requester_machine_id)

    def _relinquish_workspace_for_missing_root(self) -> None:
        relinquish_workspace_for_missing_root(self)

    def _shutdown_for_requested_idle_disconnect(self, *, reason: str) -> None:
        shutdown_for_requested_idle_disconnect(self, reason=reason)

    def _shutdown_if_unowned_and_idle(self, *, reason: str) -> None:
        shutdown_if_unowned_and_idle(self, reason=reason)

    def _wake_listener(self) -> None:
        try:
            daemon_request(self.paths, {"command": "daemon_ping"}, timeout=0.5)
        except Exception:
            pass

    def _shutdown(self) -> None:
        shutdown(self)
def main(
    argv: list[str] | None = None,
    *,
    workspace_service=None,
    workspace_service_factory=None,
    resolve_paths_func=None,
) -> int:
    """Module entrypoint for launching one workspace daemon process."""
    return run_daemon_module(
        DataEngineDaemonService,
        argv,
        workspace_service=workspace_service,
        workspace_service_factory=workspace_service_factory,
        resolve_paths_func=resolve_paths_func,
        serve_workspace_daemon_func=lambda service_type, **kwargs: serve_workspace_daemon(**kwargs),
    )


def serve_workspace_daemon(
    *,
    workspace_root=None,
    workspace_id=None,
    lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT,
    workspace_service=None,
    resolve_paths_func=None,
) -> int:
    return serve_daemon_entrypoint(
        DataEngineDaemonService,
        workspace_root=workspace_root,
        workspace_id=workspace_id,
        lifecycle_policy=lifecycle_policy,
        workspace_service=workspace_service,
        resolve_paths_func=resolve_paths_func,
    )


__all__ = [
    "CHECKPOINT_INTERVAL_SECONDS",
    "DaemonClientError",
    "DataEngineDaemonService",
    "STALE_AFTER_SECONDS",
    "WorkspaceLeaseError",
    "force_shutdown_daemon_process",
    "_remove_stale_unix_endpoint",
    "daemon_request",
    "default_workspace_service_factory",
    "is_daemon_live",
    "serve_workspace_daemon",
    "spawn_daemon_process",
]


if __name__ == "__main__":
    raise SystemExit(main())
