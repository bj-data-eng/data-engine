"""Host-agnostic runtime and daemon-control use cases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from collections.abc import Iterable

from data_engine.core.model import FlowStoppedError
from data_engine.domain import (
    DaemonLifecyclePolicy,
    DaemonStatusState,
    FlowLogEntry,
    OperationSessionState,
    RuntimeSessionState,
    WorkspaceControlState,
    default_flow_state,
)
from data_engine.domain.catalog import FlowCatalogLike
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager
from data_engine.platform.instrumentation import dev_instrumentation_enabled, new_request_id, timed_operation
from data_engine.platform.workspace_models import WorkspacePaths, authored_workspace_is_available
from data_engine.services import DaemonService, DaemonStateService, SharedStateService


@dataclass(frozen=True)
class DaemonCommandResult:
    """Normalized outcome of one daemon command request."""

    ok: bool
    error: str = ""
    payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class RuntimeSyncState:
    """Normalized daemon/runtime sync state shared by hosts."""

    daemon_status: DaemonStatusState
    workspace_control_state: WorkspaceControlState
    runtime_session: RuntimeSessionState
    snapshot_source: str
    snapshot: object


@dataclass(frozen=True)
class RuntimeLogMessage:
    """One host-neutral log line emitted by a runtime completion path."""

    text: str
    flow_name: str | None = None


@dataclass(frozen=True)
class RuntimeSnapshotPresentation:
    """Normalized runtime snapshot state rebuilt from persisted log history."""

    operation_tracker: OperationSessionState
    flow_states: dict[str, str]
    active_runtime_flow_names: tuple[str, ...] = ()

    def signature_for(self, runtime_session: RuntimeSessionState) -> tuple[object, ...]:
        """Return a stable signature for render-diff decisions."""
        return (
            tuple(sorted(self.flow_states.items())),
            tuple(sorted(self.active_runtime_flow_names or runtime_session.active_runtime_flow_names)),
            tuple(sorted(runtime_session.active_manual_runs.items())),
            runtime_session.workspace_owned,
            runtime_session.leased_by_machine_id,
        )


@dataclass(frozen=True)
class FlowStateRefreshPlan:
    """Normalized host-agnostic flow-state refresh plan."""

    flow_states: dict[str, str]
    changed_flow_names: frozenset[str]
    signature: tuple[object, ...]

    @property
    def states_changed(self) -> bool:
        """Return whether any flow-state value changed."""
        return bool(self.changed_flow_names)


@dataclass(frozen=True)
class ManualRunCompletion:
    """Normalized manual-run completion state for operator surfaces."""

    runtime_session: RuntimeSessionState
    state_updates: dict[str, str]
    log_messages: tuple[RuntimeLogMessage, ...]
    capture_results: bool = False
    normalize_operations: bool = False
    render_durations: bool = False
    show_error_text: str | None = None


@dataclass(frozen=True)
class EngineRunCompletion:
    """Normalized engine-run completion state for operator surfaces."""

    runtime_session: RuntimeSessionState
    state_updates: dict[str, str]
    failed_flow_names: tuple[str, ...] = ()
    log_messages: tuple[RuntimeLogMessage, ...] = ()


class RuntimeApplication:
    """Own host-neutral daemon sync and runtime command use cases."""

    def __init__(
        self,
        *,
        daemon_service: DaemonService,
        daemon_state_service: DaemonStateService,
        shared_state_service: SharedStateService,
        daemon_lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.EPHEMERAL,
    ) -> None:
        self.daemon_service = daemon_service
        self.daemon_state_service = daemon_state_service
        self.shared_state_service = shared_state_service
        self.daemon_lifecycle_policy = daemon_lifecycle_policy

    def sync_state(
        self,
        *,
        paths: WorkspacePaths,
        daemon_manager: WorkspaceDaemonManager,
        flow_cards,
        runtime_ledger,
        daemon_startup_in_progress: bool = False,
    ) -> RuntimeSyncState:
        """Return normalized daemon/runtime state for one host surface."""
        snapshot = self.daemon_state_service.sync(daemon_manager)
        if snapshot.source in {"lease", "cached"} and not snapshot.workspace_owned:
            self.shared_state_service.hydrate_local_runtime(paths, runtime_ledger)
        daemon_status = DaemonStatusState.from_snapshot(snapshot)
        return RuntimeSyncState(
            daemon_status=daemon_status,
            workspace_control_state=self.daemon_state_service.control_state(
                daemon_manager,
                snapshot,
                daemon_startup_in_progress=daemon_startup_in_progress,
            ),
            runtime_session=RuntimeSessionState.from_daemon_snapshot(snapshot, flow_cards),
            snapshot_source=snapshot.source,
            snapshot=snapshot,
        )

    def build_runtime_snapshot(
        self,
        *,
        flow_cards: Iterable[FlowCatalogLike],
        log_entries: Iterable[FlowLogEntry],
        runtime_session: RuntimeSessionState,
        now: float,
    ) -> RuntimeSnapshotPresentation:
        """Return normalized runtime flow state rebuilt from catalog metadata and log history."""
        tracker = OperationSessionState.empty()
        states: dict[str, str] = {}
        cards_by_name: dict[str, FlowCatalogLike] = {}
        for card in flow_cards:
            cards_by_name[card.name] = card
            tracker = tracker.ensure_flow(card.name, card.operation_items)
            states[card.name] = card.state if card.valid else "invalid"
        for entry in log_entries:
            event = entry.event
            if event is None or event.flow_name not in cards_by_name:
                continue
            card = cards_by_name[event.flow_name]
            if event.step_name is None:
                if event.status == "failed":
                    states[event.flow_name] = "failed"
                elif event.status in {"started", "success", "stopped"}:
                    states[event.flow_name] = default_flow_state(card.mode)
                continue
            age_seconds = max((datetime.now(UTC) - entry.created_at_utc).total_seconds(), 0.0)
            tracker, _ = tracker.apply_event(
                event.flow_name,
                card.operation_items,
                event,
                now=now - age_seconds,
            )
        for flow_name in list(states):
            tracker = tracker.normalize_completed(flow_name)
        active_runtime_flow_names = runtime_session.active_runtime_flow_names
        if runtime_session.runtime_stopping:
            active_runtime_flow_names = tuple(
                flow_name
                for flow_name in runtime_session.active_runtime_flow_names
                if (
                    (flow_state := tracker.state_for(flow_name)) is None
                    or not flow_state.has_observed_activity
                    or flow_state.has_running_rows
                )
            )
        for flow_name in active_runtime_flow_names:
            card = cards_by_name.get(flow_name)
            if card is None or states.get(flow_name) == "failed":
                continue
            states[flow_name] = (
                "stopping runtime"
                if runtime_session.runtime_stopping
                else ("polling" if card.mode == "poll" else "scheduled")
            )
        for flow_name in runtime_session.active_manual_runs.values():
            if flow_name in states and states.get(flow_name) != "failed":
                states[flow_name] = "running"
        return RuntimeSnapshotPresentation(
            operation_tracker=tracker,
            flow_states=states,
            active_runtime_flow_names=active_runtime_flow_names,
        )

    def plan_flow_state_refresh(
        self,
        *,
        previous_states: dict[str, str] | None,
        next_states: dict[str, str],
        runtime_session: RuntimeSessionState,
    ) -> FlowStateRefreshPlan:
        """Return one shared diff/signature plan for flow-state refresh decisions."""
        previous = previous_states or {}
        changed_flow_names = frozenset(
            flow_name
            for flow_name in set(previous) | set(next_states)
            if previous.get(flow_name) != next_states.get(flow_name)
        )
        signature = (
            tuple(sorted(next_states.items())),
            tuple(sorted(runtime_session.active_runtime_flow_names)),
            tuple(sorted(runtime_session.active_manual_runs.items())),
            runtime_session.workspace_owned,
            runtime_session.leased_by_machine_id,
        )
        return FlowStateRefreshPlan(
            flow_states=dict(next_states),
            changed_flow_names=changed_flow_names,
            signature=signature,
        )

    def run_flow(self, paths: WorkspacePaths, *, name: str, wait: bool = False, timeout: float = 2.0) -> DaemonCommandResult:
        """Request one manual flow run through the daemon."""
        if not authored_workspace_is_available(paths):
            return DaemonCommandResult(ok=False, error="Workspace root is no longer available.")
        return self._spawn_and_request(
            paths,
            {"command": "run_flow", "name": name, "wait": wait},
            timeout=timeout,
        )

    def start_engine(self, paths: WorkspacePaths, *, timeout: float = 2.0) -> DaemonCommandResult:
        """Request automated runtime start through the daemon."""
        if not authored_workspace_is_available(paths):
            return DaemonCommandResult(ok=False, error="Workspace root is no longer available.")
        return self._spawn_and_request(paths, {"command": "start_engine"}, timeout=timeout)

    def refresh_flows(self, paths: WorkspacePaths, *, timeout: float = 5.0) -> DaemonCommandResult:
        """Request one daemon-side flow refresh through the daemon."""
        return self._spawn_and_request(paths, {"command": "refresh_flows"}, timeout=timeout)

    def stop_engine(self, paths: WorkspacePaths, *, timeout: float = 2.0) -> DaemonCommandResult:
        """Request automated runtime stop through the daemon."""
        return self._request(paths, {"command": "stop_engine"}, timeout=timeout)

    def stop_flow(self, paths: WorkspacePaths, *, name: str, timeout: float = 2.0) -> DaemonCommandResult:
        """Request one manual flow stop through the daemon."""
        return self._request(paths, {"command": "stop_flow", "name": name}, timeout=timeout)

    def daemon_status(self, paths: WorkspacePaths, *, timeout: float = 0.0) -> DaemonCommandResult:
        """Request raw daemon status for host/CLI inspection."""
        return self._request(paths, {"command": "daemon_status"}, timeout=timeout)

    def shutdown_daemon(self, paths: WorkspacePaths, *, timeout: float = 0.0) -> DaemonCommandResult:
        """Request daemon shutdown for one workspace."""
        return self._request(paths, {"command": "shutdown_daemon"}, timeout=timeout)

    def force_shutdown_daemon(self, paths: WorkspacePaths, *, timeout: float = 0.5) -> DaemonCommandResult:
        """Force-stop the local daemon for one workspace."""
        try:
            self.daemon_service.force_shutdown(paths, timeout=timeout)
        except self.daemon_service.client_error_type as exc:
            return DaemonCommandResult(
                ok=False,
                error=_daemon_command_error_text({"command": "force_shutdown_daemon"}, exc),
            )
        return DaemonCommandResult(ok=True)

    def spawn_daemon(self, paths: WorkspacePaths) -> DaemonCommandResult:
        """Start the local daemon using the application lifecycle policy."""
        try:
            self.daemon_service.spawn(
                paths,
                lifecycle_policy=self.daemon_lifecycle_policy,
            )
        except self.daemon_service.client_error_type as exc:
            return DaemonCommandResult(ok=False, error=str(exc))
        return DaemonCommandResult(ok=True)

    def complete_manual_run(
        self,
        *,
        runtime_session: RuntimeSessionState,
        flow_name: str,
        group_name: str | None,
        flow_mode: str,
        results: object,
        error: object,
        stop_requested: bool,
    ) -> ManualRunCompletion:
        """Return normalized state/log outcomes for one completed manual run."""
        next_runtime = runtime_session.without_manual_group(group_name)
        default_state = _default_state_for_mode(flow_mode)
        if isinstance(error, Exception):
            error_text = _error_text(error)
            if isinstance(error, FlowStoppedError) or stop_requested:
                return ManualRunCompletion(
                    runtime_session=next_runtime,
                    state_updates={flow_name: default_state},
                    log_messages=(RuntimeLogMessage(f"Flow stopped: {flow_name}", flow_name=flow_name),),
                )
            return ManualRunCompletion(
                runtime_session=next_runtime,
                state_updates={flow_name: "failed"},
                log_messages=(RuntimeLogMessage(f"Flow failed: {flow_name}: {error_text}", flow_name=flow_name),),
                show_error_text=f"{flow_name} failed.\n\n{error_text}" if flow_mode == "manual" else None,
            )
        result_count = len(results or [])
        return ManualRunCompletion(
            runtime_session=next_runtime,
            state_updates={flow_name: default_state},
            log_messages=(RuntimeLogMessage(f"Flow finished: {flow_name} with {result_count} result(s)", flow_name=flow_name),),
            capture_results=True,
            normalize_operations=True,
            render_durations=True,
        )

    def complete_engine_run(
        self,
        *,
        runtime_session: RuntimeSessionState,
        flow_names: tuple[str, ...],
        flow_modes_by_name: dict[str, str],
        error: object,
        runtime_stop_requested: bool,
        flow_stop_requested: bool,
    ) -> EngineRunCompletion:
        """Return normalized state/log outcomes for one completed engine run."""
        next_runtime = runtime_session.with_runtime_flags(active=False, stopping=False).with_active_runtime_flow_names(())
        default_states = {flow_name: _default_state_for_mode(flow_modes_by_name[flow_name]) for flow_name in flow_names}
        if isinstance(error, Exception):
            if isinstance(error, FlowStoppedError) or runtime_stop_requested or flow_stop_requested:
                return EngineRunCompletion(
                    runtime_session=next_runtime,
                    state_updates=default_states,
                    log_messages=tuple(RuntimeLogMessage("Runtime flow stop.", flow_name=flow_name) for flow_name in flow_names),
                )
            return EngineRunCompletion(
                runtime_session=next_runtime,
                state_updates=default_states,
                failed_flow_names=flow_names,
                log_messages=tuple(
                    RuntimeLogMessage(f"Runtime failed: {error}", flow_name=flow_name)
                    for flow_name in flow_names
                ),
            )
        return EngineRunCompletion(
            runtime_session=next_runtime,
            state_updates=default_states,
            log_messages=tuple(RuntimeLogMessage("Runtime flow finish.", flow_name=flow_name) for flow_name in flow_names),
        )

    def _spawn_and_request(
        self,
        paths: WorkspacePaths,
        payload: dict[str, Any],
        *,
        timeout: float = 0.0,
    ) -> DaemonCommandResult:
        request_payload = self._instrumented_payload(payload)
        with timed_operation(
            self._client_timing_log_path(paths),
            scope="client.daemon",
            event=f"spawn_and_request:{request_payload.get('command', 'unknown')}",
            fields={"request_id": request_payload.get("request_id"), "workspace": paths.workspace_id},
        ):
            spawn_result = self.spawn_daemon(paths)
            if not spawn_result.ok:
                return DaemonCommandResult(
                    ok=False,
                    error=_daemon_command_error_text(request_payload, spawn_result.error),
                )
            return self._request(paths, request_payload, timeout=timeout)

    def _request(
        self,
        paths: WorkspacePaths,
        payload: dict[str, Any],
        *,
        timeout: float = 0.0,
    ) -> DaemonCommandResult:
        request_payload = self._instrumented_payload(payload)
        with timed_operation(
            self._client_timing_log_path(paths),
            scope="client.daemon",
            event=str(request_payload.get("command", "unknown")),
            fields={"request_id": request_payload.get("request_id"), "timeout": timeout, "workspace": paths.workspace_id},
        ):
            try:
                response = self.daemon_service.request(paths, request_payload, timeout=timeout)
            except self.daemon_service.client_error_type as exc:
                return DaemonCommandResult(ok=False, error=_daemon_command_error_text(request_payload, exc))
            if not response.get("ok"):
                return DaemonCommandResult(
                    ok=False,
                    error=_daemon_command_error_text(request_payload, response.get("error")),
                    payload=response,
                )
            return DaemonCommandResult(ok=True, payload=response)

    @staticmethod
    def _client_timing_log_path(paths: WorkspacePaths):
        if not paths.workspace_configured:
            return None
        return paths.runtime_state_dir / "client_timing.log"

    @staticmethod
    def _instrumented_payload(payload: dict[str, Any]) -> dict[str, Any]:
        if "request_id" in payload or not dev_instrumentation_enabled():
            return dict(payload)
        instrumented = dict(payload)
        instrumented["request_id"] = new_request_id(str(payload.get("command", "cmd")))
        return instrumented


def _daemon_command_error_text(payload: dict[str, Any], detail: object | None) -> str:
    """Return a verbose daemon-command failure message with any available detail."""
    text = str(detail).strip() if detail is not None else ""
    if text:
        return text
    return f"Failed to {_daemon_command_action(payload.get('command'))}. The daemon returned no additional detail."


def _daemon_command_action(command: object) -> str:
    if command == "run_flow":
        return "run the selected flow"
    if command == "start_engine":
        return "start the automated engine"
    if command == "refresh_flows":
        return "refresh flow definitions"
    if command == "stop_engine":
        return "stop the engine"
    if command == "stop_flow":
        return "stop the selected flow"
    if command == "daemon_status":
        return "retrieve daemon status"
    if command == "shutdown_daemon":
        return "shut down the daemon"
    if command == "force_shutdown_daemon":
        return "force-stop the daemon"
    return "complete the requested daemon command"


def _default_state_for_mode(mode: str | None) -> str:
    if mode == "poll":
        return "poll ready"
    if mode == "schedule":
        return "schedule ready"
    return "manual"


def _error_text(error: Exception) -> str:
    """Return a non-blank user-facing error detail string."""
    detail = str(error).strip()
    if detail:
        return detail
    return type(error).__name__


__all__ = [
    "DaemonCommandResult",
    "EngineRunCompletion",
    "ManualRunCompletion",
    "RuntimeApplication",
    "RuntimeLogMessage",
    "RuntimeSnapshotPresentation",
    "RuntimeSyncState",
]
