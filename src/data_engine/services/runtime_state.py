"""Read-side runtime state port and live workspace snapshot services."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

from data_engine.application.runtime import RuntimeApplication
from data_engine.domain import (
    ActiveRunState,
    FlowActivityState,
    FlowRunState,
    OperationSessionState,
    RuntimeSessionState,
    StepOutputIndex,
    WorkspaceControlState,
)
from data_engine.domain.catalog import FlowCatalogLike
from data_engine.services.logs import LogService
from data_engine.services.runtime_binding import WorkspaceRuntimeBinding, WorkspaceRuntimeBindingService

ControlAvailability = Literal["available", "leased", "stale", "requested"]
EngineStateName = Literal["idle", "starting", "running", "stopping"]
TransportStateName = Literal["disconnected", "heartbeat", "subscription"]
FlowStateName = Literal["idle", "starting", "running", "stopping", "scheduled", "polling", "failed"]
RunStateName = Literal["starting", "running", "stopping", "success", "failed", "stopped"]


@dataclass(frozen=True)
class ControlSnapshot:
    """Live workspace control availability for one operator surface."""

    state: ControlAvailability
    leased_by_machine_id: str | None = None
    request_pending: bool = False
    control_status_text: str | None = None
    blocked_status_text: str = "Takeover available."
    takeover_remaining_seconds: int | None = None


@dataclass(frozen=True)
class EngineSnapshot:
    """Live engine state for one workspace."""

    state: EngineStateName
    daemon_live: bool = False
    transport: TransportStateName = "heartbeat"
    stop_requested: bool = False
    active_flow_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class FlowLiveSummary:
    """Live flow-level summary for one discovered flow."""

    flow_name: str
    group_name: str
    state: FlowStateName
    stop_requested: bool = False
    active_run_count: int = 0
    queued_run_count: int = 0
    running_step_counts: dict[str, int] = field(default_factory=dict)
    last_started_at_utc: str | None = None
    last_finished_at_utc: str | None = None
    last_error_text: str | None = None


@dataclass(frozen=True)
class RunLiveSnapshot:
    """Live run-level snapshot derived from current grouped runtime history."""

    run_id: str
    flow_name: str
    group_name: str
    source_path: str | None
    state: RunStateName
    current_step_name: str | None = None
    current_step_started_at_utc: str | None = None
    started_at_utc: str | None = None
    finished_at_utc: str | None = None
    elapsed_seconds: float | None = None
    error_text: str | None = None


@dataclass(frozen=True)
class OperatorNotice:
    """One operator-facing notice carried on the live state channel."""

    notice_id: str
    level: Literal["info", "warning", "error"]
    text: str
    created_at_utc: str


@dataclass(frozen=True)
class WorkspaceSnapshot:
    """Authoritative live workspace snapshot for UI consumption."""

    workspace_id: str
    version: int
    control: ControlSnapshot
    engine: EngineSnapshot
    flows: dict[str, FlowLiveSummary]
    active_runs: dict[str, RunLiveSnapshot]
    notices: tuple[OperatorNotice, ...] = ()


@dataclass(frozen=True)
class IncrementalDaemonProjection:
    """One client-side streamed runtime projection update from daemon truth."""

    runtime_session: RuntimeSessionState
    workspace_snapshot: WorkspaceSnapshot
    flow_states: dict[str, str]
    changed_flow_names: tuple[str, ...]
    requires_log_reload: bool


def flow_state_text_from_live_summary(card: FlowCatalogLike | None, live_state: str) -> str:
    """Map one live flow summary state into the surface-facing flow-state text."""
    if card is None:
        return live_state
    if live_state == "idle":
        return card.state if card.valid else "invalid"
    if live_state == "starting":
        return "starting runtime" if card.mode in {"poll", "schedule"} else "starting flow"
    if live_state == "stopping":
        return "stopping runtime" if card.mode in {"poll", "schedule"} else "stopping flow"
    return live_state


def flow_state_texts_from_workspace_snapshot(
    snapshot: WorkspaceSnapshot,
    flow_cards: Iterable[FlowCatalogLike],
) -> dict[str, str]:
    """Project one live workspace snapshot into surface-facing flow-state text."""
    card_by_name = {card.name: card for card in flow_cards}
    return {
        flow_name: flow_state_text_from_live_summary(card_by_name.get(flow_name), summary.state)
        for flow_name, summary in snapshot.flows.items()
    }


def runtime_session_from_workspace_snapshot(snapshot: WorkspaceSnapshot) -> RuntimeSessionState:
    """Project one live streamed workspace snapshot into command-gating session state."""
    active_engine_flow_names = (
        snapshot.engine.active_flow_names
        if snapshot.engine.state in {"running", "stopping"}
        else ()
    )
    engine_flow_names = set(snapshot.engine.active_flow_names)
    manual_runs = {
        run.group_name: run.flow_name
        for run in snapshot.active_runs.values()
        if run.flow_name not in engine_flow_names and run.state in {"starting", "running", "stopping"}
    }
    return RuntimeSessionState(
        workspace_owned=snapshot.control.state != "leased" or snapshot.control.leased_by_machine_id is None,
        leased_by_machine_id=snapshot.control.leased_by_machine_id,
        runtime_active=snapshot.engine.state in {"running", "stopping"},
        runtime_stopping=snapshot.engine.state == "stopping",
        active_runtime_flow_names=active_engine_flow_names,
        manual_runs=(),
    ).with_manual_runs_map(manual_runs)


@dataclass(frozen=True)
class RuntimeEvent:
    """One incremental live-state event for a workspace snapshot subscriber."""

    workspace_id: str
    event_type: str
    version: int
    timestamp_utc: str
    correlation_id: str | None
    payload: dict[str, Any]


class RuntimeStateSubscriber(Protocol):
    """Callable contract for runtime-state event subscribers."""

    def __call__(self, event: RuntimeEvent) -> None: ...


class RuntimeStatePort(Protocol):
    """Read-side live-state boundary consumed by operator surfaces."""

    def current_snapshot(
        self,
        binding: WorkspaceRuntimeBinding,
        *,
        runtime_application: RuntimeApplication,
        flow_cards: Iterable[FlowCatalogLike],
        now: float,
        daemon_startup_in_progress: bool = False,
    ) -> WorkspaceSnapshot: ...

    def subscribe(self, *, workspace_id: str, callback: RuntimeStateSubscriber) -> object: ...

    def unsubscribe(self, token: object) -> None: ...


@dataclass(frozen=True)
class WorkspaceRuntimeProjection:
    """Live runtime presentation state for one workspace binding."""

    runtime_session: RuntimeSessionState
    operation_tracker: OperationSessionState
    flow_states: dict[str, str]
    active_runtime_flow_names: tuple[str, ...]
    step_output_index: StepOutputIndex


class RuntimeStateService:
    """Own the live-state read boundary and transitional surface projection bundle."""

    def __init__(
        self,
        *,
        runtime_binding_service: WorkspaceRuntimeBindingService,
        log_service: LogService,
    ) -> None:
        self.runtime_binding_service = runtime_binding_service
        self.log_service = log_service
        self._snapshot_versions: dict[str, int] = {}
        self._last_workspace_snapshots: dict[str, WorkspaceSnapshot] = {}
        self._subscribers_by_workspace: dict[str, dict[object, RuntimeStateSubscriber]] = {}
        self._subscription_workspace_by_token: dict[object, str] = {}

    @staticmethod
    def _effective_runtime_session(
        runtime_session: RuntimeSessionState,
        *,
        active_runtime_flow_names: tuple[str, ...],
    ) -> RuntimeSessionState:
        if runtime_session.runtime_active or runtime_session.runtime_stopping:
            return runtime_session.with_active_runtime_flow_names(active_runtime_flow_names)
        return runtime_session

    @staticmethod
    def _control_snapshot(control_state: WorkspaceControlState, runtime_session: RuntimeSessionState) -> ControlSnapshot:
        if control_state.local_request_pending:
            state: ControlAvailability = "requested"
        elif runtime_session.workspace_owned or runtime_session.leased_by_machine_id is None:
            state = "available"
        elif control_state.takeover_remaining_seconds is not None and control_state.takeover_remaining_seconds <= 0:
            state = "stale"
        else:
            state = "leased"
        return ControlSnapshot(
            state=state,
            leased_by_machine_id=runtime_session.leased_by_machine_id,
            request_pending=control_state.local_request_pending,
            control_status_text=control_state.control_status_text,
            blocked_status_text=control_state.blocked_status_text,
            takeover_remaining_seconds=control_state.takeover_remaining_seconds,
        )

    @staticmethod
    def _engine_snapshot(
        runtime_session: RuntimeSessionState,
        *,
        daemon_live: bool,
        transport_mode: str,
        daemon_startup_in_progress: bool,
        daemon_engine_starting: bool,
        daemon_active_flow_names: tuple[str, ...],
    ) -> EngineSnapshot:
        if daemon_live:
            if runtime_session.runtime_stopping:
                state: EngineStateName = "stopping"
            elif runtime_session.runtime_active:
                state = "running"
            elif daemon_engine_starting:
                state = "starting"
            else:
                state = "idle"
            active_flow_names = daemon_active_flow_names
            stop_requested = runtime_session.runtime_stopping
        else:
            state = "idle"
            active_flow_names = ()
            stop_requested = False
        return EngineSnapshot(
            state=state,
            daemon_live=daemon_live,
            transport=(
                transport_mode
                if transport_mode in {"disconnected", "heartbeat", "subscription"}
                else "heartbeat"
            ),
            stop_requested=stop_requested,
            active_flow_names=active_flow_names,
        )

    @staticmethod
    def _flow_state_name(state_text: str) -> FlowStateName:
        normalized = str(state_text or "").strip().lower()
        if normalized in {"polling"}:
            return "polling"
        if normalized in {"scheduled"}:
            return "scheduled"
        if normalized in {"running"}:
            return "running"
        if normalized in {"starting runtime", "starting flow"}:
            return "starting"
        if normalized in {"stopping runtime", "stopping flow"}:
            return "stopping"
        if normalized == "failed":
            return "failed"
        return "idle"

    @staticmethod
    def _run_state_name(run_group: FlowRunState, *, flow_state: FlowStateName) -> RunStateName:
        status = str(run_group.status or "").strip().lower()
        if status in {"success", "failed", "stopped"}:
            return status  # type: ignore[return-value]
        if flow_state == "stopping":
            return "stopping"
        if status == "started":
            return "running"
        return "starting"

    @staticmethod
    def _current_step_name(run_group: FlowRunState) -> str | None:
        for step in reversed(run_group.steps):
            if step.status == "started":
                return step.step_name
        return None

    @staticmethod
    def _run_started_at(run_group: FlowRunState) -> str | None:
        if not run_group.entries:
            return None
        return run_group.entries[0].created_at_utc.isoformat()

    @staticmethod
    def _run_finished_at(run_group: FlowRunState) -> str | None:
        if run_group.status not in {"success", "failed", "stopped"}:
            return None
        if run_group.summary_entry is None:
            return None
        return run_group.summary_entry.created_at_utc.isoformat()

    @staticmethod
    def _run_error_text(run_group: FlowRunState) -> str | None:
        if run_group.status != "failed":
            return None
        if run_group.summary_entry is None:
            return None
        return run_group.summary_entry.line

    @staticmethod
    def _live_flow_state_name(
        card: FlowCatalogLike,
        *,
        daemon_live: bool,
        daemon_active_flow_names: tuple[str, ...],
        daemon_activity: FlowActivityState | None,
    ) -> FlowStateName:
        if not daemon_live:
            return "idle"
        if card.name in daemon_active_flow_names:
            if daemon_activity is None or daemon_activity.stopping_run_count == 0:
                return "polling" if card.mode == "poll" else "scheduled"
        if daemon_activity is None:
            return "idle"
        if daemon_activity.stopping_run_count > 0:
            return "stopping"
        if daemon_activity.manual_run_count > 0:
            return "running"
        if daemon_activity.engine_run_count > 0:
            return "polling" if card.mode == "poll" else "scheduled"
        if daemon_activity.queued_run_count > 0:
            return "starting"
        return "idle"

    @staticmethod
    def _latest_run_times(run_groups: tuple[FlowRunState, ...]) -> tuple[str | None, str | None, str | None]:
        last_started_at = None
        last_finished_at = None
        last_error_text = None
        if run_groups:
            newest = run_groups[-1]
            last_started_at = RuntimeStateService._run_started_at(newest)
            last_finished_at = RuntimeStateService._run_finished_at(newest)
            last_error_text = RuntimeStateService._run_error_text(newest)
        return last_started_at, last_finished_at, last_error_text

    @staticmethod
    def _snapshot_signature(snapshot: WorkspaceSnapshot) -> tuple[object, ...]:
        flow_items = tuple(
            sorted(
                (
                    flow_name,
                    summary.state,
                    summary.stop_requested,
                    summary.active_run_count,
                    summary.queued_run_count,
                    tuple(sorted(summary.running_step_counts.items())),
                    summary.last_started_at_utc,
                    summary.last_finished_at_utc,
                    summary.last_error_text,
                )
                for flow_name, summary in snapshot.flows.items()
            )
        )
        run_items = tuple(
            sorted(
                (
                    run_id,
                    run.flow_name,
                    run.group_name,
                    run.source_path,
                    run.state,
                    run.current_step_name,
                    run.current_step_started_at_utc,
                    run.started_at_utc,
                    run.finished_at_utc,
                    run.elapsed_seconds,
                    run.error_text,
                )
                for run_id, run in snapshot.active_runs.items()
            )
        )
        return (
            snapshot.workspace_id,
            snapshot.control.state,
            snapshot.control.leased_by_machine_id,
            snapshot.control.request_pending,
            snapshot.engine.state,
            snapshot.engine.stop_requested,
            snapshot.engine.active_flow_names,
            flow_items,
            run_items,
            tuple((notice.notice_id, notice.level, notice.text, notice.created_at_utc) for notice in snapshot.notices),
        )

    def subscribe(self, *, workspace_id: str, callback: RuntimeStateSubscriber) -> object:
        """Register one live-state subscriber for a workspace id."""
        token = object()
        self._subscribers_by_workspace.setdefault(workspace_id, {})[token] = callback
        self._subscription_workspace_by_token[token] = workspace_id
        return token

    def unsubscribe(self, token: object) -> None:
        """Remove one previously registered live-state subscriber."""
        workspace_id = self._subscription_workspace_by_token.pop(token, None)
        if workspace_id is None:
            return
        subscribers = self._subscribers_by_workspace.get(workspace_id)
        if subscribers is None:
            return
        subscribers.pop(token, None)
        if not subscribers:
            self._subscribers_by_workspace.pop(workspace_id, None)

    def rebuild_projection(
        self,
        binding: WorkspaceRuntimeBinding,
        *,
        runtime_application: RuntimeApplication,
        flow_cards: Iterable[FlowCatalogLike],
        runtime_session: RuntimeSessionState,
        now: float,
    ) -> WorkspaceRuntimeProjection:
        """Rebuild live runtime presentation state from local binding resources."""
        flow_cards_tuple = tuple(flow_cards)
        self.runtime_binding_service.reload_logs(binding)
        flow_cards_by_name = {card.name: card for card in flow_cards_tuple}
        step_output_index = self.runtime_binding_service.rebuild_step_outputs(binding, flow_cards_by_name)
        presentation = runtime_application.build_runtime_snapshot(
            flow_cards=flow_cards_tuple,
            log_entries=self.log_service.all_entries(binding.log_store),
            runtime_session=runtime_session,
            now=now,
        )
        effective_runtime_session = self._effective_runtime_session(
            runtime_session,
            active_runtime_flow_names=presentation.active_runtime_flow_names,
        )
        return WorkspaceRuntimeProjection(
            runtime_session=effective_runtime_session,
            operation_tracker=presentation.operation_tracker,
            flow_states=presentation.flow_states,
            active_runtime_flow_names=presentation.active_runtime_flow_names,
            step_output_index=step_output_index,
        )

    def _build_workspace_snapshot(
        self,
        *,
        binding: WorkspaceRuntimeBinding,
        flow_cards: tuple[FlowCatalogLike, ...],
        runtime_session: RuntimeSessionState,
        workspace_control_state: WorkspaceControlState,
        daemon_live: bool,
        daemon_startup_in_progress: bool,
        daemon_projection_version: int,
        daemon_transport_mode: str,
        daemon_engine_starting: bool,
        daemon_active_flow_names: tuple[str, ...],
        daemon_active_runs: tuple[ActiveRunState, ...],
        daemon_flow_activity: tuple[FlowActivityState, ...],
        operation_tracker: OperationSessionState,
        flow_states: dict[str, str],
    ) -> WorkspaceSnapshot:
        workspace_id = binding.workspace_paths.workspace_id
        runs_by_flow = {
            card.name: self.log_service.runs_for_flow(binding.log_store, card.name)
            for card in flow_cards
        }
        flow_summaries: dict[str, FlowLiveSummary] = {}
        active_runs: dict[str, RunLiveSnapshot] = {}
        if daemon_active_runs:
            active_runs = {
                run.run_id: RunLiveSnapshot(
                    run_id=run.run_id,
                    flow_name=run.flow_name,
                    group_name=run.group_name,
                    source_path=run.source_path,
                    state=run.state if run.state in {"starting", "running", "stopping", "success", "failed", "stopped"} else "running",
                    current_step_name=run.current_step_name,
                    current_step_started_at_utc=run.current_step_started_at_utc,
                    started_at_utc=run.started_at_utc,
                    finished_at_utc=run.finished_at_utc,
                    elapsed_seconds=run.elapsed_seconds,
                    error_text=run.error_text,
                )
                for run in daemon_active_runs
            }

        for card in flow_cards:
            run_groups = runs_by_flow.get(card.name, ())
            daemon_activity = next((item for item in daemon_flow_activity if item.flow_name == card.name), None)
            flow_state_name = self._live_flow_state_name(
                card,
                daemon_live=daemon_live,
                daemon_active_flow_names=daemon_active_flow_names,
                daemon_activity=daemon_activity,
            )
            last_started_at, last_finished_at, last_error_text = self._latest_run_times(run_groups)
            flow_summaries[card.name] = FlowLiveSummary(
                flow_name=card.name,
                group_name=card.group or "",
                state=flow_state_name,
                stop_requested=flow_state_name == "stopping",
                active_run_count=daemon_activity.active_run_count if daemon_activity is not None else sum(1 for run in active_runs.values() if run.flow_name == card.name),
                queued_run_count=daemon_activity.queued_run_count if daemon_activity is not None else 0,
                running_step_counts=dict(daemon_activity.running_step_counts) if daemon_activity is not None else {},
                last_started_at_utc=last_started_at,
                last_finished_at_utc=last_finished_at,
                last_error_text=last_error_text,
            )

        control = self._control_snapshot(workspace_control_state, runtime_session)
        engine = self._engine_snapshot(
            runtime_session,
            daemon_live=daemon_live,
            transport_mode=daemon_transport_mode,
            daemon_startup_in_progress=daemon_startup_in_progress,
            daemon_engine_starting=daemon_engine_starting,
            daemon_active_flow_names=daemon_active_flow_names,
        )
        previous = self._last_workspace_snapshots.get(workspace_id)
        provisional = WorkspaceSnapshot(
            workspace_id=workspace_id,
            version=0,
            control=control,
            engine=engine,
            flows=flow_summaries,
            active_runs=active_runs,
            notices=(),
        )
        previous_signature = self._snapshot_signature(previous) if previous is not None else None
        current_signature = self._snapshot_signature(provisional)
        version = self._snapshot_versions.get(workspace_id, 0)
        if previous_signature != current_signature:
            version = max(version + 1, daemon_projection_version)
        else:
            version = max(version, daemon_projection_version)
        self._snapshot_versions[workspace_id] = version
        snapshot = WorkspaceSnapshot(
            workspace_id=workspace_id,
            version=version,
            control=control,
            engine=engine,
            flows=flow_summaries,
            active_runs=active_runs,
            notices=(),
        )
        self._publish_snapshot_events(previous, snapshot)
        self._last_workspace_snapshots[workspace_id] = snapshot
        return snapshot

    def incremental_snapshot_from_daemon(
        self,
        previous: WorkspaceSnapshot,
        *,
        flow_cards: Iterable[FlowCatalogLike],
        daemon_status,
    ) -> WorkspaceSnapshot:
        """Update one workspace snapshot from daemon-native live truth only.

        This path intentionally avoids log reloads and runtime projection
        rebuilds. It is used for hot subscription lanes where the daemon already
        owns the operational truth and the surface only needs an updated live
        snapshot.
        """
        flow_cards_tuple = tuple(flow_cards)
        runtime_session = daemon_status.as_runtime_session(flow_cards_tuple)
        control = ControlSnapshot(
            state=previous.control.state,
            leased_by_machine_id=daemon_status.leased_by_machine_id,
            request_pending=previous.control.request_pending,
            control_status_text=previous.control.control_status_text,
            blocked_status_text=previous.control.blocked_status_text,
            takeover_remaining_seconds=previous.control.takeover_remaining_seconds,
        )
        engine = self._engine_snapshot(
            runtime_session,
            daemon_live=True,
            transport_mode=daemon_status.transport_mode,
            daemon_startup_in_progress=False,
            daemon_engine_starting=daemon_status.engine_starting,
            daemon_active_flow_names=daemon_status.active_engine_flow_names,
        )
        active_runs = {
            run.run_id: RunLiveSnapshot(
                run_id=run.run_id,
                flow_name=run.flow_name,
                group_name=run.group_name,
                source_path=run.source_path,
                state=run.state if run.state in {"starting", "running", "stopping", "success", "failed", "stopped"} else "running",
                current_step_name=run.current_step_name,
                current_step_started_at_utc=run.current_step_started_at_utc,
                started_at_utc=run.started_at_utc,
                finished_at_utc=run.finished_at_utc,
                elapsed_seconds=run.elapsed_seconds,
                error_text=run.error_text,
            )
            for run in daemon_status.active_runs
        }
        previous_flows = previous.flows
        activity_by_flow = {item.flow_name: item for item in daemon_status.flow_activity}
        flow_summaries: dict[str, FlowLiveSummary] = {}
        for card in flow_cards_tuple:
            prior = previous_flows.get(card.name)
            daemon_activity = activity_by_flow.get(card.name)
            last_started_at = None if prior is None else prior.last_started_at_utc
            last_finished_at = None if prior is None else prior.last_finished_at_utc
            last_error_text = None if prior is None else prior.last_error_text
            flow_summaries[card.name] = FlowLiveSummary(
                flow_name=card.name,
                group_name=card.group or "",
                state=self._live_flow_state_name(
                    card,
                    daemon_live=True,
                    daemon_active_flow_names=daemon_status.active_engine_flow_names,
                    daemon_activity=daemon_activity,
                ),
                stop_requested=daemon_activity.stopping_run_count > 0 if daemon_activity is not None else False,
                active_run_count=daemon_activity.active_run_count if daemon_activity is not None else 0,
                queued_run_count=daemon_activity.queued_run_count if daemon_activity is not None else 0,
                running_step_counts={} if daemon_activity is None else dict(daemon_activity.running_step_counts),
                last_started_at_utc=last_started_at,
                last_finished_at_utc=last_finished_at,
                last_error_text=last_error_text,
            )
        provisional = WorkspaceSnapshot(
            workspace_id=previous.workspace_id,
            version=0,
            control=control,
            engine=engine,
            flows=flow_summaries,
            active_runs=active_runs,
            notices=previous.notices,
        )
        prior_signature = self._snapshot_signature(previous)
        current_signature = self._snapshot_signature(provisional)
        version = max(previous.version, int(getattr(daemon_status, "projection_version", 0) or 0))
        if prior_signature != current_signature:
            version = max(version, previous.version + 1)
        snapshot = WorkspaceSnapshot(
            workspace_id=previous.workspace_id,
            version=version,
            control=control,
            engine=engine,
            flows=flow_summaries,
            active_runs=active_runs,
            notices=previous.notices,
        )
        self._publish_snapshot_events(previous, snapshot)
        self._last_workspace_snapshots[previous.workspace_id] = snapshot
        self._snapshot_versions[previous.workspace_id] = version
        return snapshot

    def incremental_projection_from_daemon(
        self,
        previous: WorkspaceSnapshot,
        *,
        flow_cards: Iterable[FlowCatalogLike],
        previous_flow_states: dict[str, str],
        daemon_status,
        changed_flow_names: Iterable[str],
        requires_log_reload: bool = False,
    ) -> IncrementalDaemonProjection:
        """Return one updated client-side streamed projection from daemon truth."""
        flow_cards_tuple = tuple(flow_cards)
        card_by_name = {card.name: card for card in flow_cards_tuple}
        runtime_session = daemon_status.as_runtime_session(flow_cards_tuple)
        workspace_snapshot = self.incremental_snapshot_from_daemon(
            previous,
            flow_cards=flow_cards_tuple,
            daemon_status=daemon_status,
        )
        next_flow_states = dict(previous_flow_states)
        for flow_name in changed_flow_names:
            summary = workspace_snapshot.flows.get(flow_name)
            if summary is None:
                continue
            next_flow_states[flow_name] = flow_state_text_from_live_summary(
                card_by_name.get(flow_name),
                summary.state,
            )
        return IncrementalDaemonProjection(
            runtime_session=runtime_session,
            workspace_snapshot=workspace_snapshot,
            flow_states=next_flow_states,
            changed_flow_names=tuple(sorted(set(changed_flow_names))),
            requires_log_reload=requires_log_reload,
        )

    def _publish_snapshot_events(self, previous: WorkspaceSnapshot | None, current: WorkspaceSnapshot) -> None:
        subscribers = self._subscribers_by_workspace.get(current.workspace_id)
        if not subscribers:
            return
        timestamp_utc = datetime.now(UTC).isoformat()
        events: list[RuntimeEvent] = []
        if previous is None or previous.control != current.control:
            events.append(
                RuntimeEvent(
                    workspace_id=current.workspace_id,
                    event_type="control.changed",
                    version=current.version,
                    timestamp_utc=timestamp_utc,
                    correlation_id=None,
                    payload=asdict(current.control),
                )
            )
        if previous is None or previous.engine != current.engine:
            events.append(
                RuntimeEvent(
                    workspace_id=current.workspace_id,
                    event_type="engine.changed",
                    version=current.version,
                    timestamp_utc=timestamp_utc,
                    correlation_id=None,
                    payload=asdict(current.engine),
                )
            )
        previous_flows = {} if previous is None else previous.flows
        for flow_name, summary in current.flows.items():
            if previous_flows.get(flow_name) == summary:
                continue
            events.append(
                RuntimeEvent(
                    workspace_id=current.workspace_id,
                    event_type="flow.changed",
                    version=current.version,
                    timestamp_utc=timestamp_utc,
                    correlation_id=None,
                    payload={"flow_name": flow_name, **asdict(summary)},
                )
            )
        previous_runs = {} if previous is None else previous.active_runs
        for run_id, run in current.active_runs.items():
            prior = previous_runs.get(run_id)
            event_type = "run.started" if prior is None else "run.progressed"
            if run.state == "stopping":
                event_type = "run.stopping"
            events.append(
                RuntimeEvent(
                    workspace_id=current.workspace_id,
                    event_type=event_type,
                    version=current.version,
                    timestamp_utc=run.started_at_utc or "",
                    correlation_id=None,
                    payload={"run_id": run_id, **asdict(run)},
                )
            )
        for run_id, run in previous_runs.items():
            if run_id in current.active_runs:
                continue
            events.append(
                RuntimeEvent(
                    workspace_id=current.workspace_id,
                    event_type="run.finished",
                    version=current.version,
                    timestamp_utc=run.finished_at_utc or "",
                    correlation_id=None,
                    payload={"run_id": run_id, **asdict(run)},
                )
            )
        for callback in tuple(subscribers.values()):
            for event in events:
                callback(event)

    def current_snapshot(
        self,
        binding: WorkspaceRuntimeBinding,
        *,
        runtime_application: RuntimeApplication,
        flow_cards: Iterable[FlowCatalogLike],
        now: float,
        daemon_startup_in_progress: bool = False,
    ) -> WorkspaceSnapshot:
        """Return the authoritative live workspace snapshot for one binding."""
        flow_cards_tuple = tuple(flow_cards)
        sync_state = self.runtime_binding_service.sync_runtime_state(
            binding,
            runtime_application=runtime_application,
            flow_cards=flow_cards_tuple,
            daemon_startup_in_progress=daemon_startup_in_progress,
        )
        projection = self.rebuild_projection(
            binding,
            runtime_application=runtime_application,
            flow_cards=flow_cards_tuple,
            runtime_session=sync_state.runtime_session,
            now=now,
        )
        daemon_live = bool(getattr(sync_state.snapshot, "live", False))
        daemon_projection_version = int(getattr(sync_state.snapshot, "projection_version", 0) or 0)
        daemon_transport_mode = str(getattr(sync_state.snapshot, "transport_mode", "heartbeat") or "heartbeat")
        daemon_engine_starting = bool(getattr(sync_state.snapshot, "engine_starting", False))
        daemon_active_flow_names = tuple(getattr(sync_state.snapshot, "active_engine_flow_names", ()) or ())
        daemon_active_runs = tuple(getattr(sync_state.snapshot, "active_runs", ()) or ())
        daemon_flow_activity = tuple(getattr(sync_state.snapshot, "flow_activity", ()) or ())
        return self.snapshot_from_projection(
            binding=binding,
            flow_cards=flow_cards_tuple,
            projection=projection,
            workspace_control_state=sync_state.workspace_control_state,
            daemon_live=daemon_live,
            daemon_startup_in_progress=daemon_startup_in_progress,
            daemon_projection_version=daemon_projection_version,
            daemon_transport_mode=daemon_transport_mode,
            daemon_engine_starting=daemon_engine_starting,
            daemon_active_flow_names=daemon_active_flow_names,
            daemon_active_runs=daemon_active_runs,
            daemon_flow_activity=daemon_flow_activity,
        )

    def snapshot_from_projection(
        self,
        *,
        binding: WorkspaceRuntimeBinding,
        flow_cards: Iterable[FlowCatalogLike],
        projection: WorkspaceRuntimeProjection,
        workspace_control_state: WorkspaceControlState,
        daemon_live: bool,
        daemon_startup_in_progress: bool = False,
        daemon_projection_version: int = 0,
        daemon_transport_mode: str = "heartbeat",
        daemon_engine_starting: bool = False,
        daemon_active_flow_names: tuple[str, ...] = (),
        daemon_active_runs: tuple[ActiveRunState, ...] = (),
        daemon_flow_activity: tuple[FlowActivityState, ...] = (),
    ) -> WorkspaceSnapshot:
        """Build one authoritative workspace snapshot from an explicit projection."""
        flow_cards_tuple = tuple(flow_cards)
        return self._build_workspace_snapshot(
            binding=binding,
            flow_cards=flow_cards_tuple,
            runtime_session=projection.runtime_session,
            workspace_control_state=workspace_control_state,
            daemon_live=daemon_live,
            daemon_startup_in_progress=daemon_startup_in_progress,
            daemon_projection_version=daemon_projection_version,
            daemon_transport_mode=daemon_transport_mode,
            daemon_engine_starting=daemon_engine_starting,
            daemon_active_flow_names=daemon_active_flow_names,
            daemon_active_runs=daemon_active_runs,
            daemon_flow_activity=daemon_flow_activity,
            operation_tracker=projection.operation_tracker,
            flow_states=projection.flow_states,
        )

__all__ = [
    "ControlSnapshot",
    "EngineSnapshot",
    "FlowLiveSummary",
    "IncrementalDaemonProjection",
    "OperatorNotice",
    "RunLiveSnapshot",
    "RuntimeEvent",
    "RuntimeStatePort",
    "RuntimeStateService",
    "RuntimeStateSubscriber",
    "WorkspaceRuntimeProjection",
    "WorkspaceSnapshot",
    "flow_state_text_from_live_summary",
    "flow_state_texts_from_workspace_snapshot",
    "runtime_session_from_workspace_snapshot",
]
