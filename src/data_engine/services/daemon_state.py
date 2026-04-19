"""Workspace daemon state and control services."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from dataclasses import dataclass
from threading import Event, Thread
from typing import Literal

from data_engine.domain import FlowLogEntry, RuntimeStepEvent, WorkspaceControlState, short_source_label
from data_engine.domain.time import parse_utc_text
from data_engine.hosts.daemon.manager import WorkspaceDaemonManager, WorkspaceDaemonSnapshot
from data_engine.hosts.daemon.shared_state import DaemonSharedStateAdapter
from data_engine.platform.workspace_models import WorkspacePaths

DaemonLaneName = Literal["control", "engine", "flow_activity", "run_lifecycle", "step_activity", "log_events"]


@dataclass(frozen=True)
class DaemonLaneUpdate:
    """One lane-scoped daemon update derived from snapshot changes."""

    lane: DaemonLaneName
    flow_names: tuple[str, ...] = ()
    run_ids: tuple[str, ...] = ()
    completed_run_ids: tuple[str, ...] = ()
    step_events: tuple[RuntimeStepEvent, ...] = ()
    log_entries: tuple[FlowLogEntry, ...] = ()


@dataclass(frozen=True)
class DaemonUpdateBatch:
    """One daemon snapshot plus the narrow update lanes it changed."""

    snapshot: WorkspaceDaemonSnapshot
    updates: tuple[DaemonLaneUpdate, ...]
    requires_full_sync: bool = False

    @property
    def changed_flow_names(self) -> tuple[str, ...]:
        """Return the union of flow names mentioned by this batch."""
        flow_names: set[str] = set()
        for update in self.updates:
            flow_names.update(update.flow_names)
        return tuple(sorted(flow_names))

    @property
    def completed_run_ids(self) -> tuple[str, ...]:
        """Return the union of completed run ids mentioned by this batch."""
        run_ids: set[str] = set()
        for update in self.updates:
            run_ids.update(update.completed_run_ids)
        return tuple(sorted(run_ids))


def _all_flow_names(snapshot: WorkspaceDaemonSnapshot) -> set[str]:
    names = {item.flow_name for item in snapshot.active_runs}
    names.update(item.flow_name for item in snapshot.flow_activity)
    names.update(snapshot.active_engine_flow_names)
    names.update(snapshot.manual_runs)
    return names


def _changed_flow_activity_names(
    previous: WorkspaceDaemonSnapshot,
    current: WorkspaceDaemonSnapshot,
) -> tuple[str, ...]:
    previous_activity = {item.flow_name: item for item in previous.flow_activity}
    current_activity = {item.flow_name: item for item in current.flow_activity}
    changed = {
        flow_name
        for flow_name in set(previous_activity) | set(current_activity)
        if previous_activity.get(flow_name) != current_activity.get(flow_name)
    }
    return tuple(sorted(changed))


def _updates_from_recent_events(
    current: WorkspaceDaemonSnapshot,
) -> tuple[DaemonLaneUpdate, ...]:
    lifecycle_flow_names: set[str] = set()
    changed_run_ids: set[str] = set()
    completed_run_ids: set[str] = set()
    step_flow_names: set[str] = set()
    step_run_ids: set[str] = set()
    step_events: list[RuntimeStepEvent] = []
    log_flow_names: set[str] = set()
    log_run_ids: set[str] = set()
    log_entries: list[FlowLogEntry] = []
    for item in current.recent_events:
        event_type = str(item.get("event_type", "")).strip()
        payload = item.get("payload")
        if not isinstance(payload, dict):
            continue
        payload_body = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
        if not isinstance(payload_body, dict):
            payload_body = payload
        event_time = parse_utc_text(item.get("timestamp_utc")) or datetime.now(UTC)
        if event_type == "runtime.run_started":
            run_id = _optional_text(payload_body.get("run_id"))
            flow_name = _optional_text(payload_body.get("flow_name"))
            if run_id is None or flow_name is None:
                continue
            source_path = _optional_text(payload_body.get("source_path"))
            lifecycle_flow_names.add(flow_name)
            changed_run_ids.add(run_id)
            log_flow_names.add(flow_name)
            log_run_ids.add(run_id)
            message = f"run={run_id} flow={flow_name} source={source_path} status=started"
            log_entries.append(
                FlowLogEntry(
                    line=FlowLogEntry.format_runtime_message(message),
                    kind="flow",
                    flow_name=flow_name,
                    workspace_id=None,
                    created_at_utc=parse_utc_text(payload_body.get("started_at_utc")) or event_time,
                    event=RuntimeStepEvent(
                        run_id=run_id,
                        flow_name=flow_name,
                        step_name=None,
                        source_label=short_source_label(source_path),
                        status="started",
                        elapsed_seconds=None,
                    ),
                )
            )
            continue
        if event_type == "runtime.run_finished":
            run_id = _optional_text(payload_body.get("run_id"))
            flow_name = _optional_text(payload_body.get("flow_name"))
            if run_id is None or flow_name is None:
                continue
            source_path = _optional_text(payload_body.get("source_path"))
            status = _optional_text(payload_body.get("status")) or "success"
            elapsed = _run_elapsed_seconds(
                started_at_utc=_optional_text(payload_body.get("started_at_utc")),
                finished_at_utc=_optional_text(payload_body.get("finished_at_utc")),
                fallback_elapsed_seconds=None,
                now_utc=event_time,
            )
            lifecycle_flow_names.add(flow_name)
            changed_run_ids.add(run_id)
            completed_run_ids.add(run_id)
            log_flow_names.add(flow_name)
            log_run_ids.add(run_id)
            message = f"run={run_id} flow={flow_name} source={source_path} status={status}"
            if elapsed is not None:
                message = f"{message} elapsed={elapsed:.6f}"
            log_entries.append(
                FlowLogEntry(
                    line=FlowLogEntry.format_runtime_message(message),
                    kind="flow",
                    flow_name=flow_name,
                    workspace_id=None,
                    created_at_utc=parse_utc_text(payload_body.get("finished_at_utc")) or event_time,
                    event=RuntimeStepEvent(
                        run_id=run_id,
                        flow_name=flow_name,
                        step_name=None,
                        source_label=short_source_label(source_path),
                        status=status,
                        elapsed_seconds=elapsed,
                    ),
                )
            )
            continue
        if event_type == "runtime.step_started":
            run_id = _optional_text(payload_body.get("run_id"))
            flow_name = _optional_text(payload_body.get("flow_name"))
            step_name = _optional_text(payload_body.get("step_label"))
            source_path = _optional_text(payload_body.get("source_path"))
            if run_id is None or flow_name is None or step_name is None:
                continue
            step_flow_names.add(flow_name)
            step_run_ids.add(run_id)
            step_events.append(
                RuntimeStepEvent(
                    run_id=run_id,
                    flow_name=flow_name,
                    step_name=step_name,
                    source_label=short_source_label(source_path),
                    status="started",
                    elapsed_seconds=None,
                )
            )
            continue
        if event_type == "runtime.step_finished":
            run_id = _optional_text(payload_body.get("run_id"))
            flow_name = _optional_text(payload_body.get("flow_name"))
            step_name = _optional_text(payload_body.get("step_label"))
            source_path = _optional_text(payload_body.get("source_path"))
            if run_id is None or flow_name is None or step_name is None:
                continue
            elapsed_ms = payload_body.get("elapsed_ms")
            elapsed_seconds = (
                max(float(elapsed_ms) / 1000.0, 0.0)
                if isinstance(elapsed_ms, int | float)
                else _step_elapsed_seconds(
                    started_at_utc=_optional_text(payload_body.get("started_at_utc")),
                    finished_at_utc=_optional_text(payload_body.get("finished_at_utc")),
                    fallback_elapsed_seconds=None,
                    now_utc=event_time,
                )
            )
            step_flow_names.add(flow_name)
            step_run_ids.add(run_id)
            step_events.append(
                RuntimeStepEvent(
                    run_id=run_id,
                    flow_name=flow_name,
                    step_name=step_name,
                    source_label=short_source_label(source_path),
                    status=_optional_text(payload_body.get("status")) or "success",
                    elapsed_seconds=elapsed_seconds,
                )
            )
    updates: list[DaemonLaneUpdate] = []
    if changed_run_ids or completed_run_ids:
        updates.append(
            DaemonLaneUpdate(
                "run_lifecycle",
                flow_names=tuple(sorted(lifecycle_flow_names)),
                run_ids=tuple(sorted(changed_run_ids)),
                completed_run_ids=tuple(sorted(completed_run_ids)),
            )
        )
    if step_events:
        updates.append(
            DaemonLaneUpdate(
                "step_activity",
                flow_names=tuple(sorted(step_flow_names)),
                run_ids=tuple(sorted(step_run_ids)),
                step_events=tuple(step_events),
            )
        )
    if log_entries:
        updates.append(
            DaemonLaneUpdate(
                "log_events",
                flow_names=tuple(sorted(log_flow_names)),
                run_ids=tuple(sorted(log_run_ids)),
                completed_run_ids=tuple(sorted(completed_run_ids)),
                log_entries=tuple(log_entries),
            )
        )
    return tuple(updates)


def _optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _step_elapsed_seconds(
    *,
    started_at_utc: str | None,
    finished_at_utc: str | None,
    fallback_elapsed_seconds: float | None,
    now_utc: datetime,
) -> float | None:
    if fallback_elapsed_seconds is not None:
        return max(float(fallback_elapsed_seconds), 0.0)
    started = parse_utc_text(started_at_utc)
    finished = parse_utc_text(finished_at_utc)
    if started is None:
        return None
    if finished is None:
        finished = now_utc
    return max((finished - started).total_seconds(), 0.0)


def _run_elapsed_seconds(
    *,
    started_at_utc: str | None,
    finished_at_utc: str | None,
    fallback_elapsed_seconds: float | None,
    now_utc: datetime,
) -> float | None:
    if fallback_elapsed_seconds is not None:
        return max(float(fallback_elapsed_seconds), 0.0)
    started = parse_utc_text(started_at_utc)
    finished = parse_utc_text(finished_at_utc)
    if started is None:
        return None
    if finished is None:
        finished = now_utc
    return max((finished - started).total_seconds(), 0.0)


class DaemonUpdateSubscription:
    """Own client-side daemon subscription state for one workspace manager."""

    def __init__(
        self,
        *,
        daemon_state_service: "DaemonStateService",
        manager: WorkspaceDaemonManager,
        clock: Callable[[], float],
        timeout_seconds: float = 1.5,
        stale_after_seconds: float = 15.0,
    ) -> None:
        self.daemon_state_service = daemon_state_service
        self.manager = manager
        self.clock = clock
        self.timeout_seconds = timeout_seconds
        self.stale_after_seconds = stale_after_seconds
        self.stop_event = Event()
        self.thread: Thread | None = None
        self.last_sync_monotonic = 0.0
        self.last_subscription_monotonic = 0.0

    def is_alive(self) -> bool:
        """Return whether the background subscription worker is alive."""
        thread = self.thread
        return bool(thread is not None and thread.is_alive())

    def mark_sync(self, now_monotonic: float | None = None) -> None:
        """Record one successful foreground sync timestamp."""
        self.last_sync_monotonic = float(self.clock() if now_monotonic is None else now_monotonic)

    def mark_subscription(self, now_monotonic: float | None = None) -> None:
        """Record one successful subscription update timestamp."""
        self.last_subscription_monotonic = float(self.clock() if now_monotonic is None else now_monotonic)

    def should_run_heartbeat(self, snapshot) -> bool:
        """Return whether fallback heartbeat sync should run for the current snapshot."""
        if snapshot is None:
            return True
        return self.daemon_state_service.should_run_heartbeat(
            daemon_live=snapshot.engine.daemon_live,
            transport_mode=snapshot.engine.transport,
            wait_worker_alive=self.is_alive(),
            now_monotonic=self.clock(),
            last_sync_monotonic=self.last_sync_monotonic,
            last_subscription_monotonic=self.last_subscription_monotonic,
            stale_after_seconds=self.stale_after_seconds,
        )

    def ensure_started(
        self,
        *,
        workspace_available: Callable[[], bool],
        on_update: Callable[[DaemonUpdateBatch], None],
        start_worker: Callable[[Callable[[], None]], Thread],
    ) -> Thread | None:
        """Start the background subscription worker if it is not already alive.

        `start_worker` must start the thread before returning it.
        """
        if self.is_alive():
            return self.thread
        self.stop_event.clear()

        def _run() -> None:
            def _handle_update(batch: DaemonUpdateBatch) -> None:
                self.mark_subscription()
                on_update(batch)

            self.daemon_state_service.run_subscription_loop(
                self.manager,
                stop_event=self.stop_event,
                workspace_available=workspace_available,
                on_update=_handle_update,
                timeout_seconds=self.timeout_seconds,
            )

        self.thread = start_worker(_run)
        return self.thread

    def stop(self) -> None:
        """Request the background subscription worker to stop."""
        self.stop_event.set()


class DaemonStateService:
    """Own workspace daemon-manager construction and normalized snapshot access."""

    def __init__(self, *, shared_state_adapter: DaemonSharedStateAdapter | None = None) -> None:
        self.shared_state_adapter = shared_state_adapter or DaemonSharedStateAdapter()

    def create_manager(self, paths: WorkspacePaths) -> WorkspaceDaemonManager:
        """Create one daemon-state manager for a workspace."""
        return WorkspaceDaemonManager(paths, shared_state_adapter=self.shared_state_adapter)

    def sync(self, manager: WorkspaceDaemonManager) -> WorkspaceDaemonSnapshot:
        """Fetch one normalized daemon snapshot."""
        return manager.sync()

    def wait_for_update(
        self,
        manager: WorkspaceDaemonManager,
        *,
        timeout_seconds: float = 5.0,
    ) -> WorkspaceDaemonSnapshot:
        """Wait for one daemon projection update and return the normalized snapshot."""
        return manager.wait_for_update(timeout_seconds=timeout_seconds)

    def run_subscription_loop(
        self,
        manager: WorkspaceDaemonManager,
        *,
        stop_event: Event,
        workspace_available: Callable[[], bool],
        on_update: Callable[[DaemonUpdateBatch], None],
        timeout_seconds: float = 1.5,
    ) -> None:
        """Drive one long-poll subscription loop until stopped.

        The daemon-state service owns the transport semantics here:
        authored-workspace gating, long-poll waiting, unchanged-snapshot
        suppression, and lane-scoped batch derivation. Surfaces provide only the
        stop signal and the update sink.
        """
        while not stop_event.is_set():
            if not workspace_available():
                if stop_event.wait(timeout_seconds):
                    return
                continue
            previous_snapshot = getattr(manager, "_last_snapshot", None)
            snapshot = self.wait_for_update(manager, timeout_seconds=timeout_seconds)
            if stop_event.is_set():
                return
            if previous_snapshot is not None and snapshot == previous_snapshot:
                continue
            batch = self.diff_update_batch(previous_snapshot, snapshot)
            if not batch.updates and not batch.requires_full_sync:
                continue
            on_update(batch)

    @staticmethod
    def diff_update_batch(
        previous: WorkspaceDaemonSnapshot | None,
        current: WorkspaceDaemonSnapshot,
    ) -> DaemonUpdateBatch:
        """Return lane-scoped update information for one daemon snapshot change."""
        if previous is None:
            flow_names = tuple(sorted(_all_flow_names(current)))
            run_ids = tuple(sorted(run.run_id for run in current.active_runs))
            return DaemonUpdateBatch(
                snapshot=current,
                updates=(
                    DaemonLaneUpdate("control"),
                    DaemonLaneUpdate("engine", flow_names=flow_names),
                    DaemonLaneUpdate("flow_activity", flow_names=flow_names),
                    DaemonLaneUpdate("run_lifecycle", flow_names=flow_names, run_ids=run_ids),
                    DaemonLaneUpdate(
                        "step_activity",
                        flow_names=flow_names,
                        run_ids=run_ids,
                        step_events=tuple(
                            RuntimeStepEvent(
                                run_id=run.run_id,
                                flow_name=run.flow_name,
                                step_name=run.current_step_name,
                                source_label=short_source_label(run.source_path),
                                status="started",
                                elapsed_seconds=None,
                            )
                            for run in current.active_runs
                            if run.current_step_name
                        ),
                    ),
                ),
                requires_full_sync=True,
            )
        if current.daemon_id != previous.daemon_id or current.events_truncated:
            return DaemonUpdateBatch(
                snapshot=current,
                updates=(DaemonLaneUpdate("control"), DaemonLaneUpdate("engine")),
                requires_full_sync=True,
            )
        updates: list[DaemonLaneUpdate] = []
        if (
            current.live != previous.live
            or current.workspace_owned != previous.workspace_owned
            or current.leased_by_machine_id != previous.leased_by_machine_id
            or current.source != previous.source
        ):
            updates.append(DaemonLaneUpdate("control"))
        if (
            current.runtime_active != previous.runtime_active
            or current.runtime_stopping != previous.runtime_stopping
            or current.engine_starting != previous.engine_starting
            or current.transport_mode != previous.transport_mode
            or current.active_engine_flow_names != previous.active_engine_flow_names
        ):
            engine_flows = tuple(sorted(set(current.active_engine_flow_names) | set(previous.active_engine_flow_names)))
            updates.append(DaemonLaneUpdate("engine", flow_names=engine_flows))
        changed_activity_flows = _changed_flow_activity_names(previous, current)
        if changed_activity_flows:
            updates.append(DaemonLaneUpdate("flow_activity", flow_names=changed_activity_flows))
        updates.extend(_updates_from_recent_events(current))
        return DaemonUpdateBatch(snapshot=current, updates=tuple(updates), requires_full_sync=False)

    @staticmethod
    def should_run_heartbeat(
        *,
        daemon_live: bool,
        transport_mode: str,
        wait_worker_alive: bool,
        now_monotonic: float,
        last_sync_monotonic: float,
        last_subscription_monotonic: float,
        stale_after_seconds: float = 15.0,
    ) -> bool:
        """Return whether fallback heartbeat sync should run right now."""
        if not daemon_live:
            return True
        if transport_mode != "subscription":
            return True
        if not wait_worker_alive:
            return True
        freshest = max(float(last_sync_monotonic or 0.0), float(last_subscription_monotonic or 0.0))
        return (float(now_monotonic) - freshest) >= max(float(stale_after_seconds), 0.0)

    def control_state(
        self,
        manager: WorkspaceDaemonManager,
        snapshot: WorkspaceDaemonSnapshot,
        *,
        daemon_startup_in_progress: bool = False,
    ) -> WorkspaceControlState:
        """Build structured workspace control state from one daemon snapshot."""
        return manager.control_state(snapshot, daemon_startup_in_progress=daemon_startup_in_progress)

    def request_control(self, manager: WorkspaceDaemonManager) -> str:
        """Request workspace control through one daemon-state manager."""
        return manager.request_control()


def merge_update_batches(
    existing: DaemonUpdateBatch | None,
    incoming: DaemonUpdateBatch,
) -> DaemonUpdateBatch:
    """Merge two pending daemon update batches into one coalesced batch."""
    if existing is None:
        return incoming
    merged_by_lane: dict[DaemonLaneName, DaemonLaneUpdate] = {update.lane: update for update in existing.updates}
    for update in incoming.updates:
        current = merged_by_lane.get(update.lane)
        if current is None:
            merged_by_lane[update.lane] = update
            continue
        merged_step_events = current.step_events
        if update.step_events:
            seen = {
                (
                    event.run_id,
                    event.flow_name,
                    event.step_name,
                    event.source_label,
                    event.status,
                    event.elapsed_seconds,
                )
                for event in current.step_events
            }
            merged_list = list(current.step_events)
            for event in update.step_events:
                signature = (
                    event.run_id,
                    event.flow_name,
                    event.step_name,
                    event.source_label,
                    event.status,
                    event.elapsed_seconds,
                )
                if signature in seen:
                    continue
                seen.add(signature)
                merged_list.append(event)
            merged_step_events = tuple(merged_list)
        merged_log_entries = current.log_entries
        if update.log_entries:
            seen = {entry.fingerprint() for entry in current.log_entries}
            merged_list = list(current.log_entries)
            for entry in update.log_entries:
                signature = entry.fingerprint()
                if signature in seen:
                    continue
                seen.add(signature)
                merged_list.append(entry)
            merged_log_entries = tuple(merged_list)
        merged_by_lane[update.lane] = DaemonLaneUpdate(
            update.lane,
            flow_names=tuple(sorted(set(current.flow_names) | set(update.flow_names))),
            run_ids=tuple(sorted(set(current.run_ids) | set(update.run_ids))),
            completed_run_ids=tuple(sorted(set(current.completed_run_ids) | set(update.completed_run_ids))),
            step_events=merged_step_events,
            log_entries=merged_log_entries,
        )
    return DaemonUpdateBatch(
        snapshot=incoming.snapshot,
        updates=tuple(merged_by_lane.values()),
        requires_full_sync=existing.requires_full_sync or incoming.requires_full_sync,
    )


__all__ = [
    "DaemonLaneName",
    "DaemonLaneUpdate",
    "DaemonStateService",
    "DaemonUpdateBatch",
    "DaemonUpdateSubscription",
    "merge_update_batches",
]
