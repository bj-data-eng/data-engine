"""Runtime/daemon controllers for the terminal UI."""

from __future__ import annotations

from dataclasses import replace
import threading
from typing import TYPE_CHECKING

from textual.css.query import NoMatches
from textual.widgets import Button, ListView, Static

from data_engine.application import RuntimeApplication
from data_engine.services import (
    DaemonService,
    HistoryQueryService,
    RuntimeStateService,
    flow_state_texts_from_workspace_snapshot,
    runtime_session_from_workspace_snapshot,
)
from data_engine.domain import DaemonStatusState, RuntimeSessionState
from data_engine.services.daemon_state import DaemonUpdateBatch
from data_engine.views import (
    TuiActionState,
    WORKSPACE_UNAVAILABLE_TEXT,
    build_operator_action_context,
    surface_control_status_text,
)
from data_engine.ui.tui.widgets import FlowListItem

if TYPE_CHECKING:
    from data_engine.ui.tui.app import DataEngineTui


class TuiRuntimeController:
    """Own daemon/runtime orchestration for the terminal UI."""

    def __init__(
        self,
        *,
        runtime_application: RuntimeApplication,
        daemon_service: DaemonService,
        history_query_service: HistoryQueryService,
        runtime_state_service: RuntimeStateService,
    ) -> None:
        self.runtime_application = runtime_application
        self.daemon_service = daemon_service
        self.history_query_service = history_query_service
        self.runtime_state_service = runtime_state_service

    def refresh_flow_list_items(self, window: "DataEngineTui") -> None:
        list_view = window.query_one("#flow-list", ListView)
        for child in list_view.children:
            if isinstance(child, FlowListItem):
                child.refresh_view(window.flow_states.get(child.card.name, child.card.state))

    def _refresh_runtime_projection_from_logs(self, window: "DataEngineTui"):
        """Rebuild the local runtime projection from persisted runtime history."""
        return self.runtime_state_service.rebuild_projection(
            window.runtime_binding,
            runtime_application=self.runtime_application,
            flow_cards=window.flow_cards,
            runtime_session=self._current_runtime_session(window),
            now=window._monotonic(),
        )

    def daemon_wait_worker(self, window: "DataEngineTui") -> None:
        window.daemon_state_service.run_subscription_loop(
            window.runtime_binding.daemon_manager,
            stop_event=window.daemon_subscription.stop_event,
            workspace_available=lambda: window._has_authored_workspace(),
            on_update=lambda batch: (
                window.daemon_subscription.mark_subscription(window._monotonic()),
                window._schedule_daemon_update_batch(batch),
            )[-1],
            timeout_seconds=window.daemon_subscription.timeout_seconds,
        )

    def _apply_daemon_live_snapshot(self, window: "DataEngineTui", batch: DaemonUpdateBatch) -> tuple[set[str], bool]:
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        if workspace_snapshot is None:
            self.sync_daemon_state(window)
            return set(card.name for card in window.flow_cards), True
        daemon_status = DaemonStatusState.from_snapshot(batch.snapshot)
        projection = self.runtime_state_service.incremental_projection_from_daemon(
            window.workspace_snapshot,
            flow_cards=window.flow_cards,
            previous_flow_states=window.flow_states,
            daemon_status=daemon_status,
            changed_flow_names=batch.changed_flow_names,
            requires_log_reload=bool(batch.completed_run_ids),
        )
        window.runtime_session = projection.runtime_session
        window.workspace_snapshot = projection.workspace_snapshot
        window.flow_states.update(projection.flow_states)
        return set(projection.changed_flow_names), projection.requires_log_reload

    @staticmethod
    def _apply_step_event(window: "DataEngineTui", event) -> None:
        card = next((candidate for candidate in window.flow_cards if candidate.name == event.flow_name), None)
        if card is None:
            return
        window.operation_tracker, _ = window.operation_tracker.apply_event(
            event.flow_name,
            card.operation_items,
            event,
            now=window._monotonic(),
        )

    def apply_daemon_update_batch(self, window: "DataEngineTui") -> None:
        batch = getattr(window, "_pending_daemon_update_batch", None)
        window._pending_daemon_update_batch = None
        if batch is None:
            return
        if batch.requires_full_sync or not window._has_authored_workspace():
            self.sync_daemon_state(window)
            return
        changed_flow_names, requires_log_reload = self._apply_daemon_live_snapshot(window, batch)
        if requires_log_reload:
            window.runtime_binding_service.reload_logs(window.runtime_binding)
        window.query_one("#control-status", Static).update(
            surface_control_status_text(window.workspace_snapshot.control.control_status_text)
        )
        selected_flow_name = window.selected_flow_name
        selected_flow_affected = selected_flow_name in changed_flow_names if selected_flow_name is not None else False
        refresh_flow_list = bool(changed_flow_names)
        refresh_buttons = False
        refresh_selection = False
        for update in batch.updates:
            if update.lane == "control":
                self.sync_daemon_state(window)
                return
            if update.lane in {"engine", "flow_activity", "run_lifecycle"}:
                refresh_buttons = True
            if update.lane == "log_events":
                for entry in update.log_entries:
                    synthesized_entry = (
                        entry
                        if entry.workspace_id is not None
                        else replace(entry, workspace_id=window.workspace_paths.workspace_id)
                    )
                    window.log_service.append_entry(window.runtime_binding.log_store, synthesized_entry)
                if selected_flow_name is not None and any(entry.flow_name == selected_flow_name for entry in update.log_entries):
                    refresh_selection = True
                if update.log_entries:
                    refresh_buttons = True
            if update.lane == "flow_activity" and selected_flow_affected:
                refresh_selection = True
            if update.lane == "run_lifecycle" and selected_flow_affected:
                refresh_selection = True
            if update.lane == "step_activity":
                for event in update.step_events:
                    self._apply_step_event(window, event)
                if selected_flow_affected:
                    refresh_selection = True
        if selected_flow_affected and batch.completed_run_ids:
            self._refresh_runtime_projection_from_logs(window)
        if refresh_flow_list:
            self.refresh_flow_list_items(window)
        if refresh_buttons:
            self.refresh_buttons(window)
        if refresh_selection:
            window.flow_controller.render_selected_flow(window)

    @staticmethod
    def _blocked_status_text(window: "DataEngineTui") -> str:
        snapshot = getattr(window, "workspace_snapshot", None)
        if snapshot is None:
            return "Takeover available."
        return snapshot.control.blocked_status_text

    @staticmethod
    def _current_runtime_session(window: "DataEngineTui") -> RuntimeSessionState:
        snapshot = getattr(window, "workspace_snapshot", None)
        if snapshot is None:
            return window.runtime_session
        return runtime_session_from_workspace_snapshot(snapshot)

    def refresh_buttons(self, window: "DataEngineTui") -> None:
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        effective_runtime_session = self._current_runtime_session(window)
        action_state = TuiActionState.from_context(
            build_operator_action_context(
                card=window._selected_card(),
                flow_states=window.flow_states,
                runtime_session=effective_runtime_session,
                flow_groups_by_name={card.name: card.group for card in window.flow_cards},
                active_flow_states=window._ACTIVE_FLOW_STATES,
                engine_state=(
                    workspace_snapshot.engine.state
                    if workspace_snapshot is not None
                    else "stopping"
                    if effective_runtime_session.runtime_stopping
                    else "running"
                    if effective_runtime_session.runtime_active
                    else "idle"
                ),
                engine_truth_known=workspace_snapshot is not None,
                live_runs=(workspace_snapshot.active_runs if workspace_snapshot is not None else None),
                has_logs=bool(
                    window.selected_flow_name is not None
                    and self.history_query_service.list_run_groups(
                        window.runtime_binding.log_store,
                        flow_name=window.selected_flow_name,
                        limit=1,
                    )
                ),
                has_automated_flows=any(card.valid and card.mode in {"poll", "schedule"} for card in window.flow_cards),
                workspace_available=window._has_authored_workspace(),
                selected_run_group_present=window.flow_controller.selected_run_group(window) is not None,
            )
        )
        window.query_one("#refresh", Button).disabled = action_state.refresh_disabled
        window.query_one("#run-once", Button).disabled = action_state.run_once_disabled
        window.query_one("#start-engine", Button).disabled = action_state.start_engine_disabled
        window.query_one("#stop-engine", Button).disabled = action_state.stop_engine_disabled
        window.query_one("#view-config", Button).disabled = action_state.view_config_disabled
        window.query_one("#view-log", Button).disabled = action_state.view_log_disabled
        window.query_one("#clear-flow-log", Button).disabled = action_state.clear_flow_log_disabled

    def sync_daemon_state(self, window: "DataEngineTui") -> None:
        if not window._has_authored_workspace():
            window.workspace_snapshot = None
            window.runtime_session = RuntimeSessionState.empty()
            window.flow_controller.reload_workspace_options(window)
            window.flow_controller.load_flows(window)
            try:
                window.query_one("#control-status", Static).update(WORKSPACE_UNAVAILABLE_TEXT)
            except NoMatches:
                return
            return
        sync_state = window.runtime_binding_service.sync_runtime_state(
            window.runtime_binding,
            runtime_application=self.runtime_application,
            flow_cards=tuple(window.flow_cards),
            daemon_startup_in_progress=window._daemon_startup_in_progress,
        )
        projection = self.runtime_state_service.rebuild_projection(
            window.runtime_binding,
            runtime_application=self.runtime_application,
            flow_cards=window.flow_cards,
            runtime_session=sync_state.runtime_session,
            now=window._monotonic(),
        )
        window.workspace_snapshot = self.runtime_state_service.snapshot_from_projection(
            binding=window.runtime_binding,
            flow_cards=window.flow_cards,
            projection=projection,
            workspace_control_state=sync_state.workspace_control_state,
            daemon_live=bool(getattr(sync_state.snapshot, "live", False)),
            daemon_startup_in_progress=window._daemon_startup_in_progress,
            daemon_transport_mode=str(getattr(sync_state.snapshot, "transport_mode", "heartbeat") or "heartbeat"),
        )
        if not window.workspace_snapshot.engine.daemon_live:
            self.ensure_daemon_started(window)
        window.runtime_session = projection.runtime_session
        window.daemon_subscription.mark_sync(window._monotonic())
        try:
            window.query_one("#control-status", Static).update(
                surface_control_status_text(window.workspace_snapshot.control.control_status_text)
            )
        except NoMatches:
            return
        window.operation_tracker = projection.operation_tracker
        next_flow_states = dict(projection.flow_states)
        if window.workspace_snapshot.engine.daemon_live:
            next_flow_states = flow_state_texts_from_workspace_snapshot(
                window.workspace_snapshot,
                window.flow_cards,
            )
        refresh_plan = self.runtime_application.plan_flow_state_refresh(
            previous_states=window.flow_states,
            next_states=next_flow_states,
            runtime_session=self._current_runtime_session(window),
        )
        states_changed = refresh_plan.signature != window._last_rendered_flow_signature
        window.flow_states = refresh_plan.flow_states
        if not window.runtime_session.workspace_owned:
            window._set_status(self._blocked_status_text(window))
        if states_changed:
            self.refresh_flow_list_items(window)
            window._last_rendered_flow_signature = refresh_plan.signature
        self.refresh_buttons(window)
        window.flow_controller.render_selected_flow(window)

    def ensure_daemon_started(self, window: "DataEngineTui") -> bool:
        if not window._has_authored_workspace():
            return False
        try:
            if self.daemon_service.is_live(window.workspace_paths):
                return True
        except Exception:
            pass
        if window._daemon_startup_in_progress:
            return False
        now = window._monotonic()
        if now - window._last_daemon_spawn_attempt < 2.0:
            return False
        window._last_daemon_spawn_attempt = now
        window._daemon_startup_in_progress = True
        threading.Thread(target=window._start_daemon_worker, daemon=True).start()
        return False

    def start_daemon_worker(self, window: "DataEngineTui") -> None:
        success = False
        error_text = ""
        spawn_result = self.runtime_application.spawn_daemon(window.workspace_paths)
        if not spawn_result.ok:
            error_text = spawn_result.error
        else:
            success = self.daemon_service.is_live(window.workspace_paths)
        if not success and not error_text:
            error_text = "Daemon startup did not provide any additional error details."
        window.call_from_thread(window._finish_daemon_startup, success, error_text)

    def finish_daemon_startup(self, window: "DataEngineTui", success: bool, error_text: str) -> None:
        window._daemon_startup_in_progress = False
        if success:
            self.sync_daemon_state(window)
            return
        if error_text:
            window._set_status(error_text)
        else:
            window._set_status("Daemon startup did not provide any additional error details.")
        self.sync_daemon_state(window)

    def rebuild_runtime_snapshot(self, window: "DataEngineTui") -> None:
        projection = self.runtime_state_service.rebuild_projection(
            window.runtime_binding,
            runtime_application=self.runtime_application,
            flow_cards=window.flow_cards,
            runtime_session=self._current_runtime_session(window),
            now=window._monotonic(),
        )
        next_flow_states = dict(projection.flow_states)
        if window.workspace_snapshot is not None and window.workspace_snapshot.engine.daemon_live:
            next_flow_states = flow_state_texts_from_workspace_snapshot(
                window.workspace_snapshot,
                window.flow_cards,
            )
        refresh_plan = self.runtime_application.plan_flow_state_refresh(
            previous_states=window.flow_states,
            next_states=next_flow_states,
            runtime_session=projection.runtime_session,
        )
        window.runtime_session = projection.runtime_session
        window.operation_tracker = projection.operation_tracker
        states_changed = refresh_plan.signature != window._last_rendered_flow_signature
        window.flow_states = refresh_plan.flow_states
        if not window.runtime_session.workspace_owned:
            window._set_status(self._blocked_status_text(window))
        if states_changed:
            self.refresh_flow_list_items(window)
            window._last_rendered_flow_signature = refresh_plan.signature
        self.refresh_buttons(window)
        window.flow_controller.render_selected_flow(window)


__all__ = ["TuiRuntimeController"]
