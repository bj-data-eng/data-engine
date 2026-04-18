"""Runtime/daemon controllers for the terminal UI."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from textual.css.query import NoMatches
from textual.widgets import Button, ListView, Select, Static

from data_engine.application import RuntimeApplication
from data_engine.services import DaemonService, HistoryQueryService, RuntimeStateService
from data_engine.domain import RuntimeSessionState
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

    def daemon_wait_worker(self, window: "DataEngineTui") -> None:
        stop_event = getattr(window, "_daemon_wait_stop_event", None)
        while True:
            if stop_event is not None and stop_event.is_set():
                return
            if not window._has_authored_workspace():
                if stop_event is not None and stop_event.wait(1.5):
                    return
                continue
            manager = window.runtime_binding.daemon_manager
            previous_snapshot = getattr(manager, "_last_snapshot", None)
            snapshot = window.daemon_state_service.wait_for_update(manager, timeout_seconds=1.5)
            if stop_event is not None and stop_event.is_set():
                return
            if previous_snapshot is not None and snapshot == previous_snapshot:
                continue
            window._schedule_daemon_update_sync()

    @staticmethod
    def _blocked_status_text(window: "DataEngineTui") -> str:
        snapshot = getattr(window, "workspace_snapshot", None)
        if snapshot is None:
            return "Takeover available."
        return snapshot.control.blocked_status_text

    def refresh_buttons(self, window: "DataEngineTui") -> None:
        action_state = TuiActionState.from_context(
            build_operator_action_context(
                card=window._selected_card(),
                flow_states=window.flow_states,
                runtime_session=window.runtime_session,
                flow_groups_by_name={card.name: card.group for card in window.flow_cards},
                active_flow_states=window._ACTIVE_FLOW_STATES,
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
        window.query_one("#workspace-select", Select).disabled = action_state.workspace_select_disabled

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
        )
        if not window.workspace_snapshot.engine.daemon_live:
            self.ensure_daemon_started(window)
        window.runtime_session = projection.runtime_session
        try:
            window.query_one("#control-status", Static).update(
                surface_control_status_text(window.workspace_snapshot.control.control_status_text)
            )
        except NoMatches:
            return
        window.operation_tracker = projection.operation_tracker
        refresh_plan = self.runtime_application.plan_flow_state_refresh(
            previous_states=window.flow_states,
            next_states=projection.flow_states,
            runtime_session=window.runtime_session,
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
            runtime_session=window.runtime_session,
            now=window._monotonic(),
        )
        refresh_plan = self.runtime_application.plan_flow_state_refresh(
            previous_states=window.flow_states,
            next_states=projection.flow_states,
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
