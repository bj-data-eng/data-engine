"""Runtime/daemon controllers for the terminal UI."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from textual.css.query import NoMatches
from textual.widgets import Button, ListView, Select, Static

from data_engine.application import RuntimeApplication
from data_engine.services import DaemonService, LogService
from data_engine.domain import RuntimeSessionState, WorkspaceControlState
from data_engine.views import TuiActionState, WORKSPACE_UNAVAILABLE_TEXT, surface_control_status_text
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
        log_service: LogService,
    ) -> None:
        self.runtime_application = runtime_application
        self.daemon_service = daemon_service
        self.log_service = log_service

    def refresh_flow_list_items(self, window: "DataEngineTui") -> None:
        list_view = window.query_one("#flow-list", ListView)
        for child in list_view.children:
            if isinstance(child, FlowListItem):
                child.refresh_view(window.flow_states.get(child.card.name, child.card.state))

    def refresh_buttons(self, window: "DataEngineTui") -> None:
        action_state = TuiActionState.from_context(
            window.action_state_application.build_action_context(
                card=window._selected_card(),
                flow_states=window.flow_states,
                runtime_session=window.runtime_session,
                flow_groups_by_name={card.name: card.group for card in window.flow_cards},
                active_flow_states=window._ACTIVE_FLOW_STATES,
                has_logs=bool(
                    window.selected_flow_name is not None
                    and self.log_service.entries_for_flow(window.runtime_binding.log_store, window.selected_flow_name)
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
            window.runtime_session = RuntimeSessionState.empty()
            window.workspace_control_state = WorkspaceControlState.empty()
            window.flow_controller.reload_workspace_options(window)
            window.flow_controller.load_flows(window)
            try:
                window.query_one("#control-status", Static).update(WORKSPACE_UNAVAILABLE_TEXT)
            except NoMatches:
                return
            return
        try:
            live = self.daemon_service.is_live(window.workspace_paths)
        except Exception:
            live = False
        if not live:
            self.ensure_daemon_started(window)
        sync_state = self.runtime_application.sync_state(
            paths=window.workspace_paths,
            daemon_manager=window.runtime_binding.daemon_manager,
            flow_cards=window.flow_cards,
            runtime_ledger=window.runtime_binding.runtime_ledger,
            daemon_startup_in_progress=window._daemon_startup_in_progress,
        )
        window.runtime_session = sync_state.runtime_session
        window.workspace_control_state = sync_state.workspace_control_state
        try:
            window.query_one("#control-status", Static).update(
                surface_control_status_text(window.workspace_control_state.control_status_text)
            )
        except NoMatches:
            return
        self.rebuild_runtime_snapshot(window)

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
        self.log_service.reload(window.runtime_binding.log_store)
        snapshot = self.runtime_application.build_runtime_snapshot(
            flow_cards=window.flow_cards,
            log_entries=self.log_service.all_entries(window.runtime_binding.log_store),
            runtime_session=window.runtime_session,
            now=window._monotonic(),
        )
        refresh_plan = self.runtime_application.plan_flow_state_refresh(
            previous_states=window.flow_states,
            next_states=snapshot.flow_states,
            runtime_session=window.runtime_session,
        )
        window.operation_tracker = snapshot.operation_tracker
        states_changed = refresh_plan.signature != window._last_rendered_flow_signature
        window.flow_states = refresh_plan.flow_states
        if not window.runtime_session.workspace_owned:
            window._set_status(window.workspace_control_state.blocked_status_text)
        if states_changed:
            self.refresh_flow_list_items(window)
            window._last_rendered_flow_signature = refresh_plan.signature
        self.refresh_buttons(window)
        window.flow_controller.render_selected_flow(window)


__all__ = ["TuiRuntimeController"]
