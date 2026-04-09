"""Runtime and daemon orchestration controllers for the desktop GUI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_engine.application import RuntimeApplication
from data_engine.services import DaemonService, LogService
from data_engine.domain import DaemonStatusState, RuntimeSessionState, WorkspaceControlState
from data_engine.platform.identity import APP_DISPLAY_NAME
from data_engine.ui.gui.helpers import start_worker_thread
if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


class GuiRuntimeController:
    """Own daemon/runtime orchestration for the desktop GUI."""

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

    def sync_from_daemon(self, window: "DataEngineWindow") -> None:
        if not window._has_authored_workspace():
            window.daemon_status = DaemonStatusState.empty()
            window.workspace_control_state = WorkspaceControlState.empty()
            window.runtime_session = RuntimeSessionState.empty()
            window.flow_controller.reload_workspace_options(window)
            window.flow_controller.load_flows(window)
            return
        try:
            live = self.daemon_service.is_live(window.workspace_paths)
        except Exception:
            live = False
        if not live and window._auto_daemon_enabled:
            self.ensure_daemon_started(window)
        sync_state = self.runtime_application.sync_state(
            paths=window.workspace_paths,
            daemon_manager=window.runtime_binding.daemon_manager,
            flow_cards=window.flow_cards.values(),
            runtime_ledger=window.runtime_binding.runtime_ledger,
            daemon_startup_in_progress=window._daemon_startup_in_progress,
        )
        window.daemon_status = sync_state.daemon_status
        window.workspace_control_state = sync_state.workspace_control_state
        window.runtime_session = sync_state.runtime_session
        window._apply_daemon_snapshot(sync_state.snapshot)
        self.rebuild_runtime_snapshot(window)

    def ensure_daemon_started(self, window: "DataEngineWindow") -> bool:
        if not window._has_authored_workspace():
            return False
        try:
            if self.daemon_service.is_live(window.workspace_paths):
                return True
        except Exception:
            pass
        if window._daemon_startup_in_progress or not window._auto_daemon_enabled or window.ui_closing:
            return False
        now = window._monotonic()
        if now - window._last_daemon_spawn_attempt < 2.0:
            return False
        window._last_daemon_spawn_attempt = now
        window._daemon_startup_in_progress = True
        start_worker_thread(window, target=window._start_daemon_worker)
        return False

    def start_daemon_worker(self, window: "DataEngineWindow") -> None:
        success = False
        error_text = ""
        spawn_result = self.runtime_application.spawn_daemon(window.workspace_paths)
        if not spawn_result.ok:
            error_text = spawn_result.error
        else:
            success = self.daemon_service.is_live(window.workspace_paths)
        if not success and not error_text:
            error_text = "Daemon startup did not provide any additional error details."
        window.signals.daemon_startup_finished.emit(success, error_text)

    def rebuild_runtime_snapshot(self, window: "DataEngineWindow") -> None:
        self.log_service.reload(window.runtime_binding.log_store)
        window._rehydrate_step_outputs_from_ledger()
        snapshot = self.runtime_application.build_runtime_snapshot(
            flow_cards=window.flow_cards.values(),
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
        window.flow_states = refresh_plan.flow_states
        window._refresh_sidebar_state_views(set(refresh_plan.changed_flow_names))
        if window.selected_flow_name is not None and window.selected_flow_name in window.flow_cards:
            window.flow_controller.refresh_selection(window, window.flow_cards[window.selected_flow_name])
        window.flow_controller.refresh_summary(window)
        window._refresh_workspace_visibility_panel()
        window._refresh_log_view()
        window.flow_controller.refresh_action_buttons(window)

    def start_runtime(self, window: "DataEngineWindow") -> None:
        if not window._has_authored_workspace():
            window._sync_from_daemon()
            return
        result = window.control_application.start_engine(
            paths=window.workspace_paths,
            runtime_session=window.runtime_session,
            has_automated_flows=any(card.valid and card.mode in {"poll", "schedule"} for card in window.flow_cards.values()),
            blocked_status_text=window.workspace_control_state.blocked_status_text,
            timeout=2.0,
        )
        if result.error_text is not None:
            window._show_message_box_later(
                title=APP_DISPLAY_NAME,
                text=result.error_text,
                tone="error",
            )
            return
        if result.sync_after:
            window._sync_from_daemon()

    def stop_runtime(self, window: "DataEngineWindow") -> None:
        result = window.control_application.stop_pipeline(
            paths=window.workspace_paths,
            runtime_session=window.runtime_session,
            selected_flow_group=None,
            blocked_status_text=window.workspace_control_state.blocked_status_text,
            timeout=2.0,
        )
        if result.error_text is not None:
            window._show_message_box_later(
                title=APP_DISPLAY_NAME,
                text=result.error_text,
                tone="error",
            )
            return
        if result.sync_after:
            window._sync_from_daemon()

    def toggle_runtime(self, window: "DataEngineWindow") -> None:
        if window.runtime_session.runtime_active:
            self.stop_runtime(window)
            return
        self.start_runtime(window)

    def stop_pipeline(self, window: "DataEngineWindow") -> None:
        card = window.flow_cards.get(window.selected_flow_name or "")
        result = window.control_application.stop_pipeline(
            paths=window.workspace_paths,
            runtime_session=window.runtime_session,
            selected_flow_group=card.group if card is not None else None,
            blocked_status_text=window.workspace_control_state.blocked_status_text,
            timeout=2.0,
        )
        if result.error_text is not None:
            window._show_message_box_later(
                title=APP_DISPLAY_NAME,
                text=result.error_text,
                tone="error",
            )
            return
        if result.sync_after:
            window._sync_from_daemon()

    def finish_run(self, window: "DataEngineWindow", flow_name: object, results: object, error: object) -> None:
        assert isinstance(flow_name, str)
        card = window.flow_cards.get(flow_name)
        group_name = card.group if card is not None else next(
            (run.group_name for run in window.runtime_session.manual_runs if run.flow_name == flow_name),
            None,
        )
        stop_event = window.manual_flow_stop_events.pop(group_name, None)
        stop_requested = stop_event.is_set() if stop_event is not None else False
        if stop_event is not None:
            stop_event.clear()
        completion = self.runtime_application.complete_manual_run(
            runtime_session=window.runtime_session,
            flow_name=flow_name,
            group_name=group_name,
            flow_mode=card.mode if card is not None else "manual",
            results=results,
            error=error,
            stop_requested=stop_requested,
        )
        window.runtime_session = completion.runtime_session
        window.flow_controller.set_flow_states(window, completion.state_updates)
        for message in completion.log_messages:
            window._append_log_line(message.text, flow_name=message.flow_name)
        if completion.capture_results:
            window._capture_step_outputs(flow_name, results)
        if completion.normalize_operations:
            window._normalize_completed_operation_rows(flow_name)
        if completion.render_durations:
            window._render_operation_durations(flow_name)
        if completion.show_error_text is not None:
            window._show_message_box_later(
                title=APP_DISPLAY_NAME,
                text=completion.show_error_text,
                tone="error",
            )
        window.flow_controller.refresh_action_buttons(window)

    def finish_runtime(self, window: "DataEngineWindow", flow_names: object, results: object, error: object) -> None:
        active_runtime_flow_names = tuple(flow_names) if isinstance(flow_names, tuple) else window.runtime_session.active_runtime_flow_names
        runtime_stop_requested = window.engine_runtime_stop_event.is_set()
        flow_stop_requested = window.engine_flow_stop_event.is_set()
        completion = self.runtime_application.complete_engine_run(
            runtime_session=window.runtime_session,
            flow_names=active_runtime_flow_names,
            flow_modes_by_name={
                flow_name: (window.flow_cards[flow_name].mode if flow_name in window.flow_cards else None)
                for flow_name in active_runtime_flow_names
            },
            error=error,
            runtime_stop_requested=runtime_stop_requested,
            flow_stop_requested=flow_stop_requested,
        )
        window.runtime_session = completion.runtime_session
        window.engine_runtime_stop_event.clear()
        window.engine_flow_stop_event.clear()
        window.flow_controller.set_flow_states(window, completion.state_updates)
        for message in completion.log_messages:
            window._append_log_line(message.text, flow_name=message.flow_name)
        for failed_flow_name in completion.failed_flow_names:
            window.flow_controller.set_flow_state(window, failed_flow_name, "failed")
        window.flow_controller.refresh_action_buttons(window)

    def is_group_active(self, window: "DataEngineWindow", group_name: str) -> bool:
        return window.runtime_session.is_group_active(
            group_name,
            {flow_name: card.group for flow_name, card in window.flow_cards.items()},
        )


__all__ = ["GuiRuntimeController"]
