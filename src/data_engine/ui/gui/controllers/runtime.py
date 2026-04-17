"""Runtime and daemon orchestration controllers for the desktop GUI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_engine.application import RuntimeApplication
from data_engine.services import DaemonService, RuntimeStateService
from data_engine.domain import DaemonStatusState, RuntimeSessionState
from data_engine.platform.identity import APP_DISPLAY_NAME
from data_engine.platform.instrumentation import append_timing_line, timed_operation
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
        runtime_state_service: RuntimeStateService,
    ) -> None:
        self.runtime_application = runtime_application
        self.daemon_service = daemon_service
        self.runtime_state_service = runtime_state_service

    def _apply_runtime_projection(self, window: "DataEngineWindow", *, runtime_session, operation_tracker, flow_states, step_output_index) -> None:
        active_manual_groups = {run.group_name for run in runtime_session.manual_runs}
        window.manual_flow_stopping_groups.intersection_update(active_manual_groups)
        refresh_plan = self.runtime_application.plan_flow_state_refresh(
            previous_states=window.flow_states,
            next_states=flow_states,
            runtime_session=runtime_session,
        )
        next_flow_states = dict(refresh_plan.flow_states)
        for group_name in window.manual_flow_stopping_groups:
            flow_name = runtime_session.manual_flow_name_for_group(group_name)
            if flow_name is not None and next_flow_states.get(flow_name) != "failed":
                next_flow_states[flow_name] = "stopping flow"
        refresh_plan = self.runtime_application.plan_flow_state_refresh(
            previous_states=window.flow_states,
            next_states=next_flow_states,
            runtime_session=runtime_session,
        )
        window.runtime_session = runtime_session
        window.step_output_index = step_output_index
        window.operation_tracker = operation_tracker
        window.flow_states = refresh_plan.flow_states
        window._refresh_sidebar_state_views(set(refresh_plan.changed_flow_names))
        if window.selected_flow_name is not None and window.selected_flow_name in window.flow_cards:
            window.flow_controller.refresh_selection(window, window.flow_cards[window.selected_flow_name])
        window.flow_controller.refresh_summary(window)
        window._refresh_workspace_visibility_panel()
        window._refresh_log_view()
        window.flow_controller.refresh_action_buttons(window)

    @staticmethod
    def _blocked_status_text(window: "DataEngineWindow") -> str:
        snapshot = getattr(window, "workspace_snapshot", None)
        if snapshot is None:
            return "Takeover available."
        return snapshot.control.blocked_status_text

    def sync_from_daemon(self, window: "DataEngineWindow") -> None:
        if window._daemon_sync_in_progress:
            window._daemon_sync_pending = True
            return
        window._daemon_sync_in_progress = True
        rerun_requested = False
        try:
            with timed_operation(
                window._ui_timing_log_path,
                scope="gui.sync",
                event="sync_from_daemon",
                fields={"workspace": window.workspace_paths.workspace_id},
            ):
                if not window._has_authored_workspace():
                    window.workspace_snapshot = None
                    window.daemon_status = DaemonStatusState.empty()
                    window.runtime_session = RuntimeSessionState.empty()
                    window.flow_controller.reload_workspace_options(window)
                    window.flow_controller.load_flows(window)
                    return
                sync_state = window.runtime_binding_service.sync_runtime_state(
                    window.runtime_binding,
                    runtime_application=self.runtime_application,
                    flow_cards=tuple(window.flow_cards.values()),
                    daemon_startup_in_progress=window._daemon_startup_in_progress,
                )
                projection = self.runtime_state_service.rebuild_projection(
                    window.runtime_binding,
                    runtime_application=self.runtime_application,
                    flow_cards=window.flow_cards.values(),
                    runtime_session=sync_state.runtime_session,
                    now=window._monotonic(),
                )
                window.workspace_snapshot = self.runtime_state_service.snapshot_from_projection(
                    binding=window.runtime_binding,
                    flow_cards=window.flow_cards.values(),
                    projection=projection,
                    workspace_control_state=sync_state.workspace_control_state,
                    daemon_live=bool(getattr(sync_state.snapshot, "live", False)),
                    daemon_startup_in_progress=window._daemon_startup_in_progress,
                )
                if not window.workspace_snapshot.engine.daemon_live and window._auto_daemon_enabled:
                    self.ensure_daemon_started(window)
                window.daemon_status = sync_state.daemon_status
                self._apply_runtime_projection(
                    window,
                    runtime_session=projection.runtime_session,
                    operation_tracker=projection.operation_tracker,
                    flow_states=projection.flow_states,
                    step_output_index=projection.step_output_index,
                )
        finally:
            window._daemon_sync_in_progress = False
            rerun_requested = window._daemon_sync_pending and not window.ui_closing
            window._daemon_sync_pending = False
        if rerun_requested:
            self.sync_from_daemon(window)

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
        if window.ui_closing:
            window.signals.daemon_startup_finished.emit(False, "")
            return
        success = False
        error_text = ""
        spawn_result = self.runtime_application.spawn_daemon(window.workspace_paths)
        if not spawn_result.ok:
            error_text = spawn_result.error
        elif window.ui_closing:
            self._shutdown_started_daemon_if_orphaned(window)
        else:
            success = self.daemon_service.is_live(window.workspace_paths)
        if not success and not error_text:
            error_text = "Daemon startup did not provide any additional error details."
        window.signals.daemon_startup_finished.emit(success, error_text)

    def _shutdown_started_daemon_if_orphaned(self, window: "DataEngineWindow") -> None:
        """Shut down one late-starting ephemeral daemon when no local clients remain."""
        temporary_binding = None
        try:
            temporary_binding = window.runtime_binding_service.open_binding(window.workspace_paths)
            remaining_clients = window.runtime_binding_service.count_live_client_sessions(temporary_binding)
        except Exception:
            remaining_clients = 1
        finally:
            if temporary_binding is not None:
                try:
                    window.runtime_binding_service.close_binding(temporary_binding)
                except Exception:
                    pass
        if remaining_clients != 0:
            return
        try:
            window._shutdown_daemon_on_close()
        except Exception:
            return

    def rebuild_runtime_snapshot(self, window: "DataEngineWindow") -> None:
        with timed_operation(
            window._ui_timing_log_path,
            scope="gui.sync",
            event="rebuild_runtime_snapshot",
            fields={"workspace": window.workspace_paths.workspace_id},
        ):
            projection = self.runtime_state_service.rebuild_projection(
                window.runtime_binding,
                runtime_application=self.runtime_application,
                flow_cards=window.flow_cards.values(),
                runtime_session=window.runtime_session,
                now=window._monotonic(),
            )
        self._apply_runtime_projection(
            window,
            runtime_session=projection.runtime_session,
            operation_tracker=projection.operation_tracker,
            flow_states=projection.flow_states,
            step_output_index=projection.step_output_index,
        )

    @staticmethod
    def _begin_control_action(window: "DataEngineWindow", action_name: str, *, target, args: tuple[object, ...] = ()) -> bool:
        if action_name in window._pending_control_actions or window.ui_closing:
            return False
        window._pending_control_actions.add(action_name)
        start_worker_thread(window, target=target, args=args)
        window.flow_controller.refresh_action_buttons(window)
        return True

    @staticmethod
    def _emit_control_action_finished(window: "DataEngineWindow", action_name: str, payload: object) -> None:
        if window.ui_closing:
            return
        try:
            window.signals.control_action_finished.emit(action_name, payload)
        except RuntimeError:
            pass

    def run_selected_flow(self, window: "DataEngineWindow") -> None:
        if not window._has_authored_workspace():
            window._sync_from_daemon()
            return
        card = window.flow_cards.get(window.selected_flow_name or "")
        if (
            card is not None
            and card.name == window.runtime_session.manual_flow_name_for_group(card.group)
        ):
            self.stop_pipeline(window)
            return
        action_args = (
            window,
            {
                "paths": window.workspace_paths,
                "runtime_session": window.runtime_session,
                "selected_flow_name": card.name if card is not None else None,
                "selected_flow_valid": bool(card is not None and card.valid),
                "selected_flow_group": card.group if card is not None else None,
                "selected_flow_group_active": bool(card is not None and self.is_group_active(window, card.group)) if card is not None else False,
                "blocked_status_text": self._blocked_status_text(window),
                "timeout": 5.0,
            },
            card.name if card is not None else None,
        )
        self._begin_control_action(window, "run_selected_flow", target=self._run_selected_flow_worker, args=action_args)

    def _run_selected_flow_worker(
        self,
        window: "DataEngineWindow",
        action_kwargs: dict[str, object],
        card_name: str | None,
    ) -> None:
        with timed_operation(
            window._ui_timing_log_path,
            scope="gui.action",
            event="run_selected_flow",
            fields={"flow": card_name},
        ):
            result = window.control_application.run_selected_flow(**action_kwargs)
        self._emit_control_action_finished(
            window,
            "run_selected_flow",
            {"result": result, "card_name": card_name},
        )

    def start_runtime(self, window: "DataEngineWindow") -> None:
        if not window._has_authored_workspace():
            window._sync_from_daemon()
            return
        action_kwargs = {
            "paths": window.workspace_paths,
            "runtime_session": window.runtime_session,
            "has_automated_flows": any(card.valid and card.mode in {"poll", "schedule"} for card in window.flow_cards.values()),
            "blocked_status_text": self._blocked_status_text(window),
            "timeout": 5.0,
        }
        self._begin_control_action(window, "start_runtime", target=self._start_runtime_worker, args=(window, action_kwargs))

    def _start_runtime_worker(self, window: "DataEngineWindow", action_kwargs: dict[str, object]) -> None:
        with timed_operation(window._ui_timing_log_path, scope="gui.action", event="start_engine"):
            result = window.control_application.start_engine(**action_kwargs)
        self._emit_control_action_finished(window, "start_runtime", result)

    def stop_runtime(self, window: "DataEngineWindow") -> None:
        if window.runtime_session.runtime_active and not window.runtime_session.runtime_stopping:
            window.runtime_session = window.runtime_session.with_runtime_flags(active=True, stopping=True)
            stopping_updates = {
                flow_name: "stopping runtime"
                for flow_name in window.runtime_session.active_runtime_flow_names
                if flow_name in window.flow_states and window.flow_states.get(flow_name) != "failed"
            }
            if stopping_updates:
                window.flow_controller.set_flow_states(window, stopping_updates)
        action_kwargs = {
            "paths": window.workspace_paths,
            "runtime_session": window.runtime_session,
            "selected_flow_group": None,
            "blocked_status_text": self._blocked_status_text(window),
            "timeout": 5.0,
        }
        self._begin_control_action(window, "stop_runtime", target=self._stop_runtime_worker, args=(window, action_kwargs))

    def _stop_runtime_worker(self, window: "DataEngineWindow", action_kwargs: dict[str, object]) -> None:
        with timed_operation(window._ui_timing_log_path, scope="gui.action", event="stop_engine"):
            result = window.control_application.stop_pipeline(**action_kwargs)
        self._emit_control_action_finished(window, "stop_runtime", result)

    def toggle_runtime(self, window: "DataEngineWindow") -> None:
        if window.runtime_session.runtime_active:
            self.stop_runtime(window)
            return
        self.start_runtime(window)

    def stop_pipeline(self, window: "DataEngineWindow") -> None:
        card = window.flow_cards.get(window.selected_flow_name or "")
        selected_manual_running = bool(
            card is not None
            and card.name == window.runtime_session.manual_flow_name_for_group(card.group)
        )
        if (
            window.runtime_session.runtime_active
            and not window.runtime_session.runtime_stopping
            and not selected_manual_running
        ):
            window.runtime_session = window.runtime_session.with_runtime_flags(active=True, stopping=True)
            stopping_updates = {
                flow_name: "stopping runtime"
                for flow_name in window.runtime_session.active_runtime_flow_names
                if flow_name in window.flow_states and window.flow_states.get(flow_name) != "failed"
            }
            if stopping_updates:
                window.flow_controller.set_flow_states(window, stopping_updates)
        action_kwargs = {
            "paths": window.workspace_paths,
            "runtime_session": window.runtime_session,
            "selected_flow_group": card.group if card is not None else None,
            "blocked_status_text": self._blocked_status_text(window),
            "timeout": 5.0,
        }
        if self._begin_control_action(window, "stop_pipeline", target=self._stop_pipeline_worker, args=(window, action_kwargs)):
            if selected_manual_running:
                window.manual_flow_stopping_groups.add(card.group)
                if card.name in window.flow_states and window.flow_states.get(card.name) != "failed":
                    window.flow_controller.set_flow_state(window, card.name, "stopping flow")
                window.flow_controller.refresh_action_buttons(window)

    def _stop_pipeline_worker(self, window: "DataEngineWindow", action_kwargs: dict[str, object]) -> None:
        with timed_operation(window._ui_timing_log_path, scope="gui.action", event="stop_pipeline"):
            result = window.control_application.stop_pipeline(**action_kwargs)
        self._emit_control_action_finished(window, "stop_pipeline", result)

    def finish_control_action(self, window: "DataEngineWindow", action_name: str, payload: object) -> None:
        window._pending_control_actions.discard(action_name)
        window.flow_controller.refresh_action_buttons(window)
        if window.ui_closing:
            return
        if action_name == "run_selected_flow":
            assert isinstance(payload, dict)
            result = payload.get("result")
            card_name = payload.get("card_name")
            if getattr(result, "error_text", None) is not None:
                window._show_message_box_later(
                    title=APP_DISPLAY_NAME,
                    text=result.error_text,
                    tone="error",
                )
                return
            if not getattr(result, "requested", False) or not isinstance(card_name, str):
                return
            append_timing_line(
                window._ui_timing_log_path,
                scope="gui.action",
                event="run_selected_flow",
                phase="accepted",
                fields={"flow": card_name},
            )
            window._append_log_line(f"Starting one-time flow run: {card_name}", flow_name=card_name)
            if getattr(result, "sync_after", False):
                window._sync_from_daemon()
            return
        result = payload
        if getattr(result, "error_text", None) is not None:
            if action_name == "stop_pipeline":
                card = window.flow_cards.get(window.selected_flow_name or "")
                if card is not None:
                    window.manual_flow_stopping_groups.discard(card.group)
                    window.flow_controller.refresh_action_buttons(window)
            window._show_message_box_later(
                title=APP_DISPLAY_NAME,
                text=result.error_text,
                tone="error",
            )
            return
        if action_name == "start_runtime" and getattr(result, "requested", False):
            automated_flow_names = tuple(
                flow_name
                for flow_name, card in window.flow_cards.items()
                if card.valid and card.mode in {"poll", "schedule"}
            )
            window.runtime_session = window.runtime_session.with_runtime_flags(active=True, stopping=False).with_active_runtime_flow_names(
                automated_flow_names
            )
            starting_updates = {
                flow_name: ("polling" if window.flow_cards[flow_name].mode == "poll" else "scheduled")
                for flow_name in automated_flow_names
                if window.flow_states.get(flow_name) != "failed"
            }
            if starting_updates:
                window.flow_controller.set_flow_states(window, starting_updates)
        if getattr(result, "sync_after", False):
            window._sync_from_daemon()

    def finish_run(self, window: "DataEngineWindow", flow_name: object, results: object, error: object) -> None:
        assert isinstance(flow_name, str)
        card = window.flow_cards.get(flow_name)
        group_name = card.group if card is not None else next(
            (run.group_name for run in window.runtime_session.manual_runs if run.flow_name == flow_name),
            None,
        )
        stop_event = window.manual_flow_stop_events.pop(group_name, None)
        window.manual_flow_stopping_groups.discard(group_name)
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
