"""Runtime and daemon orchestration controllers for the desktop GUI."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from data_engine.application import RuntimeApplication
from data_engine.services import (
    CommandPort,
    DaemonService,
    RuntimeStateService,
    flow_state_texts_from_workspace_snapshot,
    runtime_session_from_workspace_snapshot,
)
from data_engine.services.daemon_state import DaemonUpdateBatch
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
        command_service: CommandPort,
    ) -> None:
        self.runtime_application = runtime_application
        self.daemon_service = daemon_service
        self.runtime_state_service = runtime_state_service
        self.command_service = command_service

    def _apply_runtime_projection(self, window: "DataEngineWindow", *, runtime_session, operation_tracker, flow_states, step_output_index) -> None:
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        daemon_truth_known = workspace_snapshot is not None
        active_manual_groups = self._active_manual_groups_from_snapshot(window) if daemon_truth_known else {run.group_name for run in runtime_session.manual_runs}
        window.manual_flow_stopping_groups.intersection_update(active_manual_groups)
        self._prune_pending_manual_run_requests(window, active_manual_groups=active_manual_groups)
        next_flow_states = dict(flow_states)
        if daemon_truth_known:
            next_flow_states = flow_state_texts_from_workspace_snapshot(
                workspace_snapshot,
                window.flow_cards.values(),
            )
        refresh_plan = self.runtime_application.plan_flow_state_refresh(
            previous_states=window.flow_states,
            next_states=next_flow_states,
            runtime_session=runtime_session,
        )
        next_flow_states = dict(refresh_plan.flow_states)
        for group_name in window.manual_flow_stopping_groups:
            flow_name = None
            if workspace_snapshot is not None:
                for run in workspace_snapshot.active_runs.values():
                    if run.group_name == group_name and run.state in {"starting", "running", "stopping"}:
                        flow_name = run.flow_name
                        break
            if flow_name is None:
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
    def _current_runtime_session(window: "DataEngineWindow") -> RuntimeSessionState:
        snapshot = getattr(window, "workspace_snapshot", None)
        if snapshot is None:
            return window.runtime_session
        return runtime_session_from_workspace_snapshot(snapshot)

    @staticmethod
    def _effective_operation_tracker(window: "DataEngineWindow", rebuilt_tracker):
        """Prefer the live transient tracker once the UI has observed step activity."""
        current_tracker = window.operation_tracker
        for flow_state in getattr(current_tracker, "flow_states", {}).values():
            if flow_state.has_observed_activity:
                return current_tracker
        return rebuilt_tracker

    def _refresh_runtime_projection_from_logs(self, window: "DataEngineWindow"):
        """Rebuild the local runtime projection from persisted runtime history."""
        projection = self.runtime_state_service.rebuild_projection(
            window.runtime_binding,
            runtime_application=self.runtime_application,
            flow_cards=window.flow_cards.values(),
            runtime_session=self._current_runtime_session(window),
            now=window._monotonic(),
        )
        return replace(
            projection,
            operation_tracker=self._effective_operation_tracker(window, projection.operation_tracker),
        )

    def daemon_wait_worker(self, window: "DataEngineWindow") -> None:
        window.daemon_state_service.run_subscription_loop(
            window.runtime_binding.daemon_manager,
            stop_event=window.daemon_subscription.stop_event,
            workspace_available=lambda: not window.ui_closing and window._has_authored_workspace(),
            on_update=lambda batch: (
                window.daemon_subscription.mark_subscription(window._monotonic()),
                None if window.ui_closing else window._schedule_daemon_update_batch(batch),
            )[-1],
            timeout_seconds=window.daemon_subscription.timeout_seconds,
        )

    def _emit_daemon_sync_finished(self, window: "DataEngineWindow", payload: object) -> None:
        if window.ui_closing:
            return
        try:
            window.signals.daemon_sync_finished.emit(payload)
        except RuntimeError:
            return

    @staticmethod
    def _active_manual_groups_from_snapshot(window: "DataEngineWindow") -> set[str | None]:
        snapshot = getattr(window, "workspace_snapshot", None)
        if snapshot is None:
            return {run.group_name for run in window.runtime_session.manual_runs}
        engine_flow_names = set(snapshot.engine.active_flow_names)
        return {
            run.group_name
            for run in snapshot.active_runs.values()
            if run.flow_name not in engine_flow_names and run.state in {"starting", "running", "stopping"}
        }

    @staticmethod
    def _mark_pending_manual_run_request(window: "DataEngineWindow", *, flow_name: str, group_name: str | None) -> None:
        window.pending_manual_run_requests[group_name] = (
            flow_name,
            datetime.now(UTC).isoformat(),
            window._monotonic(),
        )

    def _prune_pending_manual_run_requests(
        self,
        window: "DataEngineWindow",
        *,
        active_manual_groups: set[str | None] | None,
    ) -> None:
        pending = getattr(window, "pending_manual_run_requests", None)
        if not pending:
            return
        snapshot = getattr(window, "workspace_snapshot", None)
        flow_summaries = {} if snapshot is None else snapshot.flows
        now = window._monotonic()
        for group_name, (flow_name, _requested_at_utc, requested_at_monotonic) in tuple(pending.items()):
            if active_manual_groups is not None and group_name in active_manual_groups:
                pending.pop(group_name, None)
                continue
            flow_summary = flow_summaries.get(flow_name)
            if flow_summary is not None and (
                flow_summary.active_run_count > 0
                or flow_summary.queued_run_count > 0
                or flow_summary.state in {"starting", "running", "stopping"}
            ):
                continue
            if now - requested_at_monotonic >= 10.0:
                pending.pop(group_name, None)

    def _apply_daemon_live_snapshot(self, window: "DataEngineWindow", batch: DaemonUpdateBatch) -> tuple[set[str], bool]:
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        if workspace_snapshot is None:
            self.sync_from_daemon(window)
            return set(window.flow_cards), True
        daemon_status = DaemonStatusState.from_snapshot(batch.snapshot)
        projection = self.runtime_state_service.incremental_projection_from_daemon(
            window.workspace_snapshot,
            flow_cards=window.flow_cards.values(),
            previous_flow_states=window.flow_states,
            daemon_status=daemon_status,
            changed_flow_names=batch.changed_flow_names,
            requires_log_reload=bool(batch.completed_run_ids),
        )
        window.daemon_status = daemon_status
        window.runtime_session = projection.runtime_session
        window.workspace_snapshot = projection.workspace_snapshot
        window.flow_states.update(projection.flow_states)
        changed_flow_names = set(projection.changed_flow_names)
        active_manual_groups = self._active_manual_groups_from_snapshot(window)
        window.manual_flow_stopping_groups.intersection_update(active_manual_groups)
        self._prune_pending_manual_run_requests(window, active_manual_groups=active_manual_groups)
        return changed_flow_names, projection.requires_log_reload

    def apply_daemon_update_batch(self, window: "DataEngineWindow", payload: object = None) -> None:
        payload = getattr(window, "_pending_daemon_update_batch", None) if payload is None else payload
        window._pending_daemon_update_batch = None
        token, batch = window._unwrap_daemon_batch_payload(payload)
        if batch is None or window.ui_closing:
            return
        if token is not None and not window._matches_workspace_binding_token(token):
            return
        if batch.requires_full_sync or not window._has_authored_workspace():
            self.sync_from_daemon(window)
            return
        changed_flow_names, requires_log_reload = self._apply_daemon_live_snapshot(window, batch)
        if requires_log_reload:
            window.runtime_binding_service.reload_logs(window.runtime_binding)
            window._selected_flow_run_groups_dirty = True
        if changed_flow_names:
            window._refresh_sidebar_state_views(changed_flow_names)
        selected_flow_name = window.selected_flow_name
        selected_flow_affected = selected_flow_name in changed_flow_names if selected_flow_name is not None else False
        refresh_log_view = False
        refresh_selection = False
        refresh_summary = False
        refresh_buttons = False
        refresh_workspace_counts_footer = False
        for update in batch.updates:
            if update.lane == "control":
                self.sync_from_daemon(window)
                return
            if update.lane == "engine":
                refresh_summary = True
                refresh_buttons = True
            if update.lane == "flow_activity":
                refresh_summary = True
                refresh_buttons = True
                if selected_flow_affected:
                    refresh_selection = True
            if update.lane == "run_lifecycle":
                refresh_buttons = True
                refresh_workspace_counts_footer = True
                if selected_flow_affected:
                    refresh_log_view = True
                    refresh_selection = True
            if update.lane == "log_events":
                for entry in update.log_entries:
                    synthesized_entry = (
                        entry
                        if entry.workspace_id is not None
                        else replace(entry, workspace_id=window.workspace_paths.workspace_id)
                    )
                    window.log_service.append_entry(window.runtime_binding.log_store, synthesized_entry)
                    event = synthesized_entry.event
                    if (
                        event is not None
                        and event.step_name is None
                        and event.status in {"success", "failed", "stopped"}
                    ):
                        refresh_workspace_counts_footer = True
                        for group_name, (flow_name, *_rest) in tuple(window.pending_manual_run_requests.items()):
                            if flow_name == synthesized_entry.flow_name:
                                window.pending_manual_run_requests.pop(group_name, None)
                if any(entry.flow_name == selected_flow_name for entry in update.log_entries):
                    window._selected_flow_run_groups_dirty = True
                    refresh_log_view = True
                if update.log_entries:
                    refresh_buttons = True
            if update.lane == "step_activity" and selected_flow_affected:
                refresh_selection = True
        for update in batch.updates:
            if update.lane == "step_activity":
                for event in update.step_events:
                    window._apply_runtime_event(event)
        if selected_flow_affected and batch.completed_run_ids:
            projection = self._refresh_runtime_projection_from_logs(window)
            window.step_output_index = projection.step_output_index
        if batch.completed_run_ids:
            refresh_workspace_counts_footer = True
        if refresh_selection and selected_flow_name is not None and selected_flow_name in window.flow_cards:
            window.flow_controller.refresh_selection(window, window.flow_cards[selected_flow_name])
        if refresh_workspace_counts_footer:
            window._workspace_counts_footer_cache.pop(window.workspace_paths.workspace_id, None)
        if refresh_summary:
            window.flow_controller.refresh_summary(window)
            window._refresh_workspace_visibility_panel()
        elif refresh_workspace_counts_footer:
            window._refresh_workspace_visibility_panel()
        if refresh_log_view:
            window._refresh_log_view()
        if refresh_buttons:
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
        if not window._has_authored_workspace():
            window.workspace_snapshot = None
            window.daemon_status = DaemonStatusState.empty()
            window.runtime_session = RuntimeSessionState.empty()
            window.flow_controller.reload_workspace_options(window)
            window.flow_controller.load_flows(window)
            return
        window._daemon_sync_in_progress = True
        token = window._workspace_binding_token()
        start_worker_thread(window, target=self._sync_from_daemon_worker, args=(window, token))

    def _sync_from_daemon_worker(self, window: "DataEngineWindow", token: tuple[int, str]) -> None:
        payload: dict[str, object]
        try:
            with timed_operation(
                window._ui_timing_log_path,
                scope="gui.sync",
                event="sync_from_daemon",
                fields={"workspace": token[1]},
            ):
                flow_cards = tuple(window.flow_cards.values())
                sync_state = window.runtime_binding_service.sync_runtime_state(
                    window.runtime_binding,
                    runtime_application=self.runtime_application,
                    flow_cards=flow_cards,
                    daemon_startup_in_progress=window._daemon_startup_in_progress,
                )
                projection = self.runtime_state_service.rebuild_projection(
                    window.runtime_binding,
                    runtime_application=self.runtime_application,
                    flow_cards=flow_cards,
                    runtime_session=sync_state.runtime_session,
                    now=window._monotonic(),
                )
                projection = replace(
                    projection,
                    operation_tracker=self._effective_operation_tracker(window, projection.operation_tracker),
                )
                workspace_snapshot = self.runtime_state_service.snapshot_from_projection(
                    binding=window.runtime_binding,
                    flow_cards=flow_cards,
                    projection=projection,
                    workspace_control_state=sync_state.workspace_control_state,
                    daemon_live=bool(getattr(sync_state.snapshot, "live", False)),
                    daemon_startup_in_progress=window._daemon_startup_in_progress,
                    daemon_projection_version=int(getattr(sync_state.snapshot, "projection_version", 0) or 0),
                    daemon_transport_mode=str(getattr(sync_state.snapshot, "transport_mode", "heartbeat") or "heartbeat"),
                    daemon_engine_starting=bool(getattr(sync_state.snapshot, "engine_starting", False)),
                    daemon_active_flow_names=tuple(getattr(sync_state.snapshot, "active_engine_flow_names", ()) or ()),
                    daemon_active_runs=tuple(getattr(sync_state.snapshot, "active_runs", ()) or ()),
                    daemon_flow_activity=tuple(getattr(sync_state.snapshot, "flow_activity", ()) or ()),
                )
            payload = {
                "workspace_token": token,
                "sync_state": sync_state,
                "projection": projection,
                "workspace_snapshot": workspace_snapshot,
            }
        except Exception as exc:
            payload = {"workspace_token": token, "error": exc}
        self._emit_daemon_sync_finished(window, payload)

    def finish_daemon_sync(self, window: "DataEngineWindow", payload: object) -> None:
        rerun_requested = False
        try:
            if not isinstance(payload, dict):
                return
            token = payload.get("workspace_token")
            if not isinstance(token, tuple) or len(token) != 2:
                return
            if not window._matches_workspace_binding_token(token):
                return
            error = payload.get("error")
            if error is not None:
                error_text = str(error)
                if error_text != getattr(window, "_last_daemon_sync_error_text", None):
                    window._last_daemon_sync_error_text = error_text
                    if window.isVisible():
                        window._append_log_line(f"Daemon sync failed: {error_text}")
                return
            sync_state = payload.get("sync_state")
            projection = payload.get("projection")
            workspace_snapshot = payload.get("workspace_snapshot")
            if sync_state is None or projection is None or workspace_snapshot is None:
                return
            window._last_daemon_sync_error_text = None
            previous_workspace_snapshot = getattr(window, "workspace_snapshot", None)
            previous_runtime_session = getattr(window, "runtime_session", None)
            previous_operation_tracker = getattr(window, "operation_tracker", None)
            previous_flow_states = getattr(window, "flow_states", None)
            previous_step_output_index = getattr(window, "step_output_index", None)
            if (
                previous_workspace_snapshot == workspace_snapshot
                and previous_runtime_session == projection.runtime_session
                and previous_operation_tracker == projection.operation_tracker
                and previous_flow_states == projection.flow_states
                and previous_step_output_index == projection.step_output_index
            ):
                window.daemon_subscription.mark_sync(window._monotonic())
                return
            window._selected_flow_run_groups_dirty = True
            window.workspace_snapshot = workspace_snapshot
            if not window.workspace_snapshot.engine.daemon_live and window._auto_daemon_enabled:
                self.ensure_daemon_started(window)
            window.daemon_status = sync_state.daemon_status
            window.daemon_subscription.mark_sync(window._monotonic())
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
                runtime_session=self._current_runtime_session(window),
                now=window._monotonic(),
            )
            projection = replace(
                projection,
                operation_tracker=self._effective_operation_tracker(window, projection.operation_tracker),
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
        window._pending_control_action_tokens[action_name] = window._workspace_binding_token()
        start_worker_thread(window, target=target, args=args)
        window.flow_controller.refresh_action_buttons(window)
        return True

    @staticmethod
    def _emit_control_action_finished(window: "DataEngineWindow", action_name: str, payload: object) -> None:
        if window.ui_closing:
            return
        try:
            token = window._pending_control_action_tokens.get(action_name, window._workspace_binding_token())
            window.signals.control_action_finished.emit(
                action_name,
                window._control_action_payload(payload, token=token),
            )
        except RuntimeError:
            pass

    def run_selected_flow(self, window: "DataEngineWindow") -> None:
        if not window._has_authored_workspace():
            window._sync_from_daemon()
            return
        card = window.flow_cards.get(window.selected_flow_name or "")
        action_context = window.flow_controller.presentation._action_context(window, card)
        action_state = window.flow_controller.presentation._effective_action_state(window, card)
        if action_state.flow_run_state == "stop":
            if not action_state.flow_run_enabled:
                window.flow_controller.refresh_action_buttons(window)
                return
            self.stop_pipeline(window)
            return
        if not action_state.flow_run_enabled:
            window.flow_controller.refresh_action_buttons(window)
            return
        action_args = (
            window,
            {
                "paths": window.workspace_paths,
                "action_context": action_context,
                "selected_flow_name": card.name if card is not None else None,
                "selected_flow_valid": bool(card is not None and card.valid),
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
            result = self.command_service.run_selected_flow(**action_kwargs)
        self._emit_control_action_finished(
            window,
            "run_selected_flow",
            {"result": result, "card_name": card_name},
        )

    def start_runtime(self, window: "DataEngineWindow") -> None:
        if not window._has_authored_workspace():
            window._sync_from_daemon()
            return
        action_context = window.flow_controller.presentation._action_context(window)
        action_state = window.flow_controller.presentation._effective_action_state(window)
        if not action_state.engine_enabled or action_state.engine_label != "Start Engine":
            window.flow_controller.refresh_action_buttons(window)
            return
        action_kwargs = {
            "paths": window.workspace_paths,
            "action_context": action_context,
            "has_automated_flows": any(card.valid and card.mode in {"poll", "schedule"} for card in window.flow_cards.values()),
            "blocked_status_text": self._blocked_status_text(window),
            "timeout": 5.0,
        }
        self._begin_control_action(window, "start_runtime", target=self._start_runtime_worker, args=(window, action_kwargs))

    def _start_runtime_worker(self, window: "DataEngineWindow", action_kwargs: dict[str, object]) -> None:
        with timed_operation(window._ui_timing_log_path, scope="gui.action", event="start_engine"):
            result = self.command_service.start_engine(**action_kwargs)
        self._emit_control_action_finished(window, "start_runtime", result)

    def stop_runtime(self, window: "DataEngineWindow") -> None:
        action_context = window.flow_controller.presentation._action_context(window)
        action_state = window.flow_controller.presentation._effective_action_state(window)
        if not action_state.engine_enabled or action_state.engine_label != "Stop Engine":
            window.flow_controller.refresh_action_buttons(window)
            return
        action_kwargs = {
            "paths": window.workspace_paths,
            "action_context": action_context,
            "selected_flow_name": None,
            "blocked_status_text": self._blocked_status_text(window),
            "timeout": 5.0,
        }
        self._begin_control_action(window, "stop_runtime", target=self._stop_runtime_worker, args=(window, action_kwargs))

    def _stop_runtime_worker(self, window: "DataEngineWindow", action_kwargs: dict[str, object]) -> None:
        with timed_operation(window._ui_timing_log_path, scope="gui.action", event="stop_engine"):
            result = self.command_service.stop_pipeline(**action_kwargs)
        self._emit_control_action_finished(window, "stop_runtime", result)

    def toggle_runtime(self, window: "DataEngineWindow") -> None:
        if self._current_runtime_session(window).runtime_active:
            self.stop_runtime(window)
            return
        self.start_runtime(window)

    def stop_pipeline(self, window: "DataEngineWindow") -> None:
        card = window.flow_cards.get(window.selected_flow_name or "")
        action_context = window.flow_controller.presentation._action_context(window, card)
        action_state = window.flow_controller.presentation._effective_action_state(window, card)
        if action_state.flow_run_state == "stop" and not action_state.flow_run_enabled:
            window.flow_controller.refresh_action_buttons(window)
            return
        if action_state.flow_run_state != "stop" and (not action_state.engine_enabled or action_state.engine_label != "Stop Engine"):
            window.flow_controller.refresh_action_buttons(window)
            return
        action_kwargs = {
            "paths": window.workspace_paths,
            "action_context": action_context,
            "selected_flow_name": card.name if card is not None else None,
            "blocked_status_text": self._blocked_status_text(window),
            "timeout": 5.0,
        }
        if self._begin_control_action(window, "stop_pipeline", target=self._stop_pipeline_worker, args=(window, action_kwargs)):
            if action_context.selected_manual_running and card is not None:
                window.manual_flow_stopping_groups.add(card.group)
                if card.name in window.flow_states and window.flow_states.get(card.name) != "failed":
                    window.flow_controller.set_flow_state(window, card.name, "stopping flow")
                window.flow_controller.refresh_action_buttons(window)

    def _stop_pipeline_worker(self, window: "DataEngineWindow", action_kwargs: dict[str, object]) -> None:
        with timed_operation(window._ui_timing_log_path, scope="gui.action", event="stop_pipeline"):
            result = self.command_service.stop_pipeline(**action_kwargs)
        self._emit_control_action_finished(window, "stop_pipeline", result)

    def finish_control_action(self, window: "DataEngineWindow", action_name: str, payload: object) -> None:
        window._pending_control_actions.discard(action_name)
        window._pending_control_action_tokens.pop(action_name, None)
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
            card = window.flow_cards.get(card_name)
            if card is not None:
                self._mark_pending_manual_run_request(
                    window,
                    flow_name=card_name,
                    group_name=card.group,
                )
                window.flow_controller.refresh_action_buttons(window)
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
        if getattr(result, "sync_after", False):
            window._sync_from_daemon()

    def finish_run(self, window: "DataEngineWindow", flow_name: object, results: object, error: object) -> None:
        assert isinstance(flow_name, str)
        runtime_session = self._current_runtime_session(window)
        card = window.flow_cards.get(flow_name)
        group_name = card.group if card is not None else next(
            (run.group_name for run in runtime_session.manual_runs if run.flow_name == flow_name),
            None,
        )
        window.pending_manual_run_requests.pop(group_name, None)
        stop_event = window.manual_flow_stop_events.pop(group_name, None)
        window.manual_flow_stopping_groups.discard(group_name)
        stop_requested = stop_event.is_set() if stop_event is not None else False
        if stop_event is not None:
            stop_event.clear()
        completion = self.runtime_application.complete_manual_run(
            runtime_session=runtime_session,
            flow_name=flow_name,
            group_name=group_name,
            flow_mode=card.mode if card is not None else "manual",
            results=results,
            error=error,
            stop_requested=stop_requested,
        )
        window.runtime_session = completion.runtime_session
        window.runtime_binding_service.reload_logs(window.runtime_binding)
        window._selected_flow_run_groups_dirty = True
        window._workspace_counts_footer_cache.pop(window.workspace_paths.workspace_id, None)
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
        if window.selected_flow_name == flow_name:
            window._refresh_log_view()
        window.flow_controller.refresh_action_buttons(window)

    def finish_runtime(self, window: "DataEngineWindow", flow_names: object, results: object, error: object) -> None:
        runtime_session = self._current_runtime_session(window)
        active_runtime_flow_names = (
            tuple(flow_names)
            if isinstance(flow_names, tuple)
            else runtime_session.active_runtime_flow_names
        )
        runtime_stop_requested = window.engine_runtime_stop_event.is_set()
        flow_stop_requested = window.engine_flow_stop_event.is_set()
        completion = self.runtime_application.complete_engine_run(
            runtime_session=runtime_session,
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
        window.runtime_binding_service.reload_logs(window.runtime_binding)
        window._selected_flow_run_groups_dirty = True
        window.engine_runtime_stop_event.clear()
        window.engine_flow_stop_event.clear()
        window._workspace_counts_footer_cache.pop(window.workspace_paths.workspace_id, None)
        window.flow_controller.set_flow_states(window, completion.state_updates)
        for message in completion.log_messages:
            window._append_log_line(message.text, flow_name=message.flow_name)
        for failed_flow_name in completion.failed_flow_names:
            window.flow_controller.set_flow_state(window, failed_flow_name, "failed")
        if any(flow_name == window.selected_flow_name for flow_name in active_runtime_flow_names):
            window._refresh_log_view()
        window.flow_controller.refresh_action_buttons(window)

    def is_group_active(self, window: "DataEngineWindow", group_name: str) -> bool:
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        if workspace_snapshot is not None:
            engine_flow_names = set(workspace_snapshot.engine.active_flow_names)
            return any(
                run.group_name == group_name
                and run.state in {"starting", "running", "stopping"}
                and run.flow_name not in engine_flow_names
                for run in workspace_snapshot.active_runs.values()
            ) or any(
                summary.group_name == (group_name or "")
                and summary.state in {"polling", "scheduled", "starting", "stopping"}
                for summary in workspace_snapshot.flows.values()
            )
        return window.runtime_session.is_group_active(
            group_name,
            {flow_name: card.group for flow_name, card in window.flow_cards.items()},
        )


__all__ = ["GuiRuntimeController"]
