"""Flow, runtime, and log coordination helpers for the GUI shell."""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_engine.domain import FlowLogEntry, FlowRunState, RuntimeStepEvent
from data_engine.ui.gui.dialogs import show_run_log_preview
from data_engine.ui.gui.preview_models import RunLogPreviewRequest
from data_engine.ui.gui.presenters import (
    add_log_run_item as present_add_log_run_item,
    apply_daemon_snapshot as present_apply_daemon_snapshot,
    finish_daemon_startup as present_finish_daemon_startup,
    refresh_log_view as present_refresh_log_view,
)

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


class GuiControlMixin:
    """Shell coordination helpers for flows, runtime, daemon, and log refresh."""

    def _load_flows(self: "DataEngineWindow") -> None:
        self.flow_controller.load_flows(self)

    def _populate_flow_tree(self: "DataEngineWindow") -> None:
        self.flow_controller.populate_flow_tree(self)

    def _select_flow(self: "DataEngineWindow", flow_name: str | None) -> None:
        self.flow_controller.select_flow(self, flow_name)

    def _refresh_selection(self: "DataEngineWindow", card) -> None:
        self.flow_controller.refresh_selection(self, card)

    def _refresh_summary(self: "DataEngineWindow") -> None:
        self.flow_controller.refresh_summary(self)

    def _refresh_action_buttons(self: "DataEngineWindow") -> None:
        self.flow_controller.refresh_action_buttons(self)

    def _reload_workspace_options(self: "DataEngineWindow") -> None:
        self.flow_controller.reload_workspace_options(self)

    def _workspace_selection_changed(self: "DataEngineWindow", index: int) -> None:
        self.flow_controller.workspace_selection_changed(self, index)

    def _switch_workspace(self: "DataEngineWindow", workspace_id: str) -> None:
        self.flow_controller.switch_workspace(self, workspace_id)

    def _refresh_lease_status(self: "DataEngineWindow") -> None:
        self.flow_controller.refresh_lease_status(self)

    def _request_control(self: "DataEngineWindow") -> None:
        self.flow_controller.request_control(self)

    def _update_engine_button(self: "DataEngineWindow") -> None:
        self.flow_controller.update_engine_button(self)

    def _set_flow_state(self: "DataEngineWindow", flow_name: str, state: str) -> None:
        self.flow_controller.set_flow_state(self, flow_name, state)

    def _set_flow_states(self: "DataEngineWindow", updates: dict[str, str]) -> None:
        self.flow_controller.set_flow_states(self, updates)

    def _sync_from_daemon(self: "DataEngineWindow") -> None:
        self.runtime_controller.sync_from_daemon(self)

    def _ensure_daemon_started(self: "DataEngineWindow") -> bool:
        return self.runtime_controller.ensure_daemon_started(self)

    def _start_daemon_worker(self: "DataEngineWindow") -> None:
        self.runtime_controller.start_daemon_worker(self)

    def _finish_daemon_startup(self: "DataEngineWindow", success: bool, error_text: str) -> None:
        present_finish_daemon_startup(self, success, error_text)

    def _apply_daemon_snapshot(self: "DataEngineWindow", snapshot) -> None:
        present_apply_daemon_snapshot(self, snapshot)

    def _rebuild_runtime_snapshot(self: "DataEngineWindow") -> None:
        self.runtime_controller.rebuild_runtime_snapshot(self)

    def _refresh_log_view(self: "DataEngineWindow", *, force_scroll_to_bottom: bool = False) -> None:
        present_refresh_log_view(self, force_scroll_to_bottom=force_scroll_to_bottom)

    def _add_log_run_item(self: "DataEngineWindow", run_group: FlowRunState) -> None:
        present_add_log_run_item(self, run_group)

    def _refresh_flows_requested(self: "DataEngineWindow") -> None:
        self.flow_controller.refresh_flows_requested(self)

    def _clear_logs(self: "DataEngineWindow") -> None:
        self.flow_controller.clear_logs(self)

    def _poll_log_queue(self: "DataEngineWindow") -> None:
        from data_engine.ui.gui.surface import poll_log_queue as surface_poll_log_queue

        surface_poll_log_queue(self)

    def _show_run_log_preview(self: "DataEngineWindow", run_group: FlowRunState) -> None:
        self.run_log_preview_dialog = show_run_log_preview(self, RunLogPreviewRequest.from_run(run_group))

    def _show_run_error_details(self: "DataEngineWindow", run_group: FlowRunState, entry: FlowLogEntry) -> None:
        """Show persisted failure detail for one failed run or step entry."""
        event = entry.event
        title, detail_text = self.runtime_history_service.error_text_for_entry(
            self.runtime_binding.runtime_cache_ledger,
            run_group,
            entry,
        )
        if event is not None and event.step_name is not None:
            fallback_text = (
                f'No persisted error detail was available for failed step "{event.step_name}" '
                f'in run "{run_group.key[0]}".'
            )
        else:
            fallback_text = f'No persisted error detail was available for failed run "{run_group.key[0]}".'
        self._show_message_box(
            title=title,
            text=detail_text.strip() if isinstance(detail_text, str) and detail_text.strip() else fallback_text,
            tone="error",
        )

    def _run_selected_flow(self: "DataEngineWindow") -> None:
        if not self._has_authored_workspace():
            self._sync_from_daemon()
            return
        card = self.flow_cards.get(self.selected_flow_name or "")
        result = self.control_application.run_selected_flow(
            paths=self.workspace_paths,
            runtime_session=self.runtime_session,
            selected_flow_name=card.name if card is not None else None,
            selected_flow_valid=bool(card is not None and card.valid),
            selected_flow_group=card.group if card is not None else None,
            selected_flow_group_active=bool(card is not None and self._is_group_active(card.group)),
            blocked_status_text=self.workspace_control_state.blocked_status_text,
            timeout=2.0,
        )
        if result.error_text is not None:
            self._show_message_box_later(
                title="Data Engine",
                text=result.error_text,
                tone="error",
            )
            return
        if not result.requested or card is None:
            return
        self._append_log_line(f"Starting one-time flow run: {card.name}", flow_name=card.name)
        if result.sync_after:
            self._sync_from_daemon()

    def _start_runtime(self: "DataEngineWindow") -> None:
        self.runtime_controller.start_runtime(self)

    def _stop_runtime(self: "DataEngineWindow") -> None:
        self.runtime_controller.stop_runtime(self)

    def _toggle_runtime(self: "DataEngineWindow") -> None:
        self.runtime_controller.toggle_runtime(self)

    def _stop_pipeline(self: "DataEngineWindow") -> None:
        self.runtime_controller.stop_pipeline(self)

    def _safe_emit_run_finished(self: "DataEngineWindow", flow_name: str, results: object, error: object) -> None:
        from data_engine.ui.gui.surface import safe_emit_run_finished as surface_safe_emit_run_finished

        surface_safe_emit_run_finished(self, flow_name, results, error)

    def _safe_emit_runtime_finished(self: "DataEngineWindow", flow_names: tuple[str, ...], results: object, error: object) -> None:
        from data_engine.ui.gui.surface import safe_emit_runtime_finished as surface_safe_emit_runtime_finished

        surface_safe_emit_runtime_finished(self, flow_names, results, error)

    def _finish_run(self: "DataEngineWindow", flow_name: object, results: object, error: object) -> None:
        self.runtime_controller.finish_run(self, flow_name, results, error)

    def _finish_runtime(self: "DataEngineWindow", flow_names: object, results: object, error: object) -> None:
        self.runtime_controller.finish_runtime(self, flow_names, results, error)

    def _is_group_active(self: "DataEngineWindow", group_name: str) -> bool:
        return self.runtime_controller.is_group_active(self, group_name)


__all__ = ["GuiControlMixin"]
