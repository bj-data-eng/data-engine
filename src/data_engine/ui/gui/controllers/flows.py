"""Flow loading, selection, and action-state controllers for the desktop GUI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from PySide6.QtCore import QTimer

from data_engine.application import FlowCatalogApplication, WorkspaceSessionApplication
from data_engine.services import LogService
from data_engine.platform.identity import APP_DISPLAY_NAME
from data_engine.views import GuiActionState, QtFlowCard, surface_control_status_text

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


class _GuiWorkspaceCatalogController:
    """Own GUI workspace binding and catalog refresh orchestration."""

    def __init__(
        self,
        *,
        workspace_session_application: WorkspaceSessionApplication,
        flow_catalog_application: FlowCatalogApplication,
    ) -> None:
        self.workspace_session_application = workspace_session_application
        self.flow_catalog_application = flow_catalog_application

    def load_flows(self, window: "DataEngineWindow", presentation: "_GuiFlowPresentationController") -> None:
        missing_message = (
            "Workspace collection root is not configured."
            if not window.workspace_paths.workspace_configured
            else "No flow modules discovered."
        )
        result = self.flow_catalog_application.load_workspace_catalog(
            workspace_paths=window.workspace_paths,
            current_state=window.flow_catalog_state,
            missing_message=missing_message,
        )
        if not result.loaded and result.error_text is None:
            window.flow_catalog_state = result.catalog_state.with_empty_message(
                window._empty_flow_message_for_error(missing_message)
            ).with_selected_flow_name(None)
            self.populate_flow_tree(window)
            presentation.refresh_selection(window, None)
            window._refresh_log_view(force_scroll_to_bottom=True)
            presentation.refresh_action_buttons(window)
            presentation.refresh_summary(window)
            window._refresh_workspace_visibility_panel()
            return
        if result.error_text is not None:
            message = result.error_text
            window.flow_catalog_state = result.catalog_state.with_empty_message(
                window._empty_flow_message_for_error(message)
            ).with_selected_flow_name(None)
            self.populate_flow_tree(window)
            window._append_log_line(f"Failed to load flows: {message}")
            presentation.refresh_selection(window, None)
            window._refresh_log_view(force_scroll_to_bottom=True)
            presentation.refresh_action_buttons(window)
            presentation.refresh_summary(window)
            window._refresh_workspace_visibility_panel()
            if not window._is_bootstrap_ready_error(message):
                window._show_message_box(
                    title=APP_DISPLAY_NAME,
                    text=f"Failed to load flows.\n\n{message}",
                    tone="error",
                )
            return

        window.flow_catalog_state = result.catalog_state
        self.populate_flow_tree(window)
        presentation.select_flow(window, window.selected_flow_name)
        presentation.refresh_summary(window)
        window._refresh_workspace_visibility_panel()
        window._rebuild_runtime_snapshot()

    def populate_flow_tree(self, window: "DataEngineWindow") -> None:
        presentation = self.flow_catalog_application.build_presentation(
            catalog_state=window.flow_catalog_state,
        )
        window.sidebar_flow_widgets = {}
        window.sidebar_group_widgets = {}
        window.sidebar_content.setUpdatesEnabled(False)
        while window.sidebar_layout.count() > 1:
            item = window.sidebar_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        for group_name, entries in presentation.grouped_cards:
            group_widget = window._build_group_row_widget(group_name, list(entries))
            window.sidebar_group_widgets[group_name] = group_widget
            window.sidebar_layout.insertWidget(window.sidebar_layout.count() - 1, group_widget)
            for index, card in enumerate(entries, start=1):
                widget = window._build_flow_row_widget(card)
                widget.setProperty("flowIndex", index)
                window.sidebar_flow_widgets[card.name] = widget
                window.sidebar_layout.insertWidget(window.sidebar_layout.count() - 1, widget)
        window.sidebar_content.setUpdatesEnabled(True)
        window.sidebar_content.updateGeometry()
        window.sidebar_content.update()
        window._refresh_sidebar_selection()
        window._update_sidebar_scroll_cues()

    def reload_workspace_options(self, window: "DataEngineWindow") -> None:
        window.workspace_session_state = self.workspace_session_application.refresh_session(
            workspace_paths=window.workspace_paths,
            override_root=window.workspace_collection_root_override,
        )
        current_id = window.workspace_session_state.current_workspace_id
        workspace_ids = window.workspace_session_state.discovered_workspace_ids
        window.workspace_selector.blockSignals(True)
        try:
            window.workspace_selector.clear()
            if not workspace_ids:
                window.workspace_selector.addItem("(no workspace)", "")
                window.workspace_selector.setCurrentIndex(0)
                window.workspace_selector.setEnabled(False)
            else:
                for workspace_id in workspace_ids:
                    window.workspace_selector.addItem(workspace_id, workspace_id)
                selected_index = window.workspace_selector.findData(current_id)
                if selected_index < 0:
                    selected_index = 0
                window.workspace_selector.setCurrentIndex(selected_index)
                window.workspace_selector.setEnabled(True)
        finally:
            window.workspace_selector.blockSignals(False)

    def workspace_selection_changed(self, window: "DataEngineWindow", index: int) -> None:
        if index < 0:
            return
        workspace_id = str(window.workspace_selector.itemData(index) or "").strip()
        if not workspace_id or workspace_id == window.workspace_paths.workspace_id:
            return
        self.switch_workspace(window, workspace_id)

    def switch_workspace(self, window: "DataEngineWindow", workspace_id: str) -> None:
        try:
            window.workspace_selector.hidePopup()
        except Exception:
            pass
        if window.ui_closing or workspace_id == window.workspace_paths.workspace_id:
            return
        window._pending_workspace_switch_id = workspace_id
        if window._workspace_switch_scheduled:
            return
        window._workspace_switch_scheduled = True

        def _flush_pending_switch() -> None:
            window._workspace_switch_scheduled = False
            if window.ui_closing:
                return
            window._flush_deferred_ui_updates()

        QTimer.singleShot(0, _flush_pending_switch)

    def refresh_flows_requested(self, window: "DataEngineWindow", presentation: "_GuiFlowPresentationController") -> None:
        result = window.control_application.refresh_flows(
            paths=window.workspace_paths,
            runtime_session=window.runtime_session,
            has_authored_workspace=window._has_authored_workspace(),
            timeout=5.0,
        )
        if result.error_text is not None:
            window._show_message_box_later(
                title=APP_DISPLAY_NAME,
                text=result.error_text,
                tone="error",
            )
            return
        if result.reload_catalog:
            self.reload_workspace_options(window)
            self.load_flows(window, presentation)
        if result.sync_after:
            window._sync_from_daemon()
        if result.status_text is not None and window.flow_cards:
            window._append_log_line(result.status_text)
        if result.warning_text is not None:
            window._append_log_line(f"Flow refresh warning: {result.warning_text}")
            if window.flow_cards:
                window._show_message_box_later(
                    title=APP_DISPLAY_NAME,
                    text=f"Refreshed local flow definitions, but daemon refresh failed.\n\n{result.warning_text}",
                    tone="error",
                )


class _GuiFlowPresentationController:
    """Own GUI selection, action-state, and summary presentation orchestration."""

    def __init__(
        self,
        *,
        flow_catalog_application: FlowCatalogApplication,
        log_service: LogService,
    ) -> None:
        self.flow_catalog_application = flow_catalog_application
        self.log_service = log_service

    def select_flow(self, window: "DataEngineWindow", flow_name: str | None) -> None:
        window.flow_catalog_state = self.flow_catalog_application.select_flow(
            catalog_state=window.flow_catalog_state,
            flow_name=flow_name,
        )
        presentation = self.flow_catalog_application.build_presentation(
            catalog_state=window.flow_catalog_state,
        )
        if presentation.selected_card is None:
            self.refresh_selection(window, None)
            self.refresh_action_buttons(window)
            window._refresh_sidebar_selection()
            window._refresh_log_view(force_scroll_to_bottom=True)
            return
        self.refresh_selection(window, presentation.selected_card)
        self.refresh_action_buttons(window)
        window._refresh_sidebar_selection()
        window._refresh_log_view(force_scroll_to_bottom=True)

    def refresh_selection(self, window: "DataEngineWindow", card: QtFlowCard | None) -> None:
        presentation = window.detail_application.build_selected_flow_presentation(
            card=card,
            tracker=window.operation_tracker,
            flow_states=window.flow_states,
            run_groups=(),
            selected_run_key=None,
        )
        if presentation.detail_state is None:
            window.flow_error_label.clear()
            window._set_operation_cards(())
            return

        window.flow_error_label.setText(presentation.detail_state.error)
        window._set_operation_cards(tuple(row.name for row in presentation.detail_state.operation_rows))
        assert card is not None
        window._render_operation_durations(card.name)

    def refresh_summary(self, window: "DataEngineWindow") -> None:
        self.refresh_lease_status(window)

    def refresh_action_buttons(self, window: "DataEngineWindow") -> None:
        card = window.flow_cards.get(window.selected_flow_name or "")
        action_context = window.action_state_application.build_action_context(
            card=card,
            flow_states=window.flow_states,
            runtime_session=window.runtime_session,
            flow_groups_by_name={flow_name: flow_card.group for flow_name, flow_card in window.flow_cards.items()},
            active_flow_states=window._ACTIVE_FLOW_STATES,
            has_logs=bool(
                card is not None and self.log_service.entries_for_flow(window.runtime_binding.log_store, card.name)
            ),
            has_automated_flows=any(flow_card.valid and flow_card.mode in {"poll", "schedule"} for flow_card in window.flow_cards.values()),
            workspace_available=window._has_authored_workspace(),
        )
        action_state = GuiActionState.from_context(action_context)
        window.flow_run_button.setText(action_state.flow_run_label)
        window.flow_run_button.setEnabled(action_state.flow_run_enabled)
        window.flow_config_button.setEnabled(action_state.flow_config_enabled)
        window.engine_button.setEnabled(action_state.engine_enabled)
        window.engine_button.setText(action_state.engine_label)
        window.engine_button.setProperty("engineState", action_state.engine_state)
        window.refresh_button.setEnabled(action_state.refresh_enabled)
        window.clear_flow_log_button.setEnabled(action_state.clear_flow_log_enabled)
        window.request_control_button.setVisible(action_state.request_control_visible)
        window.request_control_button.setEnabled(action_state.request_control_enabled)
        if window.workspace_selector.count() > 0:
            window.workspace_selector.setEnabled(bool(window.workspace_session_state.discovered_workspace_ids))
        style = window.engine_button.style()
        style.unpolish(window.engine_button)
        style.polish(window.engine_button)
        window.engine_button.update()

    def refresh_lease_status(self, window: "DataEngineWindow") -> None:
        snapshot = window.daemon_state_service.sync(window.runtime_binding.daemon_manager)
        window.workspace_control_state = window.daemon_state_service.control_state(
            window.runtime_binding.daemon_manager,
            snapshot,
            daemon_startup_in_progress=window._daemon_startup_in_progress,
        )
        status_text = surface_control_status_text(
            window.workspace_control_state.control_status_text,
            empty_flow_message=window.empty_flow_message,
        )
        if not status_text:
            window.lease_status_label.clear()
            window.lease_status_label.setVisible(False)
            return
        window.lease_status_label.setText(status_text)
        window.lease_status_label.setVisible(True)

    def request_control(self, window: "DataEngineWindow") -> None:
        result = window.control_application.request_control(window.runtime_binding.daemon_manager)
        if result.error_text is not None:
            window._append_log_line(result.error_text.replace("\n\n", ": "))
            window._show_message_box(
                title=APP_DISPLAY_NAME,
                text=result.error_text,
                tone="error",
            )
            return
        if result.status_text is not None:
            window._append_log_line(result.status_text)
        if result.ensure_daemon_started:
            window._ensure_daemon_started()
        if result.sync_after:
            window._sync_from_daemon()

    def update_engine_button(self, window: "DataEngineWindow") -> None:
        self.refresh_action_buttons(window)

    def set_flow_state(self, window: "DataEngineWindow", flow_name: str, state: str) -> None:
        self.set_flow_states(window, {flow_name: state})

    def set_flow_states(self, window: "DataEngineWindow", updates: dict[str, str]) -> None:
        if not updates:
            return
        next_states = dict(window.flow_states)
        next_states.update(updates)
        refresh_plan = window.runtime_application.plan_flow_state_refresh(
            previous_states=window.flow_states,
            next_states=next_states,
            runtime_session=window.runtime_session,
        )
        if not refresh_plan.changed_flow_names:
            return
        window.flow_states = refresh_plan.flow_states
        window._refresh_sidebar_state_views(set(refresh_plan.changed_flow_names))
        if window.selected_flow_name is not None and window.selected_flow_name in refresh_plan.changed_flow_names:
            self.refresh_selection(window, window.flow_cards[window.selected_flow_name])
        self.refresh_summary(window)

    def refresh_flows_requested(self, window: "DataEngineWindow") -> None:
        result = window.control_application.refresh_flows(
            paths=window.workspace_paths,
            runtime_session=window.runtime_session,
            has_authored_workspace=window._has_authored_workspace(),
            timeout=5.0,
        )
        if result.error_text is not None:
            window._show_message_box_later(
                title=APP_DISPLAY_NAME,
                text=result.error_text,
                tone="error",
            )
            return
        if result.reload_catalog:
            self.reload_workspace_options(window)
            self.load_flows(window)
        if result.sync_after:
            window._sync_from_daemon()
        if result.status_text is not None and window.flow_cards:
            window._append_log_line(result.status_text)
        if result.warning_text is not None:
            window._append_log_line(f"Flow refresh warning: {result.warning_text}")
            if window.flow_cards:
                window._show_message_box_later(
                    title=APP_DISPLAY_NAME,
                    text=f"Refreshed local flow definitions, but daemon refresh failed.\n\n{result.warning_text}",
                    tone="error",
                )

    def clear_logs(self, window: "DataEngineWindow") -> None:
        if window.selected_flow_name is None:
            return
        self.log_service.clear_flow(window.runtime_binding.log_store, window.selected_flow_name)
        window._refresh_log_view(force_scroll_to_bottom=True)
        self.refresh_action_buttons(window)


class GuiFlowController:
    """Compose narrower GUI flow collaborators behind one stable controller seam."""

    def __init__(
        self,
        *,
        workspace_session_application: WorkspaceSessionApplication,
        flow_catalog_application: FlowCatalogApplication,
        log_service: LogService,
    ) -> None:
        self.workspace = _GuiWorkspaceCatalogController(
            workspace_session_application=workspace_session_application,
            flow_catalog_application=flow_catalog_application,
        )
        self.presentation = _GuiFlowPresentationController(
            flow_catalog_application=flow_catalog_application,
            log_service=log_service,
        )

    def load_flows(self, window: "DataEngineWindow") -> None:
        self.workspace.load_flows(window, self.presentation)

    def populate_flow_tree(self, window: "DataEngineWindow") -> None:
        self.workspace.populate_flow_tree(window)

    def select_flow(self, window: "DataEngineWindow", flow_name: str | None) -> None:
        self.presentation.select_flow(window, flow_name)

    def refresh_selection(self, window: "DataEngineWindow", card: QtFlowCard | None) -> None:
        self.presentation.refresh_selection(window, card)

    def refresh_summary(self, window: "DataEngineWindow") -> None:
        self.presentation.refresh_summary(window)

    def refresh_action_buttons(self, window: "DataEngineWindow") -> None:
        self.presentation.refresh_action_buttons(window)

    def reload_workspace_options(self, window: "DataEngineWindow") -> None:
        self.workspace.reload_workspace_options(window)

    def workspace_selection_changed(self, window: "DataEngineWindow", index: int) -> None:
        self.workspace.workspace_selection_changed(window, index)

    def switch_workspace(self, window: "DataEngineWindow", workspace_id: str) -> None:
        self.workspace.switch_workspace(window, workspace_id)

    def refresh_lease_status(self, window: "DataEngineWindow") -> None:
        self.presentation.refresh_lease_status(window)

    def request_control(self, window: "DataEngineWindow") -> None:
        self.presentation.request_control(window)

    def update_engine_button(self, window: "DataEngineWindow") -> None:
        self.presentation.update_engine_button(window)

    def set_flow_state(self, window: "DataEngineWindow", flow_name: str, state: str) -> None:
        self.presentation.set_flow_state(window, flow_name, state)

    def set_flow_states(self, window: "DataEngineWindow", updates: dict[str, str]) -> None:
        self.presentation.set_flow_states(window, updates)

    def refresh_flows_requested(self, window: "DataEngineWindow") -> None:
        self.workspace.refresh_flows_requested(window, self.presentation)

    def clear_logs(self, window: "DataEngineWindow") -> None:
        self.presentation.clear_logs(window)


__all__ = ["GuiFlowController"]
