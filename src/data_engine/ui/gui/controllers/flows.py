"""Flow loading, selection, and action-state controllers for the desktop GUI."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from PySide6.QtCore import QTimer

from data_engine.domain import PendingWorkspaceActionOverlay, WorkspaceSessionState
from data_engine.services import (
    CatalogPort,
    CommandPort,
    HistoryPort,
    WorkspaceService,
    runtime_session_from_workspace_snapshot,
)
from data_engine.platform.identity import APP_DISPLAY_NAME
from data_engine.platform.instrumentation import timed_operation
from data_engine.ui.gui.helpers import start_worker_thread
from data_engine.views import (
    GuiActionState,
    QtFlowCard,
    build_operator_action_context,
    build_selected_flow_presentation,
    surface_control_status_text,
)
from data_engine.domain.time import parse_utc_text

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


class _GuiWorkspaceCatalogController:
    """Own GUI workspace binding and catalog refresh orchestration."""

    def __init__(
        self,
        *,
        workspace_service: WorkspaceService,
        catalog_query_service: CatalogPort,
    ) -> None:
        self.workspace_service = workspace_service
        self.catalog_query_service = catalog_query_service

    def load_flows(self, window: "DataEngineWindow", presentation: "_GuiFlowPresentationController") -> None:
        missing_message = (
            "Workspace collection root is not configured."
            if not window.workspace_paths.workspace_configured
            else "No flow modules discovered."
        )
        result = self.catalog_query_service.load_workspace_catalog(
            workspace_root=window.workspace_paths.workspace_root,
            current_state=window.flow_catalog_state,
            missing_message=missing_message,
        )
        if not result.loaded and result.error_text is None:
            window._workspace_counts_footer_cache.pop(window.workspace_paths.workspace_id, None)
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
            window._workspace_counts_footer_cache.pop(window.workspace_paths.workspace_id, None)
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

        window._workspace_counts_footer_cache.pop(window.workspace_paths.workspace_id, None)
        window.flow_catalog_state = result.catalog_state
        self.populate_flow_tree(window)
        presentation.select_flow(window, window.selected_flow_name)
        presentation.refresh_summary(window)
        window._refresh_workspace_visibility_panel()
        window._rebuild_runtime_snapshot()

    def populate_flow_tree(self, window: "DataEngineWindow") -> None:
        presentation = self.catalog_query_service.build_catalog_presentation(
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
        discovered = self.workspace_service.discover(
            app_root=window.workspace_paths.app_root,
            workspace_collection_root=window.workspace_collection_root_override,
        )
        window.workspace_session_state = WorkspaceSessionState.from_paths(
            window.workspace_paths,
            override_root=window.workspace_collection_root_override,
            discovered_workspace_ids=(item.workspace_id for item in discovered),
        )
        current_id = window.workspace_session_state.current_workspace_id
        workspace_ids = window.workspace_session_state.discovered_workspace_ids
        target_pinned = bool(getattr(window, "_settings_workspace_target_pinned", False))
        settings_target_id = (
            str(getattr(window, "settings_workspace_target_id", current_id) or current_id)
            if target_pinned
            else current_id
        )
        if workspace_ids and settings_target_id not in workspace_ids:
            settings_target_id = current_id
            window._settings_workspace_target_pinned = False
        window.settings_workspace_target_id = settings_target_id

        operator_selector = getattr(window, "workspace_selector", None)
        if operator_selector is not None:
            operator_selector.blockSignals(True)
            try:
                operator_selector.clear()
                if not workspace_ids:
                    operator_selector.addItem("(no workspace)", "")
                    operator_selector.setCurrentIndex(0)
                    operator_selector.setEnabled(False)
                else:
                    for workspace_id in workspace_ids:
                        operator_selector.addItem(workspace_id, workspace_id)
                    selected_index = operator_selector.findData(current_id)
                    if selected_index < 0:
                        selected_index = 0
                    operator_selector.setCurrentIndex(selected_index)
                    operator_selector.setEnabled(True)
            finally:
                operator_selector.blockSignals(False)

        settings_selector = getattr(window, "workspace_settings_selector", None)
        if settings_selector is not None:
            settings_selector.blockSignals(True)
            try:
                settings_selector.clear()
                if not workspace_ids:
                    settings_selector.addItem("(no workspace)", "")
                    settings_selector.setCurrentIndex(0)
                    settings_selector.setEnabled(False)
                else:
                    for workspace_id in workspace_ids:
                        settings_selector.addItem(workspace_id, workspace_id)
                    selected_index = settings_selector.findData(settings_target_id)
                    if selected_index < 0:
                        selected_index = 0
                    settings_selector.setCurrentIndex(selected_index)
                    settings_selector.setEnabled(True)
            finally:
                settings_selector.blockSignals(False)

    def workspace_selection_changed(self, window: "DataEngineWindow", index: int) -> None:
        if index < 0:
            return
        workspace_id = ""
        sender = window.sender()
        if sender is not None and hasattr(sender, "itemData"):
            workspace_id = str(sender.itemData(index) or "").strip()
        if not workspace_id and window.workspace_selector.count() > index:
            workspace_id = str(window.workspace_selector.itemData(index) or "").strip()
        if not workspace_id or workspace_id == window.workspace_paths.workspace_id:
            return
        self.switch_workspace(window, workspace_id)

    def settings_workspace_target_changed(self, window: "DataEngineWindow", index: int) -> None:
        if index < 0:
            return
        selector = getattr(window, "workspace_settings_selector", None)
        if selector is None:
            return
        workspace_id = str(selector.itemData(index) or "").strip()
        if not workspace_id:
            return
        window.settings_workspace_target_id = workspace_id
        window._settings_workspace_target_pinned = True
        window._workspace_counts_footer_cache.pop(workspace_id, None)
        window._refresh_workspace_root_controls()

    def switch_workspace(self, window: "DataEngineWindow", workspace_id: str) -> None:
        for selector in self._workspace_selectors(window):
            try:
                selector.hidePopup()
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
        if "refresh_flows" in window._pending_control_actions or window.ui_closing:
            return
        action_context = presentation._action_context(window)
        window._pending_control_actions.add("refresh_flows")
        window._pending_control_action_tokens["refresh_flows"] = window._workspace_binding_token()
        presentation.refresh_action_buttons(window)
        start_worker_thread(
            window,
            target=self._refresh_flows_worker,
            args=(
                window,
                {
                    "paths": window.workspace_paths,
                    "action_context": action_context,
                    "has_authored_workspace": window._has_authored_workspace(),
                    "timeout": 5.0,
                },
            ),
        )

    def _refresh_flows_worker(self, window: "DataEngineWindow", action_kwargs: dict[str, object]) -> None:
        with timed_operation(window._ui_timing_log_path, scope="gui.action", event="refresh_flows"):
            result = window.command_service.refresh_flows(**action_kwargs)
        if window.ui_closing:
            return
        try:
            window.signals.control_action_finished.emit(
                "refresh_flows",
                window._control_action_payload(
                    result,
                    token=window._pending_control_action_tokens.get("refresh_flows"),
                ),
            )
        except RuntimeError:
            pass

    def finish_control_action(self, window: "DataEngineWindow", action_name: str, payload: object, presentation: "_GuiFlowPresentationController") -> None:
        if action_name not in {"refresh_flows", "request_control", "reset_flow"}:
            return
        window._pending_control_actions.discard(action_name)
        presentation.refresh_action_buttons(window)
        if window.ui_closing:
            return
        if action_name == "request_control":
            result = payload
            if getattr(result, "error_text", None) is not None:
                window._append_log_line(result.error_text.replace("\n\n", ": "))
                window._show_message_box_later(
                    title=APP_DISPLAY_NAME,
                    text=result.error_text,
                    tone="error",
                )
                return
            if getattr(result, "status_text", None) is not None:
                window._append_log_line(result.status_text)
            if getattr(result, "ensure_daemon_started", False):
                window._ensure_daemon_started()
            if getattr(result, "sync_after", False):
                window._sync_from_daemon()
            return
        if action_name == "reset_flow":
            assert isinstance(payload, dict)
            error_text = payload.get("error_text")
            flow_name = payload.get("flow_name")
            if isinstance(error_text, str) and error_text.strip():
                window._show_message_box_later(
                    title=APP_DISPLAY_NAME,
                    text=error_text,
                    tone="error",
                )
                return
            if isinstance(flow_name, str):
                window.runtime_binding_service.invalidate_flow_history(
                    window.runtime_binding,
                    flow_name=flow_name,
                )
                window._selected_flow_run_groups_dirty = True
                if window._cached_selected_flow_run_groups_flow_name == flow_name:
                    window._cached_selected_flow_run_groups = ()
                    window._cached_selected_flow_entry_count = 0
                    window._selected_flow_has_logs = False
            window._rebuild_runtime_snapshot()
            return
        result = payload
        if getattr(result, "error_text", None) is not None:
            window._show_message_box_later(
                title=APP_DISPLAY_NAME,
                text=result.error_text,
                tone="error",
            )
            return
        if getattr(result, "reload_catalog", False):
            self.reload_workspace_options(window)
            self.load_flows(window, presentation)
        if getattr(result, "sync_after", False):
            window._sync_from_daemon()
        if getattr(result, "status_text", None) is not None and window.flow_cards:
            window._append_log_line(result.status_text)
        if getattr(result, "warning_text", None) is not None:
            window._append_log_line(f"Flow refresh warning: {result.warning_text}")
            if window.flow_cards:
                window._show_message_box_later(
                    title=APP_DISPLAY_NAME,
                    text=f"Refreshed local flow definitions, but daemon refresh failed.\n\n{result.warning_text}",
                    tone="error",
                )

    @staticmethod
    def _workspace_selectors(window: "DataEngineWindow") -> tuple[object, ...]:
        selectors: list[object] = []
        for attr_name in ("workspace_selector", "workspace_settings_selector"):
            selector = getattr(window, attr_name, None)
            if selector is not None:
                selectors.append(selector)
        return tuple(selectors)


class _GuiFlowPresentationController:
    """Own GUI selection, action-state, and summary presentation orchestration."""

    def __init__(
        self,
        *,
        catalog_query_service: CatalogPort,
        history_query_service: HistoryPort,
        command_service: CommandPort,
    ) -> None:
        self.catalog_query_service = catalog_query_service
        self.history_query_service = history_query_service
        self.command_service = command_service

    def _action_context(self, window: "DataEngineWindow", card=None):
        card = window.flow_cards.get(window.selected_flow_name or "") if card is None else card
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        effective_runtime_session = (
            runtime_session_from_workspace_snapshot(workspace_snapshot)
            if workspace_snapshot is not None
            else window.runtime_session
        )
        overlay = PendingWorkspaceActionOverlay(
            control_actions=frozenset(window._pending_control_actions),
            pending_manual_run_groups=frozenset(window.pending_manual_run_requests),
            stopping_manual_run_groups=frozenset(window.manual_flow_stopping_groups),
        )
        return build_operator_action_context(
            card=card,
            flow_states=window.flow_states,
            runtime_session=effective_runtime_session,
            flow_groups_by_name={flow_name: flow_card.group for flow_name, flow_card in window.flow_cards.items()},
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
            engine_active_flow_names=(
                ()
                if workspace_snapshot is None
                else workspace_snapshot.engine.active_flow_names
            ),
            has_logs=bool(
                card is not None
                and window._selected_flow_has_logs_flow_name == card.name
                and window._selected_flow_has_logs
            ),
            has_automated_flows=any(flow_card.valid and flow_card.mode in {"poll", "schedule"} for flow_card in window.flow_cards.values()),
            workspace_available=window._has_authored_workspace(),
            local_request_pending=bool(self._control_snapshot(window) and self._control_snapshot(window).request_pending),
            overlay=overlay,
        )

    def _action_state(self, window: "DataEngineWindow", card=None) -> GuiActionState:
        return GuiActionState.from_context(self._action_context(window, card))

    def _effective_action_state(self, window: "DataEngineWindow", card=None) -> GuiActionState:
        return self._action_state(window, card)

    @staticmethod
    def _control_snapshot(window: "DataEngineWindow"):
        snapshot = getattr(window, "workspace_snapshot", None)
        return None if snapshot is None else snapshot.control

    def select_flow(self, window: "DataEngineWindow", flow_name: str | None) -> None:
        window.flow_catalog_state = self.catalog_query_service.select_flow(
            catalog_state=window.flow_catalog_state,
            flow_name=flow_name,
        )
        presentation = self.catalog_query_service.build_catalog_presentation(
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
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        presentation = build_selected_flow_presentation(
            card=card,
            tracker=window.operation_tracker,
            flow_states=window.flow_states,
            run_groups=(),
            selected_run_key=None,
            live_runs=(
                workspace_snapshot.active_runs
                if workspace_snapshot is not None and workspace_snapshot.engine.daemon_live
                else None
            ),
            live_truth_authoritative=bool(
                workspace_snapshot is not None and workspace_snapshot.engine.daemon_live
            ),
        )
        if presentation.detail_state is None:
            window.flow_error_label.clear()
            window._set_operation_cards(())
            return

        window.flow_error_label.setText(presentation.detail_state.error)
        window._set_operation_cards(tuple(row.name for row in presentation.detail_state.operation_rows))
        for index, row in enumerate(presentation.detail_state.operation_rows):
            if index >= len(window.operation_row_widgets):
                break
            row_widgets = window.operation_row_widgets[index]
            window._apply_operation_row_state(
                row_widgets.row_card,
                type("_RowState", (), {"status": row.status})(),
            )
            if row.active_count > 1:
                row_widgets.duration_label.setText(f"{row.active_count} active")
            elif row.status == "running":
                if isinstance(row.live_started_at_utc, str):
                    started = parse_utc_text(row.live_started_at_utc)
                    if started is not None:
                        row_widgets.duration_label.setText(
                            window._format_seconds(
                                max((datetime.now(UTC) - started.astimezone(UTC)).total_seconds(), 0.0)
                            )
                        )
                    elif isinstance(row.live_elapsed_seconds, (int, float)):
                        row_widgets.duration_label.setText(
                            window._format_seconds(row.live_elapsed_seconds)
                        )
                    else:
                        row_widgets.duration_label.setText(window._duration_text(card.name, row.name))
                elif isinstance(row.live_elapsed_seconds, (int, float)):
                    row_widgets.duration_label.setText(
                        window._format_seconds(row.live_elapsed_seconds)
                    )
                else:
                    row_widgets.duration_label.setText(window._duration_text(card.name, row.name))
            else:
                row_widgets.duration_label.setText(
                    window._format_seconds(row.elapsed_seconds) if isinstance(row.elapsed_seconds, (int, float)) else ""
                )
        if card is not None:
            window._refresh_operation_buttons(card.name)
        assert card is not None

    def refresh_summary(self, window: "DataEngineWindow") -> None:
        self.refresh_lease_status(window)

    def refresh_action_buttons(self, window: "DataEngineWindow") -> None:
        card = window.flow_cards.get(window.selected_flow_name or "")
        action_state = self._effective_action_state(window, card)
        previous_action_state = getattr(window, "_last_gui_action_state", None)
        flow_run_state_changed = previous_action_state is None or previous_action_state.flow_run_state != action_state.flow_run_state
        engine_state_changed = previous_action_state is None or previous_action_state.engine_state != action_state.engine_state
        flow_run_enabled_changed = previous_action_state is None or previous_action_state.flow_run_enabled != action_state.flow_run_enabled
        engine_enabled_changed = previous_action_state is None or previous_action_state.engine_enabled != action_state.engine_enabled
        flow_config_enabled_changed = previous_action_state is None or previous_action_state.flow_config_enabled != action_state.flow_config_enabled
        refresh_enabled_changed = previous_action_state is None or previous_action_state.refresh_enabled != action_state.refresh_enabled
        clear_flow_log_enabled_changed = (
            previous_action_state is None
            or previous_action_state.clear_flow_log_enabled != action_state.clear_flow_log_enabled
        )
        request_control_enabled_changed = (
            previous_action_state is None
            or previous_action_state.request_control_enabled != action_state.request_control_enabled
        )
        controls_group = getattr(window, "action_bar", None)
        if controls_group is not None:
            controls_group.setUpdatesEnabled(False)
        try:
            if window.flow_run_button.text() != action_state.flow_run_label:
                window.flow_run_button.setText(action_state.flow_run_label)
            if window.flow_run_button.isEnabled() != action_state.flow_run_enabled:
                window.flow_run_button.setEnabled(action_state.flow_run_enabled)
            if window.flow_run_button.property("flowRunState") != action_state.flow_run_state:
                window.flow_run_button.setProperty("flowRunState", action_state.flow_run_state)
                flow_run_state_changed = True
            if window.flow_config_button.isEnabled() != action_state.flow_config_enabled:
                window.flow_config_button.setEnabled(action_state.flow_config_enabled)
            if window.engine_button.isEnabled() != action_state.engine_enabled:
                window.engine_button.setEnabled(action_state.engine_enabled)
            if window.engine_button.text() != action_state.engine_label:
                window.engine_button.setText(action_state.engine_label)
            if window.engine_button.property("engineState") != action_state.engine_state:
                window.engine_button.setProperty("engineState", action_state.engine_state)
                engine_state_changed = True
            if window.refresh_button.isEnabled() != action_state.refresh_enabled:
                window.refresh_button.setEnabled(action_state.refresh_enabled)
            if window.clear_flow_log_button.text() != action_state.clear_flow_log_label:
                window.clear_flow_log_button.setText(action_state.clear_flow_log_label)
            if window.clear_flow_log_button.isEnabled() != action_state.clear_flow_log_enabled:
                window.clear_flow_log_button.setEnabled(action_state.clear_flow_log_enabled)
            if window.request_control_button.text() != action_state.request_control_label:
                window.request_control_button.setText(action_state.request_control_label)
            if window.request_control_button.isVisible() != action_state.request_control_visible:
                window.request_control_button.setVisible(action_state.request_control_visible)
            if window.request_control_button.isEnabled() != action_state.request_control_enabled:
                window.request_control_button.setEnabled(action_state.request_control_enabled)
            if engine_state_changed or engine_enabled_changed:
                style = window.engine_button.style()
                style.unpolish(window.engine_button)
                style.polish(window.engine_button)
                window.engine_button.update()
            if flow_run_state_changed or flow_run_enabled_changed:
                flow_run_style = window.flow_run_button.style()
                flow_run_style.unpolish(window.flow_run_button)
                flow_run_style.polish(window.flow_run_button)
                window.flow_run_button.update()
            if flow_config_enabled_changed:
                flow_config_style = window.flow_config_button.style()
                flow_config_style.unpolish(window.flow_config_button)
                flow_config_style.polish(window.flow_config_button)
                window.flow_config_button.update()
            if refresh_enabled_changed:
                refresh_style = window.refresh_button.style()
                refresh_style.unpolish(window.refresh_button)
                refresh_style.polish(window.refresh_button)
                window.refresh_button.update()
            if clear_flow_log_enabled_changed:
                clear_style = window.clear_flow_log_button.style()
                clear_style.unpolish(window.clear_flow_log_button)
                clear_style.polish(window.clear_flow_log_button)
                window.clear_flow_log_button.update()
            if request_control_enabled_changed:
                request_style = window.request_control_button.style()
                request_style.unpolish(window.request_control_button)
                request_style.polish(window.request_control_button)
                window.request_control_button.update()
        finally:
            if controls_group is not None:
                controls_group.setUpdatesEnabled(True)
                controls_group.update()
        window._last_gui_action_state = action_state

    @staticmethod
    def _selected_manual_running(window: "DataEngineWindow", card) -> bool:
        if card is None:
            return False
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        if workspace_snapshot is not None:
            for run in workspace_snapshot.active_runs.values():
                if run.flow_name != card.name or run.group_name != card.group:
                    continue
                if run.state in {"starting", "running", "stopping"}:
                    return True
            return False
        effective_runtime_session = (
            runtime_session_from_workspace_snapshot(workspace_snapshot)
            if workspace_snapshot is not None
            else window.runtime_session
        )
        return card.name == effective_runtime_session.manual_flow_name_for_group(card.group)

    def refresh_lease_status(self, window: "DataEngineWindow") -> None:
        control = self._control_snapshot(window)
        status_text = surface_control_status_text(
            None if control is None else control.control_status_text,
            empty_flow_message=window.empty_flow_message,
        )
        if not status_text:
            window.lease_status_label.clear()
            window.lease_status_label.setVisible(False)
            return
        window.lease_status_label.setText(status_text)
        window.lease_status_label.setVisible(True)

    def request_control(self, window: "DataEngineWindow") -> None:
        if "request_control" in window._pending_control_actions or window.ui_closing:
            return
        window._pending_control_actions.add("request_control")
        window._pending_control_action_tokens["request_control"] = window._workspace_binding_token()
        self.refresh_action_buttons(window)
        start_worker_thread(window, target=self._request_control_worker, args=(window,))

    def _request_control_worker(self, window: "DataEngineWindow") -> None:
        with timed_operation(window._ui_timing_log_path, scope="gui.action", event="request_control"):
            result = self.command_service.request_control(window.runtime_binding.daemon_manager)
        if window.ui_closing:
            return
        try:
            window.signals.control_action_finished.emit(
                "request_control",
                window._control_action_payload(
                    result,
                    token=window._pending_control_action_tokens.get("request_control"),
                ),
            )
        except RuntimeError:
            pass

    def update_engine_button(self, window: "DataEngineWindow") -> None:
        self.refresh_action_buttons(window)

    def set_flow_state(self, window: "DataEngineWindow", flow_name: str, state: str) -> None:
        self.set_flow_states(window, {flow_name: state})

    def set_flow_states(self, window: "DataEngineWindow", updates: dict[str, str]) -> None:
        if not updates:
            return
        next_states = dict(window.flow_states)
        next_states.update(updates)
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        runtime_session = (
            runtime_session_from_workspace_snapshot(workspace_snapshot)
            if workspace_snapshot is not None
            else window.runtime_session
        )
        refresh_plan = window.runtime_application.plan_flow_state_refresh(
            previous_states=window.flow_states,
            next_states=next_states,
            runtime_session=runtime_session,
        )
        if not refresh_plan.changed_flow_names:
            return
        window.flow_states = refresh_plan.flow_states
        window._refresh_sidebar_state_views(set(refresh_plan.changed_flow_names))
        if window.selected_flow_name is not None and window.selected_flow_name in refresh_plan.changed_flow_names:
            self.refresh_selection(window, window.flow_cards[window.selected_flow_name])
        self.refresh_summary(window)

    def refresh_flows_requested(self, window: "DataEngineWindow") -> None:
        action_context = self._action_context(window)
        result = self.command_service.refresh_flows(
            paths=window.workspace_paths,
            action_context=action_context,
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
        if not window.clear_flow_log_button.isEnabled() or "reset_flow" in window._pending_control_actions or window.ui_closing:
            return
        window._pending_control_actions.add("reset_flow")
        window._pending_control_action_tokens["reset_flow"] = window._workspace_binding_token()
        self.refresh_action_buttons(window)
        start_worker_thread(window, target=self._reset_flow_worker, args=(window, window.selected_flow_name))

    def _reset_flow_worker(self, window: "DataEngineWindow", flow_name: str) -> None:
        error_text: str | None = None
        try:
            with timed_operation(
                window._ui_timing_log_path,
                scope="gui.action",
                event="reset_flow",
                fields={"flow": flow_name},
            ):
                result = self.command_service.reset_flow(
                    paths=window.workspace_paths,
                    runtime_cache_ledger=window.runtime_binding.runtime_cache_ledger,
                    flow_name=flow_name,
                )
            error_text = result.error_text
        except Exception as exc:
            error_text = f"Flow reset failed.\n\n{exc}"
        if window.ui_closing:
            return
        try:
            window.signals.control_action_finished.emit(
                "reset_flow",
                window._control_action_payload(
                    {"flow_name": flow_name, "error_text": error_text},
                    token=window._pending_control_action_tokens.get("reset_flow"),
                ),
            )
        except RuntimeError:
            pass


class GuiFlowController:
    """Compose narrower GUI flow collaborators behind one stable controller seam."""

    def __init__(
        self,
        *,
        workspace_service: WorkspaceService,
        catalog_query_service: CatalogPort,
        history_query_service: HistoryPort,
        command_service: CommandPort,
    ) -> None:
        self.workspace = _GuiWorkspaceCatalogController(
            workspace_service=workspace_service,
            catalog_query_service=catalog_query_service,
        )
        self.presentation = _GuiFlowPresentationController(
            catalog_query_service=catalog_query_service,
            history_query_service=history_query_service,
            command_service=command_service,
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

    def settings_workspace_target_changed(self, window: "DataEngineWindow", index: int) -> None:
        self.workspace.settings_workspace_target_changed(window, index)

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

    def finish_control_action(self, window: "DataEngineWindow", action_name: str, payload: object) -> None:
        self.workspace.finish_control_action(window, action_name, payload, self.presentation)

    def clear_logs(self, window: "DataEngineWindow") -> None:
        self.presentation.clear_logs(window)


__all__ = ["GuiFlowController"]
