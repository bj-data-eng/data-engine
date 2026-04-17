"""Flow/workspace/detail controllers for the terminal UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual.widgets import ListView, Select, Static

from data_engine.application import FlowCatalogApplication, OperatorControlApplication, WorkspaceSessionApplication
from data_engine.services import LogService
from data_engine.services.reset import ResetService
from data_engine.domain import FlowRunState
from data_engine.views.text import render_selected_flow_lines
from data_engine.ui.tui.widgets import FlowListItem, GroupHeaderListItem, InfoModal, RunGroupListItem

if TYPE_CHECKING:
    from data_engine.ui.tui.app import DataEngineTui


class TuiFlowController:
    """Compose narrower TUI flow collaborators behind one stable controller seam."""

    def __init__(
        self,
        *,
        workspace_session_application: WorkspaceSessionApplication,
        flow_catalog_application: FlowCatalogApplication,
        control_application: OperatorControlApplication,
        log_service: LogService,
        reset_service: ResetService,
    ) -> None:
        self.workspace = _TuiWorkspaceCatalogController(
            workspace_session_application=workspace_session_application,
            flow_catalog_application=flow_catalog_application,
            control_application=control_application,
        )
        self.presentation = _TuiFlowPresentationController(
            control_application=control_application,
            log_service=log_service,
            reset_service=reset_service,
        )

    def action_refresh_flows(self, window: "DataEngineTui") -> None:
        self.workspace.action_refresh_flows(window, self.presentation)

    def action_run_selected(self, window: "DataEngineTui") -> None:
        self.presentation.action_run_selected(window)

    def action_start_engine(self, window: "DataEngineTui") -> None:
        self.presentation.action_start_engine(window)

    def action_stop_engine(self, window: "DataEngineTui") -> None:
        self.presentation.action_stop_engine(window)

    def action_view_config(self, window: "DataEngineTui") -> None:
        self.presentation.action_view_config(window)

    def action_clear_flow_log(self, window: "DataEngineTui") -> None:
        self.presentation.action_clear_flow_log(window)

    def action_view_log(self, window: "DataEngineTui") -> None:
        self.presentation.action_view_log(window)

    def load_flows(self, window: "DataEngineTui") -> None:
        self.workspace.load_flows(window, self.presentation)

    def reload_workspace_options(self, window: "DataEngineTui") -> None:
        self.workspace.reload_workspace_options(window)

    def switch_workspace(self, window: "DataEngineTui", workspace_id: str) -> None:
        self.workspace.switch_workspace(window, workspace_id, self.presentation)

    def render_selected_flow(self, window: "DataEngineTui") -> None:
        self.presentation.render_selected_flow(window)

    def selected_run_group(self, window: "DataEngineTui") -> "FlowRunState | None":
        return self.presentation.selected_run_group(window)


class _TuiWorkspaceCatalogController:
    """Own TUI workspace binding and catalog refresh orchestration."""

    def __init__(
        self,
        *,
        workspace_session_application: WorkspaceSessionApplication,
        flow_catalog_application: FlowCatalogApplication,
        control_application: OperatorControlApplication,
    ) -> None:
        self.workspace_session_application = workspace_session_application
        self.flow_catalog_application = flow_catalog_application
        self.control_application = control_application

    def action_refresh_flows(self, window: "DataEngineTui", presentation: "_TuiFlowPresentationController") -> None:
        result = self.control_application.refresh_flows(
            paths=window.workspace_paths,
            runtime_session=window.runtime_session,
            has_authored_workspace=window._has_authored_workspace(),
            timeout=5.0,
        )
        if result.error_text is not None:
            window._set_status(result.error_text)
            return
        if result.reload_catalog:
            self.reload_workspace_options(window)
            self.load_flows(window, presentation)
        if result.sync_after:
            window._sync_daemon_state()
        if result.warning_text is not None:
            window._set_status(result.warning_text)
            return
        if result.status_text is not None:
            window._set_status(result.status_text)

    def load_flows(self, window: "DataEngineTui", presentation_controller: "_TuiFlowPresentationController") -> None:
        list_view = window.query_one("#flow-list", ListView)
        list_view.clear()
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
        window.flow_catalog_state = result.catalog_state
        presentation = self.flow_catalog_application.build_presentation(
            catalog_state=window.flow_catalog_state,
        )
        if not result.loaded:
            window.selected_flow_name = None
            window.query_one("#detail-view", Static).update(window.flow_catalog_state.empty_message or missing_message)
            window.query_one("#log-run-list", ListView).clear()
            window._rebuild_runtime_snapshot()
            return
        for card in presentation.cards:
            window.operation_tracker = window.operation_tracker.ensure_flow(card.name, card.operation_items)
        for group_name, grouped in presentation.grouped_cards:
            list_view.append(GroupHeaderListItem(group_name, len(grouped)))
            for card in grouped:
                list_view.append(FlowListItem(card, window.flow_states[card.name]))
        if presentation.cards:
            window.selected_flow_name = presentation.selected_flow_name
            index = presentation.selected_list_index or 0
            list_view.index = index
            presentation_controller.render_selected_flow(window)
        else:
            window.selected_flow_name = None
            window.query_one("#detail-view", Static).update("No flows discovered.")
            window.query_one("#log-run-list", ListView).clear()
        window._rebuild_runtime_snapshot()

    def reload_workspace_options(self, window: "DataEngineTui") -> None:
        window.workspace_session_state = self.workspace_session_application.refresh_session(
            workspace_paths=window.workspace_paths,
            override_root=window.workspace_collection_root_override,
        )
        current_id = window.workspace_session_state.current_workspace_id
        workspace_ids = window.workspace_session_state.discovered_workspace_ids
        selector = window.query_one("#workspace-select", Select)
        window._workspace_switch_suppressed = True
        try:
            if not workspace_ids:
                selector.set_options([("(no workspace)", Select.BLANK)])
                selector.value = Select.BLANK
                selector.disabled = True
            else:
                selector.set_options([(workspace_id, workspace_id) for workspace_id in workspace_ids])
                if current_id in workspace_ids:
                    selector.value = current_id
                else:
                    selector.value = workspace_ids[0]
                selector.disabled = False
        finally:
            window._workspace_switch_suppressed = False

    def switch_workspace(self, window: "DataEngineTui", workspace_id: str, presentation: "_TuiFlowPresentationController") -> None:
        try:
            window.runtime_binding_service.remove_client_session(window.runtime_binding, window.client_session_id)
        except Exception:
            pass
        window.runtime_binding_service.close_binding(window.runtime_binding)
        window.workspace_paths = window.workspace_service.resolve_paths(
            workspace_id=workspace_id,
            workspace_collection_root=window.workspace_collection_root_override,
        )
        binding = self.workspace_session_application.bind_workspace(
            workspace_paths=window.workspace_paths,
            override_root=window.workspace_collection_root_override,
        )
        window._operator_session_state = binding.operator_session
        window.runtime_binding = window.runtime_binding_service.open_binding(window.workspace_paths)
        window._register_client_session()
        window.flow_cards = ()
        window.flow_states = {}
        window.selected_flow_name = None
        window._last_daemon_spawn_attempt = 0.0
        window._daemon_startup_in_progress = False
        window.selected_run_key = None
        window._last_rendered_flow_signature = None
        window._last_run_list_signature = None
        window._last_detail_signature = None
        self.reload_workspace_options(window)
        if window._has_authored_workspace():
            window._ensure_daemon_started()
        self.load_flows(window, presentation)
        window._sync_daemon_state()
        window._set_status(f"Switched to workspace {workspace_id}.")


class _TuiFlowPresentationController:
    """Own TUI selected-flow, run-list, and action orchestration."""

    def __init__(
        self,
        *,
        control_application: OperatorControlApplication,
        log_service: LogService,
        reset_service: ResetService,
    ) -> None:
        self.control_application = control_application
        self.log_service = log_service
        self.reset_service = reset_service

    @staticmethod
    def _blocked_status_text(window: "DataEngineTui") -> str:
        snapshot = getattr(window, "workspace_snapshot", None)
        if snapshot is None:
            return "Takeover available."
        return snapshot.control.blocked_status_text

    def action_run_selected(self, window: "DataEngineTui") -> None:
        window._sync_daemon_state()
        card = window._selected_card()
        result = self.control_application.run_selected_flow(
            paths=window.workspace_paths,
            runtime_session=window.runtime_session,
            selected_flow_name=card.name if card is not None else None,
            selected_flow_valid=bool(card is not None and card.valid),
            selected_flow_group=card.group if card is not None else None,
            selected_flow_group_active=bool(card is not None and window.runtime_session.is_group_active(card.group, {flow.name: flow.group for flow in window.flow_cards})),
            blocked_status_text=self._blocked_status_text(window),
            timeout=2.0,
        )
        if result.error_text is not None:
            window._set_status(result.error_text)
            return
        if result.status_text is not None:
            window._set_status(result.status_text)
        if result.sync_after:
            window._sync_daemon_state()

    def action_start_engine(self, window: "DataEngineTui") -> None:
        window._sync_daemon_state()
        result = self.control_application.start_engine(
            paths=window.workspace_paths,
            runtime_session=window.runtime_session,
            has_automated_flows=any(card.valid and card.mode in {"poll", "schedule"} for card in window.flow_cards),
            blocked_status_text=self._blocked_status_text(window),
            timeout=2.0,
        )
        if result.error_text is not None:
            window._set_status(result.error_text)
            return
        if result.status_text is not None:
            window._set_status(result.status_text)
        if result.sync_after:
            window._sync_daemon_state()

    def action_stop_engine(self, window: "DataEngineTui") -> None:
        window._sync_daemon_state()
        card = window._selected_card()
        result = self.control_application.stop_pipeline(
            paths=window.workspace_paths,
            runtime_session=window.runtime_session,
            selected_flow_group=card.group if card is not None else None,
            blocked_status_text=self._blocked_status_text(window),
            timeout=2.0,
        )
        if result.error_text is not None:
            window._set_status(result.error_text)
            return
        if result.status_text is not None:
            window._set_status(result.status_text)
        if result.sync_after:
            window._sync_daemon_state()

    def action_view_config(self, window: "DataEngineTui") -> None:
        card = window._selected_card()
        if card is None:
            window._set_status("Select one flow first.")
            return
        preview = window.catalog_query_service.get_flow_preview(card=card, flow_states=window.flow_states)
        lines = [card.title]
        if card.description:
            lines.extend(["", card.description])
        lines.extend([""])
        lines.extend(f"{label}: {value}" for label, value in preview.rows)
        window.push_screen(InfoModal(title=card.title, body="\n".join(lines)))

    def action_clear_flow_log(self, window: "DataEngineTui") -> None:
        if window.selected_flow_name is None:
            return
        if window.runtime_session.has_active_work or window.runtime_session.runtime_stopping:
            window._set_status("Stop the engine and any active manual runs before resetting a flow.")
            return
        if not window.runtime_session.control_available:
            window._set_status(self._blocked_status_text(window))
            return
        try:
            self.reset_service.reset_flow(
                paths=window.workspace_paths,
                runtime_cache_ledger=window.runtime_binding.runtime_cache_ledger,
                flow_name=window.selected_flow_name,
            )
        except Exception as exc:
            window._set_status(f"Flow reset failed: {exc}")
            return
        window._rebuild_runtime_snapshot()
        window._set_status(f"Reset flow history for {window.selected_flow_name}.")

    def action_view_log(self, window: "DataEngineTui") -> None:
        run_group = self.selected_run_group(window)
        if run_group is None:
            window._set_status("Select one run first.")
            return
        window._show_run_group_modal(run_group)

    def render_selected_flow(self, window: "DataEngineTui") -> None:
        card = window._selected_card()
        detail = window.query_one("#detail-view", Static)
        run_list = window.query_one("#log-run-list", ListView)
        run_groups = self.log_service.runs_for_flow(window.runtime_binding.log_store, card.name) if card is not None else ()
        presentation = window.detail_application.build_selected_flow_presentation(
            card=card,
            tracker=window.operation_tracker,
            flow_states=window.flow_states,
            run_groups=tuple(run_groups),
            selected_run_key=window.selected_run_key,
        )
        window.selected_run_key = presentation.selected_run_key
        if presentation.detail_state is None:
            detail.update(presentation.empty_text)
            run_list.clear()
            window._last_run_list_signature = ()
            return
        detail_lines = render_selected_flow_lines(card, window.operation_tracker)
        detail.update("\n".join(detail_lines))
        signature = presentation.run_group_signature
        if signature != window._last_run_list_signature:
            run_list.clear()
            for run_group in presentation.visible_run_groups:
                run_list.append(RunGroupListItem(run_group))
            window._last_run_list_signature = signature
        else:
            visible_items = [child for child in run_list.children if isinstance(child, RunGroupListItem)]
            for item, run_group in zip(visible_items, presentation.visible_run_groups):
                item.refresh_view(run_group)

    def selected_run_group(self, window: "DataEngineTui") -> "FlowRunState | None":
        card = window._selected_card()
        run_groups = self.log_service.runs_for_flow(window.runtime_binding.log_store, card.name) if card is not None else ()
        presentation = window.detail_application.build_selected_flow_presentation(
            card=card,
            tracker=window.operation_tracker,
            flow_states=window.flow_states,
            run_groups=tuple(run_groups),
            selected_run_key=window.selected_run_key,
        )
        window.selected_run_key = presentation.selected_run_key
        return presentation.selected_run_group


__all__ = ["TuiFlowController"]
