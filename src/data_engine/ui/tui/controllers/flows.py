"""Flow/workspace/detail controllers for the terminal UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual.widgets import ListView, Select, Static

from data_engine.domain import FlowRunState, OperatorSessionState, WorkspaceSessionState
from data_engine.services import (
    CatalogPort,
    CommandPort,
    HistoryPort,
    WorkspaceService,
    runtime_session_from_workspace_snapshot,
)
from data_engine.views import TuiActionState, build_operator_action_context, build_selected_flow_presentation
from data_engine.views.text import render_selected_flow_lines
from data_engine.ui.tui.widgets import FlowListItem, GroupHeaderListItem, InfoModal, RunGroupListItem

if TYPE_CHECKING:
    from data_engine.ui.tui.app import DataEngineTui


class TuiFlowController:
    """Compose narrower TUI flow collaborators behind one stable controller seam."""

    def __init__(
        self,
        *,
        workspace_service: WorkspaceService,
        catalog_query_service: CatalogPort,
        history_query_service: HistoryPort,
        command_service: CommandPort,
    ) -> None:
        self.workspace = _TuiWorkspaceCatalogController(
            workspace_service=workspace_service,
            catalog_query_service=catalog_query_service,
            command_service=command_service,
        )
        self.presentation = _TuiFlowPresentationController(
            command_service=command_service,
            catalog_query_service=catalog_query_service,
            history_query_service=history_query_service,
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
        workspace_service: WorkspaceService,
        catalog_query_service: CatalogPort,
        command_service: CommandPort,
    ) -> None:
        self.workspace_service = workspace_service
        self.catalog_query_service = catalog_query_service
        self.command_service = command_service

    def action_refresh_flows(self, window: "DataEngineTui", presentation: "_TuiFlowPresentationController") -> None:
        action_context = presentation._action_context(window)
        if presentation._action_state(window).refresh_disabled:
            window._set_status("Stop active engine or manual runs before refreshing flows.")
            return
        result = self.command_service.refresh_flows(
            paths=window.workspace_paths,
            action_context=action_context,
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
        result = self.catalog_query_service.load_workspace_catalog(
            workspace_root=window.workspace_paths.workspace_root,
            current_state=window.flow_catalog_state,
            missing_message=missing_message,
        )
        window.flow_catalog_state = result.catalog_state
        presentation = self.catalog_query_service.build_catalog_presentation(
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
        window.workspace_paths = self.workspace_service.resolve_paths(
            workspace_id=workspace_id,
            workspace_collection_root=window.workspace_collection_root_override,
        )
        window.workspace_session_state = WorkspaceSessionState.from_paths(
            window.workspace_paths,
            override_root=window.workspace_collection_root_override,
            discovered_workspace_ids=(item.workspace_id for item in self.workspace_service.discover(
                app_root=window.workspace_paths.app_root,
                workspace_collection_root=window.workspace_collection_root_override,
            )),
        )
        window._operator_session_state = OperatorSessionState.from_paths(
            window.workspace_paths,
            override_root=window.workspace_collection_root_override,
        ).with_workspace(window.workspace_session_state)
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
        command_service: CommandPort,
        catalog_query_service: CatalogPort,
        history_query_service: HistoryPort,
    ) -> None:
        self.command_service = command_service
        self.catalog_query_service = catalog_query_service
        self.history_query_service = history_query_service

    @staticmethod
    def _blocked_status_text(window: "DataEngineTui") -> str:
        snapshot = getattr(window, "workspace_snapshot", None)
        if snapshot is None:
            return "Takeover available."
        return snapshot.control.blocked_status_text

    @staticmethod
    def _current_runtime_session(window: "DataEngineTui"):
        snapshot = getattr(window, "workspace_snapshot", None)
        if snapshot is None:
            return window.runtime_session
        return runtime_session_from_workspace_snapshot(snapshot)

    def _action_context(self, window: "DataEngineTui"):
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        effective_runtime_session = self._current_runtime_session(window)
        return build_operator_action_context(
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
            engine_active_flow_names=(
                ()
                if workspace_snapshot is None
                else workspace_snapshot.engine.active_flow_names
            ),
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
            selected_run_group_present=self.selected_run_group(window) is not None,
        )

    def _action_state(self, window: "DataEngineTui") -> TuiActionState:
        return TuiActionState.from_context(self._action_context(window))

    def action_run_selected(self, window: "DataEngineTui") -> None:
        window._sync_daemon_state()
        action_context = self._action_context(window)
        action_state = self._action_state(window)
        if action_context.engine_starting:
            window._set_status("Wait for the automated engine to finish starting before running another flow.")
            return
        if action_state.run_once_disabled:
            window._set_status("Select an idle flow with local control before running it.")
            return
        card = window._selected_card()
        result = self.command_service.run_selected_flow(
            paths=window.workspace_paths,
            action_context=action_context,
            selected_flow_name=card.name if card is not None else None,
            selected_flow_valid=bool(card is not None and card.valid),
            blocked_status_text=self._blocked_status_text(window),
            timeout=5.0,
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
        action_context = self._action_context(window)
        action_state = self._action_state(window)
        if action_context.engine_starting:
            window._set_status("Automated engine is already starting.")
            return
        if action_state.start_engine_disabled:
            window._set_status("Local control and an idle runtime are required before starting the engine.")
            return
        result = self.command_service.start_engine(
            paths=window.workspace_paths,
            action_context=action_context,
            has_automated_flows=any(card.valid and card.mode in {"poll", "schedule"} for card in window.flow_cards),
            blocked_status_text=self._blocked_status_text(window),
            timeout=5.0,
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
        action_context = self._action_context(window)
        action_state = self._action_state(window)
        if action_context.engine_starting:
            window._set_status("Wait for the automated engine startup to finish before requesting a stop.")
            return
        if action_state.stop_engine_disabled:
            window._set_status("No active engine or selected manual flow is available to stop.")
            return
        card = window._selected_card()
        result = self.command_service.stop_pipeline(
            paths=window.workspace_paths,
            action_context=action_context,
            selected_flow_name=card.name if card is not None else None,
            blocked_status_text=self._blocked_status_text(window),
            timeout=5.0,
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
        preview = self.catalog_query_service.get_flow_preview(card=card, flow_states=window.flow_states)
        lines = [card.title]
        if card.description:
            lines.extend(["", card.description])
        lines.extend([""])
        lines.extend(f"{label}: {value}" for label, value in preview.rows)
        window.push_screen(InfoModal(title=card.title, body="\n".join(lines)))

    def action_clear_flow_log(self, window: "DataEngineTui") -> None:
        if window.selected_flow_name is None:
            return
        action_context = self._action_context(window)
        effective_runtime_session = self._current_runtime_session(window)
        if (
            action_context.engine_busy
            or action_context.manual_run_active
            or action_context.selected_flow.group_active
        ):
            window._set_status("Stop the engine and any active manual runs before resetting a flow.")
            return
        if not effective_runtime_session.control_available:
            window._set_status(self._blocked_status_text(window))
            return
        result = self.command_service.reset_flow(
            paths=window.workspace_paths,
            runtime_cache_ledger=window.runtime_binding.runtime_cache_ledger,
            flow_name=window.selected_flow_name,
        )
        if result.error_text is not None:
            window._set_status(f"Flow reset failed: {result.error_text}")
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
        run_groups = self.history_query_service.list_flow_runs(window.runtime_binding.log_store, flow_name=(card.name if card is not None else None))
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        presentation = build_selected_flow_presentation(
            card=card,
            tracker=window.operation_tracker,
            flow_states=window.flow_states,
            run_groups=tuple(run_groups),
            selected_run_key=window.selected_run_key,
            live_runs=(
                workspace_snapshot.active_runs
                if workspace_snapshot is not None and workspace_snapshot.engine.daemon_live
                else None
            ),
            live_truth_authoritative=bool(
                workspace_snapshot is not None and workspace_snapshot.engine.daemon_live
            ),
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
        run_groups = self.history_query_service.list_flow_runs(window.runtime_binding.log_store, flow_name=(card.name if card is not None else None))
        workspace_snapshot = getattr(window, "workspace_snapshot", None)
        presentation = build_selected_flow_presentation(
            card=card,
            tracker=window.operation_tracker,
            flow_states=window.flow_states,
            run_groups=tuple(run_groups),
            selected_run_key=window.selected_run_key,
            live_runs=(
                workspace_snapshot.active_runs
                if workspace_snapshot is not None and workspace_snapshot.engine.daemon_live
                else None
            ),
            live_truth_authoritative=bool(
                workspace_snapshot is not None and workspace_snapshot.engine.daemon_live
            ),
        )
        window.selected_run_key = presentation.selected_run_key
        return presentation.selected_run_group


__all__ = ["TuiFlowController"]
