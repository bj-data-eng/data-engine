"""Workspace rebinding helpers for the desktop UI."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_engine.domain import DaemonStatusState, OperationSessionState, OperatorSessionState, RuntimeSessionState, StepOutputIndex, WorkspaceSessionState
from data_engine.platform.instrumentation import maybe_start_viztracer
from data_engine.services import DaemonUpdateSubscription
from data_engine.ui.gui.helpers import register_client_session
from data_engine.ui.gui.presenters.workspace_settings import refresh_workspace_root_controls

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def _close_workspace_scoped_dialogs(window: "DataEngineWindow") -> None:
    for attr_name in ("output_preview_dialog", "config_preview_dialog", "run_log_preview_dialog"):
        dialog = getattr(window, attr_name, None)
        if dialog is None:
            continue
        try:
            dialog.close()
        except Exception:
            pass
        setattr(window, attr_name, None)


def _sync_workspace_selector(window: "DataEngineWindow") -> None:
    selector = getattr(window, "workspace_selector", None)
    if selector is not None:
        target_index = selector.findData(window.workspace_paths.workspace_id)
        if target_index >= 0:
            selector.blockSignals(True)
            try:
                selector.setCurrentIndex(target_index)
            finally:
                selector.blockSignals(False)
    settings_selector = getattr(window, "workspace_settings_selector", None)
    if settings_selector is not None:
        target_id = str(getattr(window, "settings_workspace_target_id", window.workspace_paths.workspace_id) or window.workspace_paths.workspace_id)
        target_index = settings_selector.findData(target_id)
        if target_index >= 0:
            settings_selector.blockSignals(True)
            try:
                settings_selector.setCurrentIndex(target_index)
            finally:
                settings_selector.blockSignals(False)


def _shutdown_old_workspace_if_orphaned(window: "DataEngineWindow", old_binding) -> None:
    try:
        remaining = window.runtime_binding_service.count_live_client_sessions(old_binding)
    except Exception:
        return
    if remaining != 0:
        return
    # Let the daemon own its own lifetime once this UI detaches. The daemon
    # already knows whether active engine/manual work is still running, while the
    # UI rebind path only knows the client count and would otherwise stop active
    # work when switching away from a busy workspace.
    return


def rebind_workspace_context(
    window: "DataEngineWindow",
    *,
    workspace_id: str | None = None,
    override_root: Path | None | object = ...,
) -> None:
    from data_engine.ui.gui.presenters.docs import initialize_docs_view

    window._message_box_generation += 1
    window._pending_message_box = None
    window._message_box_scheduled = False
    _close_workspace_scoped_dialogs(window)
    window._advance_workspace_binding_generation()
    old_subscription = window.daemon_subscription
    old_subscription.stop()
    old_thread = old_subscription.thread
    old_binding = window.runtime_binding
    if override_root is ...:
        override_root = window.workspace_collection_root_override
    window.workspace_paths = window._resolve_workspace_paths(
        workspace_id=workspace_id,
        workspace_collection_root=override_root,
    )
    discovered = window.services.workspace_service.discover(
        app_root=window.workspace_paths.app_root,
        workspace_collection_root=override_root,
    )
    window.workspace_session_state = WorkspaceSessionState.from_paths(
        window.workspace_paths,
        override_root=override_root,
        discovered_workspace_ids=(item.workspace_id for item in discovered),
    )
    window._operator_session_state = OperatorSessionState.from_paths(
        window.workspace_paths,
        override_root=override_root,
    ).with_workspace(window.workspace_session_state)
    window.runtime_binding = window.runtime_binding_service.open_binding(window.workspace_paths)
    window.daemon_subscription = DaemonUpdateSubscription(
        daemon_state_service=window.daemon_state_service,
        manager=window.runtime_binding.daemon_manager,
        clock=window._monotonic,
    )
    register_client_session(window)
    window.runtime_binding_service.close_binding(old_binding)
    window._ui_timing_log_path = (
        window.workspace_paths.runtime_state_dir / "ui_timing.log"
        if window.workspace_paths.workspace_configured
        else None
    )
    maybe_start_viztracer(
        None if window._ui_timing_log_path is None else window.workspace_paths.runtime_state_dir / "ui_viztrace.json",
        process_name=f"gui:{window.workspace_paths.workspace_id}",
    )
    window.daemon_status = DaemonStatusState.empty()
    window.workspace_snapshot = None
    window.runtime_session = RuntimeSessionState.empty()
    window.operation_tracker = OperationSessionState.empty()
    window.flow_states = {}
    window._daemon_startup_in_progress = False
    window._daemon_sync_in_progress = False
    window._daemon_sync_pending = False
    window._last_daemon_spawn_attempt = 0.0
    window._pending_control_actions.clear()
    window._pending_control_action_tokens.clear()
    window._pending_daemon_update_batch = None
    window.manual_flow_stop_events = {}
    window.manual_flow_stopping_groups = set()
    window.pending_manual_run_requests = {}
    window.step_output_index = StepOutputIndex.empty()
    window._last_log_view_flow_name = None
    window._last_log_view_run_keys = ()
    window._last_log_view_signature = ()
    window._cached_selected_flow_run_groups = ()
    window._cached_selected_flow_run_groups_flow_name = None
    window._cached_selected_flow_entry_count = 0
    window._selected_flow_run_groups_dirty = True
    window._selected_flow_has_logs = False
    window._selected_flow_has_logs_flow_name = None
    window.log_view.clear()
    window._workspace_counts_footer_cache.clear()
    window._last_gui_action_state = None
    window.workspace_provision_status_label.clear()
    window.force_shutdown_daemon_status_label.clear()
    window.reset_workspace_status_label.clear()
    if window.workspace_session_state.discovered_workspace_ids:
        if window.settings_workspace_target_id not in window.workspace_session_state.discovered_workspace_ids:
            window.settings_workspace_target_id = window.workspace_paths.workspace_id
            window._settings_workspace_target_pinned = False
    else:
        window.settings_workspace_target_id = window.workspace_paths.workspace_id
        window._settings_workspace_target_pinned = False
    if old_thread is not None and old_thread.is_alive():
        old_thread.join(timeout=max(old_subscription.timeout_seconds, 0.5) + 0.5)
    window._reload_workspace_options()
    _sync_workspace_selector(window)
    refresh_workspace_root_controls(window)
    window._load_flows()
    window._refresh_debug_artifacts()
    window._refresh_log_view(force_scroll_to_bottom=True)
    window._refresh_action_buttons()
    initialize_docs_view(window)
    if window._auto_daemon_enabled:
        window._ensure_daemon_wait_worker()
        window._sync_from_daemon()


__all__ = ["rebind_workspace_context"]
