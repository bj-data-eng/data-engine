"""Workspace rebinding helpers for the desktop UI."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_engine.domain import StepOutputIndex, WorkspaceControlState
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

    try:
        window.runtime_binding_service.remove_client_session(window.runtime_binding, window.client_session_id)
    except Exception:
        pass
    window.runtime_binding_service.close_binding(window.runtime_binding)
    if override_root is ...:
        override_root = window.workspace_collection_root_override
    window.workspace_paths = window._resolve_workspace_paths(
        workspace_id=workspace_id,
        workspace_collection_root=override_root,
    )
    binding = window.workspace_session_application.bind_workspace(
        workspace_paths=window.workspace_paths,
        override_root=override_root,
    )
    window._operator_session_state = binding.operator_session
    window.runtime_binding = window.runtime_binding_service.open_binding(window.workspace_paths)
    register_client_session(window)
    window.daemon_status = window.daemon_status.empty()
    window.workspace_control_state = WorkspaceControlState.empty()
    window._daemon_startup_in_progress = False
    window._last_daemon_spawn_attempt = 0.0
    window.manual_flow_stop_events = {}
    window.step_output_index = StepOutputIndex.empty()
    window._reload_workspace_options()
    refresh_workspace_root_controls(window)
    window._load_flows()
    window._refresh_log_view(force_scroll_to_bottom=True)
    window._refresh_action_buttons()
    initialize_docs_view(window)
    if window._auto_daemon_enabled:
        window._sync_from_daemon()


__all__ = ["rebind_workspace_context"]
