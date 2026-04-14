"""Presenter helpers for the desktop UI."""

from data_engine.ui.gui.presenters.logs import add_log_run_item, format_raw_log_message, refresh_log_view
from data_engine.ui.gui.presenters.docs import (
    create_docs_browser,
    initialize_docs_view,
    load_docs_page,
)
from data_engine.ui.gui.presenters.runtime_projection import (
    apply_daemon_snapshot,
    finish_daemon_startup,
)
from data_engine.ui.gui.presenters.sidebar import refresh_sidebar_selection, refresh_sidebar_state_views, repolish_widget_tree, set_hovered
from data_engine.ui.gui.presenters.steps import (
    apply_runtime_event,
    duration_text,
    format_seconds,
    normalize_completed_operation_rows,
    refresh_live_operation_durations,
    render_operation_durations,
    reset_operation_state,
)
from data_engine.ui.gui.presenters.workspace_binding import rebind_workspace_context
from data_engine.ui.gui.presenters.workspace_settings import (
    browse_workspace_collection_root_override,
    force_shutdown_daemon,
    provision_selected_workspace,
    reset_workspace,
    refresh_workspace_provisioning_controls,
    refresh_workspace_visibility_panel,
    refresh_workspace_root_controls,
    reset_workspace_collection_root_override,
    save_workspace_collection_root_override,
)

__all__ = [
    "add_log_run_item",
    "apply_runtime_event",
    "apply_daemon_snapshot",
    "browse_workspace_collection_root_override",
    "create_docs_browser",
    "duration_text",
    "finish_daemon_startup",
    "format_raw_log_message",
    "format_seconds",
    "force_shutdown_daemon",
    "initialize_docs_view",
    "load_docs_page",
    "normalize_completed_operation_rows",
    "rebind_workspace_context",
    "provision_selected_workspace",
    "reset_workspace",
    "refresh_log_view",
    "refresh_workspace_provisioning_controls",
    "refresh_workspace_visibility_panel",
    "refresh_live_operation_durations",
    "refresh_workspace_root_controls",
    "refresh_sidebar_selection",
    "refresh_sidebar_state_views",
    "reset_workspace_collection_root_override",
    "render_operation_durations",
    "repolish_widget_tree",
    "reset_operation_state",
    "save_workspace_collection_root_override",
    "set_hovered",
]
