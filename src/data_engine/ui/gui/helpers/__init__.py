"""Low-level helper functions for the desktop GUI shell."""

from data_engine.ui.gui.helpers.inspection import (
    artifact_key_for_operation,
    capture_step_outputs,
    inspect_step_output,
    is_inspectable_operation,
    refresh_operation_buttons,
    rehydrate_step_outputs_from_ledger,
    show_config_preview,
    show_output_preview,
)
from data_engine.ui.gui.helpers.lifecycle import (
    is_last_process_ui_window,
    register_client_session,
    shutdown_daemon_on_close,
    start_worker_thread,
    unregister_client_session_and_check_for_shutdown,
    wait_for_worker_threads,
)
from data_engine.ui.gui.helpers.scroll import update_operation_scroll_cues, update_sidebar_scroll_cues
from data_engine.ui.gui.helpers.theming import (
    action_bar_icon,
    apply_theme,
    group_icon,
    group_icon_color,
    log_icon,
    render_group_icon_pixmap,
    render_svg_icon_pixmap,
    sync_theme_to_system,
    toggle_theme,
    view_rail_icon,
)

__all__ = [
    "action_bar_icon",
    "apply_theme",
    "artifact_key_for_operation",
    "capture_step_outputs",
    "group_icon",
    "group_icon_color",
    "is_last_process_ui_window",
    "inspect_step_output",
    "is_inspectable_operation",
    "log_icon",
    "register_client_session",
    "refresh_operation_buttons",
    "rehydrate_step_outputs_from_ledger",
    "render_group_icon_pixmap",
    "render_svg_icon_pixmap",
    "show_config_preview",
    "show_output_preview",
    "shutdown_daemon_on_close",
    "start_worker_thread",
    "sync_theme_to_system",
    "toggle_theme",
    "unregister_client_session_and_check_for_shutdown",
    "update_operation_scroll_cues",
    "update_sidebar_scroll_cues",
    "view_rail_icon",
    "wait_for_worker_threads",
]
