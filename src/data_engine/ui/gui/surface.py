"""Surface-level helper functions for the GUI application shell."""

from __future__ import annotations

import logging
from queue import Empty
from typing import TYPE_CHECKING

from PySide6.QtCore import QTimer

from data_engine.ui.gui.bootstrap import GuiServices, build_gui_services, default_gui_service_kwargs

if TYPE_CHECKING:
    from PySide6.QtWidgets import QCloseEvent, QShowEvent

    from data_engine.domain import FlowLogEntry
    from data_engine.ui.gui.app import DataEngineWindow
def build_default_gui_services(theme_name: str) -> GuiServices:
    """Build the default GUI service bundle for the main window."""
    return build_gui_services(
        **default_gui_service_kwargs(theme_name),
    )


def handle_show_event(window: "DataEngineWindow", event: "QShowEvent") -> None:
    """Run the GUI show-event side effects."""
    super(type(window), window).showEvent(event)
    controls_group = getattr(window, "action_bar_controls_group", None)
    if controls_group is not None:
        controls_group.setVisible(False)

        def _reveal_controls() -> None:
            if window.ui_closing:
                return
            latest_controls_group = getattr(window, "action_bar_controls_group", None)
            if latest_controls_group is None:
                return
            latest_controls_group.setVisible(True)
            latest_controls_group.updateGeometry()
            latest_controls_group.update()

        QTimer.singleShot(0, _reveal_controls)
    if not window._auto_daemon_enabled:
        window._auto_daemon_enabled = True
        QTimer.singleShot(0, window._ensure_daemon_started)
    window._ensure_daemon_wait_worker()


def handle_close_event(window: "DataEngineWindow", event: "QCloseEvent") -> None:
    """Run the GUI close-event shutdown path."""
    window.ui_closing = True
    style_hints = getattr(window, "_style_hints", None)
    color_scheme_changed_slot = getattr(window, "_color_scheme_changed_slot", None)
    if style_hints is not None and hasattr(style_hints, "colorSchemeChanged") and color_scheme_changed_slot is not None:
        try:
            style_hints.colorSchemeChanged.disconnect(color_scheme_changed_slot)
        except (RuntimeError, TypeError):
            pass
    log_handler = getattr(window, "log_handler", None)
    if log_handler is not None:
        logging.getLogger("data_engine").removeHandler(log_handler)
    runtime_stop_event = getattr(window, "engine_runtime_stop_event", None)
    if runtime_stop_event is not None:
        runtime_stop_event.set()
    flow_stop_event = getattr(window, "engine_flow_stop_event", None)
    if flow_stop_event is not None:
        flow_stop_event.set()
    daemon_subscription = getattr(window, "daemon_subscription", None)
    if daemon_subscription is not None:
        daemon_subscription.stop()
    for stop_event in getattr(window, "manual_flow_stop_events", {}).values():
        stop_event.set()
    if hasattr(window, "log_timer"):
        window.log_timer.stop()
    if hasattr(window, "ui_refresh_timer"):
        window.ui_refresh_timer.stop()
    if hasattr(window, "operation_timer"):
        window.operation_timer.stop()
    if hasattr(window, "daemon_timer"):
        window.daemon_timer.stop()
    if hasattr(window, "_wait_for_worker_threads"):
        window._wait_for_worker_threads(timeout_seconds=1.5)
    if hasattr(window, "_unregister_client_session_and_check_for_shutdown") and hasattr(window, "_is_last_process_ui_window"):
        should_shutdown_daemon = window._unregister_client_session_and_check_for_shutdown(
            purge_process_ui_sessions=window._is_last_process_ui_window(),
        )
        if should_shutdown_daemon and hasattr(window, "_shutdown_daemon_on_close"):
            window._shutdown_daemon_on_close()
    if hasattr(window, "runtime_binding_service") and hasattr(window, "runtime_binding"):
        window.runtime_binding_service.close_binding(window.runtime_binding)
    super(type(window), window).closeEvent(event)


def show_message_box_later(window: "DataEngineWindow", *, title: str, text: str, tone: str) -> None:
    """Defer one application dialog until the current UI update cycle completes."""
    if window.ui_closing:
        return
    if not str(text).strip():
        return
    window._pending_message_box = (title, text, tone)
    if window._message_box_scheduled or window._message_box_open:
        return
    generation = window._message_box_generation

    def _show_pending_message_box() -> None:
        if generation != window._message_box_generation:
            return
        window._message_box_scheduled = False
        if window.ui_closing:
            return
        payload = window._pending_message_box
        window._pending_message_box = None
        if payload is None:
            return
        next_title, next_text, next_tone = payload
        window._message_box_open = True
        try:
            window._show_message_box(title=next_title, text=next_text, tone=next_tone)
        finally:
            window._message_box_open = False
        if (
            generation == window._message_box_generation
            and window._pending_message_box is not None
            and not window.ui_closing
        ):
            show_message_box_later(
                window,
                title=window._pending_message_box[0],
                text=window._pending_message_box[1],
                tone=window._pending_message_box[2],
            )

    window._message_box_scheduled = True
    QTimer.singleShot(0, _show_pending_message_box)


def log_matches_selection(window: "DataEngineWindow", entry: "FlowLogEntry") -> bool:
    """Return whether one log entry belongs to the currently selected flow."""
    return entry.kind == "flow" and window.selected_flow_name is not None and entry.flow_name == window.selected_flow_name


def append_log_entry(window: "DataEngineWindow", entry: "FlowLogEntry") -> None:
    """Append one log entry and schedule affected UI refresh work."""
    window.log_service.append_entry(window.runtime_binding.log_store, entry)
    if log_matches_selection(window, entry):
        schedule_ui_refresh(window, log_view=True, action_buttons=True)


def schedule_ui_refresh(window: "DataEngineWindow", *, log_view: bool = False, action_buttons: bool = False) -> None:
    """Schedule one deferred GUI refresh cycle."""
    if log_view:
        window._log_view_refresh_pending = True
    if action_buttons:
        window._action_buttons_refresh_pending = True
    if not window.ui_refresh_timer.isActive():
        window.ui_refresh_timer.start(0)


def flush_deferred_ui_updates(window: "DataEngineWindow") -> None:
    """Flush any deferred GUI refresh work."""
    pending_workspace_id = getattr(window, "_pending_workspace_switch_id", None)
    if pending_workspace_id is not None:
        window._pending_workspace_switch_id = None
        if not window.ui_closing and pending_workspace_id != window.workspace_paths.workspace_id:
            window._rebind_workspace_context(workspace_id=pending_workspace_id)
    if window._log_view_refresh_pending:
        window._log_view_refresh_pending = False
        window._refresh_log_view()
    if window._action_buttons_refresh_pending:
        window._action_buttons_refresh_pending = False
        window._refresh_action_buttons()


def append_log_line(window: "DataEngineWindow", line: str, *, flow_name: str | None = None) -> None:
    """Append one simple textual log line."""
    from data_engine.domain import FlowLogEntry

    kind = "flow" if flow_name is not None else "system"
    append_log_entry(window, FlowLogEntry(line=line, kind=kind, flow_name=flow_name))


def poll_log_queue(window: "DataEngineWindow") -> None:
    """Drain queued runtime log entries and schedule the minimum UI refresh work."""
    selected_flow_dirty = False
    action_buttons_dirty = False
    processed = 0
    while processed < window._MAX_LOG_EVENTS_PER_TICK:
        try:
            entry = window.log_queue.get_nowait()
        except Empty:
            break
        window.log_service.append_entry(window.runtime_binding.log_store, entry)
        if log_matches_selection(window, entry):
            selected_flow_dirty = True
            action_buttons_dirty = True
        elif entry.kind == "flow":
            action_buttons_dirty = True
        if entry.event is not None:
            window._apply_runtime_event(entry.event)
        processed += 1
    if selected_flow_dirty or action_buttons_dirty:
        schedule_ui_refresh(window, log_view=selected_flow_dirty, action_buttons=action_buttons_dirty)
    if not window.log_queue.empty() and not window.ui_closing:
        QTimer.singleShot(0, window._poll_log_queue)


def safe_emit_run_finished(window: "DataEngineWindow", flow_name: str, results: object, error: object) -> None:
    """Emit the queued run-finished signal unless the GUI is closing."""
    if window.ui_closing:
        return
    try:
        window.signals.run_finished.emit(flow_name, results, error)
    except RuntimeError:
        pass


def safe_emit_runtime_finished(window: "DataEngineWindow", flow_names: tuple[str, ...], results: object, error: object) -> None:
    """Emit the queued runtime-finished signal unless the GUI is closing."""
    if window.ui_closing:
        return
    try:
        window.signals.runtime_finished.emit(flow_names, results, error)
    except RuntimeError:
        pass


__all__ = [
    "append_log_entry",
    "append_log_line",
    "build_default_gui_services",
    "flush_deferred_ui_updates",
    "handle_close_event",
    "handle_show_event",
    "log_matches_selection",
    "poll_log_queue",
    "safe_emit_run_finished",
    "safe_emit_runtime_finished",
    "schedule_ui_refresh",
    "show_message_box_later",
]
