"""Lifecycle and worker helpers for the desktop GUI surface."""

from __future__ import annotations

import os
import threading
import time
from typing import TYPE_CHECKING

from PySide6.QtWidgets import QApplication

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def register_client_session(window: "DataEngineWindow") -> None:
    """Register this UI process as one active local client for the workspace."""
    window.runtime_binding_service.register_client_session(
        window.runtime_binding,
        client_id=window.client_session_id,
        client_kind="ui",
        pid=os.getpid(),
    )


def is_last_process_ui_window(window: "DataEngineWindow") -> bool:
    """Return whether this is the last Data Engine window still open in this process."""
    for widget in QApplication.topLevelWidgets():
        if widget is window:
            continue
        if isinstance(widget, type(window)) and not getattr(widget, "ui_closing", False):
            return False
    return True


def unregister_client_session_and_check_for_shutdown(
    window: "DataEngineWindow",
    *,
    purge_process_ui_sessions: bool = False,
) -> bool:
    """Remove this UI session and return whether no local clients remain."""
    try:
        window.runtime_binding_service.remove_client_session(window.runtime_binding, window.client_session_id)
        if purge_process_ui_sessions:
            window.runtime_binding_service.purge_process_client_sessions(
                window.runtime_binding,
                client_kind="ui",
                pid=os.getpid(),
            )
        remaining = window.runtime_binding_service.count_live_client_sessions(window.runtime_binding)
        return remaining == 0
    except Exception:
        return False


def shutdown_daemon_on_close(window: "DataEngineWindow") -> None:
    """Best-effort local daemon shutdown when the last local client closes."""
    client_error_type = Exception
    resolve_client_error_type = getattr(window, "_daemon_client_error_type", None)
    if callable(resolve_client_error_type):
        try:
            candidate = resolve_client_error_type()
        except Exception:
            candidate = None
        if isinstance(candidate, type) and issubclass(candidate, BaseException):
            client_error_type = candidate
    workspace_snapshot = getattr(window, "workspace_snapshot", None)
    runtime_session = getattr(window, "runtime_session", None)
    engine_state = (
        str(getattr(getattr(workspace_snapshot, "engine", None), "state", "") or "").strip().lower()
        if workspace_snapshot is not None
        else ""
    )
    if not engine_state:
        runtime_active = bool(getattr(runtime_session, "runtime_active", False))
        runtime_stopping = bool(getattr(runtime_session, "runtime_stopping", False))
        engine_state = "stopping" if runtime_stopping else "running" if runtime_active else "idle"
    manual_run_active = bool(getattr(runtime_session, "manual_run_active", False))
    if workspace_snapshot is not None:
        engine_flow_names = set(getattr(workspace_snapshot.engine, "active_flow_names", ()))
        manual_run_active = any(
            run.flow_name not in engine_flow_names
            and run.state in {"starting", "running", "stopping"}
            for run in getattr(workspace_snapshot, "active_runs", {}).values()
        )
    try:
        if not window._is_daemon_live(window.workspace_paths):
            return
        if engine_state in {"starting", "running", "stopping"}:
            window._daemon_request(
                window.workspace_paths,
                {"command": "stop_engine", "shutdown_when_idle": True},
                timeout=1.5,
            )
            return
        if manual_run_active:
            return
        window._daemon_request(window.workspace_paths, {"command": "shutdown_daemon"}, timeout=1.5)
    except client_error_type:
        pass
    except Exception:
        pass


def start_worker_thread(window: "DataEngineWindow", *, target, args=()) -> None:
    """Start one tracked daemon worker thread for background UI tasks."""
    thread = threading.Thread(target=run_tracked_worker, args=(window, target, args), daemon=True)
    window._register_worker_thread(thread)
    thread.start()
    return thread


def run_tracked_worker(window: "DataEngineWindow", target, args) -> None:
    """Execute one worker target and remove its thread from the tracked set."""
    current = threading.current_thread()
    try:
        target(*args)
    finally:
        window._discard_worker_thread(current)


def wait_for_worker_threads(window: "DataEngineWindow", *, timeout_seconds: float) -> None:
    """Wait briefly for tracked workers to honor stop requests before exit."""
    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    for thread in window._worker_threads_snapshot():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        is_alive = getattr(thread, "is_alive", None)
        join = getattr(thread, "join", None)
        alive = bool(is_alive()) if callable(is_alive) else False
        if alive and callable(join):
            join(timeout=min(remaining, 0.3))


__all__ = [
    "is_last_process_ui_window",
    "register_client_session",
    "shutdown_daemon_on_close",
    "start_worker_thread",
    "unregister_client_session_and_check_for_shutdown",
    "wait_for_worker_threads",
]
