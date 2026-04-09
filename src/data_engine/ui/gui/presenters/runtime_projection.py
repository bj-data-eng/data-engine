"""Daemon/runtime projection helpers for the desktop UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_engine.domain import RuntimeSessionState

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def finish_daemon_startup(window: "DataEngineWindow", success: bool, error_text: str) -> None:
    window._daemon_startup_in_progress = False
    if not success and not error_text:
        error_text = "Daemon startup did not provide any additional error details."
    if not success and error_text and window.isVisible():
        window._append_log_line(f"Daemon startup failed: {error_text}")
    window._sync_from_daemon()


def apply_daemon_snapshot(window: "DataEngineWindow", snapshot) -> None:
    window.runtime_session = RuntimeSessionState.from_daemon_snapshot(snapshot, window.flow_cards.values())


__all__ = [
    "apply_daemon_snapshot",
    "finish_daemon_startup",
]
