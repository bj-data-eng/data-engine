"""Workspace-folder settings presentation helpers for the desktop UI."""

from __future__ import annotations

from pathlib import Path
import sys
from typing import TYPE_CHECKING

from PySide6.QtWidgets import QFileDialog

from data_engine.platform.identity import APP_DISPLAY_NAME
from data_engine.ui.gui.helpers import start_worker_thread

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


def refresh_workspace_root_controls(window: "DataEngineWindow") -> None:
    window.workspace_root_input.setText(window.workspace_session_state.root.input_text)
    window.workspace_root_status_label.setText(window.workspace_session_state.root.status_text)
    refresh_workspace_provisioning_controls(window)
    refresh_workspace_visibility_panel(window)


def save_workspace_collection_root_override(window: "DataEngineWindow") -> None:
    from data_engine.ui.gui.presenters.workspace_binding import rebind_workspace_context

    raw_value = window.workspace_root_input.text().strip()
    if not raw_value:
        reset_workspace_collection_root_override(window)
        return
    target_root = Path(raw_value).expanduser().resolve()
    window.settings_service.set_workspace_collection_root(target_root)
    rebind_workspace_context(window, override_root=target_root)
    window.workspace_root_status_label.setText(f"Workspace folder: {target_root}")


def reset_workspace_collection_root_override(window: "DataEngineWindow") -> None:
    from data_engine.ui.gui.presenters.workspace_binding import rebind_workspace_context

    window.settings_service.set_workspace_collection_root(None)
    rebind_workspace_context(window, override_root=None)
    window.workspace_root_status_label.setText(window.workspace_session_state.root.status_text)


def browse_workspace_collection_root_override(window: "DataEngineWindow") -> None:
    current_text = window.workspace_root_input.text().strip()
    if current_text:
        start_dir = str(Path(current_text).expanduser())
    elif window.workspace_paths.workspace_configured:
        start_dir = str(window.workspace_paths.workspace_collection_root)
    else:
        start_dir = str(Path.home())
    selected = QFileDialog.getExistingDirectory(window, "Select Workspace Folder", start_dir)
    if selected:
        window.workspace_root_input.setText(str(Path(selected).expanduser().resolve()))
        save_workspace_collection_root_override(window)


def provision_selected_workspace(window: "DataEngineWindow") -> None:
    if not window.provision_workspace_button.isEnabled() or "provision_workspace" in window._pending_control_actions or window.ui_closing:
        return
    window._pending_control_actions.add("provision_workspace")
    refresh_workspace_visibility_panel(window)
    start_worker_thread(window, target=_provision_selected_workspace_worker, args=(window,))


def force_shutdown_daemon(window: "DataEngineWindow") -> None:
    if not window.force_shutdown_daemon_button.isEnabled() or "force_shutdown_daemon" in window._pending_control_actions or window.ui_closing:
        return
    window._pending_control_actions.add("force_shutdown_daemon")
    refresh_workspace_visibility_panel(window)
    start_worker_thread(window, target=_force_shutdown_daemon_worker, args=(window,))


def reset_workspace(window: "DataEngineWindow") -> None:
    if not window.reset_workspace_button.isEnabled() or "reset_workspace" in window._pending_control_actions or window.ui_closing:
        return
    window._pending_control_actions.add("reset_workspace")
    refresh_workspace_visibility_panel(window)
    start_worker_thread(window, target=_reset_workspace_worker, args=(window,))


def refresh_workspace_provisioning_controls(window: "DataEngineWindow") -> None:
    provision_pending = "provision_workspace" in window._pending_control_actions
    if not window.workspace_paths.workspace_configured:
        window.workspace_target_label.setText(
            "Selected workspace: choose a workspace folder first, then choose a workspace to provision."
        )
        window.provision_workspace_button.setEnabled(False)
        if not window.workspace_provision_status_label.text().strip():
            window.workspace_provision_status_label.setText(
                "Provisioning creates a workspace folder, flow_modules, and VS Code settings for the workspace selected above without overwriting existing files."
            )
        return
    workspace_root = window.workspace_paths.workspace_root
    workspace_ready = window.workspace_paths.flow_modules_dir.is_dir()
    window.workspace_target_label.setText(
        f"Selected workspace: {window.workspace_paths.workspace_id} ({workspace_root})"
    )
    window.provision_workspace_button.setEnabled(not provision_pending)
    if workspace_ready:
        window.workspace_provision_status_label.setText(
            "Workspace already has flow modules. Provisioning the selected workspace will only add missing folders or VS Code settings."
        )
    else:
        window.workspace_provision_status_label.setText(
            "Provision the selected workspace to create flow_modules and local VS Code settings."
        )


def refresh_workspace_visibility_panel(window: "DataEngineWindow") -> None:
    interpreter_path = Path(sys.executable).expanduser()
    interpreter_mode = "virtual environment" if sys.prefix != getattr(sys, "base_prefix", sys.prefix) else "system/global"
    force_pending = "force_shutdown_daemon" in window._pending_control_actions
    reset_pending = "reset_workspace" in window._pending_control_actions
    provision_pending = "provision_workspace" in window._pending_control_actions
    window.force_shutdown_daemon_button.setText("Force Stopping..." if force_pending else "Force Stop Daemon")
    window.force_shutdown_daemon_button.setEnabled(window.workspace_paths.workspace_configured and not force_pending)
    window.reset_workspace_button.setText("Resetting..." if reset_pending else "Reset Workspace")
    window.reset_workspace_button.setEnabled(
        window.workspace_paths.workspace_configured
        and not reset_pending
        and not window.runtime_session.has_active_work
        and not window.runtime_session.runtime_stopping
        and window.runtime_session.control_available
    )
    window.provision_workspace_button.setText("Provisioning..." if provision_pending else "Provision Selected Workspace")
    if not window.force_shutdown_daemon_status_label.text().strip():
        window.force_shutdown_daemon_status_label.setText(
            "Use only when normal stop does not return control."
        )
    if not window.reset_workspace_status_label.text().strip():
        window.reset_workspace_status_label.setText(
            "Reset deletes local runtime ledgers, daemon logs, and shared workspace state for the selected workspace."
        )
    window.visibility_interpreter_value.setText(str(interpreter_path))
    window.visibility_interpreter_mode_value.setText(interpreter_mode.title())
    window.workspace_counts_footer_label.setText(_workspace_counts_footer_text(window))


def finish_control_action(window: "DataEngineWindow", action_name: str, payload: object) -> None:
    if action_name not in {"provision_workspace", "force_shutdown_daemon", "reset_workspace"}:
        return
    window._pending_control_actions.discard(action_name)
    refresh_workspace_visibility_panel(window)
    if window.ui_closing:
        return
    assert isinstance(payload, dict)
    error_text = payload.get("error_text")
    if isinstance(error_text, str) and error_text.strip():
        if action_name == "provision_workspace":
            window.workspace_provision_status_label.setText(f"Provisioning failed: {error_text}")
        elif action_name == "force_shutdown_daemon":
            window.force_shutdown_daemon_status_label.setText(f"Force stop failed: {error_text}")
        else:
            window.reset_workspace_status_label.setText(f"Workspace reset failed: {error_text}")
        window._show_message_box_later(
            title=APP_DISPLAY_NAME,
            text=error_text,
            tone="error",
        )
        return
    if action_name == "provision_workspace":
        workspace_id = payload.get("workspace_id")
        created_names = payload.get("created_names")
        workspace_name = payload.get("workspace_name")
        if isinstance(workspace_id, str):
            window._rebind_workspace_context(workspace_id=workspace_id)
        if isinstance(created_names, str) and isinstance(workspace_name, str):
            window.workspace_provision_status_label.setText(
                f"Provisioned {workspace_name}: created {created_names}."
            )
        return
    if action_name == "force_shutdown_daemon":
        window.force_shutdown_daemon_status_label.setText(
            "Local daemon force-stopped. Any active engine or manual runs were terminated."
        )
        window._sync_from_daemon()
        return
    workspace_id = payload.get("workspace_id")
    window.reset_workspace_status_label.setText(
        "Workspace runtime state reset. Local ledgers, daemon log, and shared workspace state were cleared."
    )
    if isinstance(workspace_id, str):
        window._rebind_workspace_context(workspace_id=workspace_id)


def _emit_workspace_settings_action(window: "DataEngineWindow", action_name: str, payload: dict[str, object]) -> None:
    if window.ui_closing:
        return
    try:
        window.signals.control_action_finished.emit(action_name, payload)
    except RuntimeError:
        pass


def _provision_selected_workspace_worker(window: "DataEngineWindow") -> None:
    _emit_workspace_settings_action(
        window,
        "provision_workspace",
        window.command_service.provision_workspace(
            window.workspace_paths,
            interpreter_path=Path(sys.executable).expanduser(),
        ).__dict__,
    )


def _force_shutdown_daemon_worker(window: "DataEngineWindow") -> None:
    result = window.command_service.force_shutdown_daemon(window.workspace_paths, timeout=0.5)
    _emit_workspace_settings_action(
        window,
        "force_shutdown_daemon",
        result.__dict__,
    )


def _reset_workspace_worker(window: "DataEngineWindow") -> None:
    result = window.command_service.reset_workspace(
        paths=window.workspace_paths,
        runtime_cache_ledger=window.runtime_binding.runtime_cache_ledger,
        runtime_control_ledger=window.runtime_binding.runtime_control_ledger,
    )
    _emit_workspace_settings_action(
        window,
        "reset_workspace",
        result.__dict__,
    )


def _workspace_module_count(flow_modules_dir: Path) -> int:
    if not flow_modules_dir.is_dir():
        return 0
    return sum(
        1
        for path in flow_modules_dir.iterdir()
        if path.is_file() and path.suffix in {".py", ".ipynb"} and path.name != "__init__.py"
    )


def _workspace_counts_footer_text(window: "DataEngineWindow") -> str:
    cards = tuple(window.flow_cards.values())
    module_count = _workspace_module_count(window.workspace_paths.flow_modules_dir)
    group_count = len({card.group for card in cards})
    flow_count = len(cards)
    recent_runs_count = _recent_workspace_run_count(window, days=7)
    return f"{module_count} modules - {group_count} groups - {flow_count} flows - {recent_runs_count} runs last 7 days"


def _recent_workspace_run_count(window: "DataEngineWindow", *, days: int) -> int:
    return window.runtime_binding_service.recent_run_count(window.runtime_binding, days=days)


__all__ = [
    "browse_workspace_collection_root_override",
    "finish_control_action",
    "force_shutdown_daemon",
    "provision_selected_workspace",
    "reset_workspace",
    "refresh_workspace_provisioning_controls",
    "refresh_workspace_visibility_panel",
    "refresh_workspace_root_controls",
    "reset_workspace_collection_root_override",
    "save_workspace_collection_root_override",
]
