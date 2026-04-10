"""Workspace-folder settings presentation helpers for the desktop UI."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import sys
from typing import TYPE_CHECKING

from PySide6.QtWidgets import QFileDialog

from data_engine.domain.time import parse_utc_text

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
    if not window.workspace_paths.workspace_configured:
        window._show_message_box(
            title="Workspace Folder Required",
            text="Choose a workspace folder before provisioning a workspace.",
            tone="error",
        )
        return
    try:
        result = window.workspace_provisioning_service.provision_workspace(
            window.workspace_paths,
            interpreter_path=Path(sys.executable).expanduser(),
        )
    except Exception as exc:
        window.workspace_provision_status_label.setText(f"Provisioning failed: {exc}")
        window._show_message_box(
            title="Provisioning Failed",
            text=str(exc),
            tone="error",
        )
        return
    created_names = ", ".join(path.name for path in result.created_paths) if result.created_paths else "nothing new"
    window._rebind_workspace_context(workspace_id=window.workspace_paths.workspace_id)
    window.workspace_provision_status_label.setText(
        f"Provisioned {result.workspace_root.name}: created {created_names}."
    )


def force_shutdown_daemon(window: "DataEngineWindow") -> None:
    result = window.runtime_application.force_shutdown_daemon(window.workspace_paths, timeout=0.5)
    if not result.ok:
        window.force_shutdown_daemon_status_label.setText(f"Force stop failed: {result.error}")
        window._show_message_box(
            title="Force Stop Failed",
            text=result.error,
            tone="error",
        )
        return
    window.force_shutdown_daemon_status_label.setText(
        "Local daemon force-stopped. Any active engine or manual runs were terminated."
    )
    window._sync_from_daemon()


def refresh_workspace_provisioning_controls(window: "DataEngineWindow") -> None:
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
    window.provision_workspace_button.setEnabled(True)
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
    window.force_shutdown_daemon_button.setEnabled(window.workspace_paths.workspace_configured)
    if not window.force_shutdown_daemon_status_label.text().strip():
        window.force_shutdown_daemon_status_label.setText(
            "Use only when normal stop does not return control."
        )
    window.visibility_interpreter_value.setText(str(interpreter_path))
    window.visibility_interpreter_mode_value.setText(interpreter_mode.title())
    window.workspace_counts_footer_label.setText(_workspace_counts_footer_text(window))


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
    recent_runs_count = _recent_workspace_run_count(window, days=30)
    return f"{module_count} modules - {group_count} groups - {flow_count} flows - {recent_runs_count} runs last 30 days"


def _recent_workspace_run_count(window: "DataEngineWindow", *, days: int) -> int:
    cutoff = datetime.now(UTC) - timedelta(days=days)
    count = 0
    try:
        runs = window.runtime_binding.runtime_ledger.list_runs()
    except Exception:
        return 0
    for run in runs:
        started_at = parse_utc_text(run.started_at_utc)
        if started_at is not None and started_at >= cutoff:
            count += 1
    return count


__all__ = [
    "browse_workspace_collection_root_override",
    "force_shutdown_daemon",
    "provision_selected_workspace",
    "refresh_workspace_provisioning_controls",
    "refresh_workspace_visibility_panel",
    "refresh_workspace_root_controls",
    "reset_workspace_collection_root_override",
    "save_workspace_collection_root_override",
]
