"""Surface-launch command helpers for the CLI surface."""

from __future__ import annotations

import os
from pathlib import Path, WindowsPath
import subprocess
import sys
import tempfile
import time

from data_engine.authoring.model import FlowValidationError
from data_engine.platform.workspace_models import path_display


def start_surface(surface: str) -> int:
    """Launch one operator surface."""
    if surface == "gui":
        return start_gui_subprocess()
    if surface == "tui":
        return launch_terminal_ui()
    raise FlowValidationError(f"Unknown surface: {surface}")


def preferred_gui_python_executable() -> Path:
    """Return the preferred Python executable for detached GUI launches."""
    # Preserve the active interpreter path instead of resolving symlinks.
    # On macOS, resolving a venv python can collapse back to the base framework
    # interpreter, which loses the installed package context for child launches.
    executable = _host_concrete_path(sys.executable).expanduser()
    if os.name == "nt":
        candidate = executable.with_name("pythonw.exe")
        return candidate if candidate.exists() else executable
    if sys.platform == "darwin":
        try:
            candidate = executable.with_name("pythonw")
        except Exception:
            return executable
        return candidate if candidate.exists() else executable
    return executable


def _host_concrete_path(value: str) -> Path:
    """Build a host-supported concrete path even if os.name is monkeypatched."""
    if "\\" in value or (len(value) >= 2 and value[1] == ":"):
        return WindowsPath(value)
    return Path(value)


def start_gui_subprocess() -> int:
    """Spawn the desktop GUI in a detached process."""
    command = [path_display(preferred_gui_python_executable(), empty=""), "-m", "data_engine.ui.gui.launcher"]
    log_fd, log_path_text = tempfile.mkstemp(prefix="data-engine-gui-start-", suffix=".log")
    os.close(log_fd)
    log_path = Path(log_path_text)
    with log_path.open("w", encoding="utf-8") as startup_log:
        kwargs: dict[str, object] = {
            "cwd": str(Path.cwd()),
            "env": dict(os.environ),
            "stdin": subprocess.DEVNULL,
            "stdout": startup_log,
            "stderr": startup_log,
        }
        if os.name == "nt":
            creationflags = 0
            detached_process = getattr(subprocess, "DETACHED_PROCESS", 0)
            create_new_process_group = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            creationflags = detached_process | create_new_process_group
            if creationflags:
                kwargs["creationflags"] = creationflags
        else:
            kwargs["start_new_session"] = True
        process = subprocess.Popen(command, **kwargs)
    time.sleep(0.3)
    exit_code = process.poll()
    if exit_code is not None:
        startup_output = log_path.read_text(encoding="utf-8").strip()
        print(
            "Data Engine GUI exited during startup."
            f" See startup log: {log_path}"
            + (f"\n{startup_output}" if startup_output else ""),
            file=sys.stderr,
        )
        return exit_code or 1
    print("Started Data Engine GUI.")
    return 0


def launch_desktop_ui(*, theme_name: str | None = None) -> int:
    """Launch the PySide desktop UI in the current process."""
    from data_engine.ui.gui.launcher import launch

    launch(theme_name=theme_name or "light")
    return 0


def launch_terminal_ui() -> int:
    """Launch the Textual terminal UI in the current process."""
    from data_engine.ui.tui.app import main as tui_main

    tui_main()
    return 0


__all__ = [
    "launch_desktop_ui",
    "launch_terminal_ui",
    "preferred_gui_python_executable",
    "start_gui_subprocess",
    "start_surface",
]
