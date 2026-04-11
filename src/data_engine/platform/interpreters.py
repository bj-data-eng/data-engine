"""Helpers for selecting host Python executables."""

from __future__ import annotations

import os
from pathlib import Path, WindowsPath
import sys


def host_concrete_path(value: str | Path) -> Path:
    """Build a host-supported concrete path even if os.name is monkeypatched."""
    text = str(value)
    if "\\" in text or (len(text) >= 2 and text[1] == ":"):
        return WindowsPath(text)
    return Path(text)


def console_python_executable(executable: str | Path | None = None) -> Path:
    """Return a console-capable Python executable for the active environment."""
    candidate = host_concrete_path(executable or sys.executable).expanduser()
    if candidate.name.lower() == "pythonw.exe":
        sibling = candidate.with_name("python.exe")
        if sibling.exists():
            candidate = sibling
    return candidate


def preferred_gui_python_executable(executable: str | Path | None = None) -> Path:
    """Return the preferred Python executable for detached GUI launches."""
    # Preserve the active interpreter path instead of resolving symlinks.
    # On macOS, resolving a venv python can collapse back to the base framework
    # interpreter, which loses the installed package context for child launches.
    candidate = host_concrete_path(executable or sys.executable).expanduser()
    if os.name == "nt":
        pythonw = candidate.with_name("pythonw.exe")
        return pythonw if pythonw.exists() else candidate
    if sys.platform == "darwin":
        try:
            pythonw = candidate.with_name("pythonw")
        except Exception:
            return candidate
        return pythonw if pythonw.exists() else candidate
    return candidate


__all__ = [
    "console_python_executable",
    "host_concrete_path",
    "preferred_gui_python_executable",
]
