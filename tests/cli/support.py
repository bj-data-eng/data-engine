from __future__ import annotations

from data_engine.platform.interpreters import console_python_executable
import sys


def expected_vscode_interpreter_path() -> str:
    candidate = console_python_executable(sys.executable)
    try:
        return str(candidate.resolve())
    except Exception:
        return str(candidate)
