from __future__ import annotations

import os
import subprocess
import sys

from data_engine.platform.workspace_models import DATA_ENGINE_APP_ROOT_ENV_VAR
from data_engine.ui.cli.app import main


def test_cli_run_tests_lists_named_slices(capsys):
    result = main(["run", "tests", "--list-slices"])

    assert result == 0
    assert capsys.readouterr().out.splitlines() == ["all", "unit", "ui", "qt", "tui", "integration", "live"]


def test_cli_run_tests_executes_named_slice(monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    (app_root / "tests").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    recorded: list[tuple[list[str], dict[str, object]]] = []
    monkeypatch.setattr(
        "data_engine.ui.cli.commands_run.subprocess.run",
        lambda command, **kwargs: recorded.append((command, kwargs)) or type("Completed", (), {"returncode": 0})(),
    )

    result = main(["run", "tests", "qt"])

    assert result == 0
    assert recorded[0][0][:4] == [sys.executable, "-m", "pytest", "-q"]
    assert str(app_root / "tests" / "gui" / "qt") in recorded[0][0]
    if os.name == "nt":
        assert recorded[0][1]["creationflags"] == getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:
        assert "creationflags" not in recorded[0][1]


def test_cli_run_tests_defaults_to_unit_slice(monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    (app_root / "tests").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    recorded: list[tuple[list[str], dict[str, object]]] = []
    monkeypatch.setattr(
        "data_engine.ui.cli.commands_run.subprocess.run",
        lambda command, **kwargs: recorded.append((command, kwargs)) or type("Completed", (), {"returncode": 0})(),
    )

    result = main(["run", "tests"])

    assert result == 0
    assert any("--ignore=" in arg for arg in recorded[0][0])
