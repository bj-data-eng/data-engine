from __future__ import annotations

from pathlib import Path

from data_engine.ui.cli.app import main


def test_cli_start_egui_spawns_detached_surface_process(monkeypatch):
    launched: list[tuple[list[str], dict[str, object]]] = []

    monkeypatch.setattr("data_engine.ui.cli.commands_start.preferred_gui_python_executable", lambda: Path("/tmp/pythonw"))
    monkeypatch.setattr("data_engine.ui.cli.commands_start.time.sleep", lambda _: None)

    class _RunningProcess:
        def poll(self):
            return None

    monkeypatch.setattr(
        "data_engine.ui.cli.commands_start.subprocess.Popen",
        lambda command, **kwargs: launched.append((command, kwargs)) or _RunningProcess(),
    )

    result = main(["start", "egui"])

    assert result == 0
    assert launched[0][0] == ["/tmp/pythonw", "-m", "data_engine.ui.egui.launcher"]


def test_cli_start_egui_reports_immediate_startup_failure(monkeypatch, capsys):
    monkeypatch.setattr("data_engine.ui.cli.commands_start.preferred_gui_python_executable", lambda: Path("/tmp/pythonw"))
    monkeypatch.setattr("data_engine.ui.cli.commands_start.time.sleep", lambda _: None)

    class _FailedProcess:
        def poll(self):
            return 2

    monkeypatch.setattr("data_engine.ui.cli.commands_start.subprocess.Popen", lambda command, **kwargs: _FailedProcess())

    result = main(["start", "egui"])

    assert result == 2
    assert "egui surface exited during startup" in capsys.readouterr().err
