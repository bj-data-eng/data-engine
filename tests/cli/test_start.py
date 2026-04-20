from __future__ import annotations

from pathlib import Path

from data_engine.ui.cli.app import main


def test_cli_start_gui_spawns_detached_surface_process(monkeypatch):
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

    result = main(["start", "gui"])

    assert result == 0
    assert launched[0][0] == ["/tmp/pythonw", "-m", "data_engine.ui.gui.launcher"]


def test_cli_run_gui_spawns_detached_surface_process(monkeypatch):
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

    result = main(["run", "gui"])

    assert result == 0
    assert launched[0][0] == ["/tmp/pythonw", "-m", "data_engine.ui.gui.launcher"]


def test_cli_start_gui_reports_immediate_startup_failure(monkeypatch, capsys):
    monkeypatch.setattr("data_engine.ui.cli.commands_start.preferred_gui_python_executable", lambda: Path("/tmp/pythonw"))
    monkeypatch.setattr("data_engine.ui.cli.commands_start.time.sleep", lambda _: None)

    class _FailedProcess:
        def poll(self):
            return 2

    monkeypatch.setattr("data_engine.ui.cli.commands_start.subprocess.Popen", lambda command, **kwargs: _FailedProcess())

    result = main(["start", "gui"])

    assert result == 2
    assert "exited during startup" in capsys.readouterr().err


def test_cli_start_tui_launches_terminal_surface(monkeypatch):
    launched: list[str] = []
    monkeypatch.setattr("data_engine.ui.cli.commands_start.launch_terminal_ui", lambda: launched.append("tui") or 0)

    result = main(["start", "tui"])

    assert result == 0
    assert launched == ["tui"]


def test_cli_run_tui_launches_terminal_surface(monkeypatch):
    launched: list[str] = []
    monkeypatch.setattr("data_engine.ui.cli.commands_start.launch_terminal_ui", lambda: launched.append("tui") or 0)

    result = main(["run", "tui"])

    assert result == 0
    assert launched == ["tui"]


def test_preferred_gui_python_executable_preserves_venv_python_on_macos(monkeypatch, tmp_path):
    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True)
    venv_python.write_text("", encoding="utf-8")
    monkeypatch.setattr("data_engine.ui.cli.commands_start.os.name", "posix")
    monkeypatch.setattr("data_engine.ui.cli.commands_start.sys.platform", "darwin")
    monkeypatch.setattr("data_engine.ui.cli.commands_start.sys.executable", str(venv_python))

    from data_engine.ui.cli.commands_start import preferred_gui_python_executable

    assert preferred_gui_python_executable() == venv_python
