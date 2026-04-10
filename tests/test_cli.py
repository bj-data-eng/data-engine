from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

from data_engine.domain import ProcessInfo
from data_engine.ui.cli import commands_doctor
from data_engine.ui.cli.app import CliDependencies, main
from data_engine.ui.cli.commands_workspace import workspace_vscode_settings as _workspace_vscode_settings
from data_engine.ui.cli.dependencies import CliDependencyFactories, build_default_cli_dependencies
from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.workspace_models import (
    DATA_ENGINE_APP_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ID_ENV_VAR,
    WORKSPACE_FLOW_HELPERS_DIR_NAME,
)
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


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


def test_preferred_gui_python_executable_preserves_venv_python_on_macos(monkeypatch, tmp_path):
    venv_python = tmp_path / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True)
    venv_python.write_text("", encoding="utf-8")
    monkeypatch.setattr("data_engine.ui.cli.commands_start.os.name", "posix")
    monkeypatch.setattr("data_engine.ui.cli.commands_start.sys.platform", "darwin")
    monkeypatch.setattr("data_engine.ui.cli.commands_start.sys.executable", str(venv_python))

    from data_engine.ui.cli.commands_start import preferred_gui_python_executable

    assert preferred_gui_python_executable() == venv_python


def test_cli_create_workspace_scaffolds_directories_vscode_and_default_selection(monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    (app_root / "config").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_ROOT", raising=False)

    workspace_root = tmp_path / "shared_workspaces" / "claims"

    result = main(["create", "workspace", str(workspace_root)])

    assert result == 0
    assert (workspace_root / "flow_modules").is_dir()
    assert (workspace_root / "flow_modules" / WORKSPACE_FLOW_HELPERS_DIR_NAME).is_dir()
    assert (workspace_root / "config").is_dir()
    assert (workspace_root / "databases").is_dir()
    collection_vscode_settings = json.loads((workspace_root.parent / ".vscode" / "settings.json").read_text(encoding="utf-8"))
    vscode_settings = json.loads((workspace_root / ".vscode" / "settings.json").read_text(encoding="utf-8"))
    assert collection_vscode_settings["terminal.integrated.env.osx"]["DATA_ENGINE_WORKSPACE_COLLECTION_ROOT"] == str(
        workspace_root.parent.resolve()
    )
    assert collection_vscode_settings["terminal.integrated.env.windows"]["DATA_ENGINE_WORKSPACE_COLLECTION_ROOT"] == str(
        workspace_root.parent.resolve()
    )
    assert vscode_settings["terminal.integrated.env.osx"]["DATA_ENGINE_WORKSPACE_ID"] == "claims"
    assert vscode_settings["terminal.integrated.env.windows"]["DATA_ENGINE_WORKSPACE_ID"] == "claims"
    assert vscode_settings["python.defaultInterpreterPath"] == sys.executable
    store = LocalSettingsStore.open_default(app_root=app_root)
    assert store.default_workspace_id() == "claims"
    assert store.workspace_collection_root() == workspace_root.parent.resolve()


def test_cli_create_workspace_rejects_non_empty_target(monkeypatch, tmp_path, capsys):
    app_root = tmp_path / "data_engine"
    (app_root / "config").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    target = tmp_path / "shared_workspaces" / "claims"
    target.mkdir(parents=True)
    (target / "existing.txt").write_text("busy\n", encoding="utf-8")

    result = main(["create", "workspace", str(target)])

    assert result == 2
    assert "non-empty directory" in capsys.readouterr().err


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
    assert str(app_root / "tests" / "test_qt_ui.py") in recorded[0][0]
    if os.name == "nt":
        assert recorded[0][1]["creationflags"] == getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)


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


def test_cli_doctor_reports_workspace_health(monkeypatch, tmp_path, capsys):
    app_root = tmp_path / "data_engine"
    (app_root / "config").mkdir(parents=True)
    workspace_root = tmp_path / "shared_workspaces" / "claims"
    (workspace_root / "flow_modules").mkdir(parents=True)
    (workspace_root / ".vscode").mkdir(parents=True)
    (workspace_root / ".vscode" / "settings.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.setenv(DATA_ENGINE_WORKSPACE_ID_ENV_VAR, "claims")
    store = LocalSettingsStore.open_default(app_root=app_root)
    store.set_default_workspace_id("claims")
    store.set_workspace_collection_root(workspace_root.parent)

    result = main(["doctor"])

    assert result == 0
    output = capsys.readouterr().out
    assert "[OK] python executable:" in output
    assert "[WARN] runtime root:" in output
    assert "[OK] authored workspace ready:" in output


def test_cli_doctor_preserves_launch_python_path(monkeypatch, tmp_path, capsys):
    app_root = tmp_path / "data_engine"
    (app_root / "config").mkdir(parents=True)
    workspace_root = tmp_path / "shared_workspaces" / "claims"
    (workspace_root / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.setenv(DATA_ENGINE_WORKSPACE_ID_ENV_VAR, "claims")
    monkeypatch.setattr("data_engine.ui.cli.commands_doctor.sys.executable", r"C:\venv\Scripts\python.exe")
    store = LocalSettingsStore.open_default(app_root=app_root)
    store.set_default_workspace_id("claims")
    store.set_workspace_collection_root(workspace_root.parent)

    result = main(["doctor"])

    assert result == 0
    output = capsys.readouterr().out
    assert "[OK] python executable: C:/venv/Scripts/python.exe" in output


def test_cli_doctor_daemons_reports_filtered_process_and_lease_state(monkeypatch, tmp_path, capsys):
    app_root = tmp_path / "data_engine"
    (app_root / "config").mkdir(parents=True)
    workspace_root = tmp_path / "shared_workspaces" / "claims"
    (workspace_root / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.setenv(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR, str(workspace_root.parent))
    store = LocalSettingsStore.open_default(app_root=app_root)
    store.set_default_workspace_id("claims")
    store.set_workspace_collection_root(workspace_root.parent)
    settings = type(
        "_Settings",
        (),
        {
            "app_root": app_root,
            "settings_path": app_root.parent / "app_local" / "data_engine" / "settings" / "app_settings.sqlite",
            "state_root": app_root.parent / "app_local" / "data_engine",
            "runtime_root": app_root.parent / "app_local" / "data_engine" / "artifacts",
            "workspace_collection_root": workspace_root.parent,
        },
    )()

    class _AppStatePolicy:
        def load_settings(self):
            return settings

    class _WorkspaceService:
        def discover(self, **kwargs):
            del kwargs
            return (type("DW", (), {"workspace_id": "claims", "workspace_root": workspace_root})(),)

        def resolve_paths(self, **kwargs):
            del kwargs
            return RuntimeLayoutPolicy().resolve_paths(workspace_root=workspace_root, workspace_id="claims")

    class _SharedStateService:
        def read_lease_metadata(self, paths):
            del paths
            return {"machine_id": "test-host", "pid": 111, "last_checkpoint_at_utc": "2999-01-01T00:00:00+00:00"}

        def lease_is_stale(self, paths, *, stale_after_seconds):
            del paths, stale_after_seconds
            return False

    monkeypatch.setattr(
        "data_engine.ui.cli.app._run_process_listing",
        lambda: [
            ProcessInfo(pid=111, ppid=1, status="Ss", command="python -m data_engine.hosts.daemon.app --workspace /tmp/shared/claims"),
            ProcessInfo(pid=222, ppid=111, status="S+", command="python -m data_engine.ui.gui.launcher"),
            ProcessInfo(pid=333, ppid=222, status="Z", command="python -m data_engine.hosts.daemon.app --workspace /tmp/shared/claims"),
            ProcessInfo(pid=444, ppid=1, status="S", command="python something_else.py"),
        ],
    )
    monkeypatch.setattr("data_engine.ui.cli.app.machine_id_text", lambda: "test-host")

    result = main(
        ["doctor", "daemons"],
        dependencies=CliDependencies(
            app_state_policy=_AppStatePolicy(),
            shared_state_service=_SharedStateService(),
            workspace_service=_WorkspaceService(),
        ),
    )

    assert result == 0
    output = capsys.readouterr().out
    expected_live = 2 if os.name == "nt" else 1
    expected_defunct = 0 if os.name == "nt" else 1
    assert f"Live daemons: {expected_live}" in output
    assert f"Defunct daemons: {expected_defunct}" in output
    assert "Related UI processes: 1" in output
    assert "claims: lease_pid=111 state=live local" in output


def test_run_process_listing_uses_windows_powershell_json(monkeypatch):
    recorded: list[tuple[list[str], dict[str, object]]] = []

    class _Completed:
        returncode = 0
        stdout = json.dumps(
            [
                {
                    "ProcessId": 111,
                    "ParentProcessId": 1,
                    "CommandLine": "python -m data_engine.hosts.daemon.app --workspace C:\\shared\\claims",
                },
                {
                    "ProcessId": 222,
                    "ParentProcessId": 111,
                    "CommandLine": "python -m data_engine.ui.gui.launcher",
                },
            ]
        )

    monkeypatch.setattr("data_engine.ui.cli.commands_doctor.os.name", "nt")
    monkeypatch.setattr(
        "data_engine.ui.cli.commands_doctor.subprocess.run",
        lambda command, **kwargs: recorded.append((command, kwargs)) or _Completed(),
    )

    rows = commands_doctor.run_process_listing()

    assert recorded[0][0][:5] == [
        "powershell.exe",
        "-NoLogo",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
    ]
    assert rows == [
        ProcessInfo(
            pid=111,
            ppid=1,
            status="Running",
            command="python -m data_engine.hosts.daemon.app --workspace C:\\shared\\claims",
        ),
        ProcessInfo(
            pid=222,
            ppid=111,
            status="Running",
            command="python -m data_engine.ui.gui.launcher",
        ),
    ]


def test_cli_doctor_daemons_treats_windows_status_as_non_defunct(monkeypatch, tmp_path, capsys):
    app_root = tmp_path / "data_engine"
    (app_root / "config").mkdir(parents=True)
    workspace_root = tmp_path / "shared_workspaces" / "claims"
    (workspace_root / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.setenv(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR, str(workspace_root.parent))
    store = LocalSettingsStore.open_default(app_root=app_root)
    store.set_default_workspace_id("claims")
    store.set_workspace_collection_root(workspace_root.parent)
    settings = type(
        "_Settings",
        (object,),
        {
            "app_root": app_root,
            "settings_path": app_root.parent / "app_local" / "data_engine" / "settings" / "app_settings.sqlite",
            "state_root": app_root.parent / "app_local" / "data_engine",
            "runtime_root": app_root.parent / "app_local" / "data_engine" / "artifacts",
            "workspace_collection_root": workspace_root.parent,
        },
    )()

    class _WorkspaceService:
        def discover(self, **kwargs):
            del kwargs
            return (type("DW", (), {"workspace_id": "claims", "workspace_root": workspace_root})(),)

        def resolve_paths(self, **kwargs):
            del kwargs
            return RuntimeLayoutPolicy().resolve_paths(workspace_root=workspace_root, workspace_id="claims")

    class _SharedStateService:
        def read_lease_metadata(self, paths):
            del paths
            return {"machine_id": "test-host", "pid": 111, "last_checkpoint_at_utc": "2999-01-01T00:00:00+00:00"}

        def lease_is_stale(self, paths, *, stale_after_seconds):
            del paths, stale_after_seconds
            return False

    monkeypatch.setattr("data_engine.domain.diagnostics.os.name", "nt")

    result = commands_doctor.doctor_daemons(
        settings=settings,
        workspace_service=_WorkspaceService(),
        process_rows=[
            ProcessInfo(
                pid=111,
                ppid=1,
                status="Z",
                command="python -m data_engine.hosts.daemon.app --workspace C:\\shared\\claims",
            ),
            ProcessInfo(pid=222, ppid=111, status="Running", command="python -m data_engine.ui.gui.launcher"),
        ],
        read_lease_metadata_func=_SharedStateService().read_lease_metadata,
        lease_is_stale_func=_SharedStateService().lease_is_stale,
        machine_id_text_func=lambda: "test-host",
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "Live daemons: 1" in output
    assert "Defunct daemons: 0" in output
    assert "orphaned" not in output
    assert "claims: lease_pid=111 state=live local" in output


def test_cli_doctor_daemons_collapses_windows_launcher_parent_processes(monkeypatch, tmp_path, capsys):
    app_root = tmp_path / "data_engine"
    (app_root / "config").mkdir(parents=True)
    workspace_root = tmp_path / "shared_workspaces" / "claims"
    (workspace_root / "flow_modules").mkdir(parents=True)
    settings = type(
        "_Settings",
        (object,),
        {
            "app_root": app_root,
            "settings_path": app_root.parent / "app_local" / "data_engine" / "settings" / "app_settings.sqlite",
            "state_root": app_root.parent / "app_local" / "data_engine",
            "runtime_root": app_root.parent / "app_local" / "data_engine" / "artifacts",
            "workspace_collection_root": workspace_root.parent,
        },
    )()

    class _WorkspaceService:
        def discover(self, **kwargs):
            del kwargs
            return (type("DW", (), {"workspace_id": "claims", "workspace_root": workspace_root})(),)

        def resolve_paths(self, **kwargs):
            del kwargs
            return RuntimeLayoutPolicy().resolve_paths(workspace_root=workspace_root, workspace_id="claims")

    class _SharedStateService:
        def read_lease_metadata(self, paths):
            del paths
            return {"machine_id": "test-host", "pid": 320, "last_checkpoint_at_utc": "2999-01-01T00:00:00+00:00"}

        def lease_is_stale(self, paths, *, stale_after_seconds):
            del paths, stale_after_seconds
            return False

    monkeypatch.setattr("data_engine.ui.cli.commands_doctor.os.name", "nt")
    monkeypatch.setattr("data_engine.domain.diagnostics.os.name", "nt")

    result = commands_doctor.doctor_daemons(
        settings=settings,
        workspace_service=_WorkspaceService(),
        process_rows=[
            ProcessInfo(
                pid=20280,
                ppid=22240,
                status="Running",
                command="C:\\repo\\.venv\\Scripts\\pythonw.exe -m data_engine.hosts.daemon.app --workspace C:\\repo\\workspaces\\claims",
            ),
            ProcessInfo(
                pid=320,
                ppid=20280,
                status="Running",
                command="C:\\repo\\.venv\\Scripts\\pythonw.exe -m data_engine.hosts.daemon.app --workspace C:\\repo\\workspaces\\claims",
            ),
            ProcessInfo(
                pid=20288,
                ppid=11988,
                status="Running",
                command="C:\\repo\\.venv\\Scripts\\pythonw.exe -m data_engine.ui.gui.launcher",
            ),
            ProcessInfo(
                pid=17436,
                ppid=20288,
                status="Running",
                command="C:\\repo\\.venv\\Scripts\\pythonw.exe -m data_engine.ui.gui.launcher",
            ),
        ],
        read_lease_metadata_func=_SharedStateService().read_lease_metadata,
        lease_is_stale_func=_SharedStateService().lease_is_stale,
        machine_id_text_func=lambda: "test-host",
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "Live daemons: 1" in output
    assert "Related UI processes: 1" in output
    assert "daemon pid=320 ppid=20280 status=Running" in output
    assert "gui pid=17436 ppid=20288 status=Running" in output


def test_cli_main_accepts_injected_dependencies_for_doctor(capsys, tmp_path):
    app_root = tmp_path / "data_engine"
    app_local_root = tmp_path / "app_local" / "data_engine"
    workspace_root = tmp_path / "shared_workspaces" / "claims"
    (app_root / "tests").mkdir(parents=True)
    workspace_root.mkdir(parents=True)

    class _AppStatePolicy:
        def load_settings(self):
            return type(
                "_Settings",
                (),
                {
                    "app_root": app_root,
                    "settings_path": app_local_root / "settings" / "app_settings.sqlite",
                    "state_root": app_local_root,
                    "runtime_root": app_local_root / "artifacts",
                    "workspace_collection_root": workspace_root.parent,
                },
            )()

    class _WorkspaceService:
        def resolve_paths(self, **kwargs):
            del kwargs
            return type(
                "_Paths",
                (),
                {
                    "workspace_root": workspace_root,
                    "flow_modules_dir": workspace_root / "flow_modules",
                    "artifacts_dir": app_root / "artifacts",
                    "workspace_configured": True,
                },
            )()

        def discover(self, **kwargs):
            del kwargs
            return ()

    class _SharedStateService:
        def read_lease_metadata(self, paths):
            del paths
            return None

        def lease_is_stale(self, paths, *, stale_after_seconds):
            del paths, stale_after_seconds
            return False

    result = main(
        ["doctor"],
        dependencies=CliDependencies(
            app_state_policy=_AppStatePolicy(),
            shared_state_service=_SharedStateService(),
            workspace_service=_WorkspaceService(),
        ),
    )

    assert result == 0
    output = capsys.readouterr().out
    assert f"app root: {str(app_root).replace('\\', '/')}" in output


def test_workspace_vscode_settings_only_adds_checkout_specific_entries_when_present(tmp_path, monkeypatch):
    app_root = tmp_path / "installed_app"
    workspace_root = tmp_path / "workspaces" / "claims"

    settings = _workspace_vscode_settings(workspace_root, app_root=app_root)

    assert settings["python.defaultInterpreterPath"] == sys.executable
    assert settings["terminal.integrated.env.windows"]["DATA_ENGINE_WORKSPACE_ROOT"] == str(workspace_root)
    assert "python.analysis.extraPaths" not in settings
    assert "python.testing.pytestEnabled" not in settings
    assert "python.testing.pytestArgs" not in settings


def test_build_default_cli_dependencies_uses_factory_bundle():
    calls: list[str] = []

    class _Policy:
        pass

    class _SharedStateService:
        pass

    class _WorkspaceService:
        pass

    dependencies = build_default_cli_dependencies(
        factories=CliDependencyFactories(
            app_state_policy_factory=lambda: calls.append("policy") or _Policy(),
            shared_state_service_factory=lambda: calls.append("shared-state") or _SharedStateService(),
            workspace_service_factory=lambda: calls.append("workspace") or _WorkspaceService(),
        )
    )

    assert isinstance(dependencies.app_state_policy, _Policy)
    assert isinstance(dependencies.shared_state_service, _SharedStateService)
    assert isinstance(dependencies.workspace_service, _WorkspaceService)
    assert calls == ["policy", "shared-state", "workspace"]
