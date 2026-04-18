from __future__ import annotations

import json
import os

from data_engine.domain import ProcessInfo
from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.workspace_models import (
    DATA_ENGINE_APP_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ID_ENV_VAR,
)
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.ui.cli import commands_doctor
from data_engine.ui.cli.app import CliDependencies, main


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

    monkeypatch.setattr("data_engine.platform.processes.os.name", "nt")
    monkeypatch.setattr(
        "data_engine.platform.processes.subprocess.run",
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

    monkeypatch.setattr("data_engine.platform.processes.os.name", "nt")
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

