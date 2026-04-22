from __future__ import annotations

import json

from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.workspace_models import (
    DATA_ENGINE_APP_ROOT_ENV_VAR,
    WORKSPACE_FLOW_HELPERS_DIR_NAME,
)
from data_engine.ui.cli.app import main
from data_engine.ui.cli.commands_workspace import workspace_vscode_settings as _workspace_vscode_settings

from tests.cli.support import expected_vscode_interpreter_path


def test_cli_create_workspace_scaffolds_directories_vscode_and_default_selection(monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    (app_root / "config").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.delenv("DATA_ENGINE_WORKSPACE_ROOT", raising=False)

    workspace_root = tmp_path / "shared_workspaces" / "docs"

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
    assert vscode_settings["terminal.integrated.env.osx"]["DATA_ENGINE_WORKSPACE_ID"] == "docs"
    assert vscode_settings["terminal.integrated.env.windows"]["DATA_ENGINE_WORKSPACE_ID"] == "docs"
    assert vscode_settings["python.defaultInterpreterPath"] == expected_vscode_interpreter_path()
    store = LocalSettingsStore.open_default(app_root=app_root)
    assert store.default_workspace_id() == "docs"
    assert store.workspace_collection_root() == workspace_root.parent.resolve()


def test_cli_create_workspace_rejects_non_empty_target(monkeypatch, tmp_path, capsys):
    app_root = tmp_path / "data_engine"
    (app_root / "config").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    target = tmp_path / "shared_workspaces" / "docs"
    target.mkdir(parents=True)
    (target / "existing.txt").write_text("busy\n", encoding="utf-8")

    result = main(["create", "workspace", str(target)])

    assert result == 2
    assert "non-empty directory" in capsys.readouterr().err


def test_workspace_vscode_settings_only_adds_checkout_specific_entries_when_present(tmp_path, monkeypatch):
    app_root = tmp_path / "installed_app"
    workspace_root = tmp_path / "workspaces" / "docs"

    settings = _workspace_vscode_settings(workspace_root, app_root=app_root)

    assert settings["python.defaultInterpreterPath"] == expected_vscode_interpreter_path()
    assert settings["terminal.integrated.env.windows"]["DATA_ENGINE_WORKSPACE_ROOT"] == str(workspace_root)
    assert "python.analysis.extraPaths" not in settings
    assert "python.testing.pytestEnabled" not in settings
    assert "python.testing.pytestArgs" not in settings


