from __future__ import annotations


from data_engine.platform.workspace_models import WORKSPACE_FLOW_HELPERS_DIR_NAME
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.services.workspace_provisioning import (
    WorkspaceProvisioningService,
    collection_vscode_settings,
    workspace_vscode_settings,
)


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


def test_workspace_provisioning_creates_missing_workspace_assets(monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    collection_root = tmp_path / "workspaces"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", str(collection_root))
    paths = resolve_workspace_paths(workspace_id="claims")

    result = WorkspaceProvisioningService().provision_workspace(paths)

    assert result.workspace_root == collection_root / "claims"
    assert paths.flow_modules_dir.is_dir()
    assert (paths.flow_modules_dir / WORKSPACE_FLOW_HELPERS_DIR_NAME).is_dir()
    assert paths.config_dir.is_dir()
    assert paths.databases_dir.is_dir()
    assert (paths.workspace_collection_root / ".vscode" / "settings.json").is_file()
    assert (paths.workspace_root / ".vscode" / "settings.json").is_file()
    assert result.created_anything is True


def test_workspace_provisioning_preserves_existing_vscode_settings(monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    collection_root = tmp_path / "workspaces"
    workspace_root = collection_root / "claims"
    (workspace_root / "flow_modules").mkdir(parents=True)
    settings_path = workspace_root / ".vscode" / "settings.json"
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text('{"existing": true}\n', encoding="utf-8")
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", str(collection_root))
    paths = resolve_workspace_paths(workspace_id="claims")

    result = WorkspaceProvisioningService().provision_workspace(paths)

    assert settings_path.read_text(encoding="utf-8") == '{"existing": true}\n'
    assert settings_path in result.preserved_paths


def test_workspace_vscode_settings_use_current_interpreter_and_terminal_env(monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    (app_root / "src").mkdir(parents=True)
    workspace_root = tmp_path / "workspaces" / "claims"
    interpreter_path = tmp_path / ".venv" / "bin" / "python"
    interpreter_path.parent.mkdir(parents=True)
    interpreter_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    settings = workspace_vscode_settings(workspace_root, app_root=app_root, interpreter_path=interpreter_path)

    assert settings["python.defaultInterpreterPath"] == str(interpreter_path.resolve())
    assert settings["terminal.integrated.env.osx"]["DATA_ENGINE_WORKSPACE_ROOT"] == str(workspace_root)
    assert settings["terminal.integrated.env.windows"]["DATA_ENGINE_WORKSPACE_ROOT"] == str(workspace_root)
    assert settings["terminal.integrated.env.windows"] == settings["terminal.integrated.env.osx"]


def test_collection_vscode_settings_use_collection_root_terminal_env(monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    collection_root = tmp_path / "workspaces"
    interpreter_path = tmp_path / ".venv" / "bin" / "python"
    interpreter_path.parent.mkdir(parents=True)
    interpreter_path.write_text("", encoding="utf-8")

    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    settings = collection_vscode_settings(collection_root, app_root=app_root, interpreter_path=interpreter_path)

    assert settings["python.defaultInterpreterPath"] == str(interpreter_path.resolve())
    assert settings["terminal.integrated.env.osx"]["DATA_ENGINE_WORKSPACE_COLLECTION_ROOT"] == str(collection_root)
    assert settings["terminal.integrated.env.windows"]["DATA_ENGINE_WORKSPACE_COLLECTION_ROOT"] == str(collection_root)
    assert "DATA_ENGINE_WORKSPACE_ROOT" not in settings["terminal.integrated.env.osx"]
    assert "DATA_ENGINE_WORKSPACE_ROOT" not in settings["terminal.integrated.env.windows"]
