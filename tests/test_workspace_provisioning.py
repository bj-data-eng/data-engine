from __future__ import annotations

import json

import pytest

from data_engine.platform.workspace_models import WORKSPACE_FLOW_HELPERS_DIR_NAME, WORKSPACE_TEMPLATES_DIR_NAME
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy
from data_engine.runtime.shared_state import initialize_workspace_state, resolve_workspace_bundle
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
    paths = resolve_workspace_paths(workspace_root=collection_root / "docs", workspace_id="docs")

    result = WorkspaceProvisioningService().provision_workspace(paths)

    assert result.workspace_root == collection_root / "docs"
    assert paths.flow_modules_dir.is_dir()
    assert (paths.flow_modules_dir / WORKSPACE_FLOW_HELPERS_DIR_NAME).is_dir()
    assert paths.config_dir.is_dir()
    assert paths.databases_dir.is_dir()
    assert (paths.workspace_root / WORKSPACE_TEMPLATES_DIR_NAME).is_dir()
    assert (paths.workspace_collection_root / ".vscode" / "settings.json").is_file()
    assert (paths.workspace_root / ".vscode" / "settings.json").is_file()
    assert result.created_anything is True


def test_workspace_provisioning_preserves_existing_vscode_settings(monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    collection_root = tmp_path / "workspaces"
    workspace_root = collection_root / "docs"
    (workspace_root / "flow_modules").mkdir(parents=True)
    settings_path = workspace_root / ".vscode" / "settings.json"
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text('{"existing": true}\n', encoding="utf-8")
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    monkeypatch.setenv("DATA_ENGINE_WORKSPACE_COLLECTION_ROOT", str(collection_root))
    paths = resolve_workspace_paths(workspace_id="docs")

    result = WorkspaceProvisioningService().provision_workspace(paths)

    assert settings_path.read_text(encoding="utf-8") == '{"existing": true}\n'
    assert settings_path in result.preserved_paths


def test_workspace_vscode_settings_use_current_interpreter_and_terminal_env(monkeypatch, tmp_path):
    app_root = tmp_path / "data_engine"
    (app_root / "src").mkdir(parents=True)
    workspace_root = tmp_path / "workspaces" / "docs"
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


@pytest.mark.parametrize("platform", ["linux", "osx", "windows"])
def test_provisioned_terminal_environment_preserves_alias_roundtrip(tmp_path, monkeypatch, platform):
    workspace_root = tmp_path / "workspaces" / "docs"
    paths = resolve_workspace_paths(workspace_root=workspace_root, workspace_id="Analytics-Alias")
    WorkspaceProvisioningService().provision_workspace(paths)
    initialize_workspace_state(paths)
    original_bundle = resolve_workspace_bundle(paths)
    settings = json.loads((workspace_root / ".vscode" / "settings.json").read_text(encoding="utf-8"))

    terminal_env = settings[f"terminal.integrated.env.{platform}"]
    assert terminal_env["DATA_ENGINE_WORKSPACE_ID"] == "Analytics-Alias"
    for key, value in terminal_env.items():
        monkeypatch.setenv(key, value)
    reopened = resolve_workspace_paths()

    assert reopened.workspace_id == paths.workspace_id
    assert reopened.runtime_cache_db_path == paths.runtime_cache_db_path
    assert reopened.runtime_control_db_path == paths.runtime_control_db_path
    assert reopened.daemon_endpoint_path == paths.daemon_endpoint_path
    assert resolve_workspace_bundle(reopened).root == original_bundle.root


def test_provisioning_new_literal_target_leaves_existing_workspace_untouched(tmp_path):
    collection = tmp_path / "workspaces"
    existing = collection / "alpha"
    (existing / "flow_modules").mkdir(parents=True)
    sentinel = existing / "flow_modules" / "flow.py"
    sentinel.write_text("# Existing authored flow\n", encoding="utf-8")
    before = {path.relative_to(existing): path.read_bytes() for path in existing.rglob("*") if path.is_file()}

    paths = resolve_workspace_paths(workspace_root=collection / "new_workspace", workspace_collection_root=collection)
    result = WorkspaceProvisioningService().provision_workspace(paths)

    assert result.workspace_root == collection / "new_workspace"
    assert paths.workspace_id == "new_workspace"
    assert paths.flow_modules_dir.is_dir()
    assert {path.relative_to(existing): path.read_bytes() for path in existing.rglob("*") if path.is_file()} == before
    assert not (existing / "config").exists()
    assert not (existing / ".vscode").exists()


def test_stale_explicit_target_cannot_provision_an_unrelated_workspace(tmp_path):
    collection = tmp_path / "workspaces"
    existing = collection / "alpha"
    (existing / "flow_modules").mkdir(parents=True)
    sentinel = existing / "flow_modules" / "flow.py"
    sentinel.write_text("# Keep this flow\n", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="deleted"):
        paths = resolve_workspace_paths(workspace_id="deleted", workspace_collection_root=collection)
        WorkspaceProvisioningService().provision_workspace(paths)

    assert sentinel.read_text(encoding="utf-8") == "# Keep this flow\n"
    assert tuple(existing.iterdir()) == (existing / "flow_modules",)
    assert not (collection / ".vscode").exists()


def test_workspace_settings_builder_accepts_explicit_alias(tmp_path):
    settings = workspace_vscode_settings(tmp_path / "docs", app_root=tmp_path / "app", workspace_id="alias")

    assert settings["terminal.integrated.env.windows"]["DATA_ENGINE_WORKSPACE_ID"] == "alias"
