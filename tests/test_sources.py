from __future__ import annotations

import os
from pathlib import Path

import pytest

from data_engine.platform.paths import stable_path_identity_text
from data_engine.platform.workspace_models import (
    APP_ROOT_PATH,
    DATA_ENGINE_APP_ROOT_ENV_VAR,
    DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR,
    DATA_ENGINE_RUNTIME_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ID_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR,
    InvalidWorkspaceIdError,
    local_workspace_namespace,
    path_display,
    WorkspaceSettings,
)
from data_engine.platform.local_settings import DATA_ENGINE_STATE_ROOT_ENV_VAR, LocalSettingsStore
from data_engine.platform.workspace_policy import AppStatePolicy, RuntimeLayoutPolicy, WorkspaceDiscoveryPolicy


_APP_STATE_POLICY = AppStatePolicy()
_WORKSPACE_DISCOVERY_POLICY = WorkspaceDiscoveryPolicy(app_state_policy=_APP_STATE_POLICY)
_RUNTIME_LAYOUT_POLICY = RuntimeLayoutPolicy(app_state_policy=_APP_STATE_POLICY, discovery_policy=_WORKSPACE_DISCOVERY_POLICY)

load_workspace_settings = _APP_STATE_POLICY.load_settings
discover_workspaces = _WORKSPACE_DISCOVERY_POLICY.discover
resolve_workspace_paths = _RUNTIME_LAYOUT_POLICY.resolve_paths
workspace_settings_path = _APP_STATE_POLICY.settings_path
write_workspace_settings = _APP_STATE_POLICY.write_settings


def test_workspace_path_helpers_are_stable(monkeypatch):
    monkeypatch.delenv(DATA_ENGINE_APP_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_STATE_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR, raising=False)

    assert APP_ROOT_PATH.is_dir()
    assert (APP_ROOT_PATH / "pyproject.toml").is_file()
    assert path_display(None) == "(not set)"
    assert path_display(Path("/tmp/example.xlsx")) == "/tmp/example.xlsx"
    assert workspace_settings_path(app_root=APP_ROOT_PATH).name == "app_settings.sqlite"


def test_local_workspace_namespace_does_not_require_path_resolve(tmp_path, monkeypatch):
    def _resolve(*args, **kwargs):  # pragma: no cover - defensive test hook
        raise AssertionError("local_workspace_namespace should not resolve paths")

    monkeypatch.setattr(Path, "resolve", _resolve)

    namespace = local_workspace_namespace(tmp_path / "workspace" / ".." / "workspace", "claims")

    assert namespace.startswith("claims_")


def test_stable_path_identity_text_supports_case_insensitive_comparisons():
    left = stable_path_identity_text(Path("C:/Workspace/Claims"), case_insensitive=True)
    right = stable_path_identity_text(Path("c:/workspace/claims"), case_insensitive=True)

    assert left == right


def test_local_workspace_namespace_uses_platform_default_path_identity():
    upper = local_workspace_namespace(Path("C:/Workspace/Claims"), "claims")
    lower = local_workspace_namespace(Path("c:/workspace/claims"), "claims")

    if os.name == "nt":
        assert upper == lower
    else:
        assert upper != lower


def test_resolve_workspace_paths_prefers_explicit_workspace_root(tmp_path, monkeypatch):
    workspace = tmp_path / "collection" / "default"
    app_root = tmp_path / "data_engine"
    app_root.mkdir()
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    state_root = tmp_path / "app_local" / "data_engine"
    monkeypatch.setenv(DATA_ENGINE_STATE_ROOT_ENV_VAR, str(state_root))

    resolved = resolve_workspace_paths(workspace_root=workspace)
    local_namespace = local_workspace_namespace(workspace.resolve(), "default")

    assert resolved.workspace_root == workspace.resolve()
    assert resolved.workspace_id == "default"
    assert resolved.flow_modules_dir == workspace.resolve() / "flow_modules"
    assert resolved.databases_dir == workspace.resolve() / "databases"
    assert resolved.workspace_state_dir == workspace.resolve() / ".workspace_state"
    assert resolved.artifacts_dir == state_root / "artifacts"
    assert resolved.workspace_cache_dir == state_root / "artifacts" / "workspace_cache" / local_namespace
    assert resolved.compiled_flow_modules_dir == state_root / "artifacts" / "workspace_cache" / local_namespace / "compiled_flow_modules"
    assert resolved.runtime_db_path == state_root / "artifacts" / "runtime_state" / local_namespace / "runtime_cache.sqlite"
    assert resolved.runtime_cache_db_path == resolved.runtime_db_path
    assert resolved.runtime_control_db_path == state_root / "artifacts" / "runtime_state" / local_namespace / "runtime_control.sqlite"
    assert resolved.documentation_dir == state_root / "artifacts" / "documentation"


def test_resolve_workspace_paths_prefers_env_workspace_root(tmp_path, monkeypatch):
    workspace = tmp_path / "collection" / "analytics"
    monkeypatch.setenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, str(workspace))
    monkeypatch.setenv(DATA_ENGINE_WORKSPACE_ID_ENV_VAR, "analytics")

    resolved = resolve_workspace_paths()

    assert resolved.workspace_root == workspace.resolve()
    assert resolved.workspace_id == "analytics"


def test_load_settings_and_discover_workspaces_from_collection_root(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    collection_root = tmp_path / "shared_workspaces"
    (collection_root / "default" / "flow_modules").mkdir(parents=True)
    (collection_root / "analytics" / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    store = LocalSettingsStore.open_default(app_root=app_root)
    store.set_workspace_collection_root(collection_root)
    store.set_default_workspace_id("analytics")
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ID_ENV_VAR, raising=False)

    settings = load_workspace_settings()
    discovered = discover_workspaces()

    assert settings.workspace_collection_root == collection_root.resolve()
    assert settings.default_selected == "analytics"
    assert [item.workspace_id for item in discovered] == ["analytics", "default"]


def test_load_settings_supports_relative_collection_root(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    collection_root = tmp_path / "shared_workspaces"
    (collection_root / "claims" / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    store = LocalSettingsStore.open_default(app_root=app_root)
    store.set_workspace_collection_root(collection_root)
    store.set_default_workspace_id("claims")
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ID_ENV_VAR, raising=False)

    settings = load_workspace_settings()
    discovered = discover_workspaces()

    assert settings.workspace_collection_root == collection_root.resolve()
    assert settings.default_selected == "claims"
    assert [item.workspace_id for item in discovered] == ["claims"]


def test_load_settings_leaves_workspace_collection_root_unconfigured_when_unset(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ID_ENV_VAR, raising=False)

    settings = load_workspace_settings()
    discovered = discover_workspaces()
    resolved = resolve_workspace_paths()

    assert settings.workspace_collection_root is None
    assert settings.default_selected is None
    assert discovered == ()
    assert resolved.workspace_configured is False
    assert resolved.workspace_id == "unconfigured"


def test_unconfigured_workspace_ignores_stale_default_selected_when_no_collection_root(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR, raising=False)
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ID_ENV_VAR, raising=False)
    store = LocalSettingsStore.open_default(app_root=app_root)
    store.set_default_workspace_id("claims")

    resolved = resolve_workspace_paths()

    assert resolved.workspace_configured is False
    assert resolved.workspace_id == "unconfigured"


def test_explicit_placeholder_workspace_root_normalizes_workspace_id(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))

    resolved = resolve_workspace_paths(workspace_root=app_root / ".workspace_unconfigured")

    assert resolved.workspace_root == (app_root / ".workspace_unconfigured").resolve()
    assert resolved.workspace_id == "unconfigured"


def test_write_workspace_settings_persists_default_and_collection_root(tmp_path):
    app_root = tmp_path / "data_engine"
    settings_path = workspace_settings_path(app_root=app_root)
    state_root = tmp_path / "app_local" / "data_engine"
    settings = WorkspaceSettings(
        app_root=app_root,
        settings_path=settings_path,
        state_root=state_root,
        runtime_root=state_root / "artifacts",
        workspace_collection_root=tmp_path / "shared_workspaces",
        default_selected="claims",
    )

    write_workspace_settings(settings)

    store = LocalSettingsStore(settings_path)
    assert store.default_workspace_id() == "claims"
    assert store.workspace_collection_root() == (tmp_path / "shared_workspaces").resolve()
    assert store.runtime_root() == (state_root / "artifacts").resolve()


def test_runtime_db_env_var_name_is_stable():
    assert DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR == "DATA_ENGINE_RUNTIME_DB_PATH"


def test_runtime_root_env_var_overrides_settings(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.setenv(DATA_ENGINE_RUNTIME_ROOT_ENV_VAR, str(tmp_path / "custom_runtime"))

    resolved = resolve_workspace_paths(workspace_root=tmp_path / "workspace")

    assert resolved.artifacts_dir == (tmp_path / "custom_runtime").resolve()


def test_same_named_workspaces_get_distinct_local_runtime_namespaces(tmp_path, monkeypatch):
    app_root = tmp_path / "data_engine"
    root_a = tmp_path / "inside" / "example_workspace"
    root_b = tmp_path / "outside" / "example_workspace"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))

    resolved_a = resolve_workspace_paths(workspace_root=root_a, workspace_id="example_workspace")
    resolved_b = resolve_workspace_paths(workspace_root=root_b, workspace_id="example_workspace")

    assert resolved_a.workspace_id == resolved_b.workspace_id == "example_workspace"
    assert resolved_a.workspace_root != resolved_b.workspace_root
    assert resolved_a.workspace_cache_dir != resolved_b.workspace_cache_dir
    assert resolved_a.runtime_state_dir != resolved_b.runtime_state_dir
    assert resolved_a.runtime_db_path != resolved_b.runtime_db_path
    assert resolved_a.runtime_control_db_path != resolved_b.runtime_control_db_path
    assert resolved_a.daemon_endpoint_path != resolved_b.daemon_endpoint_path


@pytest.mark.parametrize("workspace_id", ["../escape", "foo/bar", r"foo\bar", ".", "..", ""])
def test_resolve_workspace_paths_rejects_unsafe_workspace_ids(tmp_path, monkeypatch, workspace_id):
    app_root = tmp_path / "data_engine"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))

    with pytest.raises(InvalidWorkspaceIdError):
        resolve_workspace_paths(workspace_root=tmp_path / "workspace", workspace_id=workspace_id)


@pytest.mark.parametrize("workspace_id", ["../escape", "foo/bar", r"foo\bar", ".", "..", ""])
def test_local_workspace_namespace_rejects_unsafe_workspace_ids(tmp_path, workspace_id):
    with pytest.raises(InvalidWorkspaceIdError):
        local_workspace_namespace(tmp_path / "workspace", workspace_id)
