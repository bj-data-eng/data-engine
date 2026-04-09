from __future__ import annotations

from pathlib import Path

from data_engine.platform.local_settings import (
    DATA_ENGINE_STATE_ROOT_ENV_VAR,
    LocalSettingsStore,
    default_settings_db_path,
    default_state_root,
)


def test_local_settings_store_persists_workspace_collection_root(tmp_path):
    app_root = tmp_path / "data_engine"
    store = LocalSettingsStore.open_default(app_root=app_root)
    target = tmp_path / "shared_workspaces"

    assert store.workspace_collection_root() is None

    store.set_workspace_collection_root(target)

    reopened = LocalSettingsStore.open_default(app_root=app_root)
    assert reopened.workspace_collection_root() == target.resolve()


def test_local_settings_store_clears_workspace_collection_root(tmp_path):
    app_root = tmp_path / "data_engine"
    store = LocalSettingsStore.open_default(app_root=app_root)
    target = tmp_path / "shared_workspaces"

    store.set_workspace_collection_root(target)
    store.set_workspace_collection_root(None)

    reopened = LocalSettingsStore.open_default(app_root=app_root)
    assert reopened.workspace_collection_root() is None


def test_default_settings_db_path_tracks_default_store_location(tmp_path):
    app_root = tmp_path / "data_engine"

    store = LocalSettingsStore.open_default(app_root=app_root)

    assert store.db_path == default_settings_db_path(app_root=app_root)


def test_default_state_root_uses_platform_local_app_root_when_not_overridden(monkeypatch, tmp_path):
    home = tmp_path / "home"
    monkeypatch.delenv(DATA_ENGINE_STATE_ROOT_ENV_VAR, raising=False)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("XDG_STATE_HOME", raising=False)
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.setattr("data_engine.platform.local_settings.sys_platform", lambda: "darwin")

    resolved = default_state_root(app_root=tmp_path / "data_engine")

    assert resolved == home / "Library" / "Application Support" / "data_engine"


def test_local_settings_store_recreates_parent_dir_before_reopening_connection(tmp_path):
    app_root = tmp_path / "data_engine"
    store = LocalSettingsStore.open_default(app_root=app_root)

    for child in store.db_path.parent.iterdir():
        child.unlink()
    store.db_path.parent.rmdir()

    store.set_default_workspace_id("example_workspace")

    reopened = LocalSettingsStore.open_default(app_root=app_root)
    assert reopened.default_workspace_id() == "example_workspace"
