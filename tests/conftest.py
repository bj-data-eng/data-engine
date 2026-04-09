"""Pytest bootstrap shared across the Data Engine test suite."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from data_engine.platform.workspace_models import (
    DATA_ENGINE_APP_ROOT_ENV_VAR,
    DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR,
    DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ID_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR,
)
from data_engine.platform.local_settings import DATA_ENGINE_STATE_ROOT_ENV_VAR


# Force Qt to use a headless backend so UI tests run in CI and terminal sessions.
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")


@pytest.fixture(autouse=True)
def isolated_runtime_ledger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Give each test its own runtime ledger path so persisted state stays isolated."""
    app_root = tmp_path / "data_engine"
    app_local_root = tmp_path / "app_local" / "data_engine"
    repo_workspace_collection_root = Path(__file__).resolve().parents[2] / "workspaces"
    monkeypatch.setenv(DATA_ENGINE_APP_ROOT_ENV_VAR, str(app_root))
    monkeypatch.setenv(DATA_ENGINE_STATE_ROOT_ENV_VAR, str(app_local_root))
    monkeypatch.delenv(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR, raising=False)
    monkeypatch.setenv(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR, str(repo_workspace_collection_root))
    monkeypatch.setenv(DATA_ENGINE_WORKSPACE_ID_ENV_VAR, "example_workspace")
    monkeypatch.setenv(
        DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR,
        str(app_root / "artifacts" / "runtime_state" / "example_workspace" / "runtime_ledger.sqlite"),
    )
