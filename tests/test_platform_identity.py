from __future__ import annotations

from data_engine.platform.identity import (
    APP_ARTIFACTS_DIR_NAME,
    APP_DISPLAY_NAME,
    APP_DISTRIBUTION_NAME,
    APP_ENV_PREFIX,
    APP_INTERNAL_ID,
    APP_RUNTIME_NAMESPACE,
    RUNTIME_STATE_DIR_NAME,
    WORKSPACE_CACHE_DIR_NAME,
    env_var,
)


def test_platform_identity_constants_are_stable():
    assert APP_INTERNAL_ID == "data_engine"
    assert APP_DISTRIBUTION_NAME == "data-engine"
    assert APP_DISPLAY_NAME == "Data Engine"
    assert APP_ENV_PREFIX == "DATA_ENGINE"
    assert APP_RUNTIME_NAMESPACE == "data_engine"
    assert APP_ARTIFACTS_DIR_NAME == "artifacts"
    assert WORKSPACE_CACHE_DIR_NAME == "workspace_cache"
    assert RUNTIME_STATE_DIR_NAME == "runtime_state"


def test_env_var_normalizes_and_prefixes_names():
    assert env_var("theme") == "DATA_ENGINE_THEME"
    assert env_var(" runtime_db_path ") == "DATA_ENGINE_RUNTIME_DB_PATH"
