"""Centralized application identity and naming helpers."""

from __future__ import annotations

from typing import Final

APP_INTERNAL_ID: Final[str] = "data_engine"
APP_DISTRIBUTION_NAME: Final[str] = "py-data-engine"
APP_DISPLAY_NAME: Final[str] = "Data Engine"
APP_VERSION: Final[str] = "0.3.0"
APP_ENV_PREFIX: Final[str] = "DATA_ENGINE"
APP_CACHE_DIR_NAME: Final[str] = "data_engine"
APP_RUNTIME_NAMESPACE: Final[str] = "data_engine"
APP_ARTIFACTS_DIR_NAME: Final[str] = "artifacts"
WORKSPACE_CACHE_DIR_NAME: Final[str] = "workspace_cache"
RUNTIME_STATE_DIR_NAME: Final[str] = "runtime_state"


def env_var(name: str) -> str:
    """Return one application-scoped environment variable name."""
    normalized = name.strip().upper()
    return f"{APP_ENV_PREFIX}_{normalized}"


__all__ = [
    "APP_CACHE_DIR_NAME",
    "APP_DISPLAY_NAME",
    "APP_DISTRIBUTION_NAME",
    "APP_ENV_PREFIX",
    "APP_INTERNAL_ID",
    "APP_RUNTIME_NAMESPACE",
    "APP_VERSION",
    "APP_ARTIFACTS_DIR_NAME",
    "RUNTIME_STATE_DIR_NAME",
    "WORKSPACE_CACHE_DIR_NAME",
    "env_var",
]
