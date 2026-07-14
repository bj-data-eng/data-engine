"""Shared workspace path models and pure helpers."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import sys
from typing import Final

from data_engine.platform.identity import APP_INTERNAL_ID, env_var
from data_engine.platform.paths import (
    normalized_path_text,
    path_display,
    stable_absolute_path as _stable_workspace_path,
    stable_path_identity_text as _stable_workspace_identity_text,
    toml_path_text,
)


def _resolve_app_root_path() -> Path:
    """Resolve the application root in both dev and frozen executable contexts."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[3]


APP_ROOT_PATH: Path = _resolve_app_root_path()
DATA_ENGINE_APP_ROOT_ENV_VAR: str = env_var("app_root")
DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR: str = env_var("workspace_root")
DATA_ENGINE_WORKSPACE_ID_ENV_VAR: str = env_var("workspace_id")
DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR: str = env_var("workspace_collection_root")
DATA_ENGINE_RUNTIME_ROOT_ENV_VAR: str = env_var("runtime_root")
DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR: str = env_var("runtime_cache_db_path")
DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR: str = env_var("runtime_control_db_path")
DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR: str = env_var("runtime_db_path")
WORKSPACE_CONFIG_DIR_NAME: str = "config"
WORKSPACE_FLOW_MODULES_DIR_NAME: str = "flow_modules"
WORKSPACE_FLOW_HELPERS_DIR_NAME: str = "flow_helpers"
WORKSPACE_DATABASES_DIR_NAME: str = "databases"
WORKSPACE_TEMPLATES_DIR_NAME: str = "templates"
WORKSPACE_STATE_DIR_NAME: str = ".workspace_state"
WORKSPACE_AVAILABLE_MARKERS_DIR_NAME: str = "available"
WORKSPACE_LEASED_MARKERS_DIR_NAME: str = "leased"
WORKSPACE_STALE_MARKERS_DIR_NAME: str = "stale"
WORKSPACE_LEASE_METADATA_DIR_NAME: str = "leases"
WORKSPACE_CONTROL_REQUESTS_DIR_NAME: str = "control_requests"
WORKSPACE_SHARED_STATE_DIR_NAME: str = "state"
WORKSPACE_SHARED_RUNS_DIR_NAME: str = "runs"
WORKSPACE_SHARED_STEP_RUNS_DIR_NAME: str = "step_runs"
WORKSPACE_SHARED_LOGS_DIR_NAME: str = "logs"
WORKSPACE_SHARED_FILE_STATE_DIR_NAME: str = "file_state"
MAX_WORKSPACE_ID_UTF8_BYTES: Final[int] = 64
_LOCAL_WORKSPACE_NAMESPACE_DIGEST_HEX_CHARS: Final[int] = 12
_WINDOWS_RESERVED_WORKSPACE_CHARACTERS: Final[frozenset[str]] = frozenset('<>:"/\\|?*')
_WINDOWS_RESERVED_WORKSPACE_BASENAMES: Final[frozenset[str]] = frozenset(
    {
        "aux",
        "con",
        "conin$",
        "conout$",
        "nul",
        "prn",
        *(f"com{index}" for index in range(1, 10)),
        *(f"lpt{index}" for index in range(1, 10)),
        *(f"com{index}" for index in "¹²³"),
        *(f"lpt{index}" for index in "¹²³"),
    }
)


class InvalidWorkspaceIdError(ValueError):
    """Raised when a workspace id contains unsafe path components."""


def validate_workspace_id(workspace_id: str) -> str:
    """Return a portable, bounded workspace path component.

    Workspace ids may contain Unicode text but must fit within 64 UTF-8 bytes.
    Windows-reserved characters, ASCII control characters, boundary
    whitespace, trailing dots, and reserved device basenames are rejected on
    every platform.
    Device basenames are matched case-insensitively, including when followed
    by one or more filename extensions.
    """
    candidate = str(workspace_id)
    if not candidate.strip():
        raise InvalidWorkspaceIdError("Workspace id cannot be empty.")
    if candidate != candidate.strip():
        raise InvalidWorkspaceIdError("Workspace id cannot begin or end with whitespace.")
    if candidate in {".", ".."}:
        raise InvalidWorkspaceIdError(f"Workspace id {candidate!r} is not allowed.")
    try:
        encoded_length = len(candidate.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise InvalidWorkspaceIdError("Workspace id must be valid UTF-8 text.") from exc
    if encoded_length > MAX_WORKSPACE_ID_UTF8_BYTES:
        raise InvalidWorkspaceIdError(f"Workspace id must not exceed {MAX_WORKSPACE_ID_UTF8_BYTES} UTF-8 bytes; got {encoded_length}.")
    if candidate.endswith("."):
        raise InvalidWorkspaceIdError("Workspace id cannot end with a dot.")
    if any(ord(character) < 32 or ord(character) == 127 or character in _WINDOWS_RESERVED_WORKSPACE_CHARACTERS for character in candidate):
        raise InvalidWorkspaceIdError(f"Workspace id {candidate!r} contains a Windows-reserved character.")
    device_basename = candidate.partition(".")[0].rstrip(" .").casefold()
    if device_basename in _WINDOWS_RESERVED_WORKSPACE_BASENAMES:
        raise InvalidWorkspaceIdError(f"Workspace id {candidate!r} uses a Windows-reserved device basename.")
    return candidate


def local_workspace_namespace(workspace_root: Path | str, workspace_id: str) -> str:
    """Return the bounded machine-local namespace for one workspace root."""
    workspace_id = validate_workspace_id(workspace_id)
    digest = hashlib.sha1(_stable_workspace_identity_text(workspace_root).encode("utf-8")).hexdigest()[:_LOCAL_WORKSPACE_NAMESPACE_DIGEST_HEX_CHARS]
    return f"{workspace_id}_{digest}"


@dataclass(frozen=True)
class WorkspaceSettings:
    """Machine-local workspace discovery settings."""

    app_root: Path
    settings_path: Path
    state_root: Path
    runtime_root: Path
    workspace_collection_root: Path | None
    default_selected: str | None


@dataclass(frozen=True)
class DiscoveredWorkspace:
    """One discovered authored workspace."""

    workspace_id: str
    workspace_root: Path


@dataclass(frozen=True)
class WorkspacePaths:
    """Resolved authored, shared, and local-artifact paths for one workspace."""

    app_root: Path
    workspace_collection_root: Path
    workspace_id: str
    workspace_root: Path
    config_dir: Path
    flow_modules_dir: Path
    databases_dir: Path
    workspace_state_dir: Path
    available_markers_dir: Path
    leased_markers_dir: Path
    stale_markers_dir: Path
    lease_metadata_dir: Path
    lease_metadata_path: Path
    control_requests_dir: Path
    control_request_path: Path
    shared_state_dir: Path
    shared_runs_path: Path
    shared_step_runs_path: Path
    shared_logs_path: Path
    shared_file_state_path: Path
    artifacts_dir: Path
    workspace_cache_dir: Path
    compiled_flow_modules_dir: Path
    runtime_state_dir: Path
    runtime_db_path: Path
    daemon_log_path: Path
    documentation_dir: Path
    daemon_endpoint_kind: str
    daemon_endpoint_path: str
    sphinx_source_dir: Path
    workspace_configured: bool = True
    runtime_cache_db_path: Path | None = None
    runtime_control_db_path: Path | None = None

    def __post_init__(self) -> None:
        """Populate split runtime-ledger paths while preserving the legacy cache alias."""
        runtime_cache_db_path = self.runtime_cache_db_path or self.runtime_db_path
        runtime_control_db_path = self.runtime_control_db_path or runtime_cache_db_path.with_name("runtime_control.sqlite")
        object.__setattr__(self, "runtime_cache_db_path", runtime_cache_db_path)
        object.__setattr__(self, "runtime_control_db_path", runtime_control_db_path)


def authored_workspace_is_available(paths: WorkspacePaths) -> bool:
    """Return whether one authored workspace root is still present and usable."""
    return paths.workspace_configured and paths.workspace_root.is_dir() and paths.flow_modules_dir.is_dir()


__all__ = [
    "APP_INTERNAL_ID",
    "APP_ROOT_PATH",
    "DATA_ENGINE_APP_ROOT_ENV_VAR",
    "DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR",
    "DATA_ENGINE_RUNTIME_CONTROL_DB_PATH_ENV_VAR",
    "DATA_ENGINE_RUNTIME_DB_PATH_ENV_VAR",
    "DATA_ENGINE_RUNTIME_ROOT_ENV_VAR",
    "DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR",
    "DATA_ENGINE_WORKSPACE_ID_ENV_VAR",
    "DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR",
    "DiscoveredWorkspace",
    "InvalidWorkspaceIdError",
    "MAX_WORKSPACE_ID_UTF8_BYTES",
    "WORKSPACE_AVAILABLE_MARKERS_DIR_NAME",
    "WORKSPACE_CONFIG_DIR_NAME",
    "WORKSPACE_CONTROL_REQUESTS_DIR_NAME",
    "WORKSPACE_DATABASES_DIR_NAME",
    "WORKSPACE_FLOW_HELPERS_DIR_NAME",
    "WORKSPACE_FLOW_MODULES_DIR_NAME",
    "WORKSPACE_LEASED_MARKERS_DIR_NAME",
    "WORKSPACE_LEASE_METADATA_DIR_NAME",
    "WORKSPACE_SHARED_FILE_STATE_DIR_NAME",
    "WORKSPACE_SHARED_LOGS_DIR_NAME",
    "WORKSPACE_SHARED_RUNS_DIR_NAME",
    "WORKSPACE_SHARED_STATE_DIR_NAME",
    "WORKSPACE_SHARED_STEP_RUNS_DIR_NAME",
    "WORKSPACE_STALE_MARKERS_DIR_NAME",
    "WORKSPACE_STATE_DIR_NAME",
    "WORKSPACE_TEMPLATES_DIR_NAME",
    "WorkspacePaths",
    "WorkspaceSettings",
    "_stable_workspace_path",
    "authored_workspace_is_available",
    "local_workspace_namespace",
    "normalized_path_text",
    "path_display",
    "toml_path_text",
    "validate_workspace_id",
]
