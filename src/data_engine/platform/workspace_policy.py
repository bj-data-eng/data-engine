"""Workspace state, discovery, and runtime-layout policy services."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from data_engine.platform.identity import (
    APP_ARTIFACTS_DIR_NAME,
    APP_RUNTIME_NAMESPACE,
    RUNTIME_STATE_DIR_NAME,
    WORKSPACE_CACHE_DIR_NAME,
)
from data_engine.platform.local_settings import LocalSettingsStore, default_settings_db_path, default_state_root
from data_engine.platform.workspace_models import (
    APP_ROOT_PATH,
    DATA_ENGINE_APP_ROOT_ENV_VAR,
    DATA_ENGINE_RUNTIME_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ID_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR,
    DiscoveredWorkspace,
    WORKSPACE_AVAILABLE_MARKERS_DIR_NAME,
    WORKSPACE_CONFIG_DIR_NAME,
    WORKSPACE_CONTROL_REQUESTS_DIR_NAME,
    WORKSPACE_DATABASES_DIR_NAME,
    WORKSPACE_FLOW_MODULES_DIR_NAME,
    WORKSPACE_LEASED_MARKERS_DIR_NAME,
    WORKSPACE_LEASE_METADATA_DIR_NAME,
    WORKSPACE_SHARED_FILE_STATE_DIR_NAME,
    WORKSPACE_SHARED_LOGS_DIR_NAME,
    WORKSPACE_SHARED_RUNS_DIR_NAME,
    WORKSPACE_SHARED_STATE_DIR_NAME,
    WORKSPACE_SHARED_STEP_RUNS_DIR_NAME,
    WORKSPACE_STALE_MARKERS_DIR_NAME,
    WORKSPACE_STATE_DIR_NAME,
    WorkspacePaths,
    WorkspaceSettings,
    _stable_workspace_path,
    local_workspace_namespace as workspace_local_namespace,
    normalized_path_text,
    validate_workspace_id,
)


class AppStatePolicy:
    """Resolve machine-local app settings and mutable-state roots."""

    def effective_app_root(self, *, app_root: Path | None = None) -> Path:
        """Resolve the effective project/app root from an explicit argument or env."""
        if app_root is not None:
            return _stable_workspace_path(app_root)
        env_value = os.environ.get(DATA_ENGINE_APP_ROOT_ENV_VAR)
        if env_value and env_value.strip():
            return _stable_workspace_path(env_value)
        return APP_ROOT_PATH

    def settings_path(self, *, app_root: Path | None = None) -> Path:
        """Return the machine-local app settings database path."""
        return default_settings_db_path(app_root=app_root)

    def load_settings(self, *, app_root: Path | None = None) -> WorkspaceSettings:
        """Load machine-local workspace settings or synthesize defaults."""
        root = self.effective_app_root(app_root=app_root)
        settings_path = self.settings_path(app_root=root)
        state_root = default_state_root(app_root=root)
        store = LocalSettingsStore(settings_path)
        env_collection = os.environ.get(DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR)
        env_workspace_root = os.environ.get(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR)
        env_runtime_root = os.environ.get(DATA_ENGINE_RUNTIME_ROOT_ENV_VAR)
        runtime_root = (
            _stable_workspace_path(env_runtime_root)
            if env_runtime_root and env_runtime_root.strip()
            else store.runtime_root() or state_root / APP_ARTIFACTS_DIR_NAME
        )
        stored_collection_root = store.workspace_collection_root()
        stored_default_selected = store.default_workspace_id()
        if env_collection and env_collection.strip():
            collection_root = _stable_workspace_path(env_collection)
            if stored_collection_root is not None:
                collection_root = stored_collection_root
            default_selected = os.environ.get(DATA_ENGINE_WORKSPACE_ID_ENV_VAR) or stored_default_selected
            return WorkspaceSettings(root, settings_path, state_root, runtime_root, collection_root, default_selected)
        if env_workspace_root and env_workspace_root.strip():
            explicit_root = _stable_workspace_path(env_workspace_root)
            collection_root = explicit_root.parent
            default_selected = os.environ.get(DATA_ENGINE_WORKSPACE_ID_ENV_VAR) or explicit_root.name or stored_default_selected
            return WorkspaceSettings(root, settings_path, state_root, runtime_root, collection_root, default_selected)
        return WorkspaceSettings(
            app_root=root,
            settings_path=settings_path,
            state_root=state_root,
            runtime_root=runtime_root,
            workspace_collection_root=stored_collection_root,
            default_selected=stored_default_selected,
        )

    def write_settings(self, settings: WorkspaceSettings) -> None:
        """Persist machine-local settings through the SQLite settings store."""
        store = LocalSettingsStore(settings.settings_path)
        store.set_workspace_collection_root(settings.workspace_collection_root)
        store.set_default_workspace_id(settings.default_selected)
        store.set_runtime_root(settings.runtime_root)


class WorkspaceDiscoveryPolicy:
    """Resolve authored workspace discovery and selection."""

    PLACEHOLDER_WORKSPACE_ROOT_NAME = ".workspace_unconfigured"
    PLACEHOLDER_WORKSPACE_ID = "unconfigured"

    def __init__(self, *, app_state_policy: AppStatePolicy | None = None) -> None:
        self.app_state_policy = app_state_policy or AppStatePolicy()

    def _normalize_workspace_id(self, candidate: str, *, fallback: str | None = None) -> str:
        value = str(candidate).strip()
        if value == self.PLACEHOLDER_WORKSPACE_ROOT_NAME:
            value = self.PLACEHOLDER_WORKSPACE_ID
        if not value and fallback is not None:
            value = fallback
        return validate_workspace_id(value)

    def _placeholder_workspace(self, *, app_root: Path, preferred_id: str | None = None) -> tuple[DiscoveredWorkspace, Path, bool]:
        placeholder_root = app_root / self.PLACEHOLDER_WORKSPACE_ROOT_NAME
        placeholder_id = (
            self._normalize_workspace_id(preferred_id, fallback=self.PLACEHOLDER_WORKSPACE_ID)
            if preferred_id is not None
            else self.PLACEHOLDER_WORKSPACE_ID
        )
        return (
            DiscoveredWorkspace(workspace_id=placeholder_id, workspace_root=placeholder_root),
            placeholder_root.parent,
            False,
        )

    def discover(
        self,
        *,
        app_root: Path | None = None,
        workspace_collection_root: Path | None = None,
        explicit_workspace_root: Path | None = None,
    ) -> tuple[DiscoveredWorkspace, ...]:
        """Discover valid workspaces beneath the collection root."""
        if explicit_workspace_root is not None:
            root = _stable_workspace_path(explicit_workspace_root)
            workspace_id = os.environ.get(DATA_ENGINE_WORKSPACE_ID_ENV_VAR) or root.name or "default"
            return (DiscoveredWorkspace(workspace_id=workspace_id, workspace_root=root),)

        settings = self.app_state_policy.load_settings(app_root=app_root)
        collection_root = (
            _stable_workspace_path(workspace_collection_root)
            if workspace_collection_root is not None
            else settings.workspace_collection_root
        )
        if collection_root is None:
            return ()
        if not collection_root.exists():
            return ()
        if (collection_root / WORKSPACE_FLOW_MODULES_DIR_NAME).is_dir():
            workspace_id = os.environ.get(DATA_ENGINE_WORKSPACE_ID_ENV_VAR) or "default"
            return (DiscoveredWorkspace(workspace_id=workspace_id, workspace_root=collection_root),)
        discovered: list[DiscoveredWorkspace] = []
        for candidate in sorted(path for path in collection_root.iterdir() if path.is_dir()):
            if (candidate / WORKSPACE_FLOW_MODULES_DIR_NAME).is_dir():
                discovered.append(DiscoveredWorkspace(workspace_id=candidate.name, workspace_root=candidate))
        return tuple(discovered)

    def select_workspace(
        self,
        *,
        app_root: Path,
        workspace_id: str | None,
        workspace_root: Path | None,
        workspace_collection_root: Path | None,
        data_root: Path | None,
    ) -> tuple[DiscoveredWorkspace, Path, bool]:
        """Select one authored workspace and collection root from the current policy."""
        explicit_root = workspace_root if workspace_root is not None else data_root
        env_workspace_root = os.environ.get(DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR)
        if explicit_root is not None:
            discovered_root = _stable_workspace_path(explicit_root)
            fallback_id = self.PLACEHOLDER_WORKSPACE_ID if discovered_root.name == self.PLACEHOLDER_WORKSPACE_ROOT_NAME else (discovered_root.name or "default")
            selected_id = self._normalize_workspace_id(workspace_id) if workspace_id is not None else fallback_id
            collection_root = (
                _stable_workspace_path(workspace_collection_root)
                if workspace_collection_root is not None
                else discovered_root.parent
            )
            return DiscoveredWorkspace(workspace_id=selected_id, workspace_root=discovered_root), collection_root, True
        if env_workspace_root and env_workspace_root.strip():
            discovered_root = _stable_workspace_path(env_workspace_root)
            env_workspace_id = os.environ.get(DATA_ENGINE_WORKSPACE_ID_ENV_VAR)
            if workspace_id is not None:
                selected_id = self._normalize_workspace_id(workspace_id)
            elif env_workspace_id and env_workspace_id.strip():
                selected_id = self._normalize_workspace_id(env_workspace_id)
            else:
                fallback_id = self.PLACEHOLDER_WORKSPACE_ID if discovered_root.name == self.PLACEHOLDER_WORKSPACE_ROOT_NAME else (discovered_root.name or "default")
                selected_id = fallback_id
            collection_root = (
                _stable_workspace_path(workspace_collection_root)
                if workspace_collection_root is not None
                else discovered_root.parent
            )
            return DiscoveredWorkspace(workspace_id=selected_id, workspace_root=discovered_root), collection_root, True

        settings = self.app_state_policy.load_settings(app_root=app_root)
        collection_root = (
            _stable_workspace_path(workspace_collection_root)
            if workspace_collection_root is not None
            else settings.workspace_collection_root
        )
        if collection_root is None:
            return self._placeholder_workspace(
                app_root=app_root,
                preferred_id=workspace_id or settings.default_selected,
            )
        discovered_all = self.discover(app_root=app_root, workspace_collection_root=collection_root)
        if not discovered_all:
            if workspace_id is not None:
                selected_id = self._normalize_workspace_id(workspace_id)
            else:
                env_workspace_id = os.environ.get(DATA_ENGINE_WORKSPACE_ID_ENV_VAR)
                if env_workspace_id and env_workspace_id.strip():
                    selected_id = self._normalize_workspace_id(env_workspace_id)
                else:
                    selected_id = (
                        self._normalize_workspace_id(settings.default_selected, fallback=self.PLACEHOLDER_WORKSPACE_ID)
                        if settings.default_selected is not None
                else self.PLACEHOLDER_WORKSPACE_ID
            )
            discovered = DiscoveredWorkspace(workspace_id=selected_id, workspace_root=collection_root / selected_id)
            return discovered, collection_root, True

        if workspace_id is not None:
            requested_id = self._normalize_workspace_id(workspace_id)
        else:
            env_workspace_id = os.environ.get(DATA_ENGINE_WORKSPACE_ID_ENV_VAR)
            requested_id = env_workspace_id if env_workspace_id and env_workspace_id.strip() else settings.default_selected
        if requested_id is None:
            return discovered_all[0], collection_root, True
        requested_id = self._normalize_workspace_id(requested_id, fallback=self.PLACEHOLDER_WORKSPACE_ID)
        by_id = {item.workspace_id: item for item in discovered_all}
        return by_id.get(requested_id, discovered_all[0]), collection_root, True


class RuntimeLayoutPolicy:
    """Resolve local runtime and artifact layout for one workspace."""

    def __init__(self, *, app_state_policy: AppStatePolicy | None = None, discovery_policy: WorkspaceDiscoveryPolicy | None = None) -> None:
        self.app_state_policy = app_state_policy or AppStatePolicy()
        self.discovery_policy = discovery_policy or WorkspaceDiscoveryPolicy(app_state_policy=self.app_state_policy)

    def resolve_paths(
        self,
        *,
        workspace_id: str | None = None,
        workspace_root: Path | None = None,
        workspace_collection_root: Path | None = None,
        data_root: Path | None = None,
        app_root: Path | None = None,
    ) -> WorkspacePaths:
        """Resolve authored, shared, and local-artifact paths for one selected workspace."""
        resolved_app_root = self.app_state_policy.effective_app_root(app_root=app_root)
        discovered, collection_root, workspace_configured = self.discovery_policy.select_workspace(
            app_root=resolved_app_root,
            workspace_id=workspace_id,
            workspace_root=workspace_root,
            workspace_collection_root=workspace_collection_root,
            data_root=data_root,
        )
        settings = self.app_state_policy.load_settings(app_root=resolved_app_root)
        workspace_state_dir = discovered.workspace_root / WORKSPACE_STATE_DIR_NAME
        shared_state_dir = workspace_state_dir / WORKSPACE_SHARED_STATE_DIR_NAME
        lease_metadata_dir = workspace_state_dir / WORKSPACE_LEASE_METADATA_DIR_NAME
        control_requests_dir = workspace_state_dir / WORKSPACE_CONTROL_REQUESTS_DIR_NAME
        local_namespace = self.local_workspace_namespace(discovered.workspace_root, discovered.workspace_id)
        artifacts_dir = settings.runtime_root
        workspace_cache_dir = artifacts_dir / WORKSPACE_CACHE_DIR_NAME / local_namespace
        runtime_state_dir = artifacts_dir / RUNTIME_STATE_DIR_NAME / local_namespace
        runtime_cache_db_path = runtime_state_dir / "runtime_cache.sqlite"
        runtime_control_db_path = runtime_state_dir / "runtime_control.sqlite"
        daemon_endpoint_kind, daemon_endpoint_path = self.daemon_endpoint(
            runtime_state_dir=runtime_state_dir,
            workspace_id=discovered.workspace_id,
        )
        return WorkspacePaths(
            app_root=resolved_app_root,
            workspace_collection_root=collection_root,
            workspace_id=discovered.workspace_id,
            workspace_root=discovered.workspace_root,
            config_dir=discovered.workspace_root / WORKSPACE_CONFIG_DIR_NAME,
            flow_modules_dir=discovered.workspace_root / WORKSPACE_FLOW_MODULES_DIR_NAME,
            databases_dir=discovered.workspace_root / WORKSPACE_DATABASES_DIR_NAME,
            workspace_state_dir=workspace_state_dir,
            available_markers_dir=workspace_state_dir / WORKSPACE_AVAILABLE_MARKERS_DIR_NAME,
            leased_markers_dir=workspace_state_dir / WORKSPACE_LEASED_MARKERS_DIR_NAME,
            stale_markers_dir=workspace_state_dir / WORKSPACE_STALE_MARKERS_DIR_NAME,
            lease_metadata_dir=lease_metadata_dir,
            lease_metadata_path=lease_metadata_dir / f"{discovered.workspace_id}.parquet",
            control_requests_dir=control_requests_dir,
            control_request_path=control_requests_dir / f"{discovered.workspace_id}.parquet",
            shared_state_dir=shared_state_dir,
            shared_runs_path=shared_state_dir / WORKSPACE_SHARED_RUNS_DIR_NAME / f"{discovered.workspace_id}.parquet",
            shared_step_runs_path=shared_state_dir / WORKSPACE_SHARED_STEP_RUNS_DIR_NAME / f"{discovered.workspace_id}.parquet",
            shared_logs_path=shared_state_dir / WORKSPACE_SHARED_LOGS_DIR_NAME / f"{discovered.workspace_id}.parquet",
            shared_file_state_path=shared_state_dir / WORKSPACE_SHARED_FILE_STATE_DIR_NAME / f"{discovered.workspace_id}.parquet",
            artifacts_dir=artifacts_dir,
            workspace_cache_dir=workspace_cache_dir,
            compiled_flow_modules_dir=workspace_cache_dir / "compiled_flow_modules",
            runtime_state_dir=runtime_state_dir,
            runtime_db_path=runtime_cache_db_path,
            daemon_log_path=runtime_state_dir / "daemon.log",
            documentation_dir=artifacts_dir / "documentation",
            daemon_endpoint_kind=daemon_endpoint_kind,
            daemon_endpoint_path=daemon_endpoint_path,
            sphinx_source_dir=resolved_app_root / "src" / "data_engine" / "docs" / "sphinx_source",
            workspace_configured=workspace_configured,
            runtime_cache_db_path=runtime_cache_db_path,
            runtime_control_db_path=runtime_control_db_path,
        )

    @staticmethod
    def local_workspace_namespace(workspace_root: Path | str, workspace_id: str) -> str:
        """Return the machine-local namespace for one workspace root."""
        return workspace_local_namespace(workspace_root, workspace_id)

    @staticmethod
    def daemon_endpoint(*, runtime_state_dir: Path, workspace_id: str) -> tuple[str, str]:
        """Return the cross-platform local IPC endpoint."""
        workspace_id = validate_workspace_id(workspace_id)
        digest = hashlib.sha1(normalized_path_text(runtime_state_dir).encode("utf-8")).hexdigest()[:12]
        if os.name == "nt":
            return "pipe", rf"\\.\pipe\{APP_RUNTIME_NAMESPACE}_{workspace_id}_{digest}"
        return "unix", normalized_path_text(Path("/tmp") / f"{APP_RUNTIME_NAMESPACE}_{workspace_id}_{digest}.sock")


__all__ = [
    "AppStatePolicy",
    "RuntimeLayoutPolicy",
    "WorkspaceDiscoveryPolicy",
]
