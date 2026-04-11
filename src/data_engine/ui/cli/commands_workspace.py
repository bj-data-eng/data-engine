"""Workspace scaffolding helpers for the CLI surface."""

from __future__ import annotations

from pathlib import Path

from data_engine.authoring.model import FlowValidationError
from data_engine.platform.workspace_models import (
    WORKSPACE_CONFIG_DIR_NAME,
    WORKSPACE_DATABASES_DIR_NAME,
    WORKSPACE_FLOW_HELPERS_DIR_NAME,
    WORKSPACE_FLOW_MODULES_DIR_NAME,
    WorkspaceSettings,
    validate_workspace_id,
)
from data_engine.services.workspace_provisioning import (
    collection_vscode_settings as build_collection_vscode_settings,
    write_collection_vscode_settings as persist_collection_vscode_settings,
    workspace_vscode_settings as build_workspace_vscode_settings,
    write_workspace_vscode_settings as persist_workspace_vscode_settings,
)


def create_command(args, *, dependencies) -> int:
    """Dispatch one create subcommand."""
    if args.create_command == "workspace":
        return create_workspace(args.path, dependencies=dependencies)
    raise FlowValidationError(f"Unknown create command: {args.create_command}")


def create_workspace(path: Path, *, dependencies) -> int:
    """Create one authored workspace scaffold and select it as default."""
    target = path.expanduser().resolve()
    workspace_id = validate_workspace_id(target.name)
    if target.exists():
        if not target.is_dir():
            raise FlowValidationError(f"Workspace path is not a directory: {target}")
        if any(target.iterdir()):
            raise FlowValidationError(f"Refusing to create workspace in a non-empty directory: {target}")
    target.mkdir(parents=True, exist_ok=True)
    for child in (
        WORKSPACE_FLOW_MODULES_DIR_NAME,
        f"{WORKSPACE_FLOW_MODULES_DIR_NAME}/{WORKSPACE_FLOW_HELPERS_DIR_NAME}",
        WORKSPACE_CONFIG_DIR_NAME,
        WORKSPACE_DATABASES_DIR_NAME,
    ):
        (target / child).mkdir(parents=True, exist_ok=True)
    write_collection_vscode_settings(target.parent, dependencies=dependencies)
    write_workspace_vscode_settings(target, dependencies=dependencies)
    settings = dependencies.app_state_policy.load_settings()
    dependencies.app_state_policy.write_settings(
        WorkspaceSettings(
            app_root=settings.app_root,
            settings_path=settings.settings_path,
            state_root=settings.state_root,
            runtime_root=settings.runtime_root,
            workspace_collection_root=target.parent,
            default_selected=workspace_id,
        )
    )
    print(f"Created workspace: {target}")
    print(f"Selected default workspace: {workspace_id}")
    return 0


def workspace_vscode_settings(workspace_root: Path, *, app_root: Path) -> dict[str, object]:
    """Return VS Code settings for one workspace, with dev extras only for checkout roots."""
    return build_workspace_vscode_settings(workspace_root, app_root=app_root)


def collection_vscode_settings(collection_root: Path, *, app_root: Path) -> dict[str, object]:
    """Return VS Code settings for one workspace collection root."""
    return build_collection_vscode_settings(collection_root, app_root=app_root)


def write_collection_vscode_settings(collection_root: Path, *, dependencies) -> None:
    """Write the collection-root VS Code settings file."""
    app_root = dependencies.app_state_policy.load_settings().app_root
    persist_collection_vscode_settings(collection_root, app_root=app_root, overwrite=True)


def write_workspace_vscode_settings(workspace_root: Path, *, dependencies) -> None:
    """Write the workspace-local VS Code settings file."""
    app_root = dependencies.app_state_policy.load_settings().app_root
    persist_workspace_vscode_settings(workspace_root, app_root=app_root, overwrite=True)


__all__ = [
    "collection_vscode_settings",
    "create_command",
    "create_workspace",
    "write_collection_vscode_settings",
    "workspace_vscode_settings",
    "write_workspace_vscode_settings",
]
