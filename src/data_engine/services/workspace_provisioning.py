"""Workspace provisioning helpers shared by CLI and GUI surfaces."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path

from data_engine.platform.interpreters import console_python_executable
from data_engine.platform.workspace_models import (
    DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR,
    WORKSPACE_FLOW_HELPERS_DIR_NAME,
    WorkspacePaths,
    validate_workspace_id,
)


def checkout_source_dir(app_root: Path) -> Path | None:
    """Return the repo-local source directory when app_root points at a checkout."""
    src_dir = app_root / "src"
    return src_dir if src_dir.is_dir() else None


def checkout_tests_dir(app_root: Path) -> Path | None:
    """Return the repo-local tests directory when app_root points at a checkout."""
    tests_dir = app_root / "tests"
    return tests_dir if tests_dir.is_dir() else None


def _vscode_interpreter_path(*, settings_root: Path, app_root: Path, interpreter_path: Path | None = None) -> str:
    """Return the interpreter executable path backing the running Data Engine environment."""
    del settings_root, app_root
    candidate = console_python_executable(interpreter_path or sys.executable)
    try:
        return str(candidate.resolve())
    except Exception:
        return str(candidate)


def workspace_vscode_settings(
    workspace_root: Path,
    *,
    app_root: Path,
    interpreter_path: Path | None = None,
) -> dict[str, object]:
    """Return VS Code settings for one workspace root."""
    workspace_id = validate_workspace_id(workspace_root.name)
    terminal_env = {
        "DATA_ENGINE_APP_ROOT": str(app_root),
        "DATA_ENGINE_WORKSPACE_ROOT": str(workspace_root),
        "DATA_ENGINE_WORKSPACE_ID": workspace_id,
    }
    settings: dict[str, object] = {
        "python.defaultInterpreterPath": _vscode_interpreter_path(
            settings_root=workspace_root,
            app_root=app_root,
            interpreter_path=interpreter_path,
        ),
        "files.exclude": {".workspace_state": True},
        "search.exclude": {".workspace_state": True},
        "terminal.integrated.env.linux": terminal_env,
        "terminal.integrated.env.osx": terminal_env,
        "terminal.integrated.env.windows": terminal_env,
    }
    src_dir = checkout_source_dir(app_root)
    if src_dir is not None:
        settings["python.analysis.extraPaths"] = [str(src_dir)]
    tests_dir = checkout_tests_dir(app_root)
    if tests_dir is not None:
        settings["python.testing.pytestEnabled"] = True
        settings["python.testing.pytestArgs"] = [str(tests_dir)]
    return settings


def collection_vscode_settings(
    collection_root: Path,
    *,
    app_root: Path,
    interpreter_path: Path | None = None,
) -> dict[str, object]:
    """Return VS Code settings for one workspace collection root."""
    terminal_env = {
        "DATA_ENGINE_APP_ROOT": str(app_root),
        DATA_ENGINE_WORKSPACE_COLLECTION_ROOT_ENV_VAR: str(collection_root),
    }
    settings: dict[str, object] = {
        "python.defaultInterpreterPath": _vscode_interpreter_path(
            settings_root=collection_root,
            app_root=app_root,
            interpreter_path=interpreter_path,
        ),
        "files.exclude": {"**/.workspace_state": True},
        "search.exclude": {"**/.workspace_state": True},
        "terminal.integrated.env.linux": terminal_env,
        "terminal.integrated.env.osx": terminal_env,
        "terminal.integrated.env.windows": terminal_env,
    }
    src_dir = checkout_source_dir(app_root)
    if src_dir is not None:
        settings["python.analysis.extraPaths"] = [str(src_dir)]
    tests_dir = checkout_tests_dir(app_root)
    if tests_dir is not None:
        settings["python.testing.pytestEnabled"] = True
        settings["python.testing.pytestArgs"] = [str(tests_dir)]
    return settings


def write_workspace_vscode_settings(
    workspace_root: Path,
    *,
    app_root: Path,
    interpreter_path: Path | None = None,
    overwrite: bool = False,
) -> Path | None:
    """Write workspace-local VS Code settings unless an existing file should be preserved."""
    settings_path = workspace_root / ".vscode" / "settings.json"
    if settings_path.exists() and not overwrite:
        return None
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text(
        json.dumps(
            workspace_vscode_settings(
                workspace_root,
                app_root=app_root,
                interpreter_path=interpreter_path,
            ),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return settings_path


def write_collection_vscode_settings(
    collection_root: Path,
    *,
    app_root: Path,
    interpreter_path: Path | None = None,
    overwrite: bool = False,
) -> Path | None:
    """Write collection-root VS Code settings unless an existing file should be preserved."""
    settings_path = collection_root / ".vscode" / "settings.json"
    if settings_path.exists() and not overwrite:
        return None
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text(
        json.dumps(
            collection_vscode_settings(
                collection_root,
                app_root=app_root,
                interpreter_path=interpreter_path,
            ),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return settings_path


@dataclass(frozen=True)
class WorkspaceProvisioningResult:
    """Describe which workspace assets were created during provisioning."""

    workspace_root: Path
    created_paths: tuple[Path, ...]
    preserved_paths: tuple[Path, ...]

    @property
    def created_anything(self) -> bool:
        """Return whether provisioning created any new files or directories."""
        return bool(self.created_paths)


class WorkspaceProvisioningService:
    """Own safe workspace-folder provisioning for operator surfaces."""

    def provision_workspace(
        self,
        workspace_paths: WorkspacePaths,
        *,
        interpreter_path: Path | None = None,
    ) -> WorkspaceProvisioningResult:
        """Provision missing authored-workspace folders without overwriting existing content."""
        created_paths: list[Path] = []
        preserved_paths: list[Path] = []
        workspace_root = workspace_paths.workspace_root
        if workspace_root.exists() and not workspace_root.is_dir():
            raise ValueError(f"Workspace path is not a directory: {workspace_root}")
        if not workspace_root.exists():
            workspace_root.mkdir(parents=True, exist_ok=True)
            created_paths.append(workspace_root)
        collection_settings_path = write_collection_vscode_settings(
            workspace_paths.workspace_collection_root,
            app_root=workspace_paths.app_root,
            interpreter_path=interpreter_path,
            overwrite=False,
        )
        if collection_settings_path is None:
            preserved_paths.append(workspace_paths.workspace_collection_root / ".vscode" / "settings.json")
        else:
            created_paths.append(collection_settings_path)
        for directory in (
            workspace_paths.flow_modules_dir,
            workspace_paths.flow_modules_dir / WORKSPACE_FLOW_HELPERS_DIR_NAME,
            workspace_paths.config_dir,
            workspace_paths.databases_dir,
        ):
            if directory.exists():
                preserved_paths.append(directory)
                continue
            directory.mkdir(parents=True, exist_ok=True)
            created_paths.append(directory)
        settings_path = write_workspace_vscode_settings(
            workspace_root,
            app_root=workspace_paths.app_root,
            interpreter_path=interpreter_path,
            overwrite=False,
        )
        if settings_path is None:
            preserved_paths.append(workspace_root / ".vscode" / "settings.json")
        else:
            created_paths.append(settings_path)
        return WorkspaceProvisioningResult(
            workspace_root=workspace_root,
            created_paths=tuple(created_paths),
            preserved_paths=tuple(preserved_paths),
        )


__all__ = [
    "WorkspaceProvisioningResult",
    "WorkspaceProvisioningService",
    "_vscode_interpreter_path",
    "collection_vscode_settings",
    "checkout_source_dir",
    "checkout_tests_dir",
    "write_collection_vscode_settings",
    "workspace_vscode_settings",
    "write_workspace_vscode_settings",
]
