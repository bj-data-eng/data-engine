"""Module entrypoints for launching one workspace daemon process."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Callable

from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.server import serve_workspace_daemon as serve_daemon_process
from data_engine.platform.workspace_models import (
    DATA_ENGINE_APP_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ID_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR,
)
from data_engine.services import WorkspaceService


def default_workspace_service_factory() -> WorkspaceService:
    """Build the default workspace-service collaborator for daemon entrypoints."""
    return WorkspaceService()


def serve_workspace_daemon(
    service_type,
    *,
    workspace_root: Path | None = None,
    workspace_id: str | None = None,
    lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT,
    workspace_service: WorkspaceService | None = None,
    resolve_paths_func=None,
) -> int:
    """Start serving one workspace daemon in the current process."""
    return serve_daemon_process(
        service_type,
        workspace_root=workspace_root,
        workspace_id=workspace_id,
        lifecycle_policy=lifecycle_policy,
        workspace_service=workspace_service,
        resolve_paths_func=resolve_paths_func,
    )


def build_parser() -> argparse.ArgumentParser:
    """Build the daemon module parser."""
    parser = argparse.ArgumentParser(description="Run one Data Engine workspace daemon.")
    parser.add_argument("--workspace", type=Path, required=True, help="Authored workspace root to host.")
    parser.add_argument("--app-root", type=Path, default=None, help="Data Engine app root used for local artifacts.")
    parser.add_argument("--workspace-id", default=None, help="Explicit workspace id override.")
    parser.add_argument(
        "--lifecycle-policy",
        choices=tuple(policy.value for policy in DaemonLifecyclePolicy),
        default=DaemonLifecyclePolicy.PERSISTENT.value,
        help="Daemon lifetime policy.",
    )
    return parser


def main(
    service_type,
    argv: list[str] | None = None,
    *,
    workspace_service: WorkspaceService | None = None,
    workspace_service_factory: Callable[[], WorkspaceService] | None = None,
    resolve_paths_func=None,
    serve_workspace_daemon_func=None,
) -> int:
    """Run the daemon module entrypoint for one concrete daemon service type."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.app_root is not None:
        os.environ[DATA_ENGINE_APP_ROOT_ENV_VAR] = str(args.app_root.expanduser().resolve())
    os.environ[DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR] = str(args.workspace.expanduser().resolve())
    if args.workspace_id:
        os.environ[DATA_ENGINE_WORKSPACE_ID_ENV_VAR] = args.workspace_id
    if resolve_paths_func is None:
        workspace_service = workspace_service or (workspace_service_factory or default_workspace_service_factory)()
        resolve_paths_func = workspace_service.resolve_paths
    paths = resolve_paths_func(workspace_root=args.workspace, workspace_id=args.workspace_id)
    serve_workspace_daemon_func = serve_workspace_daemon_func or serve_workspace_daemon
    return serve_workspace_daemon_func(
        service_type,
        workspace_root=paths.workspace_root,
        workspace_id=paths.workspace_id,
        lifecycle_policy=args.lifecycle_policy,
        workspace_service=workspace_service,
        resolve_paths_func=resolve_paths_func,
    )


__all__ = [
    "build_parser",
    "default_workspace_service_factory",
    "main",
    "serve_workspace_daemon",
]
