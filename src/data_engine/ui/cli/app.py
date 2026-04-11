"""Operator-focused CLI surface."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from data_engine.core.model import FlowValidationError
from data_engine.platform.workspace_models import (
    DATA_ENGINE_APP_ROOT_ENV_VAR,
    DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR,
    InvalidWorkspaceIdError,
    machine_id_text,
)
from data_engine.ui.cli import commands_doctor as _commands_doctor
from data_engine.ui.cli import commands_run as _commands_run
from data_engine.ui.cli.commands_start import (
    launch_desktop_ui as _launch_desktop_ui,
    launch_terminal_ui as _launch_terminal_ui,
    preferred_gui_python_executable as _preferred_gui_python_executable,
    start_gui_subprocess as _start_gui_subprocess,
    start_surface as _start_surface,
)
from data_engine.ui.cli.commands_workspace import (
    create_command as _create_command,
    workspace_vscode_settings as _workspace_vscode_settings,
)
from data_engine.ui.cli.dependencies import (
    CliDependencies,
    CliDependencyFactories,
    build_default_cli_dependencies,
    default_cli_dependency_factories,
)
from data_engine.ui.cli.parser import _HelpFormatter, build_parser


def _default_cli_dependencies() -> CliDependencies:
    return build_default_cli_dependencies()


def main(argv: list[str] | None = None, *, dependencies: CliDependencies | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    dependencies = dependencies or _default_cli_dependencies()
    _apply_environment(args)

    try:
        if args.command == "start":
            return _start_surface(args.start_command)
        if args.command == "create":
            return _create_command(args, dependencies=dependencies)
        if args.command == "run":
            return _run_command(args, dependencies=dependencies)
        if args.command == "doctor":
            return _doctor_command(args, dependencies=dependencies)
    except (FlowValidationError, InvalidWorkspaceIdError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Stopped.", file=sys.stderr)
        return 130
    parser.error(f"Unknown command: {args.command}")
    return 2


def _apply_environment(args: argparse.Namespace) -> None:
    if args.app_root is not None:
        os.environ[DATA_ENGINE_APP_ROOT_ENV_VAR] = str(args.app_root.expanduser().resolve())
    else:
        inferred = _infer_project_root_from_cwd(Path.cwd())
        if inferred is not None:
            os.environ.setdefault(DATA_ENGINE_APP_ROOT_ENV_VAR, str(inferred))
    if args.workspace is not None:
        os.environ[DATA_ENGINE_WORKSPACE_ROOT_ENV_VAR] = str(args.workspace.expanduser().resolve())


def _infer_project_root_from_cwd(cwd: Path) -> Path | None:
    candidate = cwd.expanduser().resolve()
    if (candidate / "pyproject.toml").is_file() and (candidate / "src" / "data_engine").is_dir():
        return candidate
    return None


def _run_command(args: argparse.Namespace, *, dependencies: CliDependencies) -> int:
    if args.run_command == "tests":
        return _run_tests(slice_name=args.slice, list_slices=args.list_slices, dependencies=dependencies)
    raise FlowValidationError(f"Unknown run command: {args.run_command}")


def _test_slice_args(slice_name: str, *, app_root: Path) -> tuple[str, ...]:
    return _commands_run.test_slice_args(slice_name, app_root=app_root)


def _run_tests(*, slice_name: str, list_slices: bool, dependencies: CliDependencies) -> int:
    app_root = dependencies.app_state_policy.load_settings().app_root
    return _commands_run.run_tests(slice_name=slice_name, list_slices=list_slices, app_root=app_root)


def _doctor(*, dependencies: CliDependencies) -> int:
    settings = dependencies.app_state_policy.load_settings()
    paths = dependencies.workspace_service.resolve_paths()
    return _commands_doctor.doctor(settings=settings, paths=paths)


def _doctor_command(args: argparse.Namespace, *, dependencies: CliDependencies) -> int:
    if getattr(args, "doctor_command", None) == "daemons":
        return _doctor_daemons(dependencies=dependencies)
    return _doctor(dependencies=dependencies)


def _run_process_listing():
    return _commands_doctor.run_process_listing()


def _classify_process_kind(command: str) -> str | None:
    return _commands_doctor.classify_process_kind(command)

def _doctor_daemons(*, dependencies: CliDependencies) -> int:
    settings = dependencies.app_state_policy.load_settings()
    return _commands_doctor.doctor_daemons(
        settings=settings,
        workspace_service=dependencies.workspace_service,
        process_listing_func=_run_process_listing,
        classify_process_kind_func=_classify_process_kind,
        read_lease_metadata_func=dependencies.shared_state_service.read_lease_metadata,
        lease_is_stale_func=lambda paths, stale_after_seconds: dependencies.shared_state_service.lease_is_stale(
            paths,
            stale_after_seconds=stale_after_seconds,
        ),
        machine_id_text_func=machine_id_text,
    )


__all__ = [
    "CliDependencies",
    "CliDependencyFactories",
    "_HelpFormatter",
    "_apply_environment",
    "_classify_process_kind",
    "_doctor",
    "_doctor_command",
    "_doctor_daemons",
    "_infer_project_root_from_cwd",
    "_launch_desktop_ui",
    "_launch_terminal_ui",
    "_preferred_gui_python_executable",
    "_run_command",
    "_run_process_listing",
    "_run_tests",
    "_start_gui_subprocess",
    "_start_surface",
    "_test_slice_args",
    "_workspace_vscode_settings",
    "build_default_cli_dependencies",
    "build_parser",
    "default_cli_dependency_factories",
    "main",
]
