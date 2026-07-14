"""Containment-first daemon launcher with a gated POSIX second stage."""

from __future__ import annotations

from collections.abc import Callable
import json
import os
import select
import sys
from typing import Any


_PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_BOOTSTRAP_PATH = os.path.abspath(__file__)
_BOOTSTRAP_DIRECTORY = os.path.dirname(_BOOTSTRAP_PATH)
_LAUNCH_READY_FD_OPTION = "--launch-ready-fd"
_LAUNCH_RELEASE_FD_OPTION = "--launch-release-fd"
_ARMED_WATCHDOG_PID_OPTION = "--armed-watchdog-pid"
_LAUNCH_RELEASE_TIMEOUT_SECONDS = 30.0
_MAX_LAUNCH_IDENTITY_BYTES = 16_384


def _containment_nonce_from_argv(argv: list[str]) -> str:
    values: list[str] = []
    index = 0
    while index < len(argv):
        argument = argv[index]
        if argument == "--containment-nonce":
            if index + 1 >= len(argv):
                break
            values.append(argv[index + 1])
            index += 2
            continue
        prefix = "--containment-nonce="
        if argument.startswith(prefix):
            values.append(argument.removeprefix(prefix))
        index += 1
    if len(values) != 1:
        raise SystemExit("The daemon bootstrap requires exactly one --containment-nonce.")
    return values[0]


def _extract_internal_integer_option(
    argv: list[str],
    option: str,
) -> tuple[int | None, list[str]]:
    values: list[str] = []
    remaining: list[str] = []
    index = 0
    prefix = f"{option}="
    while index < len(argv):
        argument = argv[index]
        if argument == option:
            if index + 1 >= len(argv):
                raise SystemExit(f"The daemon bootstrap requires a value for {option}.")
            values.append(argv[index + 1])
            index += 2
            continue
        if argument.startswith(prefix):
            values.append(argument.removeprefix(prefix))
            index += 1
            continue
        remaining.append(argument)
        index += 1
    if len(values) > 1:
        raise SystemExit(f"The daemon bootstrap accepts at most one {option}.")
    if not values:
        return None, remaining
    try:
        value = int(values[0])
    except ValueError as exc:
        raise SystemExit(f"The daemon bootstrap requires an integer for {option}.") from exc
    if value < 0:
        raise SystemExit(f"The daemon bootstrap requires a nonnegative value for {option}.")
    return value, remaining


def _verify_current_windows_containment(containment_nonce: str) -> None:
    from data_engine.platform.processes import (
        ProcessInspectionError,
        inspect_process_identity,
        open_verified_windows_kill_on_close_job,
    )

    identity = inspect_process_identity(os.getpid())
    if identity is None:
        raise ProcessInspectionError(
            "Unable to inspect the current Windows daemon process identity."
        )
    job = open_verified_windows_kill_on_close_job(
        identity,
        nonce=containment_nonce,
    )
    job.close()


def _write_all(fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:
            raise RuntimeError("The daemon launch readiness pipe closed unexpectedly.")
        offset += written


def _publish_launch_identity(ready_fd: int) -> None:
    from data_engine.platform.processes import (
        ProcessInspectionError,
        inspect_process_identity,
    )

    identity = inspect_process_identity(os.getpid())
    if identity is None:
        raise ProcessInspectionError(
            "Unable to inspect the contained daemon bootstrap identity."
        )
    if (
        identity.process_group_id != identity.pid
        or identity.process_session_id != identity.pid
    ):
        raise ProcessInspectionError(
            "The contained daemon bootstrap is outside its dedicated session."
        )
    payload = json.dumps(
        {
            "pid": identity.pid,
            "start_key": identity.start_key,
            "executable_path": identity.executable_path,
            "process_group_id": identity.process_group_id,
            "process_session_id": identity.process_session_id,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(payload) > _MAX_LAUNCH_IDENTITY_BYTES:
        raise ProcessInspectionError("The daemon launch identity is unexpectedly large.")
    _write_all(ready_fd, payload)


def _await_launch_release(release_fd: int) -> None:
    readable, _, _ = select.select(
        [release_fd],
        [],
        [],
        _LAUNCH_RELEASE_TIMEOUT_SECONDS,
    )
    if not readable:
        raise RuntimeError("Timed out waiting for the daemon launcher release.")
    if os.read(release_fd, 1) != b"1":
        raise RuntimeError("The daemon launcher closed before releasing startup.")


def _exec_normal_posix_stage(
    arguments: list[str],
    *,
    watchdog_pid: int,
) -> None:
    exec_arguments = [
        sys.executable,
        "-P",
        "-m",
        "data_engine.daemon_bootstrap",
        _ARMED_WATCHDOG_PID_OPTION,
        str(watchdog_pid),
        *arguments,
    ]
    os.execv(sys.executable, exec_arguments)


def _prepare_isolated_package_import_path() -> None:
    """Expose the package root only for the absolute-script bootstrap stage."""
    bootstrap_directory_identity = os.path.normcase(_BOOTSTRAP_DIRECTORY)
    sys.path[:] = [
        entry
        for entry in sys.path
        if os.path.normcase(os.path.abspath(entry or os.curdir))
        != bootstrap_directory_identity
    ]
    if _PACKAGE_ROOT not in sys.path:
        sys.path.insert(0, _PACKAGE_ROOT)


def main(
    argv: list[str] | None = None,
    *,
    arm_watchdog_func: Callable[..., object] | None = None,
    adopt_watchdog_func: Callable[..., object] | None = None,
    verify_windows_containment_func: Callable[[str], None] | None = None,
    app_main_func: Callable[[list[str]], Any] | None = None,
    exec_normal_stage_func: Callable[..., None] | None = None,
) -> int:
    """Establish platform containment, then import and run the daemon host."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    ready_fd, arguments = _extract_internal_integer_option(
        arguments,
        _LAUNCH_READY_FD_OPTION,
    )
    release_fd, arguments = _extract_internal_integer_option(
        arguments,
        _LAUNCH_RELEASE_FD_OPTION,
    )
    armed_watchdog_pid, arguments = _extract_internal_integer_option(
        arguments,
        _ARMED_WATCHDOG_PID_OPTION,
    )
    if (ready_fd is None) != (release_fd is None):
        raise SystemExit(
            "The daemon bootstrap requires both launch-handshake descriptors."
        )
    if armed_watchdog_pid is not None and ready_fd is not None:
        raise SystemExit(
            "The daemon bootstrap cannot adopt and create a watchdog in one stage."
        )
    containment_nonce = _containment_nonce_from_argv(arguments)
    if os.name == "posix" and armed_watchdog_pid is None:
        _prepare_isolated_package_import_path()

    if os.name == "posix":
        if armed_watchdog_pid is None:
            if arm_watchdog_func is None:
                from data_engine.platform.posix_watchdog import (
                    arm_posix_process_group_watchdog,
                )

                arm_watchdog_func = arm_posix_process_group_watchdog
            watchdog = arm_watchdog_func(containment_nonce=containment_nonce)
            if ready_fd is not None and release_fd is not None:
                try:
                    _publish_launch_identity(ready_fd)
                finally:
                    os.close(ready_fd)
                try:
                    _await_launch_release(release_fd)
                finally:
                    os.close(release_fd)
                watchdog_pid = getattr(watchdog, "pid", None)
                if (
                    isinstance(watchdog_pid, bool)
                    or not isinstance(watchdog_pid, int)
                    or watchdog_pid <= 0
                ):
                    raise RuntimeError(
                        "The armed POSIX watchdog returned an invalid process identifier."
                    )
                (exec_normal_stage_func or _exec_normal_posix_stage)(
                    arguments,
                    watchdog_pid=watchdog_pid,
                )
                raise RuntimeError("The normal daemon interpreter unexpectedly returned.")
        else:
            if adopt_watchdog_func is None:
                from data_engine.platform.posix_watchdog import (
                    adopt_posix_process_group_watchdog,
                )

                adopt_watchdog_func = adopt_posix_process_group_watchdog
            adopt_watchdog_func(
                armed_watchdog_pid,
                containment_nonce=containment_nonce,
            )
    elif os.name == "nt":
        if ready_fd is not None or armed_watchdog_pid is not None:
            raise SystemExit("Windows daemon bootstrap received POSIX launch state.")
        (verify_windows_containment_func or _verify_current_windows_containment)(
            containment_nonce
        )
    if app_main_func is None:
        from data_engine.hosts.daemon.app import main as daemon_main

        app_main_func = daemon_main
    return int(app_main_func(arguments))


if __name__ == "__main__":
    raise SystemExit(main())
