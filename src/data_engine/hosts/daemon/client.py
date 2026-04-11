"""Daemon transport, liveness, and startup helpers."""

from __future__ import annotations

import ctypes
from datetime import UTC, datetime
import getpass
import hashlib
import json
import os
from multiprocessing import AuthenticationError
from multiprocessing.connection import Client
from pathlib import Path
import secrets
import subprocess
import sys
import time
from typing import Any

from data_engine.domain import DaemonLifecyclePolicy
from data_engine.domain.time import parse_utc_text
from data_engine.hosts.daemon.constants import (
    CHECKPOINT_INTERVAL_SECONDS,
    DAEMON_STARTUP_LOCK_STALE_SECONDS,
    STALE_AFTER_SECONDS,
)
from data_engine.hosts.daemon.shared_state import DaemonSharedStateAdapter
from data_engine.platform.processes import (
    ProcessInspectionError,
    force_kill_process_tree,
    process_is_running,
    windows_subprocess_creationflags,
)
from data_engine.platform.workspace_models import WorkspacePaths, machine_id_text


class DaemonClientError(RuntimeError):
    """Raised when local daemon communication fails."""


class WorkspaceLeaseError(RuntimeError):
    """Raised when a workspace cannot be claimed."""


DAEMON_AUTHKEY_FILE_NAME = ".daemon-authkey"
_SHARED_STATE_ADAPTER = DaemonSharedStateAdapter()
_WINDOWS_ERROR_ALREADY_EXISTS = 183
_WINDOWS_STARTUP_MUTEXES: dict[str, int] = {}


def endpoint_address(paths: WorkspacePaths) -> str:
    """Return the Listener/Client address for one workspace."""
    return paths.daemon_endpoint_path


def endpoint_family(paths: WorkspacePaths) -> str:
    """Return the multiprocessing.connection family for one workspace."""
    return "AF_PIPE" if paths.daemon_endpoint_kind == "pipe" else "AF_UNIX"


def _daemon_authkey_path(paths: WorkspacePaths) -> Path:
    """Return the per-workspace local daemon authkey path."""
    return paths.runtime_state_dir / DAEMON_AUTHKEY_FILE_NAME


def daemon_authkey(paths: WorkspacePaths) -> bytes:
    """Load or create the per-workspace daemon authkey."""
    authkey_path = _daemon_authkey_path(paths)
    authkey_path.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            token = authkey_path.read_text(encoding="ascii").strip()
        except FileNotFoundError:
            authkey = secrets.token_bytes(32)
            try:
                fd = os.open(authkey_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            except FileExistsError:
                continue
            with os.fdopen(fd, "w", encoding="ascii") as handle:
                handle.write(authkey.hex())
            _harden_private_file_permissions(authkey_path)
            return authkey
        if not token:
            try:
                authkey_path.unlink()
            except FileNotFoundError:
                pass
            continue
        return bytes.fromhex(token)


def _encode_message(payload: dict[str, Any]) -> bytes:
    """Encode one daemon message as UTF-8 JSON bytes."""
    if not isinstance(payload, dict):
        raise DaemonClientError("Daemon payload must be a JSON object.")
    try:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise DaemonClientError("Daemon payload is not JSON serializable.") from exc


def _decode_message(raw: bytes) -> dict[str, Any]:
    """Decode one UTF-8 JSON daemon message."""
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DaemonClientError("Daemon returned an invalid message.") from exc
    if not isinstance(payload, dict):
        raise DaemonClientError("Daemon returned an invalid response.")
    return payload


def daemon_request(paths: WorkspacePaths, payload: dict[str, Any], *, timeout: float = 5.0) -> dict[str, Any]:
    """Send one request to the local workspace daemon and return its response."""
    try:
        with Client(
            endpoint_address(paths),
            family=endpoint_family(paths),
            authkey=daemon_authkey(paths),
        ) as connection:
            connection.send_bytes(_encode_message(payload))
            if timeout > 0:
                deadline = time.monotonic() + timeout
                while not connection.poll(0.05):
                    if time.monotonic() >= deadline:
                        raise DaemonClientError("Timed out waiting for daemon response.")
            response = _decode_message(connection.recv_bytes())
    except (AuthenticationError, EOFError, FileNotFoundError, ConnectionRefusedError, OSError) as exc:
        raise DaemonClientError("Daemon is not reachable.") from exc
    return response


def is_daemon_live(paths: WorkspacePaths) -> bool:
    """Return whether a local daemon is reachable for one workspace."""
    try:
        response = daemon_request(paths, {"command": "daemon_ping"}, timeout=1.0)
    except DaemonClientError:
        return False
    return bool(response.get("ok"))


def _kill_pid(pid: int) -> None:
    """Forcefully terminate one local process id."""
    try:
        force_kill_process_tree(pid)
    except ProcessInspectionError as exc:
        raise DaemonClientError(str(exc)) from exc


def _harden_private_file_permissions(path: Path) -> None:
    """Best-effort hardening for one private local file."""
    if os.name != "nt":
        return
    username = os.environ.get("USERNAME") or getpass.getuser()
    if not username.strip():
        return
    try:
        subprocess.run(
            [
                "icacls",
                str(path),
                "/inheritance:r",
                "/grant:r",
                f"{username}:(F)",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError:
        return


def _pid_is_live(pid: int | None) -> bool:
    """Return whether one OS process id currently exists."""
    return process_is_running(pid)


def _same_machine_unreachable_lease_metadata(paths: WorkspacePaths) -> dict[str, Any] | None:
    """Return lease metadata when the workspace is leased locally but IPC is unavailable."""
    metadata = _SHARED_STATE_ADAPTER.read_lease_metadata(paths)
    if metadata is None:
        return None
    owner = metadata.get("machine_id")
    if not isinstance(owner, str) or owner.strip() != machine_id_text():
        return None
    if is_daemon_live(paths):
        return None
    return metadata


def _same_machine_live_lease_process(paths: WorkspacePaths) -> int | None:
    """Return the owning local lease pid when one is still alive."""
    metadata = _SHARED_STATE_ADAPTER.read_lease_metadata(paths)
    if metadata is None:
        return None
    owner = metadata.get("machine_id")
    if not isinstance(owner, str) or owner.strip() != machine_id_text():
        return None
    pid_value = metadata.get("pid")
    try:
        pid = int(pid_value)
    except (TypeError, ValueError):
        return None
    if pid <= 0 or not _pid_is_live(pid):
        return None
    return pid


def _same_machine_lease_pid(paths: WorkspacePaths) -> int | None:
    """Return the owning local lease pid when one is recorded."""
    metadata = _SHARED_STATE_ADAPTER.read_lease_metadata(paths)
    if metadata is None:
        return None
    owner = metadata.get("machine_id")
    if not isinstance(owner, str) or owner.strip() != machine_id_text():
        return None
    try:
        pid = int(metadata.get("pid"))
    except (TypeError, ValueError):
        return None
    return pid if pid > 0 else None


def _reachable_daemon_pid(paths: WorkspacePaths) -> int | None:
    """Return the daemon pid when the local daemon answers status requests."""
    try:
        response = daemon_request(paths, {"command": "daemon_status"}, timeout=0.5)
    except DaemonClientError:
        return None
    status = response.get("status")
    if not isinstance(status, dict):
        return None
    try:
        pid = int(status.get("pid"))
    except (TypeError, ValueError):
        return None
    return pid if pid > 0 else None


def _cleanup_forced_shutdown(paths: WorkspacePaths) -> None:
    """Best-effort cleanup after a forced daemon termination."""
    try:
        _SHARED_STATE_ADAPTER.remove_lease_metadata(paths)
    except Exception:
        pass
    try:
        _remove_stale_unix_endpoint(paths)
    except Exception:
        pass


def _lease_checkpoint_age_seconds(metadata: dict[str, Any]) -> float | None:
    """Return the age in seconds of one lease checkpoint timestamp when available."""
    checkpoint = parse_utc_text(str(metadata.get("last_checkpoint_at_utc")))
    if checkpoint is None:
        return None
    return max((datetime.now(UTC) - checkpoint).total_seconds(), 0.0)


def _wait_for_fresh_local_daemon(paths: WorkspacePaths) -> bool:
    """Give one recently checked-in same-machine daemon a brief chance to answer."""
    metadata = _same_machine_unreachable_lease_metadata(paths)
    if metadata is None:
        return False
    age_seconds = _lease_checkpoint_age_seconds(metadata)
    if age_seconds is None or age_seconds >= CHECKPOINT_INTERVAL_SECONDS:
        return False
    deadline = time.monotonic() + min(2.0, max(CHECKPOINT_INTERVAL_SECONDS - age_seconds, 0.0))
    while time.monotonic() < deadline:
        if is_daemon_live(paths):
            return True
        time.sleep(0.1)
    return is_daemon_live(paths)


def _should_force_recover_local_lease(paths: WorkspacePaths) -> bool:
    """Return whether an unreachable same-machine lease is stale enough to reclaim."""
    metadata = _same_machine_unreachable_lease_metadata(paths)
    if metadata is None:
        return False
    return _SHARED_STATE_ADAPTER.lease_is_stale(paths, stale_after_seconds=STALE_AFTER_SECONDS)


def _recover_broken_local_lease(paths: WorkspacePaths) -> bool:
    """Recover one unreachable same-machine lease after it becomes stale."""
    return _SHARED_STATE_ADAPTER.recover_stale_workspace(
        paths,
        machine_id=machine_id_text(),
        stale_after_seconds=STALE_AFTER_SECONDS,
        reclaim=False,
    )


def _remove_stale_unix_endpoint(paths: WorkspacePaths) -> None:
    """Delete one dead Unix socket file before binding a new daemon listener."""
    if paths.daemon_endpoint_kind != "unix":
        return
    endpoint_path = Path(paths.daemon_endpoint_path)
    if not endpoint_path.exists():
        return
    if is_daemon_live(paths):
        return
    try:
        endpoint_path.unlink()
    except FileNotFoundError:
        pass


def _startup_lock_path(paths: WorkspacePaths) -> Path:
    """Return the per-workspace local startup lock path."""
    return paths.runtime_state_dir / ".daemon-start.lock"


def _windows_startup_mutex_name(paths: WorkspacePaths) -> str:
    """Return the per-workspace Windows startup mutex name."""
    digest = hashlib.sha1(endpoint_address(paths).encode("utf-8")).hexdigest()[:12]
    return f"Local\\data_engine_startup_{paths.workspace_id}_{digest}"


def _configure_ctypes_function(func: Any, *, argtypes: list[Any], restype: Any) -> None:
    """Best-effort ctypes metadata setup for real Win32 callables and simple test doubles."""
    try:
        func.argtypes = argtypes
        func.restype = restype
    except AttributeError:
        pass


def _acquire_startup_lock(paths: WorkspacePaths) -> bool:
    """Try to acquire the per-workspace daemon startup lock."""
    if os.name == "nt":
        mutex_name = _windows_startup_mutex_name(paths)
        kernel32 = ctypes.windll.kernel32
        _configure_ctypes_function(
            kernel32.CreateMutexW,
            argtypes=[ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p],
            restype=ctypes.c_void_p,
        )
        _configure_ctypes_function(kernel32.GetLastError, argtypes=[], restype=ctypes.c_ulong)
        handle = kernel32.CreateMutexW(None, False, mutex_name)
        if not handle:
            return False
        if kernel32.GetLastError() == _WINDOWS_ERROR_ALREADY_EXISTS:
            kernel32.CloseHandle(handle)
            return False
        _WINDOWS_STARTUP_MUTEXES[mutex_name] = handle
        return True
    lock_path = _startup_lock_path(paths)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                age_seconds = time.time() - lock_path.stat().st_mtime
            except FileNotFoundError:
                continue
            if age_seconds > DAEMON_STARTUP_LOCK_STALE_SECONDS:
                try:
                    lock_path.unlink()
                except FileNotFoundError:
                    pass
                continue
            return False
        else:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(str(os.getpid()))
            return True


def _release_startup_lock(paths: WorkspacePaths) -> None:
    """Release the per-workspace daemon startup lock when held."""
    if os.name == "nt":
        mutex_name = _windows_startup_mutex_name(paths)
        handle = _WINDOWS_STARTUP_MUTEXES.pop(mutex_name, None)
        if handle is None:
            return
        kernel32 = ctypes.windll.kernel32
        _configure_ctypes_function(kernel32.ReleaseMutex, argtypes=[ctypes.c_void_p], restype=ctypes.c_int)
        _configure_ctypes_function(kernel32.CloseHandle, argtypes=[ctypes.c_void_p], restype=ctypes.c_int)
        try:
            kernel32.ReleaseMutex(handle)
        finally:
            kernel32.CloseHandle(handle)
        return
    try:
        _startup_lock_path(paths).unlink()
    except FileNotFoundError:
        pass


def _wait_for_daemon_live(paths: WorkspacePaths, *, timeout_seconds: float) -> bool:
    """Wait for one workspace daemon to become reachable."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if is_daemon_live(paths):
            return True
        time.sleep(0.1)
    return is_daemon_live(paths)


def spawn_daemon_process(
    paths: WorkspacePaths,
    *,
    lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT,
) -> int:
    """Start the daemon process in the background for one workspace."""
    lifecycle_policy = DaemonLifecyclePolicy.coerce(lifecycle_policy)
    if is_daemon_live(paths):
        return 0
    if _wait_for_fresh_local_daemon(paths):
        return 0
    local_pid = _same_machine_live_lease_process(paths)
    if local_pid is not None:
        if _wait_for_daemon_live(paths, timeout_seconds=2.0):
            return 0
        raise DaemonClientError(f"Local daemon process {local_pid} already owns this workspace.")
    if _should_force_recover_local_lease(paths):
        _recover_broken_local_lease(paths)
        if is_daemon_live(paths):
            return 0
    elif _same_machine_unreachable_lease_metadata(paths) is not None:
        raise DaemonClientError("This workstation already has control, but the local daemon is not responding yet.")
    acquired = _acquire_startup_lock(paths)
    if not acquired:
        if _wait_for_daemon_live(paths, timeout_seconds=10.0):
            return 0
        raise DaemonClientError("Timed out waiting for daemon startup.")
    command = [
        sys.executable,
        "-m",
        "data_engine.hosts.daemon.app",
        "--app-root",
        str(paths.app_root),
        "--workspace",
        str(paths.workspace_root),
        "--lifecycle-policy",
        lifecycle_policy.value,
    ]
    kwargs: dict[str, Any] = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "stdin": subprocess.DEVNULL,
        "close_fds": True,
    }
    if os.name == "nt":
        kwargs["creationflags"] = windows_subprocess_creationflags(new_process_group=True, no_window=True)
    else:
        kwargs["start_new_session"] = True
    try:
        subprocess.Popen(command, **kwargs)
        if _wait_for_daemon_live(paths, timeout_seconds=10.0):
            return 0
        raise DaemonClientError("Timed out waiting for daemon startup.")
    finally:
        _release_startup_lock(paths)


def force_shutdown_daemon_process(paths: WorkspacePaths, *, timeout: float = 0.5) -> None:
    """Stop the local workspace daemon, escalating to an OS kill when needed."""
    pid = _reachable_daemon_pid(paths) or _same_machine_lease_pid(paths)
    if pid is None:
        if not is_daemon_live(paths):
            _cleanup_forced_shutdown(paths)
            return
        raise DaemonClientError("Local daemon is reachable, but its process id is unavailable.")
    try:
        daemon_request(paths, {"command": "shutdown_daemon"}, timeout=timeout)
    except DaemonClientError:
        pass
    graceful_deadline = time.monotonic() + max(timeout, 0.0)
    while time.monotonic() < graceful_deadline:
        if not _pid_is_live(pid):
            _cleanup_forced_shutdown(paths)
            return
        time.sleep(0.05)
    if _pid_is_live(pid):
        try:
            _kill_pid(pid)
        except OSError as exc:
            if _pid_is_live(pid):
                raise DaemonClientError(f"Failed to terminate local daemon process {pid}.") from exc
    kill_deadline = time.monotonic() + 2.0
    while time.monotonic() < kill_deadline:
        if not _pid_is_live(pid):
            _cleanup_forced_shutdown(paths)
            return
        time.sleep(0.05)
    raise DaemonClientError(f"Failed to stop local daemon process {pid}.")


__all__ = [
    "DAEMON_AUTHKEY_FILE_NAME",
    "DaemonClientError",
    "WorkspaceLeaseError",
    "_acquire_startup_lock",
    "_decode_message",
    "_encode_message",
    "_lease_checkpoint_age_seconds",
    "_pid_is_live",
    "_reachable_daemon_pid",
    "_recover_broken_local_lease",
    "_release_startup_lock",
    "_remove_stale_unix_endpoint",
    "_same_machine_lease_pid",
    "_same_machine_live_lease_process",
    "_same_machine_unreachable_lease_metadata",
    "_should_force_recover_local_lease",
    "_startup_lock_path",
    "_wait_for_daemon_live",
    "_wait_for_fresh_local_daemon",
    "daemon_authkey",
    "daemon_request",
    "endpoint_address",
    "endpoint_family",
    "force_shutdown_daemon_process",
    "is_daemon_live",
    "spawn_daemon_process",
]
