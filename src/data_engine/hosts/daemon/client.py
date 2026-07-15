"""Daemon transport, liveness, and startup helpers."""

from __future__ import annotations

from contextlib import contextmanager
import ctypes
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import errno
import getpass
import hashlib
import json
import math
import os
from multiprocessing import AuthenticationError
from multiprocessing.connection import Client, answer_challenge, deliver_challenge
from pathlib import Path
import secrets
import select
import socket
import sqlite3
import struct
import subprocess
import sys
import threading
import time
from typing import Any

from data_engine.domain import DaemonLifecyclePolicy
from data_engine.domain.time import parse_utc_text
from data_engine.hosts.daemon.constants import (
    APP_VERSION,
    CHECKPOINT_INTERVAL_SECONDS,
    STALE_AFTER_SECONDS,
)
from data_engine.hosts.daemon.shared_state import DaemonSharedStateAdapter
from data_engine.platform.machine_identity import machine_id_text
from data_engine.platform.paths import normalized_path_text, stable_path_identity_text
from data_engine.platform.processes import (
    ProcessIdentity,
    ProcessInspectionError,
    WindowsKillOnCloseJob,
    ensure_windows_containment_job_stopped,
    force_kill_verified_contained_process_tree,
    inspect_process_identity,
    new_process_containment_nonce,
    open_verified_windows_kill_on_close_job,
    process_is_running,
    wait_for_posix_process_group_exit,
    windows_subprocess_creationflags,
)
from data_engine.platform.posix_watchdog import _cleanup_posix_watchdog_endpoint
from data_engine.platform.windows_spawn import spawn_windows_contained_process
from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.runtime.shared_state import (
    WorkspaceLeaseLostError,
    WorkspaceStateCorruptError,
    daemon_process_lease_identity,
)
from data_engine.runtime.runtime_control_store import RuntimeControlLedger


class DaemonClientError(RuntimeError):
    """Raised when local daemon communication fails."""


class WorkspaceLeaseError(RuntimeError):
    """Raised when a workspace cannot be claimed."""


DAEMON_AUTHKEY_FILE_NAME = ".daemon-authkey"
_DAEMON_AUTHKEY_BYTE_LENGTH = 32
_DAEMON_AUTHKEY_LOCK_FILE_NAME = ".daemon-authkey.lock"
_DAEMON_AUTHKEY_LOCK_TIMEOUT_SECONDS = 2.0
_DAEMON_BOOTSTRAP_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "daemon_bootstrap.py")
)
_POSIX_LAUNCH_IDENTITY_TIMEOUT_SECONDS = 5.0
_MAX_POSIX_LAUNCH_IDENTITY_BYTES = 16_384
_MAX_DAEMON_REQUEST_BYTES = 1 * 1024 * 1024
_MAX_DAEMON_RESPONSE_BYTES = 64 * 1024 * 1024
_SHARED_STATE_ADAPTER = DaemonSharedStateAdapter()
_WINDOWS_ERROR_ALREADY_EXISTS = 183
_WINDOWS_STARTUP_MUTEXES: dict[str, int] = {}
_POSIX_STARTUP_LOCK_FDS: dict[Path, int] = {}
_POSIX_STARTUP_LOCKS_LOCK = threading.Lock()
_AUTHKEY_MUTATION_THREAD_LOCK = threading.RLock()
_POSIX_DAEMON_REAPERS_LOCK = threading.Lock()
_POSIX_DAEMON_PROCESSES: dict[ProcessIdentity, subprocess.Popen[bytes]] = {}
_WINDOWS_LAUNCH_JOBS_LOCK = threading.Lock()
_WINDOWS_LAUNCH_JOBS: dict[ProcessIdentity, WindowsKillOnCloseJob] = {}
_HOST_OS_NAME = os.name


def _close_inherited_posix_startup_locks() -> None:
    """Drop inherited flock descriptions in a pure-fork child process."""
    inherited_fds = tuple(_POSIX_STARTUP_LOCK_FDS.values())
    _POSIX_STARTUP_LOCK_FDS.clear()
    for fd in inherited_fds:
        try:
            os.close(fd)
        except OSError:
            pass


def _finish_posix_startup_lock_fork_in_child() -> None:
    try:
        _close_inherited_posix_startup_locks()
    finally:
        _POSIX_STARTUP_LOCKS_LOCK.release()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_POSIX_STARTUP_LOCKS_LOCK.acquire,
        after_in_parent=_POSIX_STARTUP_LOCKS_LOCK.release,
        after_in_child=_finish_posix_startup_lock_fork_in_child,
    )


@dataclass(frozen=True, slots=True)
class _DaemonProcessRecord:
    """Complete daemon incarnation and containment identity."""

    daemon_id: str
    process_identity: ProcessIdentity
    containment_nonce: str


def endpoint_address(paths: WorkspacePaths) -> str:
    """Return the Listener/Client address for one workspace."""
    return paths.daemon_endpoint_path


def endpoint_family(paths: WorkspacePaths) -> str:
    """Return the multiprocessing.connection family for one workspace."""
    return "AF_PIPE" if paths.daemon_endpoint_kind == "pipe" else "AF_UNIX"


def _daemon_authkey_path(paths: WorkspacePaths) -> Path:
    """Return the per-workspace local daemon authkey path."""
    return paths.runtime_state_dir / DAEMON_AUTHKEY_FILE_NAME


def _read_daemon_authkey(authkey_path: Path) -> tuple[bytes | None, bool]:
    """Return a validated daemon authkey and whether its file exists."""
    try:
        token = authkey_path.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        return None, False
    except UnicodeDecodeError:
        return None, True
    try:
        authkey = bytes.fromhex(token)
    except ValueError:
        return None, True
    if len(authkey) != _DAEMON_AUTHKEY_BYTE_LENGTH:
        return None, True
    return authkey, True


def _try_lock_authkey_file(fd: int) -> None:
    """Try to lock the first byte of one authkey lock file."""
    if _HOST_OS_NAME == "nt":
        import msvcrt

        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        return
    import fcntl

    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_authkey_file(fd: int) -> None:
    """Unlock the first byte of one authkey lock file."""
    if _HOST_OS_NAME == "nt":
        import msvcrt

        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(fd, fcntl.LOCK_UN)


@contextmanager
def _authkey_mutation_lock(authkey_path: Path):
    """Serialize authkey creation and repair across local processes."""
    with _AUTHKEY_MUTATION_THREAD_LOCK:
        lock_path = authkey_path.with_name(_DAEMON_AUTHKEY_LOCK_FILE_NAME)
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        acquired = False
        try:
            if os.fstat(fd).st_size == 0:
                os.write(fd, b"\0")
            deadline = time.monotonic() + _DAEMON_AUTHKEY_LOCK_TIMEOUT_SECONDS
            while True:
                try:
                    _try_lock_authkey_file(fd)
                except OSError as exc:
                    if time.monotonic() >= deadline:
                        raise DaemonClientError(
                            "Timed out waiting to repair the daemon auth key."
                        ) from exc
                    time.sleep(0.01)
                else:
                    acquired = True
                    break
            yield
        finally:
            if acquired:
                try:
                    _unlock_authkey_file(fd)
                except OSError:
                    pass
            os.close(fd)


def _daemon_process_record_from_metadata(
    metadata: dict[str, Any],
    *,
    source: str,
) -> _DaemonProcessRecord:
    daemon_id = metadata.get("daemon_id")
    if not isinstance(daemon_id, str) or not daemon_id.strip():
        raise DaemonClientError(f"{source} is missing a valid daemon identity.")
    try:
        persisted = daemon_process_lease_identity(metadata)
    except (TypeError, ValueError, WorkspaceStateCorruptError) as exc:
        raise DaemonClientError(
            f"{source} has incomplete or malformed process-containment metadata."
        ) from exc
    return _DaemonProcessRecord(
        daemon_id=daemon_id.strip(),
        process_identity=persisted.process_identity,
        containment_nonce=persisted.containment_nonce,
    )


def _recorded_local_daemon_process(
    paths: WorkspacePaths,
) -> _DaemonProcessRecord | None:
    """Return the complete machine-local daemon record for this exact workspace."""
    db_path = paths.runtime_control_db_path
    if db_path is None or not db_path.is_file():
        return None
    try:
        ledger = RuntimeControlLedger(db_path)
        try:
            state = ledger.daemon_state.get(paths.workspace_id)
        finally:
            ledger.close()
    except sqlite3.Error as exc:
        raise DaemonClientError(
            "The machine-local daemon record cannot provide a verified process identity."
        ) from exc
    if state is None:
        return None
    path_case_insensitive = _HOST_OS_NAME == "nt"
    endpoint_matches = (
        normalized_path_text(state.endpoint_path).casefold()
        == normalized_path_text(paths.daemon_endpoint_path).casefold()
        if paths.daemon_endpoint_kind == "pipe"
        else stable_path_identity_text(
            state.endpoint_path,
            case_insensitive=path_case_insensitive,
        )
        == stable_path_identity_text(
            paths.daemon_endpoint_path,
            case_insensitive=path_case_insensitive,
        )
    )
    binding_matches = (
        state.workspace_id == paths.workspace_id
        and state.endpoint_kind == paths.daemon_endpoint_kind
        and endpoint_matches
        and stable_path_identity_text(
            state.app_root,
            case_insensitive=path_case_insensitive,
        )
        == stable_path_identity_text(
            paths.app_root,
            case_insensitive=path_case_insensitive,
        )
        and stable_path_identity_text(
            state.workspace_root,
            case_insensitive=path_case_insensitive,
        )
        == stable_path_identity_text(
            paths.workspace_root,
            case_insensitive=path_case_insensitive,
        )
    )
    if not binding_matches:
        raise DaemonClientError(
            "The machine-local daemon record does not match the selected workspace."
        )
    return _daemon_process_record_from_metadata(
        {
            "daemon_id": state.daemon_id,
            "pid": state.pid,
            "process_start_key": state.process_start_key,
            "process_executable_path": state.process_executable_path,
            "process_group_id": state.process_group_id,
            "process_session_id": state.process_session_id,
            "containment_nonce": state.containment_nonce,
        },
        source="The machine-local daemon record",
    )


def _authkey_recovery_is_safe(paths: WorkspacePaths) -> bool:
    """Return whether no known local daemon can own the current authkey."""
    try:
        local_process = _recorded_local_daemon_process(paths)
    except (DaemonClientError, OSError, sqlite3.Error, TypeError, ValueError):
        return False
    if local_process is not None:
        try:
            local_process_is_running = _expected_process_is_running(
                local_process.process_identity
            )
        except DaemonClientError:
            return False
        if local_process_is_running:
            return local_process.process_identity.pid == os.getpid()
    try:
        metadata = _SHARED_STATE_ADAPTER.read_lease_metadata(paths)
    except Exception:
        return False
    if metadata is None:
        return True
    owner = metadata.get("machine_id")
    if not isinstance(owner, str) or not owner.strip():
        return False
    if owner.strip() != machine_id_text(app_root=paths.app_root):
        return True
    try:
        lease_process = _daemon_process_record_from_metadata(
            metadata,
            source="The current same-machine workspace lease",
        )
        lease_process_is_running = _expected_process_is_running(
            lease_process.process_identity
        )
    except (DaemonClientError, TypeError, ValueError):
        return False
    if not lease_process_is_running:
        return True
    return lease_process.process_identity.pid == os.getpid()


def _quarantine_authkey(authkey_path: Path) -> Path:
    """Atomically move one malformed daemon authkey to a unique quarantine path."""
    quarantine_path = authkey_path.with_name(f"{authkey_path.name}.invalid-{secrets.token_hex(8)}")
    os.replace(authkey_path, quarantine_path)
    _harden_private_file_permissions(quarantine_path)
    return quarantine_path


def _create_daemon_authkey(authkey_path: Path) -> bytes | None:
    """Create one authkey exclusively, or return ``None`` after losing a race."""
    authkey = secrets.token_bytes(_DAEMON_AUTHKEY_BYTE_LENGTH)
    try:
        fd = os.open(authkey_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        return None
    try:
        with os.fdopen(fd, "w", encoding="ascii") as handle:
            handle.write(authkey.hex())
    except Exception:
        try:
            authkey_path.unlink()
        except FileNotFoundError:
            pass
        raise
    _harden_private_file_permissions(authkey_path)
    return authkey


def daemon_authkey(paths: WorkspacePaths) -> bytes:
    """Load or create the per-workspace daemon authkey."""
    authkey_path = _daemon_authkey_path(paths)
    authkey_path.parent.mkdir(parents=True, exist_ok=True)
    while True:
        authkey, _ = _read_daemon_authkey(authkey_path)
        if authkey is not None:
            return authkey
        with _authkey_mutation_lock(authkey_path):
            authkey, authkey_exists = _read_daemon_authkey(authkey_path)
            if authkey is not None:
                return authkey
            if authkey_exists:
                if not _authkey_recovery_is_safe(paths):
                    raise DaemonClientError(
                        "Daemon auth key is malformed and cannot be replaced while a local daemon may still own it."
                    )
                _quarantine_authkey(authkey_path)
            authkey = _create_daemon_authkey(authkey_path)
            if authkey is not None:
                return authkey


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


class _DeadlineBoundConnection:
    """Apply an absolute deadline to complete framed reads and writes."""

    __slots__ = (
        "_clock",
        "_connection",
        "_deadline",
        "_family",
        "_timeout_message",
        "_winapi",
    )

    def __init__(
        self,
        connection: Any,
        *,
        deadline: float,
        timeout_message: str,
        family: str | None = None,
        clock: Callable[[], float] | None = None,
        winapi: Any | None = None,
    ) -> None:
        self._connection = connection
        self._deadline = deadline
        self._timeout_message = timeout_message
        self._family = family or ("AF_PIPE" if _HOST_OS_NAME == "nt" else "AF_UNIX")
        self._clock = clock or time.monotonic
        self._winapi = winapi

    def _remaining_timeout(self) -> float | None:
        remaining = self._deadline - self._clock()
        if remaining <= 0:
            raise DaemonClientError(self._timeout_message)
        return None if math.isinf(remaining) else remaining

    def _handle(self) -> int | None:
        fileno = getattr(self._connection, "fileno", None)
        if not callable(fileno):
            return None
        handle = fileno()
        if isinstance(handle, bool) or not isinstance(handle, int) or handle < 0:
            raise OSError("Daemon connection returned an invalid transport handle.")
        if self._family == "AF_UNIX":
            os.set_blocking(handle, False)
        return handle

    def _wait_for_unix_io(self, handle: int, *, writable: bool) -> None:
        poll_factory = getattr(select, "poll", None)
        if callable(poll_factory):
            poller = poll_factory()
            poller.register(
                handle,
                select.POLLOUT if writable else select.POLLIN,
            )
            while True:
                timeout = self._remaining_timeout()
                timeout_milliseconds = (
                    -1 if timeout is None else max(int(timeout * 1000), 0)
                )
                try:
                    events = poller.poll(timeout_milliseconds)
                except InterruptedError:
                    continue
                if events:
                    return
                if self._deadline <= self._clock():
                    raise DaemonClientError(self._timeout_message)

        while True:
            timeout = self._remaining_timeout()
            try:
                readable, writable_handles, _ = select.select(
                    [] if writable else [handle],
                    [handle] if writable else [],
                    [],
                    timeout,
                )
            except InterruptedError:
                continue
            if writable_handles if writable else readable:
                return
            raise DaemonClientError(self._timeout_message)

    def _write_unix_all(self, handle: int, payload: bytes | memoryview) -> None:
        remaining = memoryview(payload)
        while remaining:
            self._wait_for_unix_io(handle, writable=True)
            try:
                written = os.write(handle, remaining)
            except (BlockingIOError, InterruptedError):
                continue
            if written <= 0:
                raise BrokenPipeError("Daemon connection closed during a framed write.")
            remaining = remaining[written:]

    def _read_unix_exact(self, handle: int, size: int) -> bytes:
        payload = bytearray()
        while len(payload) < size:
            self._wait_for_unix_io(handle, writable=False)
            try:
                chunk = os.read(handle, min(size - len(payload), 65_536))
            except (BlockingIOError, InterruptedError):
                continue
            if not chunk:
                if not payload:
                    raise EOFError
                raise OSError("Daemon connection closed during a framed read.")
            payload.extend(chunk)
        return bytes(payload)

    def _send_unix_frame(self, handle: int, payload: bytes) -> None:
        size = len(payload)
        if size > 0x7FFF_FFFF:
            self._write_unix_all(handle, struct.pack("!iQ", -1, size))
            self._write_unix_all(handle, payload)
        elif size > 16_384:
            self._write_unix_all(handle, struct.pack("!i", size))
            self._write_unix_all(handle, payload)
        else:
            self._write_unix_all(handle, struct.pack("!i", size) + payload)

    def _recv_unix_frame(self, handle: int, maxlength: int | None) -> bytes:
        (size,) = struct.unpack("!i", self._read_unix_exact(handle, 4))
        if size == -1:
            (size,) = struct.unpack("!Q", self._read_unix_exact(handle, 8))
        elif size < 0:
            raise OSError("Daemon connection supplied an invalid frame length.")
        if maxlength is not None and size > maxlength:
            raise OSError("Daemon connection frame exceeds the allowed length.")
        return self._read_unix_exact(handle, size)

    def _windows_api(self) -> Any:
        if self._winapi is None:
            import _winapi  # noqa: PLC0415 - Windows-only dependency

            self._winapi = _winapi
        return self._winapi

    @staticmethod
    def _cancel_windows_io(overlapped: Any) -> None:
        try:
            overlapped.cancel()
        except OSError:
            pass
        try:
            overlapped.GetOverlappedResult(True)
        except OSError:
            pass

    def _wait_for_windows_io(
        self,
        overlapped: Any,
        initial_error: int,
        *,
        winapi: Any,
    ) -> None:
        if initial_error != winapi.ERROR_IO_PENDING:
            return
        try:
            timeout = self._remaining_timeout()
        except BaseException:
            self._cancel_windows_io(overlapped)
            raise
        wait_milliseconds = (
            winapi.INFINITE
            if timeout is None
            else min(max(int(timeout * 1000), 0), winapi.INFINITE - 1)
        )
        try:
            result = winapi.WaitForMultipleObjects(
                [overlapped.event],
                False,
                wait_milliseconds,
            )
        except BaseException:
            self._cancel_windows_io(overlapped)
            raise
        if result == getattr(winapi, "WAIT_TIMEOUT", 258):
            self._cancel_windows_io(overlapped)
            raise DaemonClientError(self._timeout_message)
        if result != getattr(winapi, "WAIT_OBJECT_0", 0):
            self._cancel_windows_io(overlapped)
            raise OSError(f"Unexpected Windows pipe wait result {result}.")

    def _send_windows_message(self, handle: int, payload: bytes) -> None:
        self._remaining_timeout()
        winapi = self._windows_api()
        overlapped, initial_error = winapi.WriteFile(
            handle,
            payload,
            overlapped=True,
        )
        self._wait_for_windows_io(
            overlapped,
            initial_error,
            winapi=winapi,
        )
        written, result_error = overlapped.GetOverlappedResult(True)
        if result_error == getattr(winapi, "ERROR_OPERATION_ABORTED", 995):
            raise OSError(errno.EPIPE, "Daemon pipe handle is closed.")
        if result_error:
            raise OSError(result_error, "Windows daemon pipe write failed.")
        if written != len(payload):
            raise OSError("Windows daemon pipe write was incomplete.")

    def _recv_windows_message(self, handle: int, maxlength: int | None) -> bytes:
        winapi = self._windows_api()
        payload = bytearray()
        while True:
            self._remaining_timeout()
            read_size = 65_536
            if maxlength is not None:
                read_size = min(read_size, maxlength + 1 - len(payload))
                if read_size <= 0:
                    raise OSError("Daemon connection message exceeds the allowed length.")
            try:
                overlapped, initial_error = winapi.ReadFile(
                    handle,
                    read_size,
                    overlapped=True,
                )
            except OSError as exc:
                if getattr(exc, "winerror", None) == winapi.ERROR_BROKEN_PIPE:
                    raise EOFError from exc
                raise
            self._wait_for_windows_io(
                overlapped,
                initial_error,
                winapi=winapi,
            )
            read, result_error = overlapped.GetOverlappedResult(True)
            if result_error == getattr(winapi, "ERROR_OPERATION_ABORTED", 995):
                raise OSError(errno.EPIPE, "Daemon pipe handle is closed.")
            if result_error not in (0, winapi.ERROR_MORE_DATA):
                if result_error == winapi.ERROR_BROKEN_PIPE:
                    raise EOFError
                raise OSError(result_error, "Windows daemon pipe read failed.")
            payload.extend(bytes(overlapped.getbuffer()[:read]))
            if maxlength is not None and len(payload) > maxlength:
                raise OSError("Daemon connection message exceeds the allowed length.")
            if result_error == 0:
                return bytes(payload)

    def send_bytes(self, payload: bytes) -> None:
        """Send one complete message before the absolute deadline."""
        handle = self._handle()
        if handle is None:
            self._remaining_timeout()
            self._connection.send_bytes(payload)
        elif self._family == "AF_PIPE":
            self._send_windows_message(handle, payload)
        elif self._family == "AF_UNIX":
            self._send_unix_frame(handle, payload)
        else:
            raise DaemonClientError(
                f"Unsupported daemon transport family: {self._family!r}."
            )

    def recv_bytes(self, maxlength: int | None = None) -> bytes:
        """Receive one complete message before the absolute deadline."""
        if maxlength is not None and maxlength < 0:
            raise ValueError("A daemon message maximum length cannot be negative.")
        handle = self._handle()
        if handle is None:
            timeout = self._remaining_timeout()
            poll = getattr(self._connection, "poll", None)
            if callable(poll) and not poll(timeout):
                raise DaemonClientError(self._timeout_message)
            if maxlength is None:
                return self._connection.recv_bytes()
            return self._connection.recv_bytes(maxlength)
        if self._family == "AF_PIPE":
            return self._recv_windows_message(handle, maxlength)
        if self._family == "AF_UNIX":
            return self._recv_unix_frame(handle, maxlength)
        raise DaemonClientError(
            f"Unsupported daemon transport family: {self._family!r}."
        )


def _connect_windows_pipe(
    address: str,
    *,
    deadline: float,
    clock: Callable[[], float] = time.monotonic,
    winapi: Any | None = None,
    connection_type: Callable[[int], Any] | None = None,
) -> Any:
    """Open one overlapped Windows named-pipe connection before a deadline."""
    if winapi is None or connection_type is None:
        import _winapi  # noqa: PLC0415 - Windows-only dependency
        from multiprocessing.connection import PipeConnection  # noqa: PLC0415 - Windows-only type

        winapi = _winapi
        connection_type = PipeConnection

    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            raise DaemonClientError("Timed out connecting to daemon.")
        try:
            handle = winapi.CreateFile(
                address,
                winapi.GENERIC_READ | winapi.GENERIC_WRITE,
                0,
                winapi.NULL,
                winapi.OPEN_EXISTING,
                winapi.FILE_FLAG_OVERLAPPED,
                winapi.NULL,
            )
        except OSError as exc:
            if getattr(exc, "winerror", None) != winapi.ERROR_PIPE_BUSY:
                raise
            remaining = deadline - clock()
            wait_milliseconds = int(min(remaining, 1.0) * 1000)
            if wait_milliseconds <= 0:
                raise DaemonClientError("Timed out connecting to daemon.") from exc
            try:
                winapi.WaitNamedPipe(address, wait_milliseconds)
            except OSError as wait_exc:
                if getattr(wait_exc, "winerror", None) not in (
                    winapi.ERROR_SEM_TIMEOUT,
                    winapi.ERROR_PIPE_BUSY,
                ):
                    raise
            continue
        break

    try:
        winapi.SetNamedPipeHandleState(handle, winapi.PIPE_READMODE_MESSAGE, None, None)
        return connection_type(handle)
    except BaseException:
        winapi.CloseHandle(handle)
        raise


def _windows_pipe_client(address: str, *, authkey: bytes, deadline: float) -> Any:
    """Connect and perform the standard multiprocessing handshake by a deadline."""
    connection = _connect_windows_pipe(address, deadline=deadline)
    bounded_connection = _DeadlineBoundConnection(
        connection,
        deadline=deadline,
        timeout_message="Timed out connecting to daemon.",
        family="AF_PIPE",
    )
    try:
        answer_challenge(bounded_connection, authkey)
        deliver_challenge(bounded_connection, authkey)
    except BaseException:
        connection.close()
        raise
    return connection


def _connect_unix_socket(
    address: str,
    *,
    deadline: float,
    clock: Callable[[], float] = time.monotonic,
    socket_factory: Callable[[int, int], Any] = socket.socket,
    connection_type: Callable[[int], Any] | None = None,
    socket_family: int | None = None,
) -> Any:
    """Open one Unix-domain socket connection before an absolute deadline."""
    if connection_type is None:
        from multiprocessing.connection import Connection  # noqa: PLC0415 - POSIX-only type

        connection_type = Connection

    remaining = deadline - clock()
    if remaining <= 0:
        raise DaemonClientError("Timed out connecting to daemon.")

    selected_family = getattr(socket, "AF_UNIX", None) if socket_family is None else socket_family
    if selected_family is None:
        raise OSError("Unix-domain sockets are unavailable on this host.")
    unix_socket = socket_factory(selected_family, socket.SOCK_STREAM)
    try:
        unix_socket.settimeout(None if math.isinf(remaining) else remaining)
        try:
            unix_socket.connect(address)
        except TimeoutError as exc:
            raise DaemonClientError("Timed out connecting to daemon.") from exc
        if clock() >= deadline:
            raise DaemonClientError("Timed out connecting to daemon.")
        unix_socket.setblocking(True)
        connection = connection_type(unix_socket.fileno())
        unix_socket.detach()
        return connection
    finally:
        unix_socket.close()


def _unix_socket_client(address: str, *, authkey: bytes, deadline: float) -> Any:
    """Connect and perform Unix socket authentication by an absolute deadline."""
    connection = _connect_unix_socket(address, deadline=deadline)
    bounded_connection = _DeadlineBoundConnection(
        connection,
        deadline=deadline,
        timeout_message="Timed out connecting to daemon.",
        family="AF_UNIX",
    )
    try:
        answer_challenge(bounded_connection, authkey)
        deliver_challenge(bounded_connection, authkey)
    except BaseException:
        connection.close()
        raise
    return connection


def daemon_request(paths: WorkspacePaths, payload: dict[str, Any], *, timeout: float = 5.0) -> dict[str, Any]:
    """Send one request to the local workspace daemon and return its response.

    A positive timeout is one absolute deadline for transport availability,
    authentication reads, and response waiting. A nonpositive timeout disables
    deadlines on either platform.
    """
    try:
        family = endpoint_family(paths)
        address = endpoint_address(paths)
        authkey = daemon_authkey(paths)
        deadline = time.monotonic() + timeout if timeout > 0 else None
        if deadline is None:
            connection = Client(address, family=family, authkey=authkey)
        elif family == "AF_PIPE":
            connection = _windows_pipe_client(address, authkey=authkey, deadline=deadline)
        elif family == "AF_UNIX":
            connection = _unix_socket_client(address, authkey=authkey, deadline=deadline)
        else:
            raise DaemonClientError(f"Unsupported daemon transport family: {family!r}.")
        with connection:
            request_connection = (
                connection
                if deadline is None
                else _DeadlineBoundConnection(
                    connection,
                    deadline=deadline,
                    timeout_message="Timed out waiting for daemon response.",
                    family=family,
                )
            )
            encoded_request = _encode_message(payload)
            if len(encoded_request) > _MAX_DAEMON_REQUEST_BYTES:
                raise DaemonClientError("Daemon request exceeds the transport limit.")
            request_connection.send_bytes(encoded_request)
            response = _decode_message(
                request_connection.recv_bytes(_MAX_DAEMON_RESPONSE_BYTES)
            )
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


def _harden_private_file_permissions(path: Path) -> None:
    """Best-effort hardening for one private local file."""
    if _HOST_OS_NAME != "nt":
        try:
            path.chmod(0o600)
        except OSError:
            pass
        return
    username = os.environ.get("USERNAME") or getpass.getuser()
    if not username.strip():
        return
    try:
        kwargs: dict[str, Any] = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
            "check": False,
        }
        if _HOST_OS_NAME == "nt":
            kwargs["creationflags"] = windows_subprocess_creationflags(no_window=True)
        subprocess.run(
            [
                "icacls",
                str(path),
                "/inheritance:r",
                "/grant:r",
                f"{username}:(F)",
            ],
            **kwargs,
        )
    except OSError:
        return


def _pid_is_live(pid: int | None) -> bool:
    """Return whether one OS process id currently exists."""
    return process_is_running(pid)


def _same_machine_lease_metadata(paths: WorkspacePaths) -> dict[str, Any] | None:
    """Return lease metadata when the workspace is leased by this installation."""
    metadata = _SHARED_STATE_ADAPTER.read_lease_metadata(paths)
    if metadata is None:
        return None
    owner = metadata.get("machine_id")
    if not isinstance(owner, str) or owner.strip() != machine_id_text(app_root=paths.app_root):
        return None
    return metadata


def _same_machine_unreachable_lease_metadata(paths: WorkspacePaths) -> dict[str, Any] | None:
    """Return lease metadata when the workspace is leased locally but IPC is unavailable."""
    metadata = _same_machine_lease_metadata(paths)
    if metadata is None:
        return None
    if is_daemon_live(paths):
        return None
    return metadata


def _same_machine_lease_process(
    paths: WorkspacePaths,
) -> _DaemonProcessRecord | None:
    """Return the exact process record for a lease owned by this installation."""
    try:
        metadata = _SHARED_STATE_ADAPTER.read_lease_metadata(paths)
    except Exception as exc:
        raise DaemonClientError(
            "The current workspace lease cannot provide a verified process identity."
        ) from exc
    if metadata is None:
        return None
    owner = metadata.get("machine_id")
    if not isinstance(owner, str) or owner.strip() != machine_id_text(app_root=paths.app_root):
        return None
    return _daemon_process_record_from_metadata(
        metadata,
        source="The current same-machine workspace lease",
    )


def _expected_process_is_running(expected: ProcessIdentity) -> bool:
    """Return whether the exact recorded process incarnation is still running."""
    try:
        actual = inspect_process_identity(expected.pid)
    except ProcessInspectionError as exc:
        raise DaemonClientError(
            f"Unable to verify local daemon process {expected.pid}."
        ) from exc
    return actual == expected


def _same_machine_live_lease_process(paths: WorkspacePaths) -> int | None:
    """Return the exact owning local lease PID only while its identity matches."""
    record = _same_machine_lease_process(paths)
    if record is None or not _expected_process_is_running(record.process_identity):
        return None
    return record.process_identity.pid


def _reachable_daemon_process(
    paths: WorkspacePaths,
) -> _DaemonProcessRecord | None:
    """Return the process record from one authenticated daemon status response."""
    try:
        response = daemon_request(paths, {"command": "daemon_status"}, timeout=0.5)
    except DaemonClientError:
        return None
    status = response.get("status")
    if not isinstance(status, dict):
        raise DaemonClientError("The reachable daemon returned invalid status metadata.")
    if status.get("workspace_id") != paths.workspace_id:
        raise DaemonClientError("The reachable daemon status belongs to another workspace.")
    if status.get("workspace_root") != str(paths.workspace_root):
        raise DaemonClientError("The reachable daemon status has a mismatched workspace root.")
    if status.get("machine_id") != machine_id_text(app_root=paths.app_root):
        raise DaemonClientError("The reachable daemon status belongs to another installation.")
    return _daemon_process_record_from_metadata(
        status,
        source="The authenticated daemon status",
    )


def _local_daemon_process(paths: WorkspacePaths) -> _DaemonProcessRecord | None:
    """Resolve the strongest current local daemon identity without PID fallback."""
    reachable = _reachable_daemon_process(paths)
    if reachable is not None:
        return reachable
    lease_process = _same_machine_lease_process(paths)
    local_process = _recorded_local_daemon_process(paths)
    if lease_process is None:
        return local_process
    if local_process is None:
        raise DaemonClientError(
            "The same-machine workspace lease is not corroborated by machine-local "
            "daemon state."
        )
    if lease_process != local_process:
        raise DaemonClientError(
            "The same-machine workspace lease does not match machine-local daemon state."
        )
    return local_process


def _wait_for_posix_daemon_group_exit(
    expected: ProcessIdentity,
    *,
    timeout_seconds: float = 2.0,
) -> None:
    """Confirm that an exited POSIX leader has no same-group descendants."""
    if _HOST_OS_NAME == "nt":
        return
    if (
        expected.process_group_id != expected.pid
        or expected.process_session_id != expected.pid
    ):
        raise DaemonClientError(
            f"The recorded daemon process {expected.pid} has no verified isolated session."
        )
    try:
        actual = inspect_process_identity(expected.pid)
    except ProcessInspectionError as exc:
        raise DaemonClientError(
            f"Unable to verify shutdown of daemon process group {expected.pid}."
        ) from exc
    if actual is not None and actual.start_key != expected.start_key:
        return
    if actual is not None and actual != expected:
        raise DaemonClientError(
            f"The recorded daemon process {expected.pid} changed containment identity."
        )
    try:
        group_exited = wait_for_posix_process_group_exit(
            expected.pid,
            timeout_seconds=timeout_seconds,
        )
    except (ProcessInspectionError, ValueError) as exc:
        raise DaemonClientError(
            f"Unable to confirm shutdown of daemon process group {expected.pid}."
        ) from exc
    if not group_exited:
        try:
            actual = inspect_process_identity(expected.pid)
        except ProcessInspectionError as exc:
            raise DaemonClientError(
                f"Unable to verify shutdown of daemon process group {expected.pid}."
            ) from exc
        if actual is not None and actual.start_key != expected.start_key:
            return
        if actual is not None and actual != expected:
            raise DaemonClientError(
                f"The recorded daemon process {expected.pid} changed containment identity."
            )
        raise DaemonClientError(
            f"Daemon process group {expected.pid} still has running descendants."
        )


def _finish_verified_daemon_exit(
    paths: WorkspacePaths,
    process_record: _DaemonProcessRecord,
    *,
    windows_job: WindowsKillOnCloseJob | None,
) -> None:
    """Confirm containment drain and clean state after an exact daemon exits."""
    expected = process_record.process_identity
    if _HOST_OS_NAME == "nt":
        try:
            if windows_job is None:
                ensure_windows_containment_job_stopped(
                    process_record.containment_nonce,
                    timeout_seconds=2.0,
                )
            else:
                windows_job.terminate(timeout_seconds=2.0)
        except (ProcessInspectionError, ValueError) as exc:
            raise DaemonClientError(
                f"Unable to confirm shutdown of daemon Job for process {expected.pid}."
            ) from exc
    else:
        _wait_for_posix_daemon_group_exit(expected)
        _cleanup_posix_watchdog_endpoint(process_record.containment_nonce)
    _cleanup_forced_shutdown(paths, process_record=process_record)


def _cleanup_forced_shutdown(
    paths: WorkspacePaths,
    *,
    process_record: _DaemonProcessRecord | None = None,
) -> None:
    """Release only a verified exited lease, or recover an unverified stale one."""
    try:
        metadata = _SHARED_STATE_ADAPTER.read_lease_metadata(paths)
    except Exception as exc:
        if process_record is not None:
            raise DaemonClientError(
                "Unable to inspect the verified daemon's workspace lease."
            ) from exc
        metadata = None
    if not isinstance(metadata, dict):
        return
    owner = metadata.get("machine_id")
    lease_token = metadata.get("lease_token")
    local_machine_id = machine_id_text(app_root=paths.app_root)
    if (
        not isinstance(owner, str)
        or owner.strip() != local_machine_id
        or not isinstance(lease_token, str)
    ):
        return
    if process_record is not None:
        if metadata.get("workspace_id") != paths.workspace_id:
            return
        try:
            lease_process_record = _daemon_process_record_from_metadata(
                metadata,
                source="The verified daemon workspace lease",
            )
        except DaemonClientError as exc:
            raise DaemonClientError(
                "The verified daemon workspace lease has invalid ownership metadata."
            ) from exc
        if lease_process_record != process_record:
            return
        try:
            _SHARED_STATE_ADAPTER.release_workspace(
                paths,
                lease_token=lease_token,
            )
        except WorkspaceLeaseLostError:
            return
        except Exception as exc:
            raise DaemonClientError(
                "Unable to release the verified daemon's workspace lease."
            ) from exc
        return
    try:
        _SHARED_STATE_ADAPTER.recover_stale_workspace(
            paths,
            lease_token=lease_token,
            machine_id=local_machine_id,
            stale_after_seconds=STALE_AFTER_SECONDS,
        )
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
    metadata = _same_machine_lease_metadata(paths)
    if metadata is None:
        return False
    if is_daemon_live(paths):
        return True
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
    lease_token = metadata.get("lease_token")
    if not isinstance(lease_token, str):
        return False
    return _SHARED_STATE_ADAPTER.lease_is_stale(
        paths,
        lease_token=lease_token,
        stale_after_seconds=STALE_AFTER_SECONDS,
    )


def _recover_broken_local_lease(paths: WorkspacePaths) -> bool:
    """Recover one unreachable same-machine lease after it becomes stale."""
    metadata = _same_machine_unreachable_lease_metadata(paths)
    lease_token = metadata.get("lease_token") if isinstance(metadata, dict) else None
    if not isinstance(lease_token, str):
        return False
    return _SHARED_STATE_ADAPTER.recover_stale_workspace(
        paths,
        lease_token=lease_token,
        machine_id=machine_id_text(app_root=paths.app_root),
        stale_after_seconds=STALE_AFTER_SECONDS,
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


def _windows_kernel32() -> Any:
    """Return the Win32 kernel32 library or fail when Windows ctypes support is unavailable."""
    windll = getattr(ctypes, "windll", None)
    if windll is None:
        raise DaemonClientError("Windows daemon startup locks require ctypes.windll.")
    return windll.kernel32


def _acquire_startup_lock(paths: WorkspacePaths) -> bool:
    """Try to acquire the per-workspace daemon startup lock."""
    if _HOST_OS_NAME == "nt":
        mutex_name = _windows_startup_mutex_name(paths)
        kernel32 = _windows_kernel32()
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
    with _POSIX_STARTUP_LOCKS_LOCK:
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            import fcntl

            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                if exc.errno in {errno.EACCES, errno.EAGAIN}:
                    os.close(fd)
                    return False
                raise
            os.fchmod(fd, 0o600)
            os.ftruncate(fd, 0)
            os.write(fd, str(os.getpid()).encode("ascii"))
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            raise
        _POSIX_STARTUP_LOCK_FDS[lock_path] = fd
        return True


def _release_startup_lock(paths: WorkspacePaths) -> None:
    """Release the per-workspace daemon startup lock when held."""
    if _HOST_OS_NAME == "nt":
        mutex_name = _windows_startup_mutex_name(paths)
        handle = _WINDOWS_STARTUP_MUTEXES.pop(mutex_name, None)
        if handle is None:
            return
        kernel32 = _windows_kernel32()
        _configure_ctypes_function(kernel32.ReleaseMutex, argtypes=[ctypes.c_void_p], restype=ctypes.c_int)
        _configure_ctypes_function(kernel32.CloseHandle, argtypes=[ctypes.c_void_p], restype=ctypes.c_int)
        try:
            kernel32.ReleaseMutex(handle)
        finally:
            kernel32.CloseHandle(handle)
        return
    lock_path = _startup_lock_path(paths)
    with _POSIX_STARTUP_LOCKS_LOCK:
        fd = _POSIX_STARTUP_LOCK_FDS.pop(lock_path, None)
        if fd is None:
            return
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _acquire_startup_lock_or_wait_for_daemon(
    paths: WorkspacePaths,
    *,
    timeout_seconds: float,
) -> bool:
    """Acquire startup ownership or observe the competing daemon become ready."""
    deadline: float | None = None
    while True:
        if _acquire_startup_lock(paths):
            return True
        if deadline is None:
            deadline = time.monotonic() + timeout_seconds
        if is_daemon_live(paths):
            return False
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DaemonClientError("Timed out waiting for daemon startup.")
        time.sleep(min(remaining, 0.1))


def _wait_for_daemon_live(paths: WorkspacePaths, *, timeout_seconds: float) -> bool:
    """Wait for one workspace daemon to become reachable."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if is_daemon_live(paths):
            return True
        time.sleep(0.1)
    return is_daemon_live(paths)


def _read_posix_launch_identity(
    ready_fd: int,
    *,
    expected_pid: int,
) -> ProcessIdentity:
    """Read and independently verify one gated POSIX bootstrap identity."""
    deadline = time.monotonic() + _POSIX_LAUNCH_IDENTITY_TIMEOUT_SECONDS
    payload = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DaemonClientError(
                "Timed out waiting for the contained daemon launch identity."
            )
        readable, _, _ = select.select([ready_fd], [], [], remaining)
        if not readable:
            raise DaemonClientError(
                "Timed out waiting for the contained daemon launch identity."
            )
        chunk = os.read(ready_fd, 4096)
        if not chunk:
            break
        payload.extend(chunk)
        if len(payload) > _MAX_POSIX_LAUNCH_IDENTITY_BYTES:
            raise DaemonClientError("The contained daemon launch identity is too large.")
    try:
        values = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DaemonClientError(
            "The contained daemon returned an invalid launch identity."
        ) from exc
    if not isinstance(values, dict):
        raise DaemonClientError("The contained daemon returned an invalid launch identity.")
    pid = values.get("pid")
    group_id = values.get("process_group_id")
    session_id = values.get("process_session_id")
    start_key = values.get("start_key")
    executable_path = values.get("executable_path")
    if (
        isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid != expected_pid
        or isinstance(group_id, bool)
        or not isinstance(group_id, int)
        or group_id != pid
        or isinstance(session_id, bool)
        or not isinstance(session_id, int)
        or session_id != pid
        or not isinstance(start_key, str)
        or not start_key
        or not isinstance(executable_path, str)
        or not executable_path
    ):
        raise DaemonClientError("The contained daemon returned an invalid launch identity.")
    identity = ProcessIdentity(
        pid=pid,
        start_key=start_key,
        executable_path=executable_path,
        process_group_id=group_id,
        process_session_id=session_id,
    )
    try:
        actual = inspect_process_identity(pid)
    except ProcessInspectionError as exc:
        raise DaemonClientError(
            f"Unable to verify newly launched daemon process {pid}."
        ) from exc
    if actual != identity:
        raise DaemonClientError(
            f"Newly launched daemon process {pid} changed identity before release."
        )
    return identity


def _cleanup_gated_posix_launch(
    process: subprocess.Popen[bytes],
    *,
    containment_nonce: str,
) -> None:
    """Terminate and reap a retained POSIX child that was not fully released."""
    if process.poll() is None:
        process.kill()
    process.wait(timeout=5.0)
    group_exited = wait_for_posix_process_group_exit(
        process.pid,
        timeout_seconds=2.0,
    )
    if not group_exited:
        raise DaemonClientError(
            f"Daemon process group {process.pid} did not drain after launch failed."
        )
    _cleanup_posix_watchdog_endpoint(containment_nonce)


def _launch_contained_daemon(
    command: list[str],
    *,
    containment_nonce: str,
    on_identity_ready: Callable[[ProcessIdentity], None] | None = None,
    on_verified_drain: Callable[[], None] | None = None,
) -> ProcessIdentity:
    """Launch one daemon and release it only after its exact identity is accepted."""
    if _HOST_OS_NAME == "nt":
        identity = spawn_windows_contained_process(
            command[0],
            command[1:],
            containment_nonce=containment_nonce,
            before_resume=on_identity_ready,
            after_verified_cleanup=on_verified_drain,
        ).process_identity
        try:
            job = open_verified_windows_kill_on_close_job(
                identity,
                nonce=containment_nonce,
            )
        except (ProcessInspectionError, ValueError) as exc:
            try:
                ensure_windows_containment_job_stopped(containment_nonce)
            except (ProcessInspectionError, ValueError) as cleanup_exc:
                exc.add_note(
                    f"Failed-launch containment cleanup also failed: {cleanup_exc}"
                )
            else:
                if on_verified_drain is not None:
                    try:
                        on_verified_drain()
                    except BaseException as callback_exc:
                        exc.add_note(
                            f"Post-cleanup callback also failed: {callback_exc}"
                        )
            raise DaemonClientError(
                f"Unable to retain containment for newly launched daemon process "
                f"{identity.pid}."
            ) from exc
        with _WINDOWS_LAUNCH_JOBS_LOCK:
            if identity in _WINDOWS_LAUNCH_JOBS:
                duplicate_error = DaemonClientError(
                    f"Daemon process identity {identity.pid} already has retained containment."
                )
                try:
                    job.terminate(timeout_seconds=2.0)
                except (ProcessInspectionError, ValueError) as exc:
                    job.close()
                    raise DaemonClientError(
                        f"Unable to drain duplicate retained containment for daemon "
                        f"process {identity.pid}."
                    ) from exc
                if on_verified_drain is not None:
                    try:
                        on_verified_drain()
                    except BaseException as callback_exc:
                        duplicate_error.add_note(
                            f"Post-cleanup callback also failed: {callback_exc}"
                        )
                job.close()
                raise duplicate_error
            _WINDOWS_LAUNCH_JOBS[identity] = job
        return identity

    if on_identity_ready is None:
        raise DaemonClientError(
            "POSIX daemon launch requires a gated stable-identity callback."
        )
    ready_read_fd, ready_write_fd = os.pipe()
    try:
        release_read_fd, release_write_fd = os.pipe()
    except BaseException:
        for fd in (ready_read_fd, ready_write_fd):
            try:
                os.close(fd)
            except OSError:
                pass
        raise
    launched_process: subprocess.Popen[bytes] | None = None
    try:
        handshake_command = [
            *command,
            "--launch-ready-fd",
            str(ready_write_fd),
            "--launch-release-fd",
            str(release_read_fd),
        ]
        launched_process = subprocess.Popen(
            handshake_command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            pass_fds=(ready_write_fd, release_read_fd),
            start_new_session=True,
        )
    except BaseException:
        os.close(ready_read_fd)
        os.close(release_write_fd)
        raise
    finally:
        os.close(ready_write_fd)
        os.close(release_read_fd)
    try:
        process = launched_process
        if process is None:
            raise DaemonClientError("Unable to launch the contained daemon process.")
        identity = _read_posix_launch_identity(
            ready_read_fd,
            expected_pid=process.pid,
        )
        on_identity_ready(identity)
        if os.write(release_write_fd, b"1") != 1:
            raise DaemonClientError(
                "Unable to release the contained daemon after identity persistence."
            )
    except BaseException as exc:
        if launched_process is not None:
            try:
                _cleanup_gated_posix_launch(
                    launched_process,
                    containment_nonce=containment_nonce,
                )
                if on_verified_drain is not None:
                    on_verified_drain()
            except BaseException as cleanup_exc:
                exc.add_note(f"Failed-launch cleanup also failed: {cleanup_exc}")
        if isinstance(exc, DaemonClientError):
            raise
        raise DaemonClientError(
            "Unable to complete the gated daemon launch."
        ) from exc
    finally:
        os.close(ready_read_fd)
        os.close(release_write_fd)
    try:
        _start_posix_daemon_reaper(
            identity,
            process,
            containment_nonce=containment_nonce,
        )
    except BaseException as exc:
        try:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5.0)
            group_exited = wait_for_posix_process_group_exit(
                identity.pid,
                timeout_seconds=2.0,
            )
            if group_exited:
                _cleanup_posix_watchdog_endpoint(containment_nonce)
                if on_verified_drain is not None:
                    on_verified_drain()
            else:
                exc.add_note(
                    f"Daemon process group {identity.pid} did not drain after "
                    "reaper startup failed."
                )
        except BaseException as cleanup_exc:
            exc.add_note(f"Failed-launch cleanup also failed: {cleanup_exc}")
        raise DaemonClientError(
            f"Unable to supervise newly launched daemon process {process.pid}."
        ) from exc
    return identity


def _start_posix_daemon_reaper(
    identity: ProcessIdentity,
    process: subprocess.Popen[bytes],
    *,
    containment_nonce: str,
) -> None:
    """Retain and reap one exact POSIX daemon child without periodic polling."""
    reaper = threading.Thread(
        target=_reap_posix_daemon,
        args=(identity, process),
        kwargs={"containment_nonce": containment_nonce},
        name=f"data-engine-daemon-reaper-{identity.pid}",
        daemon=True,
    )
    with _POSIX_DAEMON_REAPERS_LOCK:
        if identity in _POSIX_DAEMON_PROCESSES:
            raise DaemonClientError(
                f"Daemon process identity {identity.pid} is already supervised."
            )
        _POSIX_DAEMON_PROCESSES[identity] = process
    try:
        reaper.start()
    except BaseException:
        with _POSIX_DAEMON_REAPERS_LOCK:
            if _POSIX_DAEMON_PROCESSES.get(identity) is process:
                _POSIX_DAEMON_PROCESSES.pop(identity, None)
        raise


def _reap_posix_daemon(
    identity: ProcessIdentity,
    process: subprocess.Popen[bytes],
    *,
    containment_nonce: str,
) -> None:
    try:
        process.wait()
    finally:
        with _POSIX_DAEMON_REAPERS_LOCK:
            if _POSIX_DAEMON_PROCESSES.get(identity) is process:
                _POSIX_DAEMON_PROCESSES.pop(identity, None)
    try:
        group_exited = wait_for_posix_process_group_exit(
            identity.pid,
            timeout_seconds=2.0,
        )
    except (ProcessInspectionError, ValueError):
        group_exited = False
    if group_exited:
        try:
            _cleanup_posix_watchdog_endpoint(containment_nonce)
        except (OSError, ValueError):
            pass


def _retained_windows_launch_job(
    identity: ProcessIdentity,
) -> WindowsKillOnCloseJob | None:
    with _WINDOWS_LAUNCH_JOBS_LOCK:
        return _WINDOWS_LAUNCH_JOBS.get(identity)


def _release_windows_launch_job(identity: ProcessIdentity) -> None:
    with _WINDOWS_LAUNCH_JOBS_LOCK:
        job = _WINDOWS_LAUNCH_JOBS.pop(identity, None)
    if job is not None:
        job.close()


def _release_failed_launch_lease(
    paths: WorkspacePaths,
    *,
    containment_nonce: str,
) -> None:
    """Release a fresh lease only when it belongs to the failed launch nonce."""
    try:
        metadata = _SHARED_STATE_ADAPTER.read_lease_metadata(paths)
    except Exception as exc:
        raise DaemonClientError(
            "Unable to inspect the failed daemon launch workspace lease."
        ) from exc
    if not isinstance(metadata, dict):
        return
    lease_token = metadata.get("lease_token")
    if (
        metadata.get("workspace_id") != paths.workspace_id
        or metadata.get("machine_id")
        != machine_id_text(app_root=paths.app_root)
        or metadata.get("containment_nonce") != containment_nonce
        or not isinstance(lease_token, str)
    ):
        return
    try:
        _SHARED_STATE_ADAPTER.release_workspace(paths, lease_token=lease_token)
    except WorkspaceLeaseLostError:
        return
    except Exception as exc:
        raise DaemonClientError(
            "Unable to release the failed daemon launch workspace lease."
        ) from exc


def _cleanup_failed_daemon_startup(
    paths: WorkspacePaths,
    identity: ProcessIdentity,
    *,
    containment_nonce: str,
) -> None:
    """Terminate the exact contained process tree after a failed startup wait."""
    retained_windows_job = _retained_windows_launch_job(identity)
    if retained_windows_job is not None:
        try:
            retained_windows_job.terminate(timeout_seconds=2.0)
        except (ProcessInspectionError, ValueError) as exc:
            raise DaemonClientError(
                f"Failed to clean up daemon Job for process {identity.pid} after "
                "startup timed out."
            ) from exc
        _release_failed_launch_lease(
            paths,
            containment_nonce=containment_nonce,
        )
        return
    try:
        force_kill_verified_contained_process_tree(
            identity,
            containment_nonce=containment_nonce,
        )
    except (ProcessInspectionError, ValueError) as exc:
        if not _expected_process_is_running(identity):
            _wait_for_posix_daemon_group_exit(identity)
            _release_failed_launch_lease(
                paths,
                containment_nonce=containment_nonce,
            )
            return
        raise DaemonClientError(
            f"Failed to clean up daemon process {identity.pid} after startup timed out."
        ) from exc
    _wait_for_posix_daemon_group_exit(identity)
    _release_failed_launch_lease(
        paths,
        containment_nonce=containment_nonce,
    )


def _persist_provisional_daemon_launch(
    paths: WorkspacePaths,
    identity: ProcessIdentity,
    *,
    containment_nonce: str,
    expected_predecessor: _DaemonProcessRecord | None = None,
) -> None:
    """CAS-install launch identity before allowing daemon initialization."""
    timestamp = datetime.now(UTC).isoformat()
    ledger = RuntimeControlLedger(paths.runtime_control_db_path)
    try:
        installed = ledger.daemon_state.install_provisional(
            workspace_id=paths.workspace_id,
            daemon_id=f"launch-{containment_nonce}",
            process_identity=identity,
            containment_nonce=containment_nonce,
            endpoint_kind=paths.daemon_endpoint_kind,
            endpoint_path=paths.daemon_endpoint_path,
            started_at_utc=timestamp,
            last_checkpoint_at_utc=timestamp,
            status="launching",
            app_root=str(paths.app_root),
            workspace_root=str(paths.workspace_root),
            version_text=APP_VERSION,
            expected_predecessor_daemon_id=(
                expected_predecessor.daemon_id
                if expected_predecessor is not None
                else None
            ),
            expected_predecessor_identity=(
                expected_predecessor.process_identity
                if expected_predecessor is not None
                else None
            ),
            expected_predecessor_containment_nonce=(
                expected_predecessor.containment_nonce
                if expected_predecessor is not None
                else None
            ),
        )
    finally:
        ledger.close()
    if not installed:
        raise DaemonClientError(
            "Another daemon launch changed the workspace ownership generation."
        )


def _wait_for_prior_local_daemon_release(
    paths: WorkspacePaths,
    *,
    timeout_seconds: float = 2.0,
) -> _DaemonProcessRecord | None:
    """Drain a previous exact local daemon tombstone before a replacement launch."""
    process_record = _recorded_local_daemon_process(paths)
    if process_record is None:
        return None
    timeout = float(timeout_seconds)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("A prior-daemon timeout must be a finite nonnegative number.")
    expected = process_record.process_identity
    deadline = time.monotonic() + timeout
    while True:
        try:
            actual = inspect_process_identity(expected.pid)
        except ProcessInspectionError as exc:
            raise DaemonClientError(
                f"Unable to verify previous local daemon process {expected.pid}."
            ) from exc
        if actual is None or actual.start_key != expected.start_key:
            break
        if actual != expected:
            raise DaemonClientError(
                f"Previous local daemon process {expected.pid} changed containment identity."
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DaemonClientError(
                f"Previous local daemon process {expected.pid} is still shutting down."
            )
        time.sleep(min(remaining, 0.05))

    if _HOST_OS_NAME == "nt":
        try:
            ensure_windows_containment_job_stopped(
                process_record.containment_nonce,
                timeout_seconds=2.0,
            )
        except (ProcessInspectionError, ValueError) as exc:
            raise DaemonClientError(
                f"Unable to drain the previous daemon Job for process {expected.pid}."
            ) from exc
        return process_record
    _wait_for_posix_daemon_group_exit(expected, timeout_seconds=2.0)
    _cleanup_posix_watchdog_endpoint(process_record.containment_nonce)
    return process_record


def _reachable_daemon_matches_launch(
    paths: WorkspacePaths,
    *,
    launch_identity: ProcessIdentity,
    containment_nonce: str,
) -> bool:
    process_record = _reachable_daemon_process(paths)
    if process_record is None:
        return False
    if process_record.containment_nonce != containment_nonce:
        raise DaemonClientError(
            "A different daemon generation answered during startup."
        )
    if _HOST_OS_NAME != "nt":
        if process_record.process_identity != launch_identity:
            raise DaemonClientError(
                "The daemon answering after launch has a different process identity."
            )
        return True
    try:
        job = open_verified_windows_kill_on_close_job(
            process_record.process_identity,
            nonce=containment_nonce,
        )
    except (ProcessInspectionError, ValueError) as exc:
        raise DaemonClientError(
            "The daemon answering after launch is outside the retained Windows Job."
        ) from exc
    job.close()
    return True


def _wait_for_expected_daemon_live(
    paths: WorkspacePaths,
    *,
    launch_identity: ProcessIdentity,
    containment_nonce: str,
    timeout_seconds: float,
) -> bool:
    """Wait only for the exact newly launched containment generation."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        if _reachable_daemon_matches_launch(
            paths,
            launch_identity=launch_identity,
            containment_nonce=containment_nonce,
        ):
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(remaining, 0.1))


def spawn_daemon_process(
    paths: WorkspacePaths,
    *,
    lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT,
) -> int:
    """Start the daemon process in the background for one workspace."""
    lifecycle_policy = DaemonLifecyclePolicy.coerce(lifecycle_policy)
    if is_daemon_live(paths):
        return 0
    acquired = _acquire_startup_lock_or_wait_for_daemon(
        paths,
        timeout_seconds=10.0,
    )
    if not acquired:
        return 0
    identity: ProcessIdentity | None = None
    try:
        if is_daemon_live(paths):
            return 0
        if _wait_for_fresh_local_daemon(paths):
            return 0
        local_pid = _same_machine_live_lease_process(paths)
        if local_pid is not None:
            if _wait_for_daemon_live(paths, timeout_seconds=2.0):
                return 0
            raise DaemonClientError(
                f"Local daemon process {local_pid} already owns this workspace."
            )
        if _should_force_recover_local_lease(paths):
            _recover_broken_local_lease(paths)
            if is_daemon_live(paths):
                return 0
        elif _same_machine_unreachable_lease_metadata(paths) is not None:
            raise DaemonClientError(
                "This workstation already has control, but the local daemon is not "
                "responding yet."
            )
        predecessor = _wait_for_prior_local_daemon_release(paths)
        containment_nonce = new_process_containment_nonce()
        bootstrap_command = (
            [sys.executable, "-P", "-m", "data_engine.daemon_bootstrap"]
            if _HOST_OS_NAME == "nt"
            else [sys.executable, "-I", "-S", _DAEMON_BOOTSTRAP_PATH]
        )
        command = [
            *bootstrap_command,
            "--app-root",
            str(paths.app_root),
            "--workspace",
            str(paths.workspace_root),
            "--workspace-id",
            paths.workspace_id,
            "--containment-nonce",
            containment_nonce,
            "--lifecycle-policy",
            lifecycle_policy.value,
        ]

        def _persist_ready_identity(ready_identity: ProcessIdentity) -> None:
            _persist_provisional_daemon_launch(
                paths,
                ready_identity,
                containment_nonce=containment_nonce,
                expected_predecessor=predecessor,
            )

        def _release_drained_launch_lease() -> None:
            _release_failed_launch_lease(
                paths,
                containment_nonce=containment_nonce,
            )

        try:
            identity = _launch_contained_daemon(
                command,
                containment_nonce=containment_nonce,
                on_identity_ready=_persist_ready_identity,
                on_verified_drain=_release_drained_launch_lease,
            )
        except Exception as exc:
            if isinstance(exc, DaemonClientError):
                raise
            raise DaemonClientError(
                "Unable to launch and persist the contained daemon identity."
            ) from exc
        try:
            daemon_ready = _wait_for_expected_daemon_live(
                paths,
                launch_identity=identity,
                containment_nonce=containment_nonce,
                timeout_seconds=10.0,
            )
        except BaseException as exc:
            try:
                _cleanup_failed_daemon_startup(
                    paths,
                    identity,
                    containment_nonce=containment_nonce,
                )
            except BaseException as cleanup_exc:
                exc.add_note(
                    f"Failed-launch containment cleanup also failed: {cleanup_exc}"
                )
            raise
        if daemon_ready:
            return 0
        _cleanup_failed_daemon_startup(
            paths,
            identity,
            containment_nonce=containment_nonce,
        )
        raise DaemonClientError("Timed out waiting for daemon startup.")
    finally:
        if identity is not None:
            _release_windows_launch_job(identity)
        _release_startup_lock(paths)


def _open_verified_windows_daemon_job(
    process_record: _DaemonProcessRecord,
) -> WindowsKillOnCloseJob | None:
    """Retain a verified Windows Job or drain it after an exact leader race."""
    if _HOST_OS_NAME != "nt":
        return None
    expected = process_record.process_identity
    try:
        return open_verified_windows_kill_on_close_job(
            expected,
            nonce=process_record.containment_nonce,
        )
    except (ProcessInspectionError, ValueError) as exc:
        if _expected_process_is_running(expected):
            raise DaemonClientError(
                f"Unable to verify the containment Job for daemon process {expected.pid}."
            ) from exc
        try:
            ensure_windows_containment_job_stopped(
                process_record.containment_nonce,
                timeout_seconds=2.0,
            )
        except (ProcessInspectionError, ValueError) as cleanup_exc:
            raise DaemonClientError(
                f"Unable to confirm shutdown of daemon Job for process {expected.pid}."
            ) from cleanup_exc
        return None


def force_shutdown_daemon_process(paths: WorkspacePaths, *, timeout: float = 0.5) -> None:
    """Stop the local daemon and force its contained tree only after exact verification."""
    if isinstance(timeout, bool) or not isinstance(timeout, int | float):
        raise ValueError("A daemon shutdown timeout must be a finite nonnegative number.")
    timeout = float(timeout)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("A daemon shutdown timeout must be a finite nonnegative number.")
    identity_error: DaemonClientError | None = None
    try:
        process_record = _local_daemon_process(paths)
    except (DaemonClientError, WorkspaceStateCorruptError) as exc:
        identity_error = (
            exc
            if isinstance(exc, DaemonClientError)
            else DaemonClientError("The current daemon identity metadata is corrupt.")
        )
        process_record = None
    if process_record is None and identity_error is None and not is_daemon_live(paths):
        _cleanup_forced_shutdown(paths)
        return
    windows_job = (
        _open_verified_windows_daemon_job(process_record)
        if process_record is not None
        else None
    )
    try:
        _force_shutdown_daemon_process(
            paths,
            process_record=process_record,
            identity_error=identity_error,
            windows_job=windows_job,
            timeout=timeout,
        )
    finally:
        if windows_job is not None:
            windows_job.close()


def _force_shutdown_daemon_process(
    paths: WorkspacePaths,
    *,
    process_record: _DaemonProcessRecord | None,
    identity_error: DaemonClientError | None,
    windows_job: WindowsKillOnCloseJob | None,
    timeout: float,
) -> None:
    """Run graceful and forced shutdown while retaining verified containment."""
    if timeout > 0:
        try:
            daemon_request(paths, {"command": "shutdown_daemon"}, timeout=timeout)
        except DaemonClientError:
            pass
    graceful_deadline = time.monotonic() + max(timeout, 0.0)
    while time.monotonic() < graceful_deadline:
        still_running = (
            _expected_process_is_running(process_record.process_identity)
            if process_record is not None
            else is_daemon_live(paths)
        )
        if not still_running:
            if process_record is not None:
                _finish_verified_daemon_exit(
                    paths,
                    process_record,
                    windows_job=windows_job,
                )
            else:
                _cleanup_forced_shutdown(paths)
            return
        time.sleep(0.05)
    if process_record is None:
        raise identity_error or DaemonClientError(
            "The local daemon did not stop gracefully and has no verified process identity."
        )
    expected = process_record.process_identity
    if not _expected_process_is_running(expected):
        _finish_verified_daemon_exit(
            paths,
            process_record,
            windows_job=windows_job,
        )
        return
    try:
        if windows_job is not None:
            windows_job.terminate(timeout_seconds=2.0)
        else:
            force_kill_verified_contained_process_tree(
                expected,
                containment_nonce=process_record.containment_nonce,
            )
    except (ProcessInspectionError, ValueError) as exc:
        if not _expected_process_is_running(expected):
            _finish_verified_daemon_exit(
                paths,
                process_record,
                windows_job=windows_job,
            )
            return
        raise DaemonClientError(
            f"Refused to terminate unverified local daemon process {expected.pid}."
        ) from exc
    kill_deadline = time.monotonic() + 2.0
    while time.monotonic() < kill_deadline:
        if not _expected_process_is_running(expected):
            _finish_verified_daemon_exit(
                paths,
                process_record,
                windows_job=windows_job,
            )
            return
        time.sleep(0.05)
    raise DaemonClientError(f"Failed to stop local daemon process {expected.pid}.")


__all__ = [
    "DAEMON_AUTHKEY_FILE_NAME",
    "DaemonClientError",
    "WorkspaceLeaseError",
    "_acquire_startup_lock",
    "_decode_message",
    "_encode_message",
    "_lease_checkpoint_age_seconds",
    "_pid_is_live",
    "_reachable_daemon_process",
    "_recover_broken_local_lease",
    "_release_startup_lock",
    "_remove_stale_unix_endpoint",
    "_same_machine_lease_process",
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
