"""Fail-closed process-group containment for POSIX daemon processes."""

from __future__ import annotations

from contextlib import suppress
import errno
import hmac
import math
import os
from pathlib import Path
import re
import select
import signal
import socket
import stat
import subprocess
import sys
import threading
from typing import Final


_READY_MARKER: Final = "ready:"
_ERROR_MARKER: Final = "error:"
_DEFAULT_READY_TIMEOUT_SECONDS: Final = 2.0
_FALLBACK_PARENT_POLL_SECONDS: Final = 0.1
_CONTROL_REQUEST_PREFIX: Final = b"terminate:"
_CONTAINMENT_NONCE_PATTERN: Final = re.compile(r"[0-9a-f]{64}")
_CONTROL_ENDPOINT_PREFIX: Final = "data-engine-wd-"
_HOST_OS_NAME: Final = os.name


class PosixProcessGroupWatchdogError(RuntimeError):
    """Raised when a POSIX daemon watchdog cannot be armed safely."""


class PosixProcessGroupWatchdog:
    """Retain the live child process that guards one daemon process group."""

    __slots__ = (
        "_adopted_exit",
        "_pid",
        "_process",
        "_supervisor_thread",
        "control_endpoint",
        "monitor_kind",
    )

    def __init__(
        self,
        process: subprocess.Popen[bytes] | None,
        *,
        pid: int | None = None,
        monitor_kind: str,
        control_endpoint: Path,
    ) -> None:
        if process is None and pid is None:
            raise ValueError("A POSIX watchdog handle requires a process identifier.")
        self._process = process
        self._pid = process.pid if process is not None else int(pid)
        self.monitor_kind = monitor_kind
        self.control_endpoint = control_endpoint
        self._supervisor_thread: threading.Thread | None = None
        self._adopted_exit = threading.Event()

    @property
    def pid(self) -> int:
        """Return the operating-system process ID of the watchdog."""
        return self._pid

    @property
    def is_running(self) -> bool:
        """Return whether the armed watchdog child is still running."""
        if self._process is not None:
            return self._process.poll() is None
        return not self._adopted_exit.is_set()

    def _start_supervisor(
        self,
        *,
        parent_pid: int,
        group_id: int,
        session_id: int,
        control_endpoint: Path,
    ) -> None:
        if self._process is None:
            raise PosixProcessGroupWatchdogError(
                "An adopted POSIX watchdog requires the adopted supervisor."
            )
        supervisor = threading.Thread(
            target=_supervise_watchdog,
            args=(self._process,),
            kwargs={
                "parent_pid": parent_pid,
                "group_id": group_id,
                "session_id": session_id,
                "control_endpoint": control_endpoint,
            },
            name="data-engine-posix-watchdog-supervisor",
            daemon=True,
        )
        supervisor.start()
        self._supervisor_thread = supervisor

    def _start_adopted_supervisor(
        self,
        *,
        parent_pid: int,
        group_id: int,
        session_id: int,
        control_endpoint: Path,
    ) -> None:
        supervisor = threading.Thread(
            target=_supervise_adopted_watchdog,
            args=(self.pid,),
            kwargs={
                "parent_pid": parent_pid,
                "group_id": group_id,
                "session_id": session_id,
                "control_endpoint": control_endpoint,
                "exited": self._adopted_exit,
            },
            name="data-engine-posix-watchdog-supervisor",
            daemon=True,
        )
        supervisor.start()
        self._supervisor_thread = supervisor


_ARMED_WATCHDOG: PosixProcessGroupWatchdog | None = None


def arm_posix_process_group_watchdog(
    *,
    containment_nonce: str,
    ready_timeout_seconds: float = _DEFAULT_READY_TIMEOUT_SECONDS,
) -> PosixProcessGroupWatchdog:
    """Arm a direct-child watchdog for the current isolated POSIX process group.

    The watchdog remains in the daemon's dedicated process group for the entire
    daemon lifetime. When the daemon parent exits, the watchdog sends
    ``SIGKILL`` to its own group, including itself and any inherited descendants.
    Keeping the watchdog alive pins the process-group ID and prevents an
    empty-group-to-reused-ID signaling race.

    Linux and macOS wait for kernel parent-exit events without periodic wakeups.
    Other POSIX systems use a small direct-parent polling fallback.

    Args:
        containment_nonce: Canonical launch nonce authenticating forced-stop
            requests to the in-group watchdog.
        ready_timeout_seconds: Maximum time to wait for the child to validate
            its parent, process group, session, and parent-exit monitor.

    Returns:
        The live watchdog handle retained by this module.

    Raises:
        PosixProcessGroupWatchdogError: If the platform is not POSIX, the
            current process is not its isolated group and session leader, or
            the watchdog cannot be started and verified.
        ValueError: If ``ready_timeout_seconds`` is invalid.
    """
    global _ARMED_WATCHDOG

    timeout = _finite_nonnegative_timeout(ready_timeout_seconds)
    nonce = _canonical_containment_nonce(containment_nonce)
    control_endpoint = posix_watchdog_endpoint(nonce)
    armed = _ARMED_WATCHDOG
    if armed is not None:
        if armed.is_running:
            if armed.control_endpoint != control_endpoint:
                raise PosixProcessGroupWatchdogError(
                    "The POSIX process-group watchdog is already armed with a "
                    "different containment nonce."
                )
            return armed
        raise PosixProcessGroupWatchdogError(
            "The POSIX process-group watchdog exited after it was armed."
        )
    if _HOST_OS_NAME != "posix":
        raise PosixProcessGroupWatchdogError(
            "The process-group watchdog requires a POSIX host."
        )
    parent_pid = os.getpid()
    endpoint_owned = False
    try:
        group_id = os.getpgrp()
        session_id = os.getsid(0)
    except OSError as exc:
        raise PosixProcessGroupWatchdogError(
            "Unable to inspect the daemon process group and session."
        ) from exc
    if group_id != parent_pid or session_id != parent_pid:
        raise PosixProcessGroupWatchdogError(
            "The daemon must lead a dedicated process group and session before "
            "its watchdog is armed."
        )

    ready_read_fd, ready_write_fd = os.pipe()
    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            [
                sys.executable,
                "-I",
                "-S",
                str(Path(__file__).resolve()),
                "--watch-parent",
                str(parent_pid),
                str(group_id),
                str(session_id),
                str(ready_write_fd),
                nonce,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            pass_fds=(ready_write_fd,),
        )
    except (OSError, ValueError) as exc:
        os.close(ready_read_fd)
        raise PosixProcessGroupWatchdogError(
            "Unable to launch the POSIX process-group watchdog."
        ) from exc
    finally:
        os.close(ready_write_fd)

    try:
        readable, _, _ = select.select([ready_read_fd], [], [], timeout)
        if not readable:
            raise PosixProcessGroupWatchdogError(
                "Timed out waiting for the POSIX process-group watchdog."
            )
        payload = os.read(ready_read_fd, 4096).decode("utf-8", errors="replace")
        if not payload.startswith(_READY_MARKER):
            detail = payload.removeprefix(_ERROR_MARKER).strip()
            suffix = f": {detail}" if detail else "."
            raise PosixProcessGroupWatchdogError(
                f"The POSIX process-group watchdog failed during startup{suffix}"
            )
        monitor_kind = payload.removeprefix(_READY_MARKER).strip()
        if monitor_kind not in {"pidfd", "kqueue", "parent-poll"}:
            raise PosixProcessGroupWatchdogError(
                "The POSIX process-group watchdog reported an invalid monitor."
            )
        endpoint_owned = True
        if process.poll() is not None:
            raise PosixProcessGroupWatchdogError(
                "The POSIX process-group watchdog exited during startup."
            )
        _verify_private_control_endpoint(control_endpoint)
        try:
            watchdog_group_id = os.getpgid(process.pid)
            watchdog_session_id = os.getsid(process.pid)
        except (ProcessLookupError, PermissionError, OSError) as exc:
            raise PosixProcessGroupWatchdogError(
                "Unable to verify the POSIX process-group watchdog."
            ) from exc
        if watchdog_group_id != group_id or watchdog_session_id != session_id:
            raise PosixProcessGroupWatchdogError(
                "The POSIX process-group watchdog did not remain in the daemon's "
                "dedicated session."
            )
    except BaseException:
        _terminate_unready_watchdog(process)
        if endpoint_owned:
            _unlink_control_endpoint(control_endpoint)
        raise
    finally:
        os.close(ready_read_fd)

    watchdog = PosixProcessGroupWatchdog(
        process,
        monitor_kind=monitor_kind,
        control_endpoint=control_endpoint,
    )
    _ARMED_WATCHDOG = watchdog
    try:
        watchdog._start_supervisor(  # noqa: SLF001 - module-owned lifecycle
            parent_pid=parent_pid,
            group_id=group_id,
            session_id=session_id,
            control_endpoint=control_endpoint,
        )
    except BaseException:
        _ARMED_WATCHDOG = None
        _terminate_unready_watchdog(process)
        _unlink_control_endpoint(control_endpoint)
        raise
    return watchdog


def adopt_posix_process_group_watchdog(
    watchdog_pid: int,
    *,
    containment_nonce: str,
) -> PosixProcessGroupWatchdog:
    """Adopt an already-armed direct-child watchdog after an in-place exec.

    The initial isolated bootstrap arms the watchdog before releasing startup to
    its launcher, then execs a normally configured interpreter in the same
    process. This function re-establishes the in-process supervisor lost across
    that exec without creating a second watchdog.

    Args:
        watchdog_pid: Process ID returned by the first-stage watchdog arm.
        containment_nonce: Canonical nonce naming the existing control endpoint.

    Returns:
        The adopted live watchdog handle retained by this module.

    Raises:
        PosixProcessGroupWatchdogError: If the current process or watchdog no
            longer belongs to the expected isolated session, the process is not
            a live direct child, or the control endpoint is not private.
        ValueError: If the watchdog PID or containment nonce is invalid.
    """
    global _ARMED_WATCHDOG

    if _HOST_OS_NAME != "posix":
        raise PosixProcessGroupWatchdogError(
            "The process-group watchdog requires a POSIX host."
        )
    if isinstance(watchdog_pid, bool) or not isinstance(watchdog_pid, int):
        raise ValueError("A POSIX watchdog PID must be a positive integer.")
    if watchdog_pid <= 0:
        raise ValueError("A POSIX watchdog PID must be a positive integer.")
    nonce = _canonical_containment_nonce(containment_nonce)
    endpoint = posix_watchdog_endpoint(nonce)
    armed = _ARMED_WATCHDOG
    if armed is not None:
        if armed.pid != watchdog_pid or armed.control_endpoint != endpoint:
            raise PosixProcessGroupWatchdogError(
                "A different POSIX process-group watchdog is already armed."
            )
        if not armed.is_running:
            raise PosixProcessGroupWatchdogError(
                "The adopted POSIX process-group watchdog has exited."
            )
        return armed

    parent_pid = os.getpid()
    try:
        group_id = os.getpgrp()
        session_id = os.getsid(0)
        watchdog_group_id = os.getpgid(watchdog_pid)
        watchdog_session_id = os.getsid(watchdog_pid)
    except (ProcessLookupError, PermissionError, OSError) as exc:
        raise PosixProcessGroupWatchdogError(
            "Unable to inspect the adopted POSIX process-group watchdog."
        ) from exc
    if group_id != parent_pid or session_id != parent_pid:
        raise PosixProcessGroupWatchdogError(
            "The daemon must lead a dedicated process group and session before "
            "its watchdog is adopted."
        )
    if watchdog_group_id != group_id or watchdog_session_id != session_id:
        raise PosixProcessGroupWatchdogError(
            "The adopted watchdog is outside the daemon's dedicated session."
        )
    _verify_private_control_endpoint(endpoint)
    try:
        waited_pid, _status = os.waitpid(watchdog_pid, os.WNOHANG)
    except ChildProcessError as exc:
        raise PosixProcessGroupWatchdogError(
            "The adopted watchdog is not a direct child of the daemon."
        ) from exc
    except OSError as exc:
        raise PosixProcessGroupWatchdogError(
            "Unable to verify the adopted POSIX process-group watchdog."
        ) from exc
    if waited_pid != 0:
        raise PosixProcessGroupWatchdogError(
            "The adopted POSIX process-group watchdog exited before adoption."
        )

    watchdog = PosixProcessGroupWatchdog(
        None,
        pid=watchdog_pid,
        monitor_kind="adopted",
        control_endpoint=endpoint,
    )
    _ARMED_WATCHDOG = watchdog
    try:
        watchdog._start_adopted_supervisor(  # noqa: SLF001 - module-owned lifecycle
            parent_pid=parent_pid,
            group_id=group_id,
            session_id=session_id,
            control_endpoint=endpoint,
        )
    except BaseException:
        _ARMED_WATCHDOG = None
        raise
    return watchdog


def _canonical_containment_nonce(value: str) -> str:
    if not isinstance(value, str) or _CONTAINMENT_NONCE_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "A process-containment nonce must be exactly 64 lowercase hexadecimal characters."
        )
    return value


def _verify_private_control_endpoint(control_endpoint: Path) -> None:
    try:
        endpoint_stat = control_endpoint.stat()
    except OSError as exc:
        raise PosixProcessGroupWatchdogError(
            "Unable to verify the POSIX watchdog control endpoint."
        ) from exc
    if (
        not stat.S_ISSOCK(endpoint_stat.st_mode)
        or endpoint_stat.st_uid != os.geteuid()
        or stat.S_IMODE(endpoint_stat.st_mode) != 0o600
    ):
        raise PosixProcessGroupWatchdogError(
            "The POSIX watchdog control endpoint is not private to this user."
        )


def posix_watchdog_endpoint(containment_nonce: str) -> Path:
    """Return the short private control-socket path for one containment nonce."""
    nonce = _canonical_containment_nonce(containment_nonce)
    return Path("/tmp") / f"{_CONTROL_ENDPOINT_PREFIX}{nonce}.sock"


def request_posix_process_group_termination(
    *,
    containment_nonce: str,
    expected_parent_pid: int,
) -> None:
    """Ask an authenticated in-group watchdog to terminate its own process group."""
    nonce = _canonical_containment_nonce(containment_nonce)
    if isinstance(expected_parent_pid, bool) or not isinstance(expected_parent_pid, int):
        raise ValueError("A watchdog parent PID must be a positive integer.")
    if expected_parent_pid <= 0:
        raise ValueError("A watchdog parent PID must be a positive integer.")
    endpoint = posix_watchdog_endpoint(nonce)
    payload = _control_request_payload(nonce, expected_parent_pid)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as control_socket:
            control_socket.setblocking(False)
            sent = control_socket.sendto(payload, str(endpoint))
    except OSError as exc:
        raise PosixProcessGroupWatchdogError(
            f"Unable to request termination from watchdog for process {expected_parent_pid}."
        ) from exc
    if sent != len(payload):
        raise PosixProcessGroupWatchdogError(
            f"The watchdog termination request for process {expected_parent_pid} was incomplete."
        )


def _cleanup_posix_watchdog_endpoint(containment_nonce: str) -> None:
    """Remove a no-longer-live watchdog control endpoint."""
    endpoint = posix_watchdog_endpoint(containment_nonce)
    with suppress(FileNotFoundError):
        endpoint.unlink()


def _finite_nonnegative_timeout(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError("A watchdog timeout must be a finite nonnegative number.")
    timeout = float(value)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("A watchdog timeout must be a finite nonnegative number.")
    return timeout


def _terminate_unready_watchdog(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    with suppress(OSError):
        process.kill()
    with suppress(OSError, subprocess.TimeoutExpired):
        process.wait(timeout=1.0)


def _block_group_termination_signals() -> None:
    selected_signals = set(signal.valid_signals())
    selected_signals.discard(signal.SIGKILL)
    selected_signals.discard(signal.SIGSTOP)
    pthread_sigmask = getattr(signal, "pthread_sigmask", None)
    if callable(pthread_sigmask):
        pthread_sigmask(signal.SIG_BLOCK, selected_signals)
        return
    for selected_signal in selected_signals:
        with suppress(OSError, ValueError):
            signal.signal(selected_signal, signal.SIG_IGN)


def _supervise_watchdog(
    process: subprocess.Popen[bytes],
    *,
    parent_pid: int,
    group_id: int,
    session_id: int,
    control_endpoint: Path,
) -> None:
    """Fail the daemon group closed if its armed watchdog exits first."""
    process.wait()
    try:
        if os.getpid() != parent_pid:
            return
        _unlink_control_endpoint(control_endpoint)
        _kill_current_verified_group(group_id=group_id, session_id=session_id)
    except BaseException:
        os._exit(70)


def _supervise_adopted_watchdog(
    watchdog_pid: int,
    *,
    parent_pid: int,
    group_id: int,
    session_id: int,
    control_endpoint: Path,
    exited: threading.Event,
) -> None:
    """Fail the daemon group closed if its adopted watchdog exits first."""
    try:
        waited_pid, _status = os.waitpid(watchdog_pid, 0)
        if waited_pid != watchdog_pid:
            raise PosixProcessGroupWatchdogError(
                "The adopted watchdog wait returned a different process."
            )
        exited.set()
        if os.getpid() != parent_pid:
            return
        _unlink_control_endpoint(control_endpoint)
        _kill_current_verified_group(group_id=group_id, session_id=session_id)
    except BaseException:
        os._exit(70)


def _open_parent_exit_monitor(parent_pid: int) -> tuple[str, object | None]:
    pidfd_open = getattr(os, "pidfd_open", None)
    if sys.platform.startswith("linux") and callable(pidfd_open):
        try:
            return "pidfd", pidfd_open(parent_pid, 0)
        except ProcessLookupError:
            return "parent-exited", None
        except OSError as exc:
            if exc.errno not in {errno.ENOSYS, errno.EINVAL, errno.EPERM}:
                raise
    if sys.platform == "darwin" and hasattr(select, "kqueue"):
        queue = select.kqueue()
        event = select.kevent(
            parent_pid,
            filter=select.KQ_FILTER_PROC,
            flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_ONESHOT,
            fflags=select.KQ_NOTE_EXIT,
        )
        try:
            queue.control([event], 0, 0)
        except ProcessLookupError:
            queue.close()
            return "parent-exited", None
        except OSError as exc:
            queue.close()
            if exc.errno == errno.ESRCH:
                return "parent-exited", None
            raise
        return "kqueue", queue
    return "parent-poll", None


def _wait_for_parent_exit_or_control_request(
    parent_pid: int,
    *,
    monitor_kind: str,
    monitor: object | None,
    control_socket: socket.socket,
    expected_request: bytes,
) -> None:
    if monitor_kind == "pidfd":
        poller = select.poll()
        poller.register(int(monitor), select.POLLIN)
        poller.register(control_socket.fileno(), select.POLLIN)
        try:
            while True:
                for ready_fd, _event in poller.poll():
                    if ready_fd == int(monitor):
                        return
                    if ready_fd == control_socket.fileno() and _receive_valid_control_request(
                        control_socket,
                        expected_request,
                    ):
                        return
        finally:
            os.close(int(monitor))
        return
    if monitor_kind == "kqueue":
        queue = monitor
        assert isinstance(queue, select.kqueue)
        control_event = select.kevent(
            control_socket.fileno(),
            filter=select.KQ_FILTER_READ,
            flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE,
        )
        try:
            queue.control([control_event], 0, 0)
            while True:
                for event in queue.control(None, 2, None):
                    if event.filter == select.KQ_FILTER_PROC:
                        return
                    if (
                        event.filter == select.KQ_FILTER_READ
                        and event.ident == control_socket.fileno()
                        and _receive_valid_control_request(control_socket, expected_request)
                    ):
                        return
        finally:
            queue.close()
        return
    while os.getppid() == parent_pid:
        readable, _, _ = select.select(
            [control_socket],
            [],
            [],
            _FALLBACK_PARENT_POLL_SECONDS,
        )
        if readable and _receive_valid_control_request(control_socket, expected_request):
            return


def _control_request_payload(containment_nonce: str, parent_pid: int) -> bytes:
    return _CONTROL_REQUEST_PREFIX + f"{containment_nonce}:{parent_pid}".encode("ascii")


def _receive_valid_control_request(
    control_socket: socket.socket,
    expected_request: bytes,
) -> bool:
    payload = control_socket.recv(512)
    return hmac.compare_digest(payload, expected_request)


def _bind_control_socket(control_endpoint: Path) -> socket.socket:
    control_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    bound = False
    previous_umask = os.umask(0o177)
    try:
        control_socket.bind(str(control_endpoint))
        bound = True
        control_endpoint.chmod(0o600)
        return control_socket
    except BaseException:
        control_socket.close()
        if bound:
            _unlink_control_endpoint(control_endpoint)
        raise
    finally:
        os.umask(previous_umask)


def _unlink_control_endpoint(control_endpoint: Path) -> None:
    with suppress(FileNotFoundError):
        control_endpoint.unlink()


def _report_ready(ready_fd: int, monitor_kind: str) -> None:
    os.write(ready_fd, f"{_READY_MARKER}{monitor_kind}".encode("utf-8"))


def _report_error(ready_fd: int, exc: BaseException) -> None:
    detail = f"{type(exc).__name__}: {exc}"[:2048]
    with suppress(OSError):
        os.write(ready_fd, f"{_ERROR_MARKER}{detail}".encode("utf-8"))


def _kill_current_verified_group(*, group_id: int, session_id: int) -> None:
    if os.getpgrp() != group_id or os.getsid(0) != session_id:
        raise PosixProcessGroupWatchdogError(
            "The watchdog left its recorded process group or session."
        )
    os.kill(0, signal.SIGKILL)


def _watch_parent(
    *,
    parent_pid: int,
    group_id: int,
    session_id: int,
    ready_fd: int,
    containment_nonce: str,
) -> int:
    armed = False
    control_socket: socket.socket | None = None
    control_endpoint = posix_watchdog_endpoint(containment_nonce)
    try:
        _block_group_termination_signals()
        if os.getpgrp() != group_id or os.getsid(0) != session_id:
            raise PosixProcessGroupWatchdogError(
                "The watchdog did not inherit the expected process group and session."
            )
        control_socket = _bind_control_socket(control_endpoint)
        monitor_kind, monitor = _open_parent_exit_monitor(parent_pid)
        if monitor_kind == "parent-exited" or os.getppid() != parent_pid:
            _kill_current_verified_group(group_id=group_id, session_id=session_id)
        _report_ready(ready_fd, monitor_kind)
        armed = True
        os.close(ready_fd)
        ready_fd = -1
        _wait_for_parent_exit_or_control_request(
            parent_pid,
            monitor_kind=monitor_kind,
            monitor=monitor,
            control_socket=control_socket,
            expected_request=_control_request_payload(containment_nonce, parent_pid),
        )
        _unlink_control_endpoint(control_endpoint)
        _kill_current_verified_group(group_id=group_id, session_id=session_id)
    except BaseException as exc:
        if armed:
            _unlink_control_endpoint(control_endpoint)
            with suppress(BaseException):
                _kill_current_verified_group(group_id=group_id, session_id=session_id)
        else:
            _report_error(ready_fd, exc)
        return 70
    finally:
        if control_socket is not None:
            control_socket.close()
            if not armed:
                _unlink_control_endpoint(control_endpoint)
        if ready_fd >= 0:
            with suppress(OSError):
                os.close(ready_fd)
    return 0


def _watchdog_main(argv: list[str]) -> int:
    if len(argv) != 6 or argv[0] != "--watch-parent":
        return 64
    try:
        parent_pid, group_id, session_id, ready_fd = map(int, argv[1:5])
        containment_nonce = _canonical_containment_nonce(argv[5])
    except ValueError:
        return 64
    if min(parent_pid, group_id, session_id, ready_fd) < 0:
        return 64
    return _watch_parent(
        parent_pid=parent_pid,
        group_id=group_id,
        session_id=session_id,
        ready_fd=ready_fd,
        containment_nonce=containment_nonce,
    )


__all__ = [
    "PosixProcessGroupWatchdog",
    "PosixProcessGroupWatchdogError",
    "adopt_posix_process_group_watchdog",
    "arm_posix_process_group_watchdog",
    "posix_watchdog_endpoint",
    "request_posix_process_group_termination",
]


if __name__ == "__main__":
    raise SystemExit(_watchdog_main(sys.argv[1:]))
