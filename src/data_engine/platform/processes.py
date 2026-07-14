"""Platform-aware process helpers."""

from __future__ import annotations

import ctypes
import ctypes.util
from dataclasses import dataclass
import errno
import json
import math
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys

from data_engine.domain.diagnostics import (
    ClassifiedProcessInfo,
    ProcessInfo,
    is_defunct_process_status,
)
from data_engine.platform.paths import stable_path_identity_text

_WINDOWS_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_WINDOWS_SYNCHRONIZE = 0x00100000
_WINDOWS_STILL_ACTIVE = 259
_WINDOWS_WAIT_OBJECT_0 = 0
_WINDOWS_WAIT_TIMEOUT = 258
_WINDOWS_WAIT_FAILED = 0xFFFFFFFF
_WINDOWS_INFINITE = 0xFFFFFFFF
_WINDOWS_MAX_FINITE_TIMEOUT_MILLISECONDS = _WINDOWS_INFINITE - 1
_CREATE_NEW_PROCESS_GROUP = 0x00000200
_CREATE_NO_WINDOW = 0x08000000
_DETACHED_PROCESS = 0x00000008
_WINDOWS_ERROR_FILE_NOT_FOUND = 2
_WINDOWS_ERROR_ALREADY_EXISTS = 183
_WINDOWS_ERROR_INVALID_PARAMETER = 87
_WINDOWS_ERROR_NOT_FOUND = 1168
_WINDOWS_PROCESS_PATH_BUFFER_SIZE = 32_768
_WINDOWS_JOB_OBJECT_ASSIGN_PROCESS = 0x0001
_WINDOWS_JOB_OBJECT_QUERY = 0x0004
_WINDOWS_JOB_OBJECT_TERMINATE = 0x0008
_WINDOWS_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
_WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
_WINDOWS_JOB_NAME_PREFIX = "Local\\DataEngineDaemonJob-"
_WINDOWS_JOB_NONCE_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_PIDFD_SIGNAL_PROCESS_GROUP = 1 << 2
_DARWIN_PROC_PIDTBSDINFO = 3
_DARWIN_PROC_PIDPATHINFO_MAXSIZE = 4096


class ProcessInspectionError(RuntimeError):
    """Raised when the local process table cannot be inspected."""


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    """Immutable operating-system identity for one process incarnation.

    Attributes:
        pid: Operating-system process identifier.
        start_key: Boot-scoped process creation identity.
        executable_path: Normalized executable path identity.
        process_group_id: Process group identifier when the platform exposes one.
        process_session_id: Process session identifier.
    """

    pid: int
    start_key: str
    executable_path: str
    process_group_id: int | None
    process_session_id: int | None


@dataclass(frozen=True, slots=True)
class _LinuxProcStat:
    process_group_id: int
    process_session_id: int
    start_ticks: int


class _DarwinProcBsdInfo(ctypes.Structure):
    _fields_ = [
        ("pbi_flags", ctypes.c_uint32),
        ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32),
        ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32),
        ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32),
        ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32),
        ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32),
        ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * 16),
        ("pbi_name", ctypes.c_char * 32),
        ("pbi_nfiles", ctypes.c_uint32),
        ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32),
        ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32),
        ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


@dataclass(frozen=True, slots=True)
class _DarwinStartIdentity:
    pid: int
    start_seconds: int
    start_microseconds: int


class _WindowsFileTime(ctypes.Structure):
    _fields_ = [
        ("low", ctypes.c_uint32),
        ("high", ctypes.c_uint32),
    ]


class _WindowsIoCounters(ctypes.Structure):
    _fields_ = [
        ("read_operation_count", ctypes.c_uint64),
        ("write_operation_count", ctypes.c_uint64),
        ("other_operation_count", ctypes.c_uint64),
        ("read_transfer_count", ctypes.c_uint64),
        ("write_transfer_count", ctypes.c_uint64),
        ("other_transfer_count", ctypes.c_uint64),
    ]


class _WindowsJobBasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("per_process_user_time_limit", ctypes.c_int64),
        ("per_job_user_time_limit", ctypes.c_int64),
        ("limit_flags", ctypes.c_uint32),
        ("minimum_working_set_size", ctypes.c_size_t),
        ("maximum_working_set_size", ctypes.c_size_t),
        ("active_process_limit", ctypes.c_uint32),
        ("affinity", ctypes.c_size_t),
        ("priority_class", ctypes.c_uint32),
        ("scheduling_class", ctypes.c_uint32),
    ]


class _WindowsJobExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("basic_limit_information", _WindowsJobBasicLimitInformation),
        ("io_info", _WindowsIoCounters),
        ("process_memory_limit", ctypes.c_size_t),
        ("job_memory_limit", ctypes.c_size_t),
        ("peak_process_memory_used", ctypes.c_size_t),
        ("peak_job_memory_used", ctypes.c_size_t),
    ]


class WindowsKillOnCloseJob:
    """Own one native Windows Job configured to terminate all members on close.

    Use :func:`create_windows_kill_on_close_job` or
    :func:`open_windows_kill_on_close_job` to construct an instance. Call
    :meth:`close` when the owning component no longer needs the native handle.
    """

    __slots__ = ("_handle", "name", "nonce")

    def __init__(self, *, nonce: str, name: str, handle: object) -> None:
        canonical_name = windows_job_name_for_nonce(nonce)
        if name != canonical_name:
            raise ValueError("A Windows Job name must match its containment nonce.")
        if not handle:
            raise ValueError("A Windows Job wrapper requires an open native handle.")
        self.nonce = nonce
        self.name = name
        self._handle: object | None = handle

    @property
    def closed(self) -> bool:
        """Return whether this wrapper has released its native Job handle."""
        return self._handle is None

    def close(self) -> None:
        """Release this wrapper's native Job handle exactly once."""
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        _close_windows_handle(handle)

    def terminate(self, *, timeout_seconds: float = 2.0, exit_code: int = 1) -> None:
        """Terminate every process in the Job and wait until it is empty.

        Args:
            timeout_seconds: Maximum time to wait for the Job to become empty.
            exit_code: Unsigned process exit code applied by Windows.

        Raises:
            ProcessInspectionError: If termination or waiting fails.
            ValueError: If the timeout or exit code is invalid.
        """
        if isinstance(exit_code, bool) or not isinstance(exit_code, int):
            raise ValueError("A Windows Job exit code must be an integer.")
        if not 0 <= exit_code <= 0xFFFFFFFF:
            raise ValueError("A Windows Job exit code must fit an unsigned 32-bit value.")
        _finite_windows_timeout_milliseconds(timeout_seconds)
        handle = self._native_handle()
        _terminate_windows_job(handle, name=self.name, exit_code=exit_code)
        _wait_for_windows_job_empty(
            handle,
            name=self.name,
            timeout_seconds=timeout_seconds,
        )

    def __enter__(self) -> WindowsKillOnCloseJob:
        """Return this open Job wrapper for context-manager use."""
        if self._handle is None:
            raise ProcessInspectionError("The Windows Job handle is closed.")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Release the native Job handle when leaving a context."""
        self.close()

    def _native_handle(self) -> object:
        if self._handle is None:
            raise ProcessInspectionError("The Windows Job handle is closed.")
        return self._handle


def inspect_process_identity(pid: int) -> ProcessIdentity | None:
    """Return a stable identity for one live process, or ``None`` when absent.

    Raises:
        ProcessInspectionError: If the process exists but its complete identity
            cannot be read, or if it changes while being inspected.
    """
    if pid <= 0:
        return None
    if os.name == "nt":
        return _inspect_windows_process_identity(pid)
    if sys.platform == "darwin":
        return _inspect_darwin_process_identity(pid)
    if sys.platform.startswith("linux"):
        return _inspect_linux_process_identity(pid)
    raise ProcessInspectionError(
        f"Process identity inspection is unsupported on {sys.platform!r}."
    )


def new_process_containment_nonce() -> str:
    """Return a new 256-bit nonce for an operating-system containment object."""
    return secrets.token_hex(32)


def windows_job_name_for_nonce(nonce: str) -> str:
    """Return the canonical local Windows Job name for a containment nonce.

    Args:
        nonce: Exactly 64 lowercase hexadecimal characters produced by
            :func:`new_process_containment_nonce`.

    Raises:
        ValueError: If ``nonce`` is not the canonical 256-bit representation.
    """
    if not isinstance(nonce, str) or _WINDOWS_JOB_NONCE_PATTERN.fullmatch(nonce) is None:
        raise ValueError(
            "A process-containment nonce must be exactly 64 lowercase hexadecimal characters."
        )
    return f"{_WINDOWS_JOB_NAME_PREFIX}{nonce}"


def force_kill_verified_process_tree(expected: ProcessIdentity) -> None:
    """Force-kill a process tree only when its complete identity still matches.

    Args:
        expected: Previously captured process identity to verify immediately
            before signaling.

    Raises:
        ProcessInspectionError: If identity inspection fails, the process is
            absent or changed, or the platform cannot safely target its tree.
    """
    if os.name == "nt":
        raise ProcessInspectionError(
            "Verified Windows tree termination requires a nonce-named Job object."
        )
    _force_kill_verified_posix_process_group(expected)


def force_kill_verified_contained_process_tree(
    expected: ProcessIdentity,
    *,
    windows_job_nonce: str | None = None,
    timeout_seconds: float = 2.0,
) -> None:
    """Force-kill one identity-verified, operating-system-contained process tree.

    Linux targets a pidfd-bound process group when the running kernel supports
    it. Other POSIX hosts perform an exact identity reinspection immediately
    before signaling an isolated process group. Windows verifies membership in
    the nonce-named Job before terminating that Job.

    Args:
        expected: Previously captured process identity for the containment leader.
        windows_job_nonce: Persisted 256-bit containment nonce. Required on Windows
            and ignored on POSIX hosts.
        timeout_seconds: Maximum time to wait for a terminated Windows Job to empty.

    Raises:
        ProcessInspectionError: If containment or identity verification fails, the
            target disappears or changes, or termination cannot be confirmed.
        ValueError: If a Windows nonce or timeout is invalid.
    """
    if os.name == "nt":
        if windows_job_nonce is None:
            raise ProcessInspectionError(
                "Verified Windows tree termination requires a containment nonce."
            )
        _force_kill_verified_windows_job(
            expected,
            nonce=windows_job_nonce,
            timeout_seconds=timeout_seconds,
        )
        return
    _force_kill_verified_posix_process_group(expected)


def _force_kill_verified_posix_process_group(expected: ProcessIdentity) -> None:
    if sys.platform.startswith("linux"):
        _force_kill_verified_linux_process_group(expected)
        return
    _killpg_after_exact_reinspection(expected)


def _force_kill_verified_linux_process_group(expected: ProcessIdentity) -> None:
    pidfd_open = getattr(os, "pidfd_open", None)
    pidfd_send_signal = getattr(signal, "pidfd_send_signal", None)
    if not callable(pidfd_open) or not callable(pidfd_send_signal):
        _killpg_after_exact_reinspection(expected)
        return

    try:
        pidfd = pidfd_open(expected.pid, 0)
    except ProcessLookupError as exc:
        raise ProcessInspectionError(
            f"Local process {expected.pid} is no longer running."
        ) from exc
    except OSError as exc:
        if exc.errno in {errno.ENOSYS, errno.EOPNOTSUPP}:
            _killpg_after_exact_reinspection(expected)
            return
        raise ProcessInspectionError(
            f"Unable to open a pidfd for local process {expected.pid}."
        ) from exc

    try:
        caller_pid, caller_group_id = _posix_caller_identity()
        _inspect_verified_isolated_process_group(
            expected,
            caller_pid=caller_pid,
            caller_group_id=caller_group_id,
        )
        try:
            pidfd_send_signal(
                pidfd,
                signal.SIGKILL,
                None,
                _PIDFD_SIGNAL_PROCESS_GROUP,
            )
        except OSError as exc:
            if exc.errno not in {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}:
                raise ProcessInspectionError(
                    f"Failed to terminate verified local process group {expected.pid}."
                ) from exc
            _killpg_after_exact_reinspection(expected)
    finally:
        try:
            os.close(pidfd)
        except OSError:
            pass


def _killpg_after_exact_reinspection(expected: ProcessIdentity) -> None:
    caller_pid, caller_group_id = _posix_caller_identity()
    actual = _inspect_verified_isolated_process_group(
        expected,
        caller_pid=caller_pid,
        caller_group_id=caller_group_id,
    )
    try:
        os.killpg(actual.pid, signal.SIGKILL)
    except OSError as exc:
        raise ProcessInspectionError(
            f"Failed to terminate verified local process group {actual.pid}."
        ) from exc


def _posix_caller_identity() -> tuple[int, int]:
    try:
        return os.getpid(), os.getpgrp()
    except OSError as exc:
        raise ProcessInspectionError(
            "Unable to inspect the caller process group."
        ) from exc


def _inspect_verified_isolated_process_group(
    expected: ProcessIdentity,
    *,
    caller_pid: int,
    caller_group_id: int,
) -> ProcessIdentity:
    actual = inspect_process_identity(expected.pid)
    if actual is None:
        raise ProcessInspectionError(
            f"Local process {expected.pid} is no longer running."
        )
    if actual != expected:
        raise ProcessInspectionError(
            f"Local process {expected.pid} no longer matches its recorded identity."
        )
    pid = actual.pid
    if actual.process_group_id != pid or actual.process_session_id != pid:
        raise ProcessInspectionError(
            f"Local process {pid} is not the isolated leader of its process group and session."
        )
    if pid == caller_pid or actual.process_group_id == caller_group_id:
        raise ProcessInspectionError(
            f"Refusing to terminate the caller's process group {caller_group_id}."
        )
    return actual


def _inspect_linux_process_identity(pid: int) -> ProcessIdentity | None:
    first = _read_linux_proc_stat(pid)
    if first is None:
        return None
    first_executable_path = _read_linux_executable_if_live(pid)
    if first_executable_path is None:
        return None
    normalized_executable_path = _normalize_executable_path(first_executable_path)
    boot_id = _read_linux_boot_id()
    middle = _read_linux_proc_stat(pid)
    if middle is None:
        return None
    if middle != first:
        raise ProcessInspectionError(
            f"Local process {pid} changed while its identity was inspected."
        )
    second_executable_path = _read_linux_executable_if_live(pid)
    if second_executable_path is None:
        return None
    if _normalize_executable_path(second_executable_path) != normalized_executable_path:
        raise ProcessInspectionError(
            f"Local process {pid} changed executable while its identity was inspected."
        )
    final = _read_linux_proc_stat(pid)
    if final is None:
        return None
    if final != first:
        raise ProcessInspectionError(
            f"Local process {pid} changed while its identity was inspected."
        )
    return ProcessIdentity(
        pid=pid,
        start_key=f"linux:{boot_id}:{first.start_ticks}",
        executable_path=normalized_executable_path,
        process_group_id=final.process_group_id,
        process_session_id=final.process_session_id,
    )


def _read_linux_proc_stat(pid: int) -> _LinuxProcStat | None:
    path = Path("/proc") / str(pid) / "stat"
    try:
        payload = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except (PermissionError, OSError, UnicodeError) as exc:
        raise ProcessInspectionError(f"Unable to inspect local process {pid}.") from exc
    return _parse_linux_proc_stat(payload, pid=pid)


def _parse_linux_proc_stat(payload: str, *, pid: int) -> _LinuxProcStat:
    closing_parenthesis = payload.rfind(")")
    expected_prefix = f"{pid} ("
    if closing_parenthesis < len(expected_prefix) or not payload.startswith(
        expected_prefix
    ):
        raise ProcessInspectionError(
            f"Malformed /proc stat data for local process {pid}."
        )
    fields = payload[closing_parenthesis + 1 :].split()
    if len(fields) <= 19:
        raise ProcessInspectionError(
            f"Incomplete /proc stat data for local process {pid}."
        )
    try:
        process_group_id = int(fields[2])
        process_session_id = int(fields[3])
        start_ticks = int(fields[19])
    except ValueError as exc:
        raise ProcessInspectionError(
            f"Malformed /proc stat data for local process {pid}."
        ) from exc
    if process_group_id <= 0 or process_session_id <= 0 or start_ticks <= 0:
        raise ProcessInspectionError(
            f"Invalid /proc stat identity for local process {pid}."
        )
    return _LinuxProcStat(
        process_group_id=process_group_id,
        process_session_id=process_session_id,
        start_ticks=start_ticks,
    )


def _read_linux_executable(pid: int) -> str:
    try:
        return os.readlink(Path("/proc") / str(pid) / "exe")
    except FileNotFoundError:
        raise
    except (PermissionError, OSError) as exc:
        raise ProcessInspectionError(
            f"Unable to inspect executable identity for local process {pid}."
        ) from exc


def _read_linux_executable_if_live(pid: int) -> str | None:
    try:
        return _read_linux_executable(pid)
    except FileNotFoundError:
        if _read_linux_proc_stat(pid) is None:
            return None
        raise ProcessInspectionError(
            f"Executable identity is unavailable for local process {pid}."
        ) from None


def _read_linux_boot_id() -> str:
    try:
        boot_id = (
            Path("/proc/sys/kernel/random/boot_id")
            .read_text(encoding="ascii")
            .strip()
            .casefold()
        )
    except (FileNotFoundError, PermissionError, OSError, UnicodeError) as exc:
        raise ProcessInspectionError(
            "Unable to inspect the Linux boot identity."
        ) from exc
    if not boot_id:
        raise ProcessInspectionError("The Linux boot identity is empty.")
    return boot_id


def _inspect_darwin_process_identity(pid: int) -> ProcessIdentity | None:
    first = _read_darwin_start_identity(pid)
    if first is None:
        return None
    first_executable_path = _read_darwin_executable(pid)
    if first_executable_path is None:
        return None
    normalized_executable_path = _normalize_executable_path(first_executable_path)
    first_grouping = _read_darwin_process_grouping(pid)
    if first_grouping is None:
        return None
    process_group_id, process_session_id = first_grouping
    boot_session_key = _read_darwin_boot_session_key()
    second = _read_darwin_start_identity(pid)
    if second is None:
        return None
    if second != first:
        raise ProcessInspectionError(
            f"Local process {pid} changed while its identity was inspected."
        )
    second_executable_path = _read_darwin_executable(pid)
    if second_executable_path is None:
        return None
    if _normalize_executable_path(second_executable_path) != normalized_executable_path:
        raise ProcessInspectionError(
            f"Local process {pid} changed executable while its identity was inspected."
        )
    second_grouping = _read_darwin_process_grouping(pid)
    if second_grouping is None:
        return None
    final = _read_darwin_start_identity(pid)
    if final is None:
        return None
    if final != first or second_grouping != first_grouping:
        raise ProcessInspectionError(
            f"Local process {pid} changed while its identity was inspected."
        )
    return ProcessIdentity(
        pid=pid,
        start_key=(
            f"darwin:{boot_session_key}:{first.start_seconds}:{first.start_microseconds}"
        ),
        executable_path=normalized_executable_path,
        process_group_id=process_group_id,
        process_session_id=process_session_id,
    )


def _read_darwin_process_grouping(pid: int) -> tuple[int, int] | None:
    try:
        return os.getpgid(pid), os.getsid(pid)
    except ProcessLookupError:
        return None
    except (PermissionError, OSError) as exc:
        raise ProcessInspectionError(
            f"Unable to inspect process grouping for local process {pid}."
        ) from exc


def _load_darwin_library(name: str) -> ctypes.CDLL:
    path = ctypes.util.find_library(name)
    if not path:
        raise ProcessInspectionError(
            f"Unable to load the macOS {name!r} process-inspection library."
        )
    try:
        return ctypes.CDLL(path, use_errno=True)
    except OSError as exc:
        raise ProcessInspectionError(
            f"Unable to load the macOS {name!r} process-inspection library."
        ) from exc


def _read_darwin_start_identity(pid: int) -> _DarwinStartIdentity | None:
    libproc = _load_darwin_library("proc")
    proc_pidinfo = libproc.proc_pidinfo
    proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    proc_pidinfo.restype = ctypes.c_int
    info = _DarwinProcBsdInfo()
    ctypes.set_errno(0)
    byte_count = proc_pidinfo(
        pid,
        _DARWIN_PROC_PIDTBSDINFO,
        0,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    if byte_count == 0 and ctypes.get_errno() == errno.ESRCH:
        return None
    if byte_count != ctypes.sizeof(info):
        error_number = ctypes.get_errno()
        detail = (
            os.strerror(error_number) if error_number else "incomplete process data"
        )
        raise ProcessInspectionError(
            f"Unable to inspect local process {pid}: {detail}."
        )
    if (
        info.pbi_pid != pid
        or info.pbi_start_tvsec <= 0
        or info.pbi_start_tvusec >= 1_000_000
    ):
        raise ProcessInspectionError(
            f"Invalid macOS process identity for local process {pid}."
        )
    return _DarwinStartIdentity(
        pid=int(info.pbi_pid),
        start_seconds=int(info.pbi_start_tvsec),
        start_microseconds=int(info.pbi_start_tvusec),
    )


def _read_darwin_executable(pid: int) -> str | None:
    libproc = _load_darwin_library("proc")
    proc_pidpath = libproc.proc_pidpath
    proc_pidpath.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
    proc_pidpath.restype = ctypes.c_int
    buffer = ctypes.create_string_buffer(_DARWIN_PROC_PIDPATHINFO_MAXSIZE)
    ctypes.set_errno(0)
    byte_count = proc_pidpath(pid, buffer, len(buffer))
    if byte_count == 0 and ctypes.get_errno() == errno.ESRCH:
        return None
    if byte_count <= 0:
        error_number = ctypes.get_errno()
        detail = os.strerror(error_number) if error_number else "empty executable path"
        raise ProcessInspectionError(
            f"Unable to inspect executable identity for local process {pid}: {detail}."
        )
    try:
        executable_path = os.fsdecode(buffer.value)
    except UnicodeError as exc:
        raise ProcessInspectionError(
            f"Invalid executable identity for local process {pid}."
        ) from exc
    if not executable_path:
        raise ProcessInspectionError(
            f"Empty executable identity for local process {pid}."
        )
    return executable_path


def _read_darwin_boot_session_key() -> str:
    boot_session_uuid = _read_darwin_sysctl_text("kern.bootsessionuuid")
    if boot_session_uuid:
        return f"uuid:{boot_session_uuid.casefold()}"

    libc = _load_darwin_library("c")
    sysctlbyname = libc.sysctlbyname
    sysctlbyname.argtypes = [
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    sysctlbyname.restype = ctypes.c_int

    class _Timeval(ctypes.Structure):
        _fields_ = [("seconds", ctypes.c_long), ("microseconds", ctypes.c_int)]

    boot_time = _Timeval()
    size = ctypes.c_size_t(ctypes.sizeof(boot_time))
    ctypes.set_errno(0)
    result = sysctlbyname(
        b"kern.boottime",
        ctypes.byref(boot_time),
        ctypes.byref(size),
        None,
        0,
    )
    if result != 0 or size.value < ctypes.sizeof(boot_time) or boot_time.seconds <= 0:
        error_number = ctypes.get_errno()
        detail = os.strerror(error_number) if error_number else "invalid boot time"
        raise ProcessInspectionError(
            f"Unable to inspect the macOS boot identity: {detail}."
        )
    return f"time:{boot_time.seconds}:{boot_time.microseconds}"


def _read_darwin_sysctl_text(name: str) -> str | None:
    libc = _load_darwin_library("c")
    sysctlbyname = libc.sysctlbyname
    sysctlbyname.argtypes = [
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_size_t),
        ctypes.c_void_p,
        ctypes.c_size_t,
    ]
    sysctlbyname.restype = ctypes.c_int
    encoded_name = name.encode("ascii")
    size = ctypes.c_size_t()
    ctypes.set_errno(0)
    if sysctlbyname(encoded_name, None, ctypes.byref(size), None, 0) != 0:
        if ctypes.get_errno() in {errno.ENOENT, errno.EINVAL}:
            return None
        error_number = ctypes.get_errno()
        detail = os.strerror(error_number) if error_number else "unknown error"
        raise ProcessInspectionError(
            f"Unable to inspect macOS sysctl {name!r}: {detail}."
        )
    if size.value <= 1:
        return None
    buffer = ctypes.create_string_buffer(size.value)
    ctypes.set_errno(0)
    if sysctlbyname(encoded_name, buffer, ctypes.byref(size), None, 0) != 0:
        error_number = ctypes.get_errno()
        detail = os.strerror(error_number) if error_number else "unknown error"
        raise ProcessInspectionError(
            f"Unable to inspect macOS sysctl {name!r}: {detail}."
        )
    return buffer.value.decode("ascii").strip() or None


def _inspect_windows_process_identity(pid: int) -> ProcessIdentity | None:
    handle = _open_windows_process(pid)
    if handle is None:
        return None
    try:
        return _inspect_windows_process_identity_from_handle(pid, handle)
    finally:
        _close_windows_process(handle)


def _inspect_windows_process_identity_from_handle(
    pid: int, handle
) -> ProcessIdentity | None:
    first_creation_time = _read_windows_creation_time(handle, pid=pid)
    executable_path = _read_windows_executable(handle, pid=pid)
    if not _windows_process_handle_is_active(handle, pid=pid):
        return None
    process_session_id = _read_windows_session_id(pid)
    second_creation_time = _read_windows_creation_time(handle, pid=pid)
    if second_creation_time != first_creation_time:
        raise ProcessInspectionError(
            f"Local process {pid} changed while its identity was inspected."
        )
    if not _windows_process_handle_is_active(handle, pid=pid):
        return None
    return ProcessIdentity(
        pid=pid,
        start_key=f"windows:{first_creation_time}",
        executable_path=_normalize_executable_path(
            executable_path, case_insensitive=True
        ),
        process_group_id=None,
        process_session_id=process_session_id,
    )


def _windows_kernel32():
    try:
        return ctypes.WinDLL("kernel32", use_last_error=True)
    except (AttributeError, OSError) as exc:
        raise ProcessInspectionError(
            "Unable to load the Windows process-inspection API."
        ) from exc


def _windows_last_error() -> int:
    get_last_error = getattr(ctypes, "get_last_error", None)
    return int(get_last_error()) if get_last_error is not None else 0


def _open_windows_process(pid: int):
    kernel32 = _windows_kernel32()
    open_process = kernel32.OpenProcess
    open_process.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    open_process.restype = ctypes.c_void_p
    handle = open_process(
        _WINDOWS_PROCESS_QUERY_LIMITED_INFORMATION | _WINDOWS_SYNCHRONIZE,
        False,
        pid,
    )
    if handle:
        return handle
    error_number = _windows_last_error()
    if error_number in {_WINDOWS_ERROR_INVALID_PARAMETER, _WINDOWS_ERROR_NOT_FOUND}:
        return None
    detail = ctypes.FormatError(error_number) if error_number else "unknown error"
    raise ProcessInspectionError(f"Unable to open local process {pid}: {detail}.")


def _read_windows_creation_time(handle, *, pid: int) -> int:
    kernel32 = _windows_kernel32()
    get_process_times = kernel32.GetProcessTimes
    get_process_times.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_WindowsFileTime),
        ctypes.POINTER(_WindowsFileTime),
        ctypes.POINTER(_WindowsFileTime),
        ctypes.POINTER(_WindowsFileTime),
    ]
    get_process_times.restype = ctypes.c_int
    creation = _WindowsFileTime()
    exit_time = _WindowsFileTime()
    kernel = _WindowsFileTime()
    user = _WindowsFileTime()
    if not get_process_times(
        handle,
        ctypes.byref(creation),
        ctypes.byref(exit_time),
        ctypes.byref(kernel),
        ctypes.byref(user),
    ):
        error_number = _windows_last_error()
        detail = ctypes.FormatError(error_number) if error_number else "unknown error"
        raise ProcessInspectionError(
            f"Unable to inspect creation time for local process {pid}: {detail}."
        )
    creation_time = (int(creation.high) << 32) | int(creation.low)
    if creation_time <= 0:
        raise ProcessInspectionError(
            f"Invalid creation identity for local process {pid}."
        )
    return creation_time


def _read_windows_executable(handle, *, pid: int) -> str:
    kernel32 = _windows_kernel32()
    query_image_name = kernel32.QueryFullProcessImageNameW
    query_image_name.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_wchar_p,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    query_image_name.restype = ctypes.c_int
    buffer = ctypes.create_unicode_buffer(_WINDOWS_PROCESS_PATH_BUFFER_SIZE)
    size = ctypes.c_uint32(len(buffer))
    if not query_image_name(handle, 0, buffer, ctypes.byref(size)):
        error_number = _windows_last_error()
        detail = ctypes.FormatError(error_number) if error_number else "unknown error"
        raise ProcessInspectionError(
            f"Unable to inspect executable identity for local process {pid}: {detail}."
        )
    executable_path = buffer.value[: size.value]
    if not executable_path:
        raise ProcessInspectionError(
            f"Empty executable identity for local process {pid}."
        )
    return executable_path


def _read_windows_session_id(pid: int) -> int:
    kernel32 = _windows_kernel32()
    process_id_to_session_id = kernel32.ProcessIdToSessionId
    process_id_to_session_id.argtypes = [
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    process_id_to_session_id.restype = ctypes.c_int
    session_id = ctypes.c_uint32()
    if not process_id_to_session_id(pid, ctypes.byref(session_id)):
        error_number = _windows_last_error()
        detail = ctypes.FormatError(error_number) if error_number else "unknown error"
        raise ProcessInspectionError(
            f"Unable to inspect session identity for local process {pid}: {detail}."
        )
    return int(session_id.value)


def _windows_process_handle_is_active(handle, *, pid: int) -> bool:
    kernel32 = _windows_kernel32()
    wait_for_single_object = kernel32.WaitForSingleObject
    wait_for_single_object.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    wait_for_single_object.restype = ctypes.c_uint32
    wait_result = int(wait_for_single_object(handle, 0))
    if wait_result == _WINDOWS_WAIT_TIMEOUT:
        return True
    if wait_result == _WINDOWS_WAIT_OBJECT_0:
        return False
    if wait_result == _WINDOWS_WAIT_FAILED:
        error_number = _windows_last_error()
        detail = ctypes.FormatError(error_number) if error_number else "unknown error"
        raise ProcessInspectionError(
            f"Unable to inspect active state for local process {pid}: {detail}."
        )
    raise ProcessInspectionError(
        f"Unable to inspect active state for local process {pid}: unexpected wait result {wait_result}."
    )


def _close_windows_process(handle) -> None:
    _close_windows_handle(handle)


def _close_windows_handle(handle) -> None:
    kernel32 = _windows_kernel32()
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    close_handle(handle)


def create_windows_kill_on_close_job(nonce: str) -> WindowsKillOnCloseJob:
    """Create one new nonce-named Windows Job with kill-on-close enabled.

    Args:
        nonce: Exactly 64 lowercase hexadecimal containment characters.

    Returns:
        An owned native Job wrapper. The caller must keep it open for the
        required containment lifetime and close it when finished.

    Raises:
        ProcessInspectionError: If Windows Job APIs are unavailable, the name
            already exists, or the Job cannot be configured.
        ValueError: If ``nonce`` is not canonical.
    """
    if os.name != "nt":
        raise ProcessInspectionError("Windows Job objects require Windows.")
    name = windows_job_name_for_nonce(nonce)
    kernel32 = _windows_kernel32()
    create_job = kernel32.CreateJobObjectW
    create_job.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
    create_job.restype = ctypes.c_void_p
    _set_windows_last_error(0)
    handle = create_job(None, name)
    error_number = _windows_last_error()
    if not handle:
        raise ProcessInspectionError(
            f"Unable to create Windows Job {name!r}: {_windows_error_detail(error_number)}."
        )
    if error_number == _WINDOWS_ERROR_ALREADY_EXISTS:
        _close_windows_handle(handle)
        raise ProcessInspectionError(
            f"Refusing to reuse existing Windows Job {name!r}."
        )
    try:
        _configure_windows_job_kill_on_close(handle, name=name)
    except BaseException:
        _close_windows_handle(handle)
        raise
    return WindowsKillOnCloseJob(nonce=nonce, name=name, handle=handle)


def open_windows_kill_on_close_job(nonce: str) -> WindowsKillOnCloseJob | None:
    """Open and verify an existing nonce-named Windows kill-on-close Job.

    Args:
        nonce: Exactly 64 lowercase hexadecimal containment characters.

    Returns:
        An owned native Job wrapper, or ``None`` when the named Job is absent.

    Raises:
        ProcessInspectionError: If Windows Job APIs are unavailable, opening or
            inspecting the Job fails, or kill-on-close is not configured.
        ValueError: If ``nonce`` is not canonical.
    """
    if os.name != "nt":
        raise ProcessInspectionError("Windows Job objects require Windows.")
    name = windows_job_name_for_nonce(nonce)
    kernel32 = _windows_kernel32()
    open_job = kernel32.OpenJobObjectW
    open_job.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_wchar_p]
    open_job.restype = ctypes.c_void_p
    handle = open_job(
        _WINDOWS_JOB_OBJECT_ASSIGN_PROCESS
        | _WINDOWS_JOB_OBJECT_QUERY
        | _WINDOWS_JOB_OBJECT_TERMINATE
        | _WINDOWS_SYNCHRONIZE,
        False,
        name,
    )
    if not handle:
        error_number = _windows_last_error()
        if error_number == _WINDOWS_ERROR_FILE_NOT_FOUND:
            return None
        raise ProcessInspectionError(
            f"Unable to open Windows Job {name!r}: {_windows_error_detail(error_number)}."
        )
    try:
        _require_windows_job_kill_on_close(handle, name=name)
    except BaseException:
        _close_windows_handle(handle)
        raise
    return WindowsKillOnCloseJob(nonce=nonce, name=name, handle=handle)


def _configure_windows_job_kill_on_close(handle, *, name: str) -> None:
    kernel32 = _windows_kernel32()
    set_job_information = kernel32.SetInformationJobObject
    set_job_information.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint32,
    ]
    set_job_information.restype = ctypes.c_int
    information = _WindowsJobExtendedLimitInformation()
    information.basic_limit_information.limit_flags = (
        _WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    )
    if not set_job_information(
        handle,
        _WINDOWS_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
        ctypes.byref(information),
        ctypes.sizeof(information),
    ):
        error_number = _windows_last_error()
        raise ProcessInspectionError(
            f"Unable to configure Windows Job {name!r}: {_windows_error_detail(error_number)}."
        )


def _require_windows_job_kill_on_close(handle, *, name: str) -> None:
    kernel32 = _windows_kernel32()
    query_job_information = kernel32.QueryInformationJobObject
    query_job_information.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_uint32),
    ]
    query_job_information.restype = ctypes.c_int
    information = _WindowsJobExtendedLimitInformation()
    return_length = ctypes.c_uint32()
    if not query_job_information(
        handle,
        _WINDOWS_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
        ctypes.byref(information),
        ctypes.sizeof(information),
        ctypes.byref(return_length),
    ):
        error_number = _windows_last_error()
        raise ProcessInspectionError(
            f"Unable to inspect Windows Job {name!r}: {_windows_error_detail(error_number)}."
        )
    if not (
        information.basic_limit_information.limit_flags
        & _WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    ):
        raise ProcessInspectionError(
            f"Windows Job {name!r} is not configured for kill-on-close containment."
        )


def _windows_process_is_in_job(process_handle, job_handle, *, pid: int) -> bool:
    kernel32 = _windows_kernel32()
    is_process_in_job = kernel32.IsProcessInJob
    is_process_in_job.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_int),
    ]
    is_process_in_job.restype = ctypes.c_int
    result = ctypes.c_int()
    if not is_process_in_job(process_handle, job_handle, ctypes.byref(result)):
        error_number = _windows_last_error()
        raise ProcessInspectionError(
            f"Unable to inspect Job membership for local process {pid}: "
            f"{_windows_error_detail(error_number)}."
        )
    return bool(result.value)


def _terminate_windows_job(job_handle, *, name: str, exit_code: int = 1) -> None:
    kernel32 = _windows_kernel32()
    terminate_job = kernel32.TerminateJobObject
    terminate_job.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    terminate_job.restype = ctypes.c_int
    if not terminate_job(job_handle, exit_code):
        error_number = _windows_last_error()
        raise ProcessInspectionError(
            f"Unable to terminate Windows Job {name!r}: {_windows_error_detail(error_number)}."
        )


def _wait_for_windows_job_empty(
    job_handle,
    *,
    name: str,
    timeout_seconds: float,
) -> None:
    timeout_milliseconds = _finite_windows_timeout_milliseconds(timeout_seconds)
    kernel32 = _windows_kernel32()
    wait_for_single_object = kernel32.WaitForSingleObject
    wait_for_single_object.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    wait_for_single_object.restype = ctypes.c_uint32
    wait_result = int(wait_for_single_object(job_handle, timeout_milliseconds))
    if wait_result == _WINDOWS_WAIT_OBJECT_0:
        return
    if wait_result == _WINDOWS_WAIT_TIMEOUT:
        raise ProcessInspectionError(
            f"Timed out waiting for Windows Job {name!r} to terminate."
        )
    if wait_result == _WINDOWS_WAIT_FAILED:
        error_number = _windows_last_error()
        raise ProcessInspectionError(
            f"Unable to wait for Windows Job {name!r}: {_windows_error_detail(error_number)}."
        )
    raise ProcessInspectionError(
        f"Unable to wait for Windows Job {name!r}: unexpected wait result {wait_result}."
    )


def _finite_windows_timeout_milliseconds(timeout_seconds: float) -> int:
    if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int | float):
        raise ValueError("A Windows Job timeout must be a finite nonnegative number.")
    numeric_timeout = float(timeout_seconds)
    if not math.isfinite(numeric_timeout) or numeric_timeout < 0:
        raise ValueError("A Windows Job timeout must be a finite nonnegative number.")
    milliseconds = math.ceil(numeric_timeout * 1000)
    if milliseconds > _WINDOWS_MAX_FINITE_TIMEOUT_MILLISECONDS:
        raise ValueError("A Windows Job timeout is too large.")
    return milliseconds


def _force_kill_verified_windows_job(
    expected: ProcessIdentity,
    *,
    nonce: str,
    timeout_seconds: float,
) -> None:
    job = open_windows_kill_on_close_job(nonce)
    if job is None:
        raise ProcessInspectionError(
            f"The containment Job for local process {expected.pid} no longer exists."
        )
    with job:
        process_handle = _open_windows_process(expected.pid)
        if process_handle is None:
            raise ProcessInspectionError(
                f"Local process {expected.pid} is no longer running."
            )
        try:
            actual = _inspect_windows_process_identity_from_handle(
                expected.pid,
                process_handle,
            )
            if actual is None:
                raise ProcessInspectionError(
                    f"Local process {expected.pid} is no longer running."
                )
            if actual != expected:
                raise ProcessInspectionError(
                    f"Local process {expected.pid} no longer matches its recorded identity."
                )
            job_handle = job._native_handle()
            if not _windows_process_is_in_job(
                process_handle,
                job_handle,
                pid=actual.pid,
            ):
                raise ProcessInspectionError(
                    f"Local process {actual.pid} is not a member of its recorded containment Job."
                )
            job.terminate(timeout_seconds=timeout_seconds)
        finally:
            _close_windows_process(process_handle)


def _set_windows_last_error(error_number: int) -> None:
    set_last_error = getattr(ctypes, "set_last_error", None)
    if set_last_error is not None:
        set_last_error(error_number)


def _windows_error_detail(error_number: int) -> str:
    format_error = getattr(ctypes, "FormatError", None)
    if error_number and format_error is not None:
        return str(format_error(error_number)).strip()
    return f"Windows error {error_number}" if error_number else "unknown error"


def _normalize_executable_path(value: str, *, case_insensitive: bool = False) -> str:
    if not value:
        raise ProcessInspectionError("A process executable identity is empty.")
    try:
        return stable_path_identity_text(value, case_insensitive=case_insensitive)
    except (OSError, TypeError, ValueError) as exc:
        raise ProcessInspectionError(
            "Unable to normalize a process executable identity."
        ) from exc


def windows_subprocess_creationflags(
    *,
    new_process_group: bool = False,
    no_window: bool = False,
    detached: bool = False,
) -> int:
    """Return Windows subprocess creation flags supported by the host Python."""
    if os.name != "nt":
        return 0
    flags = 0
    if new_process_group:
        flags |= getattr(
            subprocess, "CREATE_NEW_PROCESS_GROUP", _CREATE_NEW_PROCESS_GROUP
        )
    if no_window:
        flags |= getattr(subprocess, "CREATE_NO_WINDOW", _CREATE_NO_WINDOW)
    if detached:
        flags |= getattr(subprocess, "DETACHED_PROCESS", _DETACHED_PROCESS)
    return flags


def process_is_running(pid: int | None, *, treat_defunct_as_dead: bool = True) -> bool:
    """Return whether one OS process id currently exists and is active."""
    if pid is None or pid <= 0:
        return False
    if os.name == "nt":
        return _windows_process_is_running(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    if not treat_defunct_as_dead:
        return True
    status = process_status(pid)
    if status is None:
        return False
    return not is_defunct_process_status(status)


def _windows_process_is_running(pid: int) -> bool:
    """Return whether one Windows process id exists and has not exited."""
    kernel32 = ctypes.windll.kernel32
    kernel32.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
    kernel32.OpenProcess.restype = ctypes.c_void_p
    kernel32.GetExitCodeProcess.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_ulong),
    ]
    kernel32.GetExitCodeProcess.restype = ctypes.c_int
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel32.CloseHandle.restype = ctypes.c_int
    handle = kernel32.OpenProcess(
        _WINDOWS_PROCESS_QUERY_LIMITED_INFORMATION, False, pid
    )
    if not handle:
        return False
    try:
        exit_code = ctypes.c_ulong()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return False
        return exit_code.value == _WINDOWS_STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def process_status(pid: int) -> str | None:
    """Return the platform process status text for one pid when available."""
    if os.name == "nt":
        return (
            "Running" if process_is_running(pid, treat_defunct_as_dead=False) else None
        )
    result = subprocess.run(
        ["ps", "-o", "stat=", "-p", str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def list_processes() -> list[ProcessInfo]:
    """Return the local process table in a normalized shape."""
    if os.name == "nt":
        return _list_windows_processes()
    return _list_posix_processes()


def _list_posix_processes() -> list[ProcessInfo]:
    result = subprocess.run(
        ["ps", "-ax", "-o", "pid=", "-o", "ppid=", "-o", "stat=", "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise ProcessInspectionError("Unable to inspect the local process table.")
    rows: list[ProcessInfo] = []
    for line in result.stdout.splitlines():
        parts = line.strip().split(None, 3)
        if len(parts) != 4:
            continue
        pid_text, ppid_text, status, command = parts
        try:
            pid = int(pid_text)
            ppid = int(ppid_text)
        except ValueError:
            continue
        rows.append(ProcessInfo(pid=pid, ppid=ppid, status=status, command=command))
    return rows


def _list_windows_processes() -> list[ProcessInfo]:
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "$processes = Get-CimInstance Win32_Process | Select-Object ProcessId, ParentProcessId, CommandLine; "
            "if ($null -eq $processes) { '[]' } else { $processes | ConvertTo-Json -Compress -Depth 3 }",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise ProcessInspectionError("Unable to inspect the local process table.")
    payload = result.stdout.strip()
    if not payload:
        return []
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ProcessInspectionError(
            "Unable to inspect the local process table."
        ) from exc
    if isinstance(parsed, dict):
        items: list[dict[str, object]] = [parsed]
    elif isinstance(parsed, list):
        items = [item for item in parsed if isinstance(item, dict)]
    else:
        return []
    rows: list[ProcessInfo] = []
    for item in items:
        try:
            pid = int(item["ProcessId"])
            ppid = int(item["ParentProcessId"])
        except KeyError, TypeError, ValueError:
            continue
        command = str(item.get("CommandLine") or "")
        rows.append(ProcessInfo(pid=pid, ppid=ppid, status="Running", command=command))
    return rows


def collapse_windows_launcher_processes(
    rows: list[ClassifiedProcessInfo],
) -> list[ClassifiedProcessInfo]:
    """Prefer the real child interpreter over a Windows venv launcher parent."""
    if os.name != "nt":
        return rows
    child_by_parent: dict[int, list[ClassifiedProcessInfo]] = {}
    for row in rows:
        child_by_parent.setdefault(row.ppid, []).append(row)
    hidden_parent_pids: set[int] = set()
    for row in rows:
        children = child_by_parent.get(row.pid, ())
        matching_children = [
            child
            for child in children
            if child.kind == row.kind and child.command == row.command
        ]
        if len(matching_children) == 1:
            hidden_parent_pids.add(row.pid)
    return [row for row in rows if row.pid not in hidden_parent_pids]


def force_kill_process_tree(pid: int) -> None:
    """Forcefully terminate one local process id and its children when supported."""
    if os.name == "nt":
        result = subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0 and process_is_running(pid):
            detail = (
                result.stderr.strip()
                or result.stdout.strip()
                or f"taskkill returned {result.returncode}"
            )
            raise ProcessInspectionError(
                f"Failed to terminate local process {pid}: {detail}"
            )
        return
    os.kill(pid, signal.SIGKILL)


__all__ = [
    "ProcessIdentity",
    "ProcessInspectionError",
    "WindowsKillOnCloseJob",
    "collapse_windows_launcher_processes",
    "force_kill_process_tree",
    "force_kill_verified_contained_process_tree",
    "force_kill_verified_process_tree",
    "create_windows_kill_on_close_job",
    "inspect_process_identity",
    "list_processes",
    "new_process_containment_nonce",
    "open_windows_kill_on_close_job",
    "process_is_running",
    "process_status",
    "windows_job_name_for_nonce",
    "windows_subprocess_creationflags",
]
