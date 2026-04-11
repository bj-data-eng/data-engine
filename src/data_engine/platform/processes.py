"""Platform-aware process helpers."""

from __future__ import annotations

import ctypes
import json
import os
import signal
import subprocess

from data_engine.domain.diagnostics import ClassifiedProcessInfo, ProcessInfo, is_defunct_process_status

_WINDOWS_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
_WINDOWS_STILL_ACTIVE = 259


class ProcessInspectionError(RuntimeError):
    """Raised when the local process table cannot be inspected."""


def windows_subprocess_creationflags(
    *,
    new_process_group: bool = False,
    no_window: bool = False,
    detached: bool = False,
) -> int:
    """Return Windows subprocess creation flags supported by the host Python."""
    flags = 0
    if new_process_group:
        flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    if no_window:
        flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0)
    if detached:
        flags |= getattr(subprocess, "DETACHED_PROCESS", 0)
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
    kernel32.GetExitCodeProcess.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_ulong)]
    kernel32.GetExitCodeProcess.restype = ctypes.c_int
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel32.CloseHandle.restype = ctypes.c_int
    handle = kernel32.OpenProcess(_WINDOWS_PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
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
        return "Running" if process_is_running(pid, treat_defunct_as_dead=False) else None
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
        raise ProcessInspectionError("Unable to inspect the local process table.") from exc
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
        except (KeyError, TypeError, ValueError):
            continue
        command = str(item.get("CommandLine") or "")
        rows.append(ProcessInfo(pid=pid, ppid=ppid, status="Running", command=command))
    return rows


def collapse_windows_launcher_processes(rows: list[ClassifiedProcessInfo]) -> list[ClassifiedProcessInfo]:
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
            detail = result.stderr.strip() or result.stdout.strip() or f"taskkill returned {result.returncode}"
            raise ProcessInspectionError(f"Failed to terminate local process {pid}: {detail}")
        return
    os.kill(pid, signal.SIGKILL)


__all__ = [
    "ProcessInspectionError",
    "collapse_windows_launcher_processes",
    "force_kill_process_tree",
    "list_processes",
    "process_is_running",
    "process_status",
    "windows_subprocess_creationflags",
]
