from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import os
import select
import secrets
import signal
import subprocess
import sys
import time

import pytest

from data_engine.platform import processes
from data_engine.platform.processes import (
    ProcessIdentity,
    ProcessInspectionError,
    force_kill_verified_contained_process_tree,
    force_kill_verified_process_tree,
    inspect_process_identity,
)


_NONCE = "ab" * 32


def _identity(**overrides) -> ProcessIdentity:
    values = {
        "pid": 321,
        "start_key": "boot:start",
        "executable_path": "/runtime/python",
        "process_group_id": 321,
        "process_session_id": 321,
    }
    values.update(overrides)
    return ProcessIdentity(**values)


def test_process_identity_is_immutable():
    identity = _identity()

    with pytest.raises(FrozenInstanceError):
        identity.pid = 999


def test_live_process_identity_inspects_current_process():
    first = inspect_process_identity(os.getpid())
    second = inspect_process_identity(os.getpid())

    assert first is not None
    assert first == second
    assert first.pid == os.getpid()
    assert first.start_key
    assert first.executable_path
    assert first.process_session_id is not None
    if os.name == "nt":
        assert first.process_group_id is None
    else:
        assert first.process_group_id is not None


def test_linux_proc_stat_parser_handles_parentheses_in_process_name():
    payload = "321 (worker) with (nested) parens) S 1 222 111 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 987654 0"

    parsed = processes._parse_linux_proc_stat(payload, pid=321)

    assert parsed == processes._LinuxProcStat(
        process_group_id=222,
        process_session_id=111,
        start_ticks=987654,
    )


def test_linux_process_identity_double_reads_start_key(monkeypatch):
    stat = processes._LinuxProcStat(
        process_group_id=321,
        process_session_id=321,
        start_ticks=987654,
    )
    reads = []
    normalized_paths = []
    monkeypatch.setattr(
        processes, "_read_linux_proc_stat", lambda pid: reads.append(pid) or stat
    )
    monkeypatch.setattr(
        processes, "_read_linux_executable", lambda pid: "/opt/runtime/python"
    )
    monkeypatch.setattr(processes, "_read_linux_boot_id", lambda: "BOOT-ID")
    monkeypatch.setattr(
        processes,
        "_normalize_executable_path",
        lambda value, **kwargs: normalized_paths.append(value) or "/opt/runtime/python",
    )

    identity = processes._inspect_linux_process_identity(321)

    assert identity == ProcessIdentity(
        pid=321,
        start_key="linux:BOOT-ID:987654",
        executable_path="/opt/runtime/python",
        process_group_id=321,
        process_session_id=321,
    )
    assert reads == [321, 321, 321]
    assert normalized_paths == ["/opt/runtime/python", "/opt/runtime/python"]


def test_linux_process_identity_fails_closed_when_start_changes(monkeypatch):
    stats = iter(
        [
            processes._LinuxProcStat(321, 321, 10),
            processes._LinuxProcStat(321, 321, 11),
        ]
    )
    monkeypatch.setattr(processes, "_read_linux_proc_stat", lambda pid: next(stats))
    monkeypatch.setattr(
        processes, "_read_linux_executable", lambda pid: "/opt/runtime/python"
    )
    monkeypatch.setattr(processes, "_read_linux_boot_id", lambda: "boot")

    with pytest.raises(ProcessInspectionError, match="changed"):
        processes._inspect_linux_process_identity(321)


def test_linux_process_identity_fails_closed_when_final_start_read_changes(
    monkeypatch,
):
    stats = iter(
        [
            processes._LinuxProcStat(321, 321, 10),
            processes._LinuxProcStat(321, 321, 10),
            processes._LinuxProcStat(321, 321, 11),
        ]
    )
    monkeypatch.setattr(processes, "_read_linux_proc_stat", lambda pid: next(stats))
    monkeypatch.setattr(
        processes, "_read_linux_executable", lambda pid: "/opt/runtime/python"
    )
    monkeypatch.setattr(processes, "_read_linux_boot_id", lambda: "boot")

    with pytest.raises(ProcessInspectionError, match="changed"):
        processes._inspect_linux_process_identity(321)


def test_linux_process_identity_fails_closed_when_executable_changes(monkeypatch):
    stat = processes._LinuxProcStat(321, 321, 10)
    executable_paths = iter(["/opt/runtime/python", "/opt/runtime/replacement"])
    monkeypatch.setattr(processes, "_read_linux_proc_stat", lambda pid: stat)
    monkeypatch.setattr(
        processes, "_read_linux_executable", lambda pid: next(executable_paths)
    )
    monkeypatch.setattr(processes, "_read_linux_boot_id", lambda: "boot")

    with pytest.raises(ProcessInspectionError, match="changed executable"):
        processes._inspect_linux_process_identity(321)


def test_linux_process_identity_fails_closed_when_grouping_changes(monkeypatch):
    stats = iter(
        [
            processes._LinuxProcStat(321, 321, 10),
            processes._LinuxProcStat(322, 321, 10),
        ]
    )
    monkeypatch.setattr(processes, "_read_linux_proc_stat", lambda pid: next(stats))
    monkeypatch.setattr(
        processes, "_read_linux_executable", lambda pid: "/opt/runtime/python"
    )
    monkeypatch.setattr(processes, "_read_linux_boot_id", lambda: "boot")

    with pytest.raises(ProcessInspectionError, match="changed"):
        processes._inspect_linux_process_identity(321)


def test_linux_process_identity_propagates_access_failure(monkeypatch):
    def _fail(pid):
        raise ProcessInspectionError("access denied")

    monkeypatch.setattr(processes, "_read_linux_proc_stat", _fail)

    with pytest.raises(ProcessInspectionError, match="access denied"):
        processes._inspect_linux_process_identity(321)


def test_linux_process_identity_returns_none_for_confirmed_absence(monkeypatch):
    monkeypatch.setattr(processes, "_read_linux_proc_stat", lambda pid: None)

    assert processes._inspect_linux_process_identity(321) is None


def test_darwin_process_identity_uses_native_start_path_and_boot_key(monkeypatch):
    start = processes._DarwinStartIdentity(
        pid=321, start_seconds=1000, start_microseconds=42
    )
    seen = []
    normalized_paths = []
    monkeypatch.setattr(
        processes, "_read_darwin_start_identity", lambda pid: seen.append(pid) or start
    )
    monkeypatch.setattr(
        processes, "_read_darwin_executable", lambda pid: "/opt/runtime/python"
    )
    monkeypatch.setattr(
        processes, "_read_darwin_boot_session_key", lambda: "uuid:boot-id"
    )
    monkeypatch.setattr(
        processes,
        "_normalize_executable_path",
        lambda value, **kwargs: normalized_paths.append(value) or "/opt/runtime/python",
    )
    monkeypatch.setattr(processes.os, "getpgid", lambda pid: 321, raising=False)
    monkeypatch.setattr(processes.os, "getsid", lambda pid: 321, raising=False)

    identity = processes._inspect_darwin_process_identity(321)

    assert identity == ProcessIdentity(
        pid=321,
        start_key="darwin:uuid:boot-id:1000:42",
        executable_path="/opt/runtime/python",
        process_group_id=321,
        process_session_id=321,
    )
    assert seen == [321, 321, 321]
    assert normalized_paths == ["/opt/runtime/python", "/opt/runtime/python"]


def test_darwin_process_identity_fails_closed_when_start_changes(monkeypatch):
    starts = iter(
        [
            processes._DarwinStartIdentity(321, 1000, 42),
            processes._DarwinStartIdentity(321, 1000, 43),
        ]
    )
    monkeypatch.setattr(
        processes, "_read_darwin_start_identity", lambda pid: next(starts)
    )
    monkeypatch.setattr(
        processes, "_read_darwin_executable", lambda pid: "/opt/runtime/python"
    )
    monkeypatch.setattr(
        processes, "_read_darwin_boot_session_key", lambda: "uuid:boot-id"
    )
    monkeypatch.setattr(processes.os, "getpgid", lambda pid: 321, raising=False)
    monkeypatch.setattr(processes.os, "getsid", lambda pid: 321, raising=False)

    with pytest.raises(ProcessInspectionError, match="changed"):
        processes._inspect_darwin_process_identity(321)


def test_darwin_process_identity_fails_closed_when_final_start_read_changes(
    monkeypatch,
):
    first = processes._DarwinStartIdentity(321, 1000, 42)
    replacement = processes._DarwinStartIdentity(321, 1000, 43)
    starts = iter([first, first, replacement])
    monkeypatch.setattr(
        processes, "_read_darwin_start_identity", lambda pid: next(starts)
    )
    monkeypatch.setattr(
        processes, "_read_darwin_executable", lambda pid: "/opt/runtime/python"
    )
    monkeypatch.setattr(
        processes, "_read_darwin_boot_session_key", lambda: "uuid:boot-id"
    )
    monkeypatch.setattr(processes.os, "getpgid", lambda pid: 321, raising=False)
    monkeypatch.setattr(processes.os, "getsid", lambda pid: 321, raising=False)

    with pytest.raises(ProcessInspectionError, match="changed"):
        processes._inspect_darwin_process_identity(321)


def test_darwin_process_identity_fails_closed_when_executable_changes(monkeypatch):
    start = processes._DarwinStartIdentity(321, 1000, 42)
    executable_paths = iter(["/opt/runtime/python", "/opt/runtime/replacement"])
    monkeypatch.setattr(processes, "_read_darwin_start_identity", lambda pid: start)
    monkeypatch.setattr(
        processes, "_read_darwin_executable", lambda pid: next(executable_paths)
    )
    monkeypatch.setattr(
        processes, "_read_darwin_boot_session_key", lambda: "uuid:boot-id"
    )
    monkeypatch.setattr(processes.os, "getpgid", lambda pid: 321, raising=False)
    monkeypatch.setattr(processes.os, "getsid", lambda pid: 321, raising=False)

    with pytest.raises(ProcessInspectionError, match="changed executable"):
        processes._inspect_darwin_process_identity(321)


def test_darwin_process_identity_fails_closed_when_grouping_changes(monkeypatch):
    start = processes._DarwinStartIdentity(321, 1000, 42)
    groupings = iter([(321, 321), (322, 321)])
    monkeypatch.setattr(processes, "_read_darwin_start_identity", lambda pid: start)
    monkeypatch.setattr(
        processes, "_read_darwin_executable", lambda pid: "/opt/runtime/python"
    )
    monkeypatch.setattr(
        processes, "_read_darwin_boot_session_key", lambda: "uuid:boot-id"
    )
    monkeypatch.setattr(
        processes, "_read_darwin_process_grouping", lambda pid: next(groupings)
    )

    with pytest.raises(ProcessInspectionError, match="changed"):
        processes._inspect_darwin_process_identity(321)


def test_darwin_process_identity_returns_none_for_confirmed_absence(monkeypatch):
    monkeypatch.setattr(processes, "_read_darwin_start_identity", lambda pid: None)

    assert processes._inspect_darwin_process_identity(321) is None


def test_windows_process_identity_reuses_one_handle_and_closes_it(monkeypatch):
    handle = object()
    seen_handles = []
    normalized_paths = []
    creation_times = iter([555, 555])
    closed = []
    monkeypatch.setattr(processes, "_open_windows_process", lambda pid: handle)
    monkeypatch.setattr(
        processes,
        "_read_windows_creation_time",
        lambda seen_handle, *, pid: (
            seen_handles.append(seen_handle) or next(creation_times)
        ),
    )
    monkeypatch.setattr(
        processes,
        "_read_windows_executable",
        lambda seen_handle, *, pid: (
            seen_handles.append(seen_handle) or r"C:\Runtime\Python.exe"
        ),
    )
    monkeypatch.setattr(processes, "_read_windows_session_id", lambda pid: 7)
    monkeypatch.setattr(
        processes,
        "_windows_process_handle_is_active",
        lambda seen_handle, *, pid: True,
    )
    monkeypatch.setattr(processes, "_close_windows_process", closed.append)
    monkeypatch.setattr(
        processes,
        "stable_path_identity_text",
        lambda value, *, case_insensitive: (
            normalized_paths.append((value, case_insensitive))
            or "c:/runtime/python.exe"
        ),
    )

    identity = processes._inspect_windows_process_identity(321)

    assert identity == ProcessIdentity(
        pid=321,
        start_key="windows:555",
        executable_path="c:/runtime/python.exe",
        process_group_id=None,
        process_session_id=7,
    )
    assert seen_handles == [handle, handle, handle]
    assert closed == [handle]
    assert normalized_paths == [(r"C:\Runtime\Python.exe", True)]


def test_windows_process_identity_closes_handle_when_start_changes(monkeypatch):
    handle = object()
    creation_times = iter([555, 556])
    closed = []
    monkeypatch.setattr(processes, "_open_windows_process", lambda pid: handle)
    monkeypatch.setattr(
        processes,
        "_read_windows_creation_time",
        lambda seen_handle, *, pid: next(creation_times),
    )
    monkeypatch.setattr(
        processes,
        "_read_windows_executable",
        lambda seen_handle, *, pid: r"C:\Runtime\Python.exe",
    )
    monkeypatch.setattr(processes, "_read_windows_session_id", lambda pid: 7)
    monkeypatch.setattr(
        processes,
        "_windows_process_handle_is_active",
        lambda seen_handle, *, pid: True,
    )
    monkeypatch.setattr(processes, "_close_windows_process", closed.append)

    with pytest.raises(ProcessInspectionError, match="changed"):
        processes._inspect_windows_process_identity(321)

    assert closed == [handle]


def test_windows_process_identity_returns_none_if_process_exits_during_read(
    monkeypatch,
):
    handle = object()
    closed = []
    monkeypatch.setattr(processes, "_open_windows_process", lambda pid: handle)
    monkeypatch.setattr(
        processes,
        "_read_windows_creation_time",
        lambda seen_handle, *, pid: 555,
    )
    monkeypatch.setattr(
        processes,
        "_read_windows_executable",
        lambda seen_handle, *, pid: r"C:\Runtime\Python.exe",
    )
    monkeypatch.setattr(processes, "_read_windows_session_id", lambda pid: 7)
    monkeypatch.setattr(
        processes,
        "_windows_process_handle_is_active",
        lambda seen_handle, *, pid: False,
    )
    monkeypatch.setattr(processes, "_close_windows_process", closed.append)

    assert processes._inspect_windows_process_identity(321) is None
    assert closed == [handle]


def test_windows_process_identity_propagates_open_access_failure(monkeypatch):
    def _fail(pid):
        raise ProcessInspectionError("access denied")

    monkeypatch.setattr(processes, "_open_windows_process", _fail)

    with pytest.raises(ProcessInspectionError, match="access denied"):
        processes._inspect_windows_process_identity(321)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("pid", 999),
        ("start_key", "different:start"),
        ("executable_path", "/different/python"),
        ("process_group_id", 999),
        ("process_session_id", 999),
    ],
)
def test_verified_kill_blocks_every_identity_field_mismatch(monkeypatch, field, value):
    expected = _identity()
    actual = replace(expected, **{field: value})
    monkeypatch.setattr(processes, "_HOST_OS_NAME", "posix")
    monkeypatch.setattr(processes.os, "getpid", lambda: 111)
    monkeypatch.setattr(processes.os, "getpgrp", lambda: 111, raising=False)
    monkeypatch.setattr(processes, "inspect_process_identity", lambda pid: actual)
    monkeypatch.setattr(
        processes,
        "request_posix_process_group_termination",
        lambda **kwargs: pytest.fail("mismatched identity must not reach the watchdog"),
    )

    with pytest.raises(ProcessInspectionError, match="no longer matches"):
        force_kill_verified_process_tree(
            expected,
            containment_nonce=_NONCE,
        )


def test_verified_posix_kill_requires_isolated_session_leader(monkeypatch):
    expected = _identity(process_session_id=999)
    monkeypatch.setattr(processes, "_HOST_OS_NAME", "posix")
    monkeypatch.setattr(processes.os, "getpid", lambda: 111)
    monkeypatch.setattr(processes.os, "getpgrp", lambda: 111, raising=False)
    monkeypatch.setattr(processes, "inspect_process_identity", lambda pid: expected)
    monkeypatch.setattr(
        processes,
        "request_posix_process_group_termination",
        lambda **kwargs: pytest.fail("nonisolated process must not reach the watchdog"),
    )

    with pytest.raises(ProcessInspectionError, match="isolated leader"):
        force_kill_verified_process_tree(
            expected,
            containment_nonce=_NONCE,
        )


def test_verified_posix_kill_requests_matching_watchdog_capability(monkeypatch):
    expected = _identity()
    requests = []
    monkeypatch.setattr(processes, "_HOST_OS_NAME", "posix")
    monkeypatch.setattr(processes.os, "getpid", lambda: 111)
    monkeypatch.setattr(processes, "inspect_process_identity", lambda pid: expected)
    monkeypatch.setattr(processes.os, "getpgrp", lambda: 111, raising=False)
    monkeypatch.setattr(
        processes,
        "request_posix_process_group_termination",
        lambda **kwargs: requests.append(kwargs),
    )

    force_kill_verified_process_tree(
        expected,
        containment_nonce=_NONCE,
    )

    assert requests == [
        {
            "containment_nonce": _NONCE,
            "expected_parent_pid": expected.pid,
        }
    ]


def test_verified_windows_kill_rejects_invalid_containment_nonce(monkeypatch):
    expected = _identity(process_group_id=None, process_session_id=7)
    monkeypatch.setattr(processes, "_HOST_OS_NAME", "nt")
    monkeypatch.setattr(
        processes.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("taskkill must not be invoked"),
    )

    with pytest.raises(ValueError, match="64 lowercase hexadecimal"):
        force_kill_verified_process_tree(
            expected,
            containment_nonce="invalid",
        )


@pytest.mark.skipif(os.name == "nt", reason="POSIX process groups are required")
def test_verified_posix_kill_targets_live_parent_and_child_group():
    containment_nonce = secrets.token_hex(32)
    parent_code = """
import os
import signal
import subprocess
import sys
from data_engine.platform.posix_watchdog import arm_posix_process_group_watchdog

arm_posix_process_group_watchdog(containment_nonce=sys.argv[1])
read_fd, write_fd = os.pipe()
child_code = "import os, signal, sys; os.write(int(sys.argv[1]), b'R'); os.close(int(sys.argv[1])); signal.pause()"
child = subprocess.Popen([sys.executable, "-c", child_code, str(write_fd)], pass_fds=(write_fd,))
os.close(write_fd)
if os.read(read_fd, 1) != b"R":
    raise SystemExit(2)
os.close(read_fd)
print(f"{os.getpid()} {child.pid}", flush=True)
signal.pause()
"""
    parent = subprocess.Popen(
        [sys.executable, "-c", parent_code, containment_nonce],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert parent.stdout is not None
    output = b""
    deadline = time.monotonic() + 10
    try:
        while b"\n" not in output:
            remaining = deadline - time.monotonic()
            assert remaining > 0, "timed out waiting for child readiness"
            readable, _, _ = select.select([parent.stdout], [], [], remaining)
            assert readable, "timed out waiting for child readiness"
            output += os.read(parent.stdout.fileno(), 4096)
        parent_pid, child_pid = (int(value) for value in output.splitlines()[0].split())
        assert parent_pid == parent.pid
        os.kill(parent_pid, 0)
        os.kill(child_pid, 0)

        identity = inspect_process_identity(parent_pid)
        assert identity is not None
        assert identity.process_group_id == parent_pid
        assert identity.process_session_id == parent_pid

        with pytest.raises(ProcessInspectionError, match="no longer matches"):
            force_kill_verified_contained_process_tree(
                replace(identity, executable_path=f"{identity.executable_path}.wrong"),
                containment_nonce=containment_nonce,
            )
        os.kill(parent_pid, 0)
        os.kill(child_pid, 0)

        force_kill_verified_contained_process_tree(
            identity,
            containment_nonce=containment_nonce,
        )
        assert parent.wait(timeout=10) == -signal.SIGKILL
        readable, _, _ = select.select([parent.stdout], [], [], 10)
        assert readable, "child process did not close the process-group readiness pipe"
        assert os.read(parent.stdout.fileno(), 1) == b""
    finally:
        try:
            os.killpg(parent.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if parent.poll() is None:
            parent.wait(timeout=10)
        parent.stdout.close()
        assert parent.stderr is not None
        parent.stderr.close()
