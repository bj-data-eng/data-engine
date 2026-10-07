from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from data_engine.platform import processes
from data_engine.platform.processes import ProcessInspectionError


@pytest.mark.parametrize("exits_during_inspection", [False, True])
def test_windows_identity_returns_none_when_executable_query_loses_process(
    monkeypatch, exits_during_inspection
):
    active = iter([True, False] if exits_during_inspection else [False])
    closed = []
    handle = object()
    monkeypatch.setattr(processes, "_open_windows_process", lambda pid: handle)
    monkeypatch.setattr(processes, "_close_windows_process", closed.append)
    monkeypatch.setattr(
        processes,
        "_windows_process_handle_is_active",
        lambda handle, *, pid: next(active),
    )
    monkeypatch.setattr(
        processes, "_read_windows_creation_time", lambda handle, *, pid: 123
    )

    def read_executable(handle, *, pid):
        if not exits_during_inspection:
            pytest.fail("an exited process must not be queried for its executable")
        raise ProcessInspectionError("executable unavailable")

    monkeypatch.setattr(processes, "_read_windows_executable", read_executable)

    assert processes._inspect_windows_process_identity(123) is None
    assert closed == [handle]


@pytest.mark.parametrize("exit_after_executable", [False, True])
def test_windows_identity_discards_successful_read_if_process_exits(
    monkeypatch, exit_after_executable
):
    active = iter([True, False] if exit_after_executable else [True, True, False])
    monkeypatch.setattr(
        processes,
        "_windows_process_handle_is_active",
        lambda handle, *, pid: next(active),
    )
    monkeypatch.setattr(
        processes, "_read_windows_creation_time", lambda handle, *, pid: 123
    )
    monkeypatch.setattr(
        processes,
        "_read_windows_executable",
        lambda handle, *, pid: "C:/runtime/python.exe",
    )
    monkeypatch.setattr(processes, "_read_windows_session_id", lambda pid: 1)

    assert (
        processes._inspect_windows_process_identity_from_handle(123, object()) is None
    )


def test_windows_identity_preserves_live_executable_query_errors(monkeypatch):
    closed = []
    handle = object()
    monkeypatch.setattr(processes, "_open_windows_process", lambda pid: handle)
    monkeypatch.setattr(processes, "_close_windows_process", closed.append)
    monkeypatch.setattr(
        processes, "_windows_process_handle_is_active", lambda handle, *, pid: True
    )
    monkeypatch.setattr(
        processes, "_read_windows_creation_time", lambda handle, *, pid: 123
    )

    def fail_read(handle, *, pid):
        raise ProcessInspectionError("access denied")

    monkeypatch.setattr(processes, "_read_windows_executable", fail_read)
    with pytest.raises(ProcessInspectionError, match="access denied"):
        processes._inspect_windows_process_identity(123)
    assert closed == [handle]


@pytest.mark.parametrize("active", [False, True])
def test_windows_liveness_uses_synchronized_handle(monkeypatch, active):
    handle = object()
    closed = []
    monkeypatch.setattr(processes, "_open_windows_process", lambda pid: handle)
    monkeypatch.setattr(processes, "_close_windows_process", closed.append)
    monkeypatch.setattr(
        processes, "_windows_process_handle_is_active", lambda handle, *, pid: active
    )

    assert processes._windows_process_is_running(123) is active
    assert closed == [handle]


def test_windows_liveness_closes_handle_when_wait_fails(monkeypatch):
    handle = object()
    closed = []
    monkeypatch.setattr(processes, "_open_windows_process", lambda pid: handle)
    monkeypatch.setattr(processes, "_close_windows_process", closed.append)

    def fail_wait(handle, *, pid):
        raise ProcessInspectionError("wait failed")

    monkeypatch.setattr(processes, "_windows_process_handle_is_active", fail_wait)
    assert not processes._windows_process_is_running(123)
    assert closed == [handle]


def test_windows_liveness_returns_false_for_absent_process(monkeypatch):
    monkeypatch.setattr(processes, "_open_windows_process", lambda pid: None)
    monkeypatch.setattr(
        processes,
        "_close_windows_process",
        lambda handle: pytest.fail("absent process has no handle to close"),
    )
    assert not processes._windows_process_is_running(123)


def test_windows_process_listing_requests_utf8_and_preserves_unicode(monkeypatch):
    command = "python \u03a9\u4e2d\u6587 \u0101"

    def run(args, **kwargs):
        assert not kwargs.get("text", False)
        assert "encoding" not in kwargs
        assert "[Console]::OutputEncoding" in args[-1]
        assert "$OutputEncoding" in args[-1]
        assert "UTF8Encoding" in args[-1]
        return subprocess.CompletedProcess(
            args,
            0,
            stdout=json.dumps(
                {"ProcessId": 123, "ParentProcessId": 12, "CommandLine": command},
                ensure_ascii=False,
            ).encode("utf-8"),
        )

    monkeypatch.setattr(processes.subprocess, "run", run)
    assert processes._list_windows_processes() == [
        processes.ProcessInfo(123, 12, "Running", command)
    ]


@pytest.mark.parametrize(
    "error",
    [
        OSError("launch failed"),
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid"),
        subprocess.TimeoutExpired("powershell", 10),
    ],
)
def test_windows_listing_failures_raise_inspection_error(monkeypatch, error):
    def fail_run(*args, **kwargs):
        raise error

    monkeypatch.setattr(processes.subprocess, "run", fail_run)
    with pytest.raises(ProcessInspectionError, match="process table") as caught:
        processes._list_windows_processes()
    assert caught.value.__cause__ is error


@pytest.mark.parametrize("payload", [None, b"{bad json", b"\xff"])
def test_windows_listing_invalid_output_is_controlled(monkeypatch, payload):
    monkeypatch.setattr(
        processes.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, stdout=payload),
    )
    with pytest.raises(ProcessInspectionError, match="process table"):
        processes._list_windows_processes()


@pytest.mark.skipif(os.name != "nt", reason="Native Windows retained process handles")
@pytest.mark.parametrize("exit_code", [0, 259])
def test_exited_windows_child_has_no_liveness_or_identity_with_retained_handle(
    exit_code,
):
    child = subprocess.Popen([sys.executable, "-c", f"raise SystemExit({exit_code})"])
    try:
        assert child.wait(timeout=10) == exit_code
        assert not processes.process_is_running(child.pid)
        assert processes.inspect_process_identity(child.pid) is None
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=10)
        child._handle.Close()


@pytest.mark.skipif(os.name != "nt", reason="Native Windows Unicode command lines")
@pytest.mark.parametrize("marker", ["\u03a9\u4e2d\u6587", "\u0101"])
def test_windows_listing_reads_unicode_child_command_line(marker):
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)", marker]
    )
    try:
        row = next(row for row in processes.list_processes() if row.pid == child.pid)
        assert marker in row.command
    finally:
        if child.poll() is None:
            child.terminate()
        child.wait(timeout=10)
        child._handle.Close()
