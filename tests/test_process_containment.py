from __future__ import annotations

import ctypes
from dataclasses import replace
import os

import pytest

from data_engine.platform import processes
from data_engine.platform.processes import (
    ProcessIdentity,
    ProcessInspectionError,
    WindowsKillOnCloseJob,
    create_windows_kill_on_close_job,
    force_kill_verified_contained_process_tree,
    new_process_containment_nonce,
    open_windows_kill_on_close_job,
    windows_job_name_for_nonce,
)


_NONCE = "ab" * 32
_JOB_NAME = f"Local\\DataEngineDaemonJob-{_NONCE}"


class _FakeWindowsFunction:
    def __init__(self, implementation):
        self.implementation = implementation
        self.argtypes = None
        self.restype = None

    def __call__(self, *args):
        return self.implementation(*args)


class _FakeKernel32:
    pass


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


def _select_windows(monkeypatch) -> None:
    monkeypatch.setattr(processes.os, "name", "nt")


def test_containment_nonce_is_canonical_256_bit_lowercase_hex():
    nonce = new_process_containment_nonce()

    assert len(nonce) == 64
    assert nonce == nonce.casefold()
    assert set(nonce) <= set("0123456789abcdef")
    assert windows_job_name_for_nonce(nonce).startswith(
        "Local\\DataEngineDaemonJob-"
    )


def test_wait_for_posix_process_group_exit_returns_when_group_is_absent(monkeypatch):
    monkeypatch.setattr(processes.os, "name", "posix")
    monkeypatch.setattr(
        processes.os,
        "killpg",
        lambda group_id, selected_signal: (_ for _ in ()).throw(ProcessLookupError()),
    )

    assert processes.wait_for_posix_process_group_exit(321, timeout_seconds=0.0) is True


def test_wait_for_posix_process_group_exit_reports_live_group_at_deadline(monkeypatch):
    monkeypatch.setattr(processes.os, "name", "posix")
    calls = []
    monkeypatch.setattr(
        processes.os,
        "killpg",
        lambda group_id, selected_signal: calls.append((group_id, selected_signal)),
    )

    assert processes.wait_for_posix_process_group_exit(321, timeout_seconds=0.0) is False
    assert calls == [(321, 0)]


def test_wait_for_posix_process_group_exit_treats_permission_error_as_live_group(monkeypatch):
    monkeypatch.setattr(processes.os, "name", "posix")
    monkeypatch.setattr(
        processes.os,
        "killpg",
        lambda group_id, selected_signal: (_ for _ in ()).throw(PermissionError()),
    )

    assert processes.wait_for_posix_process_group_exit(321, timeout_seconds=0.0) is False


@pytest.mark.parametrize(
    "nonce",
    [
        "a" * 63,
        "a" * 65,
        "A" * 64,
        "g" * 64,
        "../" + "a" * 61,
        "Local\\DataEngineDaemonJob-" + "a" * 64,
    ],
)
def test_windows_job_name_rejects_noncanonical_or_injected_names(nonce):
    with pytest.raises(ValueError, match="64 lowercase hexadecimal"):
        windows_job_name_for_nonce(nonce)


def test_posix_termination_uses_verified_watchdog_capability_without_killpg(monkeypatch):
    monkeypatch.setattr(processes.os, "name", "posix")
    expected = _identity()
    events = []
    monkeypatch.setattr(
        processes.os,
        "getpid",
        lambda: events.append(("getpid",)) or 111,
    )
    monkeypatch.setattr(
        processes.os,
        "getpgrp",
        lambda: events.append(("getpgrp",)) or 111,
        raising=False,
    )
    monkeypatch.setattr(
        processes,
        "inspect_process_identity",
        lambda pid: events.append(("inspect", pid)) or expected,
    )
    monkeypatch.setattr(
        processes.os,
        "killpg",
        lambda *args: pytest.fail("external numeric group signaling must not run"),
        raising=False,
    )
    monkeypatch.setattr(
        processes,
        "request_posix_process_group_termination",
        lambda **kwargs: events.append(("request", kwargs)),
    )

    force_kill_verified_contained_process_tree(
        expected,
        containment_nonce=_NONCE,
    )

    assert events == [
        ("getpid",),
        ("getpgrp",),
        ("inspect", 321),
        (
            "request",
            {
                "containment_nonce": _NONCE,
                "expected_parent_pid": 321,
            },
        ),
    ]


def test_posix_termination_wraps_watchdog_request_failure(monkeypatch):
    monkeypatch.setattr(processes.os, "name", "posix")
    expected = _identity()
    monkeypatch.setattr(processes.os, "getpid", lambda: 111)
    monkeypatch.setattr(processes.os, "getpgrp", lambda: 111)
    monkeypatch.setattr(processes, "inspect_process_identity", lambda pid: expected)
    monkeypatch.setattr(
        processes,
        "request_posix_process_group_termination",
        lambda **kwargs: (_ for _ in ()).throw(
            processes.PosixProcessGroupWatchdogError("missing control endpoint")
        ),
    )

    with pytest.raises(ProcessInspectionError, match="Unable to request verified termination"):
        force_kill_verified_contained_process_tree(
            expected,
            containment_nonce=_NONCE,
        )


def test_posix_termination_identity_mismatch_never_reaches_watchdog(monkeypatch):
    monkeypatch.setattr(processes.os, "name", "posix")
    expected = _identity()
    monkeypatch.setattr(processes.os, "getpid", lambda: 111)
    monkeypatch.setattr(processes.os, "getpgrp", lambda: 111)
    monkeypatch.setattr(
        processes,
        "inspect_process_identity",
        lambda pid: replace(expected, start_key="replacement"),
    )
    monkeypatch.setattr(
        processes,
        "request_posix_process_group_termination",
        lambda **kwargs: pytest.fail("mismatched identity must not reach the watchdog"),
    )

    with pytest.raises(ProcessInspectionError, match="no longer matches"):
        force_kill_verified_contained_process_tree(
            expected,
            containment_nonce=_NONCE,
        )


def test_verified_group_termination_refuses_the_pytest_process(monkeypatch):
    pytest_pid = os.getpid()
    expected = _identity(
        pid=pytest_pid,
        process_group_id=pytest_pid,
        process_session_id=pytest_pid,
    )
    monkeypatch.setattr(processes.os, "name", "posix")
    monkeypatch.setattr(processes.os, "getpgrp", lambda: pytest_pid, raising=False)
    monkeypatch.setattr(processes, "inspect_process_identity", lambda pid: expected)
    monkeypatch.setattr(
        processes,
        "request_posix_process_group_termination",
        lambda **kwargs: pytest.fail("the pytest process must never be terminated"),
    )

    with pytest.raises(ProcessInspectionError, match="caller's process group"):
        force_kill_verified_contained_process_tree(
            expected,
            containment_nonce=_NONCE,
        )


def test_create_windows_job_configures_kill_on_close(monkeypatch):
    _select_windows(monkeypatch)
    events = []
    kernel32 = _FakeKernel32()

    def _create(security_attributes, name):
        events.append(("create", security_attributes, name))
        return 801

    def _configure(handle, information_class, information_pointer, size):
        information = ctypes.cast(
            information_pointer,
            ctypes.POINTER(processes._WindowsJobExtendedLimitInformation),
        ).contents
        events.append(
            (
                "configure",
                handle,
                information_class,
                information.basic_limit_information.limit_flags,
                size,
            )
        )
        return 1

    kernel32.CreateJobObjectW = _FakeWindowsFunction(_create)
    kernel32.SetInformationJobObject = _FakeWindowsFunction(_configure)
    kernel32.CloseHandle = _FakeWindowsFunction(
        lambda handle: events.append(("close", handle)) or 1
    )
    monkeypatch.setattr(processes, "_windows_kernel32", lambda: kernel32)
    monkeypatch.setattr(processes, "_windows_last_error", lambda: 0)

    job = create_windows_kill_on_close_job(_NONCE)
    assert job.name == _JOB_NAME
    assert not job.closed
    job.close()
    job.close()

    assert events == [
        ("create", None, _JOB_NAME),
        (
            "configure",
            801,
            9,
            processes._WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            ctypes.sizeof(processes._WindowsJobExtendedLimitInformation),
        ),
        ("close", 801),
    ]


def test_create_windows_job_rejects_existing_name_without_reconfiguration(monkeypatch):
    _select_windows(monkeypatch)
    events = []
    kernel32 = _FakeKernel32()
    kernel32.CreateJobObjectW = _FakeWindowsFunction(lambda unused, name: 802)
    kernel32.SetInformationJobObject = _FakeWindowsFunction(
        lambda *args: pytest.fail("an existing Job must not be reconfigured")
    )
    kernel32.CloseHandle = _FakeWindowsFunction(
        lambda handle: events.append(("close", handle)) or 1
    )
    monkeypatch.setattr(processes, "_windows_kernel32", lambda: kernel32)
    monkeypatch.setattr(
        processes,
        "_windows_last_error",
        lambda: processes._WINDOWS_ERROR_ALREADY_EXISTS,
    )

    with pytest.raises(ProcessInspectionError, match="Refusing to reuse"):
        create_windows_kill_on_close_job(_NONCE)

    assert events == [("close", 802)]


def test_open_windows_job_verifies_kill_on_close(monkeypatch):
    _select_windows(monkeypatch)
    events = []
    kernel32 = _FakeKernel32()

    def _open(access, inherit, name):
        events.append(("open", access, inherit, name))
        return 803

    def _query(handle, information_class, information_pointer, size, return_length):
        information = ctypes.cast(
            information_pointer,
            ctypes.POINTER(processes._WindowsJobExtendedLimitInformation),
        ).contents
        information.basic_limit_information.limit_flags = (
            processes._WINDOWS_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        )
        ctypes.cast(return_length, ctypes.POINTER(ctypes.c_uint32)).contents.value = size
        events.append(("query", handle, information_class, size))
        return 1

    kernel32.OpenJobObjectW = _FakeWindowsFunction(_open)
    kernel32.QueryInformationJobObject = _FakeWindowsFunction(_query)
    kernel32.CloseHandle = _FakeWindowsFunction(
        lambda handle: events.append(("close", handle)) or 1
    )
    monkeypatch.setattr(processes, "_windows_kernel32", lambda: kernel32)

    job = open_windows_kill_on_close_job(_NONCE)
    assert job is not None
    job.close()

    expected_access = (
        processes._WINDOWS_JOB_OBJECT_QUERY | processes._WINDOWS_JOB_OBJECT_TERMINATE
    )
    assert events == [
        ("open", expected_access, False, _JOB_NAME),
        (
            "query",
            803,
            processes._WINDOWS_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
            ctypes.sizeof(processes._WindowsJobExtendedLimitInformation),
        ),
        ("close", 803),
    ]


def test_open_windows_job_rejects_unconfigured_object_and_closes(monkeypatch):
    _select_windows(monkeypatch)
    closed = []
    kernel32 = _FakeKernel32()
    kernel32.OpenJobObjectW = _FakeWindowsFunction(lambda access, inherit, name: 804)
    kernel32.QueryInformationJobObject = _FakeWindowsFunction(
        lambda handle, information_class, pointer, size, returned: 1
    )
    kernel32.CloseHandle = _FakeWindowsFunction(
        lambda handle: closed.append(handle) or 1
    )
    monkeypatch.setattr(processes, "_windows_kernel32", lambda: kernel32)

    with pytest.raises(ProcessInspectionError, match="not configured"):
        open_windows_kill_on_close_job(_NONCE)

    assert closed == [804]


def test_open_windows_job_returns_none_only_for_confirmed_absence(monkeypatch):
    _select_windows(monkeypatch)
    kernel32 = _FakeKernel32()
    kernel32.OpenJobObjectW = _FakeWindowsFunction(lambda access, inherit, name: 0)
    monkeypatch.setattr(processes, "_windows_kernel32", lambda: kernel32)
    monkeypatch.setattr(
        processes,
        "_windows_last_error",
        lambda: processes._WINDOWS_ERROR_FILE_NOT_FOUND,
    )

    assert open_windows_kill_on_close_job(_NONCE) is None


def test_ensure_windows_containment_job_stopped_terminates_and_closes(monkeypatch):
    _select_windows(monkeypatch)
    events = []

    class _Job:
        def __enter__(self):
            events.append(("enter",))
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            events.append(("close",))

        def terminate(self, *, timeout_seconds: float) -> None:
            events.append(("terminate", timeout_seconds))

    monkeypatch.setattr(
        processes,
        "open_windows_kill_on_close_job",
        lambda nonce: events.append(("open", nonce)) or _Job(),
    )

    processes.ensure_windows_containment_job_stopped(
        _NONCE,
        timeout_seconds=1.25,
    )

    assert events == [
        ("open", _NONCE),
        ("enter",),
        ("terminate", 1.25),
        ("close",),
    ]


def test_ensure_windows_containment_job_stopped_accepts_absent_job(monkeypatch):
    _select_windows(monkeypatch)
    monkeypatch.setattr(processes, "open_windows_kill_on_close_job", lambda nonce: None)

    processes.ensure_windows_containment_job_stopped(_NONCE)


def test_wait_for_windows_job_empty_polls_active_process_accounting(monkeypatch):
    _select_windows(monkeypatch)
    active_counts = iter((2, 1, 0))
    events = []
    kernel32 = _FakeKernel32()

    def _query(handle, information_class, information_pointer, size, return_length):
        information = ctypes.cast(
            information_pointer,
            ctypes.POINTER(processes._WindowsJobBasicAccountingInformation),
        ).contents
        information.active_processes = next(active_counts)
        ctypes.cast(return_length, ctypes.POINTER(ctypes.c_uint32)).contents.value = size
        events.append(("query", handle, information_class, information.active_processes, size))
        return 1

    kernel32.QueryInformationJobObject = _FakeWindowsFunction(_query)
    monkeypatch.setattr(processes, "_windows_kernel32", lambda: kernel32)
    monkeypatch.setattr(processes.time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(processes.time, "sleep", lambda seconds: events.append(("sleep", seconds)))

    processes._wait_for_windows_job_empty(
        "job",
        name=_JOB_NAME,
        timeout_seconds=1.0,
    )

    assert events == [
        (
            "query",
            "job",
            processes._WINDOWS_JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION,
            2,
            ctypes.sizeof(processes._WindowsJobBasicAccountingInformation),
        ),
        ("sleep", 0.01),
        (
            "query",
            "job",
            processes._WINDOWS_JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION,
            1,
            ctypes.sizeof(processes._WindowsJobBasicAccountingInformation),
        ),
        ("sleep", 0.01),
        (
            "query",
            "job",
            processes._WINDOWS_JOB_OBJECT_BASIC_ACCOUNTING_INFORMATION,
            0,
            ctypes.sizeof(processes._WindowsJobBasicAccountingInformation),
        ),
    ]


def test_wait_for_windows_job_empty_uses_one_bounded_deadline(monkeypatch):
    _select_windows(monkeypatch)
    query_count = 0
    clock = iter((10.0, 10.0, 10.1))
    kernel32 = _FakeKernel32()

    def _query(handle, information_class, information_pointer, size, return_length):
        nonlocal query_count
        del handle, information_class, size, return_length
        query_count += 1
        information = ctypes.cast(
            information_pointer,
            ctypes.POINTER(processes._WindowsJobBasicAccountingInformation),
        ).contents
        information.active_processes = 1
        return 1

    kernel32.QueryInformationJobObject = _FakeWindowsFunction(_query)
    monkeypatch.setattr(processes, "_windows_kernel32", lambda: kernel32)
    monkeypatch.setattr(processes.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(processes.time, "sleep", lambda seconds: None)

    with pytest.raises(ProcessInspectionError, match="Timed out waiting"):
        processes._wait_for_windows_job_empty(
            "job",
            name=_JOB_NAME,
            timeout_seconds=0.05,
        )

    assert query_count == 2


def test_wait_for_windows_job_empty_fails_closed_when_accounting_query_fails(
    monkeypatch,
):
    _select_windows(monkeypatch)
    kernel32 = _FakeKernel32()
    kernel32.QueryInformationJobObject = _FakeWindowsFunction(lambda *args: 0)
    monkeypatch.setattr(processes, "_windows_kernel32", lambda: kernel32)
    monkeypatch.setattr(processes, "_windows_last_error", lambda: 5)

    with pytest.raises(ProcessInspectionError, match="Unable to inspect Windows Job"):
        processes._wait_for_windows_job_empty(
            "job",
            name=_JOB_NAME,
            timeout_seconds=1.0,
        )


def _install_verified_windows_job_mocks(monkeypatch, expected, events):
    _select_windows(monkeypatch)

    def _open_job(nonce):
        events.append(("open_job", nonce))
        return WindowsKillOnCloseJob(nonce=nonce, name=_JOB_NAME, handle="job")

    monkeypatch.setattr(processes, "open_windows_kill_on_close_job", _open_job)
    monkeypatch.setattr(
        processes,
        "_open_windows_process",
        lambda pid: events.append(("open_process", pid)) or "process",
    )
    monkeypatch.setattr(
        processes,
        "_inspect_windows_process_identity_from_handle",
        lambda pid, handle: events.append(("inspect", pid, handle)) or expected,
    )
    monkeypatch.setattr(
        processes,
        "_windows_process_is_in_job",
        lambda process_handle, job_handle, *, pid: events.append(
            ("membership", process_handle, job_handle, pid)
        )
        or True,
    )
    monkeypatch.setattr(
        processes,
        "_terminate_windows_job",
        lambda handle, *, name, exit_code=1: events.append(
            ("terminate", handle, name, exit_code)
        ),
    )
    monkeypatch.setattr(
        processes,
        "_wait_for_windows_job_empty",
        lambda handle, *, name, timeout_seconds: events.append(
            ("wait", handle, name, timeout_seconds)
        ),
    )
    monkeypatch.setattr(
        processes,
        "_close_windows_process",
        lambda handle: events.append(("close_process", handle)),
    )
    monkeypatch.setattr(
        processes,
        "_close_windows_handle",
        lambda handle: events.append(("close_job", handle)),
    )


def test_verified_windows_job_kill_orders_identity_membership_and_termination(
    monkeypatch,
):
    expected = _identity(process_group_id=None, process_session_id=7)
    events = []
    _install_verified_windows_job_mocks(monkeypatch, expected, events)

    force_kill_verified_contained_process_tree(
        expected,
        containment_nonce=_NONCE,
        timeout_seconds=3.5,
    )

    assert events == [
        ("open_process", 321),
        ("inspect", 321, "process"),
        ("open_job", _NONCE),
        ("membership", "process", "job", 321),
        ("close_process", "process"),
        ("terminate", "job", _JOB_NAME, 1),
        ("wait", "job", _JOB_NAME, 3.5),
        ("close_job", "job"),
    ]


def test_verified_windows_job_identity_mismatch_never_queries_or_terminates(
    monkeypatch,
):
    expected = _identity(process_group_id=None, process_session_id=7)
    events = []
    _install_verified_windows_job_mocks(monkeypatch, expected, events)
    monkeypatch.setattr(
        processes,
        "_inspect_windows_process_identity_from_handle",
        lambda pid, handle: events.append(("inspect", pid, handle))
        or replace(expected, executable_path="c:/replacement/python.exe"),
    )
    monkeypatch.setattr(
        processes,
        "_windows_process_is_in_job",
        lambda *args, **kwargs: pytest.fail("membership must not be queried"),
    )
    monkeypatch.setattr(
        processes,
        "_terminate_windows_job",
        lambda *args, **kwargs: pytest.fail("Job must not be terminated"),
    )

    with pytest.raises(ProcessInspectionError, match="no longer matches"):
        force_kill_verified_contained_process_tree(
            expected,
            containment_nonce=_NONCE,
        )

    assert events == [
        ("open_process", 321),
        ("inspect", 321, "process"),
        ("close_process", "process"),
    ]


def test_verified_windows_job_membership_mismatch_never_terminates(monkeypatch):
    expected = _identity(process_group_id=None, process_session_id=7)
    events = []
    _install_verified_windows_job_mocks(monkeypatch, expected, events)
    monkeypatch.setattr(
        processes,
        "_windows_process_is_in_job",
        lambda process_handle, job_handle, *, pid: events.append(
            ("membership", process_handle, job_handle, pid)
        )
        or False,
    )
    monkeypatch.setattr(
        processes,
        "_terminate_windows_job",
        lambda *args, **kwargs: pytest.fail("Job must not be terminated"),
    )

    with pytest.raises(ProcessInspectionError, match="not a member"):
        force_kill_verified_contained_process_tree(
            expected,
            containment_nonce=_NONCE,
        )

    assert events == [
        ("open_process", 321),
        ("inspect", 321, "process"),
        ("open_job", _NONCE),
        ("membership", "process", "job", 321),
        ("close_job", "job"),
        ("close_process", "process"),
    ]


def test_verified_windows_job_invalid_timeout_never_terminates(monkeypatch):
    expected = _identity(process_group_id=None, process_session_id=7)
    events = []
    _install_verified_windows_job_mocks(monkeypatch, expected, events)
    monkeypatch.setattr(
        processes,
        "_terminate_windows_job",
        lambda *args, **kwargs: pytest.fail("Job must not be terminated"),
    )

    with pytest.raises(ValueError, match="finite nonnegative number"):
        force_kill_verified_contained_process_tree(
            expected,
            containment_nonce=_NONCE,
            timeout_seconds=float("nan"),
        )

    assert events == [
        ("open_process", 321),
        ("inspect", 321, "process"),
        ("open_job", _NONCE),
        ("membership", "process", "job", 321),
        ("close_process", "process"),
        ("close_job", "job"),
    ]


def test_verified_windows_job_rejects_invalid_nonce_before_opening_any_handle(monkeypatch):
    expected = _identity(process_group_id=None, process_session_id=7)
    _select_windows(monkeypatch)
    monkeypatch.setattr(
        processes,
        "open_windows_kill_on_close_job",
        lambda nonce: pytest.fail("no Job may be opened with an invalid nonce"),
    )
    monkeypatch.setattr(
        processes,
        "_open_windows_process",
        lambda pid: pytest.fail("no process may be opened with an invalid nonce"),
    )

    with pytest.raises(ValueError, match="64 lowercase hexadecimal"):
        force_kill_verified_contained_process_tree(
            expected,
            containment_nonce="invalid",
        )


@pytest.mark.parametrize("timeout", [-1, float("inf"), float("nan"), True, "1"])
def test_windows_job_timeout_rejects_nonfinite_or_nonnumeric_values(timeout):
    with pytest.raises(ValueError, match="finite nonnegative number"):
        processes._finite_windows_timeout_milliseconds(timeout)
