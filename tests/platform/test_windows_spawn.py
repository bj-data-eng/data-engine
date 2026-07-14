from __future__ import annotations

import ctypes
import os
import subprocess
from types import SimpleNamespace

import pytest

from data_engine.platform import windows_spawn
from data_engine.platform.processes import ProcessIdentity, ProcessInspectionError


_NONCE = "ab" * 32
_EXECUTABLE = r"C:\Program Files\Python\python.exe"


class _FakeWindowsFunction:
    def __init__(self, implementation):
        self.implementation = implementation
        self.argtypes = None
        self.restype = None

    def __call__(self, *args):
        return self.implementation(*args)


class _FakeKernel32:
    pass


class _FakeJob:
    def __init__(self, events, *, terminate_error=None):
        self.events = events
        self.terminate_error = terminate_error

    def _native_handle(self):
        self.events.append(("job_handle", 700))
        return 700

    def terminate(self, *, timeout_seconds):
        self.events.append(("job_terminate", timeout_seconds))
        if self.terminate_error is not None:
            raise self.terminate_error

    def close(self):
        self.events.append(("job_close", 700))


def _identity() -> ProcessIdentity:
    return ProcessIdentity(
        pid=303,
        start_key="windows-start-key",
        executable_path=_EXECUTABLE,
        process_group_id=None,
        process_session_id=4,
    )


def _select_windows(monkeypatch) -> None:
    monkeypatch.setattr(
        windows_spawn,
        "os",
        SimpleNamespace(name="nt", fspath=os.fspath, PathLike=os.PathLike),
    )


def _install_windows_boundary(
    monkeypatch,
    *,
    create_process_result=1,
    assign_result=1,
    resume_result=1,
    duplicate_result=1,
    wait_results=(),
    identity_result=None,
    identity_error=None,
    job_terminate_error=None,
):
    _select_windows(monkeypatch)
    events = []
    kernel32 = _FakeKernel32()
    null_handles = iter((101, 102))
    waits = iter(wait_results)

    def _create_file(
        name,
        access,
        share_mode,
        security_attributes_pointer,
        disposition,
        flags,
        template,
    ):
        security_attributes = ctypes.cast(
            security_attributes_pointer,
            ctypes.POINTER(windows_spawn._WindowsSecurityAttributes),
        ).contents
        handle = next(null_handles)
        events.append(
            (
                "create_file",
                name,
                access,
                share_mode,
                security_attributes.inherit_handle,
                disposition,
                flags,
                template,
                handle,
            )
        )
        return handle

    def _initialize(attribute_list, count, flags, size_pointer):
        if attribute_list is None:
            ctypes.cast(size_pointer, ctypes.POINTER(ctypes.c_size_t)).contents.value = 128
            events.append(("size_attributes", count, flags))
            return 0
        events.append(("initialize_attributes", count, flags))
        return 1

    def _update(
        attribute_list,
        flags,
        attribute,
        value_pointer,
        value_size,
        previous,
        return_size,
    ):
        count = value_size // ctypes.sizeof(ctypes.c_void_p)
        values = ctypes.cast(
            value_pointer,
            ctypes.POINTER(ctypes.c_void_p * count),
        ).contents
        events.append(
            (
                "handle_list",
                flags,
                attribute,
                tuple(values),
                previous,
                return_size,
            )
        )
        return 1

    def _create_process(
        application_name,
        command_line,
        process_attributes,
        thread_attributes,
        inherit_handles,
        creation_flags,
        environment_pointer,
        current_directory,
        startup_info_pointer,
        process_information_pointer,
    ):
        startup_info = ctypes.cast(
            startup_info_pointer,
            ctypes.POINTER(windows_spawn._WindowsStartupInfoEx),
        ).contents
        environment_text = None
        if environment_pointer:
            environment_text = ctypes.wstring_at(environment_pointer, 44)
        events.append(
            (
                "create_process",
                application_name,
                command_line.value,
                process_attributes,
                thread_attributes,
                inherit_handles,
                creation_flags,
                environment_text,
                current_directory,
                startup_info.startup_info.cb,
                startup_info.startup_info.flags,
                startup_info.startup_info.standard_input,
                startup_info.startup_info.standard_output,
                startup_info.startup_info.standard_error,
            )
        )
        if create_process_result:
            process_information = ctypes.cast(
                process_information_pointer,
                ctypes.POINTER(windows_spawn._WindowsProcessInformation),
            ).contents
            process_information.process_handle = 201
            process_information.thread_handle = 202
            process_information.process_id = 303
            process_information.thread_id = 304
        return create_process_result

    def _assign(job_handle, process_handle):
        events.append(("assign", job_handle, process_handle))
        return assign_result

    def _resume(thread_handle):
        events.append(("resume", thread_handle))
        return resume_result

    def _duplicate(
        source_process,
        source_handle,
        target_process,
        target_handle_pointer,
        access,
        inherit_handle,
        options,
    ):
        events.append(
            (
                "duplicate",
                source_process,
                source_handle,
                target_process,
                access,
                inherit_handle,
                options,
            )
        )
        if duplicate_result:
            ctypes.cast(
                target_handle_pointer,
                ctypes.POINTER(ctypes.c_void_p),
            ).contents.value = 901
        return duplicate_result

    def _wait(handle, timeout):
        result = next(waits, windows_spawn._WAIT_OBJECT_0)
        events.append(("wait", handle, timeout, result))
        return result

    kernel32.CreateFileW = _FakeWindowsFunction(_create_file)
    kernel32.InitializeProcThreadAttributeList = _FakeWindowsFunction(_initialize)
    kernel32.UpdateProcThreadAttribute = _FakeWindowsFunction(_update)
    kernel32.DeleteProcThreadAttributeList = _FakeWindowsFunction(
        lambda attribute_list: events.append(("delete_attributes",))
    )
    kernel32.CreateProcessW = _FakeWindowsFunction(_create_process)
    kernel32.AssignProcessToJobObject = _FakeWindowsFunction(_assign)
    kernel32.ResumeThread = _FakeWindowsFunction(_resume)
    kernel32.GetCurrentProcess = _FakeWindowsFunction(lambda: -1)
    kernel32.DuplicateHandle = _FakeWindowsFunction(_duplicate)
    kernel32.WaitForSingleObject = _FakeWindowsFunction(_wait)
    kernel32.TerminateProcess = _FakeWindowsFunction(
        lambda handle, exit_code: events.append(
            ("terminate_process", handle, exit_code)
        )
        or 1
    )
    kernel32.CloseHandle = _FakeWindowsFunction(
        lambda handle: events.append(("close", handle)) or 1
    )

    job = _FakeJob(events, terminate_error=job_terminate_error)
    monkeypatch.setattr(windows_spawn, "_windows_kernel32", lambda: kernel32)
    monkeypatch.setattr(windows_spawn, "_last_error", lambda: 122)
    monkeypatch.setattr(windows_spawn, "_set_last_error", lambda error: None)
    monkeypatch.setattr(
        windows_spawn,
        "create_windows_kill_on_close_job",
        lambda nonce: events.append(("create_job", nonce)) or job,
    )

    def _inspect(process_id, process_handle):
        events.append(("inspect", process_id, process_handle))
        if identity_error is not None:
            raise identity_error
        return _identity() if identity_result is None else identity_result

    monkeypatch.setattr(
        windows_spawn.processes,
        "_inspect_windows_process_identity_from_handle",
        _inspect,
    )
    return events


def _event_names(events) -> list[str]:
    return [event[0] for event in events]


def test_spawn_rejects_non_windows_before_creating_job(monkeypatch):
    monkeypatch.setattr(
        windows_spawn,
        "create_windows_kill_on_close_job",
        lambda nonce: pytest.fail("a Job must not be created outside Windows"),
    )

    with pytest.raises(ProcessInspectionError, match="requires Windows"):
        windows_spawn.spawn_windows_contained_process(
            _EXECUTABLE,
            (),
            containment_nonce=_NONCE,
        )


def test_spawn_quotes_command_and_transfers_noninheritable_job_after_resume(
    monkeypatch,
):
    events = _install_windows_boundary(monkeypatch)
    arguments = (
        "-m",
        "data_engine.hosts.daemon.app",
        "--workspace",
        r"C:\Shared Data\work",
        'quote"value',
        "",
        "tail\\",
    )
    env = {"Path": r"C:\Program Files\Python", "A": "space value"}

    result = windows_spawn.spawn_windows_contained_process(
        _EXECUTABLE,
        arguments,
        containment_nonce=_NONCE,
        cwd=r"C:\Shared Data\work",
        env=env,
    )

    assert result.process_identity == _identity()
    assert result.containment_nonce == _NONCE
    assert result.pid == 303
    command = next(event for event in events if event[0] == "create_process")
    assert command[1] == _EXECUTABLE
    assert command[2] == subprocess.list2cmdline([_EXECUTABLE, *arguments])
    assert command[5] is True
    assert command[6] == windows_spawn._PROCESS_CREATION_FLAGS
    assert command[7].startswith("A=space value\0Path=C:\\Program Files\\Python\0\0")
    assert command[8] == r"C:\Shared Data\work"
    assert command[9] == ctypes.sizeof(windows_spawn._WindowsStartupInfoEx)
    assert command[10:] == (windows_spawn._STARTF_USESTDHANDLES, 101, 102, 102)

    handle_list = next(event for event in events if event[0] == "handle_list")
    assert handle_list[2] == windows_spawn._PROC_THREAD_ATTRIBUTE_HANDLE_LIST
    assert handle_list[3] == (101, 102)
    assert [event[4] for event in events if event[0] == "create_file"] == [1, 1]
    duplicate = next(event for event in events if event[0] == "duplicate")
    assert duplicate == (
        "duplicate",
        -1,
        700,
        201,
        0,
        False,
        windows_spawn._DUPLICATE_SAME_ACCESS,
    )

    names = _event_names(events)
    assert names.index("create_process") < names.index("assign")
    assert names.index("assign") < names.index("inspect")
    assert names.index("inspect") < names.index("resume")
    assert names.index("resume") < names.index("duplicate")
    assert names.index("duplicate") < names.index("job_close")
    assert "job_terminate" not in names
    assert "terminate_process" not in names
    assert [event[1] for event in events if event[0] == "close"] == [
        202,
        201,
        102,
        101,
    ]


def test_create_process_failure_closes_all_launcher_resources(monkeypatch):
    events = _install_windows_boundary(monkeypatch, create_process_result=0)

    with pytest.raises(ProcessInspectionError, match="create suspended"):
        windows_spawn.spawn_windows_contained_process(
            _EXECUTABLE,
            (),
            containment_nonce=_NONCE,
        )

    names = _event_names(events)
    assert "assign" not in names
    assert "resume" not in names
    assert "duplicate" not in names
    assert "terminate_process" not in names
    assert [event[1] for event in events if event[0] == "close"] == [102, 101]
    assert names[-1] == "job_close"


def test_assignment_failure_terminates_and_waits_exact_created_process(monkeypatch):
    events = _install_windows_boundary(
        monkeypatch,
        assign_result=0,
        wait_results=(windows_spawn._WAIT_TIMEOUT, windows_spawn._WAIT_OBJECT_0),
    )

    with pytest.raises(ProcessInspectionError, match="contain created process"):
        windows_spawn.spawn_windows_contained_process(
            _EXECUTABLE,
            (),
            containment_nonce=_NONCE,
        )

    names = _event_names(events)
    assert "job_terminate" not in names
    assert "resume" not in names
    assert "duplicate" not in names
    assert names.index("assign") < names.index("terminate_process")
    assert names.index("terminate_process") < names.index("job_close")
    assert [event for event in events if event[0] == "wait"] == [
        ("wait", 201, 0, windows_spawn._WAIT_TIMEOUT),
        ("wait", 201, 5_000, windows_spawn._WAIT_OBJECT_0),
    ]


def test_identity_failure_terminates_job_and_confirms_exact_process_exit(monkeypatch):
    events = _install_windows_boundary(
        monkeypatch,
        identity_error=ProcessInspectionError("identity unavailable"),
        wait_results=(windows_spawn._WAIT_OBJECT_0,),
    )

    with pytest.raises(ProcessInspectionError, match="identity unavailable"):
        windows_spawn.spawn_windows_contained_process(
            _EXECUTABLE,
            (),
            containment_nonce=_NONCE,
        )

    names = _event_names(events)
    assert names.index("assign") < names.index("inspect")
    assert names.index("inspect") < names.index("job_terminate")
    assert names.index("job_terminate") < names.index("wait")
    assert names.index("wait") < names.index("job_close")
    assert "resume" not in names
    assert "duplicate" not in names
    assert "terminate_process" not in names


def test_resume_failure_terminates_job_without_transferring_ownership(monkeypatch):
    events = _install_windows_boundary(
        monkeypatch,
        resume_result=windows_spawn._RESUME_THREAD_FAILED,
        wait_results=(windows_spawn._WAIT_OBJECT_0,),
    )

    with pytest.raises(ProcessInspectionError, match="resume contained process"):
        windows_spawn.spawn_windows_contained_process(
            _EXECUTABLE,
            (),
            containment_nonce=_NONCE,
        )

    names = _event_names(events)
    assert names.index("resume") < names.index("job_terminate")
    assert "duplicate" not in names
    assert names.index("job_terminate") < names.index("wait")
    assert names.index("wait") < names.index("job_close")


def test_ownership_transfer_failure_after_resume_terminates_contained_job(
    monkeypatch,
):
    events = _install_windows_boundary(
        monkeypatch,
        duplicate_result=0,
        wait_results=(windows_spawn._WAIT_OBJECT_0,),
    )

    with pytest.raises(ProcessInspectionError, match="transfer Job ownership"):
        windows_spawn.spawn_windows_contained_process(
            _EXECUTABLE,
            (),
            containment_nonce=_NONCE,
        )

    names = _event_names(events)
    assert names.index("resume") < names.index("duplicate")
    assert names.index("duplicate") < names.index("job_terminate")
    assert names.index("job_terminate") < names.index("wait")
    assert names.index("wait") < names.index("job_close")


def test_job_cleanup_failure_falls_back_to_exact_process_termination(monkeypatch):
    events = _install_windows_boundary(
        monkeypatch,
        resume_result=windows_spawn._RESUME_THREAD_FAILED,
        job_terminate_error=ProcessInspectionError("job termination failed"),
        wait_results=(windows_spawn._WAIT_TIMEOUT, windows_spawn._WAIT_OBJECT_0),
    )

    with pytest.raises(ProcessInspectionError, match="resume contained process") as caught:
        windows_spawn.spawn_windows_contained_process(
            _EXECUTABLE,
            (),
            containment_nonce=_NONCE,
        )

    assert caught.value.__notes__ == [
        "Failed-process cleanup also failed: job termination failed"
    ]
    names = _event_names(events)
    assert names.index("job_terminate") < names.index("terminate_process")
    assert names.index("terminate_process") < names.index("job_close")


@pytest.mark.parametrize(
    ("executable", "arguments", "cwd", "env", "message"),
    [
        ("", (), None, None, "Executable cannot be empty"),
        ("python.exe\0wrong", (), None, None, "Executable cannot contain"),
        (_EXECUTABLE, "--flag", None, None, "arguments must be a sequence"),
        (_EXECUTABLE, ("bad\0argument",), None, None, "argument 0 cannot contain"),
        (_EXECUTABLE, (), "", None, "Working directory cannot be empty"),
        (_EXECUTABLE, (), None, {"BAD=NAME": "value"}, "cannot contain '='"),
        (_EXECUTABLE, (), None, {"GOOD": "bad\0value"}, "cannot contain null"),
    ],
)
def test_invalid_launch_inputs_fail_before_job_creation(
    monkeypatch,
    executable,
    arguments,
    cwd,
    env,
    message,
):
    _select_windows(monkeypatch)
    monkeypatch.setattr(
        windows_spawn,
        "create_windows_kill_on_close_job",
        lambda nonce: pytest.fail("invalid input must not create a Job"),
    )

    with pytest.raises((TypeError, ValueError), match=message):
        windows_spawn.spawn_windows_contained_process(
            executable,
            arguments,
            containment_nonce=_NONCE,
            cwd=cwd,
            env=env,
        )
