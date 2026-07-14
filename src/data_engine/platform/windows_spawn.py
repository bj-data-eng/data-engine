"""Atomic Windows process launch inside a kill-on-close Job."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import ctypes
from dataclasses import dataclass
import os
import subprocess

from data_engine.platform import processes
from data_engine.platform.processes import (
    ProcessIdentity,
    ProcessInspectionError,
    create_windows_kill_on_close_job,
)


_CREATE_SUSPENDED = 0x00000004
_CREATE_NEW_PROCESS_GROUP = 0x00000200
_CREATE_UNICODE_ENVIRONMENT = 0x00000400
_CREATE_NO_WINDOW = 0x08000000
_EXTENDED_STARTUPINFO_PRESENT = 0x00080000
_PROCESS_CREATION_FLAGS = (
    _CREATE_SUSPENDED
    | _CREATE_NEW_PROCESS_GROUP
    | _CREATE_UNICODE_ENVIRONMENT
    | _CREATE_NO_WINDOW
    | _EXTENDED_STARTUPINFO_PRESENT
)

_STARTF_USESTDHANDLES = 0x00000100
_PROC_THREAD_ATTRIBUTE_HANDLE_LIST = 0x00020002
_DUPLICATE_SAME_ACCESS = 0x00000002
_GENERIC_READ = 0x80000000
_GENERIC_WRITE = 0x40000000
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_OPEN_EXISTING = 3
_FILE_ATTRIBUTE_NORMAL = 0x00000080
_ERROR_INSUFFICIENT_BUFFER = 122
_WAIT_OBJECT_0 = 0
_WAIT_TIMEOUT = 258
_WAIT_FAILED = 0xFFFFFFFF
_RESUME_THREAD_FAILED = 0xFFFFFFFF
_CLEANUP_WAIT_MILLISECONDS = 5_000
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value


class _WindowsSecurityAttributes(ctypes.Structure):
    _fields_ = [
        ("length", ctypes.c_uint32),
        ("security_descriptor", ctypes.c_void_p),
        ("inherit_handle", ctypes.c_int),
    ]


class _WindowsStartupInfo(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_uint32),
        ("reserved", ctypes.c_wchar_p),
        ("desktop", ctypes.c_wchar_p),
        ("title", ctypes.c_wchar_p),
        ("x", ctypes.c_uint32),
        ("y", ctypes.c_uint32),
        ("x_size", ctypes.c_uint32),
        ("y_size", ctypes.c_uint32),
        ("x_count_chars", ctypes.c_uint32),
        ("y_count_chars", ctypes.c_uint32),
        ("fill_attribute", ctypes.c_uint32),
        ("flags", ctypes.c_uint32),
        ("show_window", ctypes.c_uint16),
        ("reserved2_size", ctypes.c_uint16),
        ("reserved2", ctypes.POINTER(ctypes.c_ubyte)),
        ("standard_input", ctypes.c_void_p),
        ("standard_output", ctypes.c_void_p),
        ("standard_error", ctypes.c_void_p),
    ]


class _WindowsStartupInfoEx(ctypes.Structure):
    _fields_ = [
        ("startup_info", _WindowsStartupInfo),
        ("attribute_list", ctypes.c_void_p),
    ]


class _WindowsProcessInformation(ctypes.Structure):
    _fields_ = [
        ("process_handle", ctypes.c_void_p),
        ("thread_handle", ctypes.c_void_p),
        ("process_id", ctypes.c_uint32),
        ("thread_id", ctypes.c_uint32),
    ]


@dataclass(frozen=True, slots=True)
class WindowsContainedProcess:
    """Identity of one process launched inside a nonce-named Windows Job.

    Attributes:
        process_identity: Exact identity read from the process handle returned by
            ``CreateProcessW`` before its primary thread was resumed.
        containment_nonce: Nonce naming the process's kill-on-close Job.
    """

    process_identity: ProcessIdentity
    containment_nonce: str

    @property
    def pid(self) -> int:
        """Return the operating-system process identifier."""
        return self.process_identity.pid


def spawn_windows_contained_process(
    executable: str | os.PathLike[str],
    arguments: Sequence[str | os.PathLike[str]],
    *,
    containment_nonce: str,
    cwd: str | os.PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> WindowsContainedProcess:
    """Launch a Windows process that cannot run outside its named Job.

    The process starts suspended with only private ``NUL`` standard handles. The
    launcher assigns the exact process handle to a new kill-on-close Job and reads
    its identity before resuming the primary thread. After resume, a
    non-inheritable Job handle is duplicated directly into the child. That remote
    handle owns the Job until process exit, so the launcher may safely close its
    handle without a child-side handshake.

    Args:
        executable: Explicit executable supplied as ``lpApplicationName`` and
            argument zero.
        arguments: Arguments following argument zero. Windows C-runtime quoting
            is applied to the complete command line.
        containment_nonce: Canonical nonce naming a new kill-on-close Job.
        cwd: Optional child working directory.
        env: Optional complete child environment. ``None`` inherits the launcher's
            environment.

    Returns:
        The exact process identity and containment nonce. No native launcher
        handles remain owned by the returned value.

    Raises:
        ProcessInspectionError: If called outside Windows or any native launch,
            containment, identity, resume, or ownership-transfer operation fails.
        TypeError: If paths, arguments, or environment entries are not strings.
        ValueError: If paths, arguments, or environment entries are empty or
            contain invalid null characters, or if the nonce is not canonical.
    """
    if os.name != "nt":
        raise ProcessInspectionError("Atomic Windows process launch requires Windows.")

    executable_text = _path_text(executable, label="Executable", allow_empty=False)
    argument_text = _argument_text(arguments)
    command_line = subprocess.list2cmdline([executable_text, *argument_text])
    current_directory = (
        None
        if cwd is None
        else _path_text(cwd, label="Working directory", allow_empty=False)
    )
    environment_block = _environment_block(env)

    job = create_windows_kill_on_close_job(containment_nonce)
    process_information = _WindowsProcessInformation()
    process_created = False
    job_assigned = False
    null_input = None
    null_output = None
    attribute_list = None
    attribute_list_buffer = None
    cleanup_error: BaseException | None = None

    try:
        null_input, null_output = _open_null_standard_handles()
        (
            attribute_list,
            attribute_list_buffer,
        ) = _create_handle_attribute_list((null_input, null_output))
        startup_info = _startup_info(
            attribute_list=attribute_list,
            standard_input=null_input,
            standard_output=null_output,
        )
        command_buffer = ctypes.create_unicode_buffer(command_line)
        environment_pointer = (
            None
            if environment_block is None
            else ctypes.cast(environment_block, ctypes.c_void_p)
        )
        _create_suspended_process(
            executable=executable_text,
            command_buffer=command_buffer,
            current_directory=current_directory,
            environment_pointer=environment_pointer,
            startup_info=startup_info,
            process_information=process_information,
        )
        process_created = True

        process_handle = process_information.process_handle
        thread_handle = process_information.thread_handle
        process_id = int(process_information.process_id)
        if not process_handle or not thread_handle or process_id <= 0:
            raise ProcessInspectionError(
                "CreateProcessW returned incomplete process information."
            )

        job_handle = job._native_handle()  # noqa: SLF001 - same-package native boundary
        _assign_process_to_job(
            job_handle=job_handle,
            process_handle=process_handle,
            process_id=process_id,
        )
        job_assigned = True

        identity = processes._inspect_windows_process_identity_from_handle(  # noqa: SLF001
            process_id,
            process_handle,
        )
        if identity is None or identity.pid != process_id:
            raise ProcessInspectionError(
                f"Unable to establish exact identity for created process {process_id}."
            )
        result = WindowsContainedProcess(
            process_identity=identity,
            containment_nonce=containment_nonce,
        )

        _resume_primary_thread(thread_handle=thread_handle, process_id=process_id)
        _duplicate_job_handle_into_child(
            job_handle=job_handle,
            process_handle=process_handle,
            process_id=process_id,
        )
        return result
    except BaseException as exc:
        if process_created and process_information.process_handle:
            cleanup_error = _terminate_failed_child(
                job=job,
                job_assigned=job_assigned,
                process_handle=process_information.process_handle,
                process_id=int(process_information.process_id),
            )
        if cleanup_error is not None:
            exc.add_note(f"Failed-process cleanup also failed: {cleanup_error}")
        raise
    finally:
        if attribute_list is not None:
            _delete_attribute_list(attribute_list)
        # Keep the backing allocation live until DeleteProcThreadAttributeList.
        del attribute_list_buffer
        _close_handle(process_information.thread_handle)
        _close_handle(process_information.process_handle)
        _close_handle(null_output)
        _close_handle(null_input)
        job.close()


def _path_text(
    value: str | os.PathLike[str],
    *,
    label: str,
    allow_empty: bool,
) -> str:
    try:
        text = os.fspath(value)
    except TypeError as exc:
        raise TypeError(f"{label} must be a string path.") from exc
    if not isinstance(text, str):
        raise TypeError(f"{label} must be a string path.")
    if "\0" in text:
        raise ValueError(f"{label} cannot contain a null character.")
    if not allow_empty and not text:
        raise ValueError(f"{label} cannot be empty.")
    return text


def _argument_text(arguments: Sequence[str | os.PathLike[str]]) -> tuple[str, ...]:
    if isinstance(arguments, str | bytes | os.PathLike):
        raise TypeError("Process arguments must be a sequence of string values.")
    values = []
    for index, argument in enumerate(arguments):
        values.append(
            _path_text(
                argument,
                label=f"Process argument {index}",
                allow_empty=True,
            )
        )
    return tuple(values)


def _environment_block(env: Mapping[str, str] | None):
    if env is None:
        return None
    if not isinstance(env, Mapping):
        raise TypeError("The process environment must be a mapping of strings.")
    entries = []
    for key, value in env.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError("Process environment names and values must be strings.")
        if not key or "=" in key:
            raise ValueError(
                "Process environment names must be nonempty and cannot contain '='."
            )
        if "\0" in key or "\0" in value:
            raise ValueError("Process environment entries cannot contain null characters.")
        entries.append((key, value))
    entries.sort(key=lambda item: item[0].casefold())
    block_text = "\0".join(f"{key}={value}" for key, value in entries) + "\0\0"
    return (ctypes.c_wchar * len(block_text))(*block_text)


def _open_null_standard_handles() -> tuple[object, object]:
    kernel32 = _windows_kernel32()
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(_WindowsSecurityAttributes),
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    security_attributes = _WindowsSecurityAttributes(
        length=ctypes.sizeof(_WindowsSecurityAttributes),
        security_descriptor=None,
        inherit_handle=True,
    )
    standard_input = create_file(
        "NUL",
        _GENERIC_READ,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE,
        ctypes.byref(security_attributes),
        _OPEN_EXISTING,
        _FILE_ATTRIBUTE_NORMAL,
        None,
    )
    if _invalid_handle(standard_input):
        raise ProcessInspectionError(
            f"Unable to open NUL for process input: {_last_error_detail()}."
        )
    standard_output = create_file(
        "NUL",
        _GENERIC_WRITE,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE,
        ctypes.byref(security_attributes),
        _OPEN_EXISTING,
        _FILE_ATTRIBUTE_NORMAL,
        None,
    )
    if _invalid_handle(standard_output):
        _close_handle(standard_input)
        raise ProcessInspectionError(
            f"Unable to open NUL for process output: {_last_error_detail()}."
        )
    return standard_input, standard_output


def _create_handle_attribute_list(
    handles: tuple[object, ...],
) -> tuple[ctypes.c_void_p, object]:
    kernel32 = _windows_kernel32()
    initialize = kernel32.InitializeProcThreadAttributeList
    initialize.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    initialize.restype = ctypes.c_int
    size = ctypes.c_size_t()
    _set_last_error(0)
    initialize(None, 1, 0, ctypes.byref(size))
    error_number = _last_error()
    if size.value == 0 or error_number not in (0, _ERROR_INSUFFICIENT_BUFFER):
        raise ProcessInspectionError(
            "Unable to size the process handle attribute list: "
            f"{_error_detail(error_number)}."
        )
    backing = ctypes.create_string_buffer(size.value)
    attribute_list = ctypes.cast(backing, ctypes.c_void_p)
    if not initialize(attribute_list, 1, 0, ctypes.byref(size)):
        raise ProcessInspectionError(
            f"Unable to initialize the process handle attribute list: {_last_error_detail()}."
        )

    handle_array = (ctypes.c_void_p * len(handles))(*handles)
    update = kernel32.UpdateProcThreadAttribute
    update.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_size_t,
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    update.restype = ctypes.c_int
    if not update(
        attribute_list,
        0,
        _PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
        ctypes.cast(handle_array, ctypes.c_void_p),
        ctypes.sizeof(handle_array),
        None,
        None,
    ):
        _delete_attribute_list(attribute_list)
        raise ProcessInspectionError(
            f"Unable to restrict inherited process handles: {_last_error_detail()}."
        )
    return attribute_list, backing


def _startup_info(
    *,
    attribute_list: ctypes.c_void_p,
    standard_input: object,
    standard_output: object,
) -> _WindowsStartupInfoEx:
    startup_info = _WindowsStartupInfoEx()
    startup_info.startup_info.cb = ctypes.sizeof(_WindowsStartupInfoEx)
    startup_info.startup_info.flags = _STARTF_USESTDHANDLES
    startup_info.startup_info.standard_input = standard_input
    startup_info.startup_info.standard_output = standard_output
    startup_info.startup_info.standard_error = standard_output
    startup_info.attribute_list = attribute_list
    return startup_info


def _create_suspended_process(
    *,
    executable: str,
    command_buffer,
    current_directory: str | None,
    environment_pointer,
    startup_info: _WindowsStartupInfoEx,
    process_information: _WindowsProcessInformation,
) -> None:
    kernel32 = _windows_kernel32()
    create_process = kernel32.CreateProcessW
    create_process.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_wchar_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_wchar_p,
        ctypes.POINTER(_WindowsStartupInfoEx),
        ctypes.POINTER(_WindowsProcessInformation),
    ]
    create_process.restype = ctypes.c_int
    if not create_process(
        executable,
        command_buffer,
        None,
        None,
        True,
        _PROCESS_CREATION_FLAGS,
        environment_pointer,
        current_directory,
        ctypes.byref(startup_info),
        ctypes.byref(process_information),
    ):
        raise ProcessInspectionError(
            f"Unable to create suspended Windows process: {_last_error_detail()}."
        )


def _assign_process_to_job(
    *,
    job_handle: object,
    process_handle: object,
    process_id: int,
) -> None:
    kernel32 = _windows_kernel32()
    assign = kernel32.AssignProcessToJobObject
    assign.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
    assign.restype = ctypes.c_int
    if not assign(job_handle, process_handle):
        raise ProcessInspectionError(
            f"Unable to contain created process {process_id}: {_last_error_detail()}."
        )


def _resume_primary_thread(*, thread_handle: object, process_id: int) -> None:
    kernel32 = _windows_kernel32()
    resume = kernel32.ResumeThread
    resume.argtypes = [ctypes.c_void_p]
    resume.restype = ctypes.c_uint32
    previous_suspend_count = int(resume(thread_handle))
    if previous_suspend_count == _RESUME_THREAD_FAILED:
        raise ProcessInspectionError(
            f"Unable to resume contained process {process_id}: {_last_error_detail()}."
        )
    if previous_suspend_count != 1:
        raise ProcessInspectionError(
            f"Contained process {process_id} had unexpected suspend count "
            f"{previous_suspend_count}."
        )


def _duplicate_job_handle_into_child(
    *,
    job_handle: object,
    process_handle: object,
    process_id: int,
) -> None:
    kernel32 = _windows_kernel32()
    get_current_process = kernel32.GetCurrentProcess
    get_current_process.argtypes = []
    get_current_process.restype = ctypes.c_void_p
    duplicate = kernel32.DuplicateHandle
    duplicate.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_uint32,
        ctypes.c_int,
        ctypes.c_uint32,
    ]
    duplicate.restype = ctypes.c_int
    remote_handle = ctypes.c_void_p()
    current_process = get_current_process()
    if not duplicate(
        current_process,
        job_handle,
        process_handle,
        ctypes.byref(remote_handle),
        0,
        False,
        _DUPLICATE_SAME_ACCESS,
    ):
        raise ProcessInspectionError(
            f"Unable to transfer Job ownership to process {process_id}: {_last_error_detail()}."
        )
    if not remote_handle.value:
        raise ProcessInspectionError(
            f"Job ownership transfer to process {process_id} returned an invalid handle."
        )


def _terminate_failed_child(
    *,
    job,
    job_assigned: bool,
    process_handle: object,
    process_id: int,
) -> BaseException | None:
    errors: list[BaseException] = []
    if job_assigned:
        try:
            job.terminate(timeout_seconds=_CLEANUP_WAIT_MILLISECONDS / 1000)
        except BaseException as exc:
            errors.append(exc)
    try:
        _terminate_process_handle_if_active(
            process_handle=process_handle,
            process_id=process_id,
        )
    except BaseException as exc:
        errors.append(exc)
    if not errors:
        return None
    if len(errors) == 1:
        return errors[0]
    return BaseExceptionGroup(
        "Multiple failed-process cleanup operations failed.",
        errors,
    )


def _terminate_process_handle_if_active(
    *,
    process_handle: object,
    process_id: int,
) -> None:
    wait_result = _wait_for_handle(process_handle, timeout_milliseconds=0)
    if wait_result == _WAIT_OBJECT_0:
        return
    if wait_result != _WAIT_TIMEOUT:
        _raise_wait_error(wait_result, process_id=process_id)

    kernel32 = _windows_kernel32()
    terminate = kernel32.TerminateProcess
    terminate.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    terminate.restype = ctypes.c_int
    if not terminate(process_handle, 1):
        error_number = _last_error()
        if _wait_for_handle(process_handle, timeout_milliseconds=0) != _WAIT_OBJECT_0:
            raise ProcessInspectionError(
                f"Unable to terminate failed process {process_id}: "
                f"{_error_detail(error_number)}."
            )
        return
    wait_result = _wait_for_handle(
        process_handle,
        timeout_milliseconds=_CLEANUP_WAIT_MILLISECONDS,
    )
    if wait_result != _WAIT_OBJECT_0:
        _raise_wait_error(wait_result, process_id=process_id)


def _wait_for_handle(handle: object, *, timeout_milliseconds: int) -> int:
    kernel32 = _windows_kernel32()
    wait = kernel32.WaitForSingleObject
    wait.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    wait.restype = ctypes.c_uint32
    return int(wait(handle, timeout_milliseconds))


def _raise_wait_error(wait_result: int, *, process_id: int) -> None:
    if wait_result == _WAIT_TIMEOUT:
        raise ProcessInspectionError(
            f"Timed out waiting for failed process {process_id} to terminate."
        )
    if wait_result == _WAIT_FAILED:
        raise ProcessInspectionError(
            f"Unable to wait for failed process {process_id}: {_last_error_detail()}."
        )
    raise ProcessInspectionError(
        f"Unable to wait for failed process {process_id}: "
        f"unexpected wait result {wait_result}."
    )


def _delete_attribute_list(attribute_list: ctypes.c_void_p) -> None:
    kernel32 = _windows_kernel32()
    delete = kernel32.DeleteProcThreadAttributeList
    delete.argtypes = [ctypes.c_void_p]
    delete.restype = None
    delete(attribute_list)


def _close_handle(handle: object | None) -> None:
    if _invalid_handle(handle):
        return
    kernel32 = _windows_kernel32()
    close = kernel32.CloseHandle
    close.argtypes = [ctypes.c_void_p]
    close.restype = ctypes.c_int
    close(handle)


def _invalid_handle(handle: object | None) -> bool:
    value = getattr(handle, "value", handle)
    return value in (None, 0, _INVALID_HANDLE_VALUE)


def _windows_kernel32():
    try:
        return ctypes.WinDLL("kernel32", use_last_error=True)
    except (AttributeError, OSError) as exc:
        raise ProcessInspectionError(
            "Unable to load the Windows process-launch API."
        ) from exc


def _set_last_error(error_number: int) -> None:
    set_last_error = getattr(ctypes, "set_last_error", None)
    if set_last_error is not None:
        set_last_error(error_number)


def _last_error() -> int:
    get_last_error = getattr(ctypes, "get_last_error", None)
    return int(get_last_error()) if get_last_error is not None else 0


def _error_detail(error_number: int) -> str:
    format_error = getattr(ctypes, "FormatError", None)
    if error_number and format_error is not None:
        return str(format_error(error_number)).strip()
    return f"Windows error {error_number}" if error_number else "unknown error"


def _last_error_detail() -> str:
    return _error_detail(_last_error())


__all__ = [
    "WindowsContainedProcess",
    "spawn_windows_contained_process",
]
