from __future__ import annotations

import json
import os
from pathlib import Path
import select
import secrets
import signal
import socket
import subprocess
import sys
import time

import pytest

import data_engine.platform.posix_watchdog as posix_watchdog
from data_engine.platform.posix_watchdog import (
    PosixProcessGroupWatchdogError,
    arm_posix_process_group_watchdog,
    request_posix_process_group_termination,
)


_NONCE = "ab" * 32
_OTHER_NONCE = "cd" * 32


_HARNESS_SOURCE = r"""
import importlib.util
import json
import os
import subprocess
import sys
import time

module_path, mode, ignored_signal, containment_nonce = sys.argv[1:]
spec = importlib.util.spec_from_file_location("_data_engine_posix_watchdog", module_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
watchdog = module.arm_posix_process_group_watchdog(containment_nonce=containment_nonce)

ready_read_fd, ready_write_fd = os.pipe()
child_source = (
    "import os, signal, sys, time; "
    "signal.signal(int(sys.argv[2]), signal.SIG_IGN); "
    "os.write(int(sys.argv[1]), b'R'); "
    "os.close(int(sys.argv[1])); "
    "time.sleep(60)"
)
child = subprocess.Popen(
    [
        sys.executable,
        "-I",
        "-S",
        "-c",
        child_source,
        str(ready_write_fd),
        ignored_signal,
    ],
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    close_fds=True,
    pass_fds=(ready_write_fd,),
)
os.close(ready_write_fd)
if os.read(ready_read_fd, 1) != b"R":
    raise RuntimeError("contained child failed to start")
os.close(ready_read_fd)
print(
    json.dumps(
        {
            "leader_pid": os.getpid(),
            "watchdog_pid": watchdog.pid,
            "child_pid": child.pid,
            "monitor_kind": watchdog.monitor_kind,
            "control_endpoint": str(watchdog.control_endpoint),
        }
    ),
    flush=True,
)
if mode == "exit":
    os._exit(0)
while True:
    time.sleep(60)
"""


def _watchdog_module_path() -> Path:
    return Path(posix_watchdog.__file__).resolve()


def _launch_harness(
    mode: str,
    *,
    ignored_signal: signal.Signals = signal.SIGTERM,
) -> tuple[subprocess.Popen[str], dict[str, object]]:
    containment_nonce = secrets.token_hex(32)
    process = subprocess.Popen(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            _HARNESS_SOURCE,
            str(_watchdog_module_path()),
            mode,
            str(int(ignored_signal)),
            containment_nonce,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        close_fds=True,
        start_new_session=True,
    )
    assert process.stdout is not None
    readable, _, _ = select.select([process.stdout], [], [], 5.0)
    if not readable:
        process.kill()
        stdout, stderr = process.communicate(timeout=2.0)
        pytest.fail(f"watchdog harness did not start: stdout={stdout!r}, stderr={stderr!r}")
    line = process.stdout.readline()
    if not line:
        stdout, stderr = process.communicate(timeout=2.0)
        pytest.fail(f"watchdog harness exited early: stdout={stdout!r}, stderr={stderr!r}")
    metadata = json.loads(line)
    metadata["containment_nonce"] = containment_nonce
    return process, metadata


def _process_group_exists(group_id: int) -> bool:
    try:
        os.killpg(group_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_for_process_group_exit(group_id: int, *, timeout_seconds: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not _process_group_exists(group_id):
            return True
        time.sleep(0.01)
    return not _process_group_exists(group_id)


def _cleanup_harness(
    process: subprocess.Popen[str],
    metadata: dict[str, object],
) -> None:
    if process.poll() is None:
        try:
            if os.getpgid(process.pid) == process.pid and os.getsid(process.pid) == process.pid:
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except (ProcessLookupError, PermissionError):
            pass
        try:
            process.communicate(timeout=1.0)
        except subprocess.TimeoutExpired:
            pass
    group_id = int(metadata["leader_pid"])
    for field_name in ("watchdog_pid", "child_pid"):
        pid = int(metadata[field_name])
        try:
            if os.getpgid(pid) == group_id:
                os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
def test_watchdog_removes_same_group_child_after_clean_leader_exit():
    process, metadata = _launch_harness("exit")
    try:
        assert int(metadata["leader_pid"]) == process.pid
        assert metadata["monitor_kind"] in {"pidfd", "kqueue", "parent-poll"}
        assert process.wait(timeout=2.0) == 0
        assert _wait_for_process_group_exit(process.pid)
    finally:
        _cleanup_harness(process, metadata)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
def test_watchdog_removes_same_group_child_after_leader_is_killed():
    process, metadata = _launch_harness("wait")
    try:
        os.kill(process.pid, signal.SIGKILL)
        assert process.wait(timeout=2.0) == -signal.SIGKILL
        assert _wait_for_process_group_exit(process.pid)
    finally:
        _cleanup_harness(process, metadata)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
@pytest.mark.parametrize(
    "group_signal",
    [
        signal.SIGTERM,
        pytest.param(
            getattr(signal, "SIGTRAP", signal.SIGTERM),
            id="sigtrap",
        ),
    ],
)
def test_watchdog_survives_group_signal_and_escalates_ignored_descendant(
    group_signal,
):
    process, metadata = _launch_harness("wait", ignored_signal=group_signal)
    try:
        os.killpg(process.pid, group_signal)
        assert process.wait(timeout=2.0) == -group_signal
        assert _wait_for_process_group_exit(process.pid)
    finally:
        _cleanup_harness(process, metadata)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
def test_daemon_supervisor_fails_group_closed_if_watchdog_is_killed():
    process, metadata = _launch_harness("wait")
    try:
        os.kill(int(metadata["watchdog_pid"]), signal.SIGKILL)
        assert process.wait(timeout=2.0) == -signal.SIGKILL
        assert _wait_for_process_group_exit(process.pid)
    finally:
        _cleanup_harness(process, metadata)


@pytest.mark.skipif(os.name != "posix", reason="POSIX process groups are required")
def test_watchdog_control_request_authenticates_then_kills_its_pinned_group():
    process, metadata = _launch_harness("wait")
    endpoint = str(metadata["control_endpoint"])
    try:
        wrong_payload = b"terminate:" + f"{_OTHER_NONCE}:{process.pid}".encode("ascii")
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as control_socket:
            assert control_socket.sendto(wrong_payload, endpoint) == len(wrong_payload)
        time.sleep(0.05)
        assert process.poll() is None
        os.kill(int(metadata["child_pid"]), 0)

        request_posix_process_group_termination(
            containment_nonce=str(metadata["containment_nonce"]),
            expected_parent_pid=process.pid,
        )

        assert process.wait(timeout=2.0) == -signal.SIGKILL
        assert _wait_for_process_group_exit(process.pid)
        assert Path(endpoint).exists() is False
    finally:
        _cleanup_harness(process, metadata)


def test_watchdog_refuses_a_nonisolated_parent(monkeypatch):
    monkeypatch.setattr(posix_watchdog.os, "name", "posix")
    monkeypatch.setattr(posix_watchdog.os, "getpid", lambda: 101)
    monkeypatch.setattr(posix_watchdog.os, "getpgrp", lambda: 202)
    monkeypatch.setattr(posix_watchdog.os, "getsid", lambda pid: 202)

    with pytest.raises(PosixProcessGroupWatchdogError, match="dedicated process group"):
        arm_posix_process_group_watchdog(containment_nonce=_NONCE)


def test_watchdog_rejects_rearm_with_a_different_nonce(monkeypatch):
    class _ArmedWatchdog:
        is_running = True
        control_endpoint = posix_watchdog.posix_watchdog_endpoint(_NONCE)

    monkeypatch.setattr(posix_watchdog, "_ARMED_WATCHDOG", _ArmedWatchdog())

    with pytest.raises(PosixProcessGroupWatchdogError, match="different containment nonce"):
        arm_posix_process_group_watchdog(containment_nonce=_OTHER_NONCE)


def test_adopt_watchdog_verifies_direct_child_and_restarts_supervision(monkeypatch):
    events = []
    monkeypatch.setattr(posix_watchdog, "_ARMED_WATCHDOG", None)
    monkeypatch.setattr(posix_watchdog.os, "name", "posix")
    monkeypatch.setattr(posix_watchdog.os, "getpid", lambda: 101)
    monkeypatch.setattr(posix_watchdog.os, "getpgrp", lambda: 101)
    monkeypatch.setattr(posix_watchdog.os, "getsid", lambda pid: 101)
    monkeypatch.setattr(posix_watchdog.os, "getpgid", lambda pid: 101)
    monkeypatch.setattr(posix_watchdog.os, "waitpid", lambda pid, flags: (0, 0))
    monkeypatch.setattr(
        posix_watchdog,
        "_verify_private_control_endpoint",
        lambda endpoint: events.append(("endpoint", endpoint)),
    )
    monkeypatch.setattr(
        posix_watchdog.PosixProcessGroupWatchdog,
        "_start_adopted_supervisor",
        lambda self, **kwargs: events.append(("supervise", self.pid, kwargs)),
    )

    adopted = posix_watchdog.adopt_posix_process_group_watchdog(
        202,
        containment_nonce=_NONCE,
    )

    assert adopted.pid == 202
    assert adopted.is_running is True
    assert posix_watchdog._ARMED_WATCHDOG is adopted
    assert events[0] == ("endpoint", posix_watchdog.posix_watchdog_endpoint(_NONCE))
    assert events[1][0:2] == ("supervise", 202)
    assert events[1][2]["parent_pid"] == 101


def test_control_request_never_blocks_on_a_full_watchdog_queue(monkeypatch):
    events = []

    class _FullSocket:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            del exc_type, exc, traceback

        def setblocking(self, value):
            events.append(("setblocking", value))

        def sendto(self, payload, endpoint):
            events.append(("sendto", payload, endpoint))
            raise BlockingIOError("watchdog queue is full")

    monkeypatch.setattr(
        posix_watchdog.socket,
        "socket",
        lambda *args, **kwargs: _FullSocket(),
    )

    with pytest.raises(PosixProcessGroupWatchdogError, match="Unable to request termination"):
        request_posix_process_group_termination(
            containment_nonce=_NONCE,
            expected_parent_pid=321,
        )

    assert events[0] == ("setblocking", False)


def test_failed_duplicate_bind_never_unlinks_the_live_owner_endpoint(monkeypatch):
    containment_nonce = secrets.token_hex(32)
    endpoint = posix_watchdog.posix_watchdog_endpoint(containment_nonce)
    endpoint.unlink(missing_ok=True)
    owner_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    owner_socket.bind(str(endpoint))
    endpoint.chmod(0o600)
    monkeypatch.setattr(posix_watchdog, "_block_group_termination_signals", lambda: None)
    monkeypatch.setattr(posix_watchdog.os, "getpgrp", lambda: 101)
    monkeypatch.setattr(posix_watchdog.os, "getsid", lambda pid: 101)
    try:
        result = posix_watchdog._watch_parent(
            parent_pid=101,
            group_id=101,
            session_id=101,
            ready_fd=-1,
            containment_nonce=containment_nonce,
        )

        assert result == 70
        assert endpoint.is_socket()
    finally:
        owner_socket.close()
        endpoint.unlink(missing_ok=True)


def test_watchdog_launch_failure_closes_both_readiness_pipe_ends(monkeypatch):
    closed_fds = []
    monkeypatch.setattr(posix_watchdog.os, "name", "posix")
    monkeypatch.setattr(posix_watchdog.os, "getpid", lambda: 101)
    monkeypatch.setattr(posix_watchdog.os, "getpgrp", lambda: 101)
    monkeypatch.setattr(posix_watchdog.os, "getsid", lambda pid: 101)
    monkeypatch.setattr(posix_watchdog.os, "pipe", lambda: (41, 42))
    monkeypatch.setattr(posix_watchdog.os, "close", closed_fds.append)
    monkeypatch.setattr(
        posix_watchdog.subprocess,
        "Popen",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("launch failed")),
    )

    with pytest.raises(PosixProcessGroupWatchdogError, match="Unable to launch"):
        arm_posix_process_group_watchdog(containment_nonce=_NONCE)

    assert closed_fds == [41, 42]


@pytest.mark.parametrize("timeout", [-1, float("inf"), True, "1"])
def test_watchdog_rejects_invalid_ready_timeout(timeout):
    with pytest.raises(ValueError, match="finite nonnegative"):
        arm_posix_process_group_watchdog(
            containment_nonce=_NONCE,
            ready_timeout_seconds=timeout,
        )
