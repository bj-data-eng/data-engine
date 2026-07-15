from __future__ import annotations

import os
import socket
import struct
import time
from types import SimpleNamespace

import pytest

import data_engine.hosts.daemon.client as daemon_client
from data_engine.hosts.daemon.client import DaemonClientError


_ERROR_FILE_NOT_FOUND = 2
_ERROR_SEM_TIMEOUT = 121
_ERROR_PIPE_BUSY = 231


class _SocketConnection:
    def __init__(self, selected_socket: socket.socket) -> None:
        self._socket = selected_socket

    def fileno(self) -> int:
        return self._socket.fileno()


def _windows_error(code: int) -> OSError:
    error = OSError(code, "simulated Windows error")
    error.winerror = code
    return error


def _fake_winapi(*, create_file, wait_named_pipe):
    return SimpleNamespace(
        ERROR_PIPE_BUSY=_ERROR_PIPE_BUSY,
        ERROR_SEM_TIMEOUT=_ERROR_SEM_TIMEOUT,
        FILE_FLAG_OVERLAPPED=0x40000000,
        GENERIC_READ=0x80000000,
        GENERIC_WRITE=0x40000000,
        NULL=0,
        OPEN_EXISTING=3,
        PIPE_READMODE_MESSAGE=2,
        CloseHandle=lambda _handle: None,
        CreateFile=create_file,
        SetNamedPipeHandleState=lambda *_args: None,
        WaitNamedPipe=wait_named_pipe,
    )


def test_windows_pipe_connect_reports_unavailable_pipe_without_waiting() -> None:
    waits: list[int] = []

    def _create_file(*_args):
        raise _windows_error(_ERROR_FILE_NOT_FOUND)

    winapi = _fake_winapi(
        create_file=_create_file,
        wait_named_pipe=lambda _address, milliseconds: waits.append(milliseconds),
    )

    with pytest.raises(OSError) as exc_info:
        daemon_client._connect_windows_pipe(  # noqa: SLF001 - platform-neutral transport boundary test
            r"\\.\pipe\missing",
            deadline=1.0,
            clock=lambda: 0.0,
            winapi=winapi,
            connection_type=lambda handle: handle,
        )

    assert exc_info.value.winerror == _ERROR_FILE_NOT_FOUND
    assert waits == []


def test_windows_pipe_connect_bounds_busy_pipe_wait_by_remaining_deadline() -> None:
    current_time = {"value": 10.0}
    waits: list[int] = []

    def _create_file(*_args):
        raise _windows_error(_ERROR_PIPE_BUSY)

    def _wait_named_pipe(_address, milliseconds: int) -> None:
        waits.append(milliseconds)
        current_time["value"] += milliseconds / 1000
        raise _windows_error(_ERROR_SEM_TIMEOUT)

    winapi = _fake_winapi(create_file=_create_file, wait_named_pipe=_wait_named_pipe)

    with pytest.raises(DaemonClientError, match="Timed out connecting"):
        daemon_client._connect_windows_pipe(  # noqa: SLF001 - platform-neutral transport boundary test
            r"\\.\pipe\busy",
            deadline=10.25,
            clock=lambda: current_time["value"],
            winapi=winapi,
            connection_type=lambda handle: handle,
        )

    assert waits == [250]
    assert current_time["value"] == pytest.approx(10.25)


def test_deadline_bound_connection_polls_only_for_remaining_time() -> None:
    polls: list[float | None] = []
    connection = SimpleNamespace(
        poll=lambda timeout: polls.append(timeout) or True,
        recv_bytes=lambda maxlength: b"ready" if maxlength == 256 else b"unexpected",
        send_bytes=lambda _payload: None,
    )
    bounded = daemon_client._DeadlineBoundConnection(  # noqa: SLF001 - authentication deadline test
        connection,
        deadline=5.0,
        timeout_message="expired",
        clock=lambda: 4.625,
    )

    assert bounded.recv_bytes(256) == b"ready"
    assert polls == [pytest.approx(0.375)]


@pytest.mark.parametrize(
    "partial_frame",
    (
        b"\x00",
        struct.pack("!i", 8) + b"partial",
    ),
    ids=("partial-header", "partial-payload"),
)
@pytest.mark.skipif(os.name != "posix", reason="POSIX descriptor I/O is required")
def test_deadline_bound_unix_receive_times_out_during_partial_frame(
    partial_frame: bytes,
) -> None:
    receiving_socket, sending_socket = socket.socketpair()
    try:
        sending_socket.sendall(partial_frame)
        bounded = daemon_client._DeadlineBoundConnection(  # noqa: SLF001 - native framing deadline regression
            _SocketConnection(receiving_socket),
            deadline=time.monotonic() + 0.05,
            timeout_message="expired",
            family="AF_UNIX",
        )

        started = time.monotonic()
        with pytest.raises(DaemonClientError, match="expired"):
            bounded.recv_bytes()

        assert time.monotonic() - started < 1.0
    finally:
        receiving_socket.close()
        sending_socket.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX descriptor I/O is required")
def test_deadline_bound_unix_send_times_out_when_peer_stops_reading() -> None:
    sending_socket, receiving_socket = socket.socketpair()
    try:
        sending_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        bounded = daemon_client._DeadlineBoundConnection(  # noqa: SLF001 - native framed-write deadline regression
            _SocketConnection(sending_socket),
            deadline=time.monotonic() + 0.05,
            timeout_message="expired",
            family="AF_UNIX",
        )

        started = time.monotonic()
        with pytest.raises(DaemonClientError, match="expired"):
            bounded.send_bytes(b"x" * 1_000_000)

        assert time.monotonic() - started < 1.0
    finally:
        sending_socket.close()
        receiving_socket.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX descriptor I/O is required")
def test_deadline_bound_unix_rejects_oversized_frame_from_header() -> None:
    receiving_socket, sending_socket = socket.socketpair()
    try:
        sending_socket.sendall(struct.pack("!i", 1025))
        bounded = daemon_client._DeadlineBoundConnection(  # noqa: SLF001 - allocation-boundary regression
            _SocketConnection(receiving_socket),
            deadline=time.monotonic() + 1.0,
            timeout_message="expired",
            family="AF_UNIX",
        )

        with pytest.raises(OSError, match="exceeds the allowed length"):
            bounded.recv_bytes(1024)
    finally:
        receiving_socket.close()
        sending_socket.close()


@pytest.mark.skipif(not hasattr(daemon_client.select, "poll"), reason="poll is unavailable")
@pytest.mark.skipif(os.name != "posix", reason="POSIX descriptor I/O is required")
def test_deadline_bound_unix_uses_high_descriptor_safe_poll(monkeypatch) -> None:
    receiving_socket, sending_socket = socket.socketpair()
    try:
        sending_socket.sendall(struct.pack("!i", 5) + b"ready")
        monkeypatch.setattr(
            daemon_client.select,
            "select",
            lambda *_args, **_kwargs: pytest.fail("select() must not gate daemon sockets"),
        )
        bounded = daemon_client._DeadlineBoundConnection(  # noqa: SLF001 - descriptor-pressure regression
            _SocketConnection(receiving_socket),
            deadline=time.monotonic() + 1.0,
            timeout_message="expired",
            family="AF_UNIX",
        )

        assert bounded.recv_bytes() == b"ready"
    finally:
        receiving_socket.close()
        sending_socket.close()


@pytest.mark.parametrize("operation", ("read", "write"))
def test_deadline_bound_windows_pipe_cancels_pending_io_at_deadline(
    operation: str,
) -> None:
    events: list[object] = []

    class _Overlapped:
        event = object()

        def cancel(self) -> None:
            events.append("cancel")

        def GetOverlappedResult(self, wait: bool):
            events.append(("result", wait))
            return 0, 995

        def getbuffer(self) -> bytes:
            return b""

    overlapped = _Overlapped()
    winapi = SimpleNamespace(
        ERROR_BROKEN_PIPE=109,
        ERROR_IO_PENDING=997,
        ERROR_MORE_DATA=234,
        ERROR_OPERATION_ABORTED=995,
        INFINITE=0xFFFF_FFFF,
        ReadFile=lambda *_args, **_kwargs: (overlapped, 997),
        WaitForMultipleObjects=lambda handles, wait_all, milliseconds: (
            events.append(("wait", handles, wait_all, milliseconds)) or 258
        ),
        WriteFile=lambda *_args, **_kwargs: (overlapped, 997),
    )
    bounded = daemon_client._DeadlineBoundConnection(  # noqa: SLF001 - platform-neutral overlapped-I/O regression
        SimpleNamespace(fileno=lambda: 42),
        deadline=1.0,
        timeout_message="expired",
        family="AF_PIPE",
        clock=lambda: 0.5,
        winapi=winapi,
    )

    with pytest.raises(DaemonClientError, match="expired"):
        if operation == "read":
            bounded.recv_bytes()
        else:
            bounded.send_bytes(b"payload")

    assert events[0][0] == "wait"
    assert events[0][3] == 500
    assert events[1:] == ["cancel", ("result", True)]


@pytest.mark.parametrize("operation", ("read", "write"))
def test_deadline_bound_windows_pipe_cancels_when_deadline_expires_after_issue(
    operation: str,
) -> None:
    events: list[object] = []
    clock_values = iter((0.5, 1.0))

    class _Overlapped:
        event = object()

        def cancel(self) -> None:
            events.append("cancel")

        def GetOverlappedResult(self, wait: bool):
            events.append(("result", wait))
            return 0, 995

        def getbuffer(self) -> bytes:
            return b""

    overlapped = _Overlapped()
    winapi = SimpleNamespace(
        ERROR_BROKEN_PIPE=109,
        ERROR_IO_PENDING=997,
        ERROR_MORE_DATA=234,
        ERROR_OPERATION_ABORTED=995,
        INFINITE=0xFFFF_FFFF,
        ReadFile=lambda *_args, **_kwargs: (overlapped, 997),
        WaitForMultipleObjects=lambda *_args: pytest.fail(
            "expired pending I/O must be cancelled before waiting"
        ),
        WriteFile=lambda *_args, **_kwargs: (overlapped, 997),
    )
    bounded = daemon_client._DeadlineBoundConnection(  # noqa: SLF001 - overlapped cancellation race regression
        SimpleNamespace(fileno=lambda: 42),
        deadline=1.0,
        timeout_message="expired",
        family="AF_PIPE",
        clock=lambda: next(clock_values),
        winapi=winapi,
    )

    with pytest.raises(DaemonClientError, match="expired"):
        if operation == "read":
            bounded.recv_bytes()
        else:
            bounded.send_bytes(b"payload")

    assert events == ["cancel", ("result", True)]


def test_windows_pipe_client_uses_standard_authentication_order_and_closes_on_failure(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []
    connection = SimpleNamespace(close=lambda: calls.append(("close", None)))
    monkeypatch.setattr(daemon_client, "_connect_windows_pipe", lambda address, deadline: connection)

    def _answer_challenge(bounded, authkey) -> None:
        calls.append(("answer", authkey))
        assert bounded._connection is connection  # noqa: SLF001 - verify the handshake wraps the opened pipe

    def _deliver_challenge(_bounded, authkey) -> None:
        calls.append(("deliver", authkey))
        raise OSError("server left")

    monkeypatch.setattr(daemon_client, "answer_challenge", _answer_challenge)
    monkeypatch.setattr(daemon_client, "deliver_challenge", _deliver_challenge)

    with pytest.raises(OSError, match="server left"):
        daemon_client._windows_pipe_client(  # noqa: SLF001 - standard authentication integration test
            r"\\.\pipe\daemon",
            authkey=b"secret",
            deadline=float("inf"),
        )

    assert calls == [("answer", b"secret"), ("deliver", b"secret"), ("close", None)]


def test_unix_socket_connect_uses_only_the_remaining_deadline() -> None:
    calls: list[tuple[str, object]] = []
    connection = object()
    socket_family = 1

    class _Socket:
        def close(self) -> None:
            calls.append(("close", None))

        def connect(self, address: str) -> None:
            calls.append(("connect", address))

        def detach(self) -> int:
            calls.append(("detach", None))
            return 42

        def fileno(self) -> int:
            return 42

        def setblocking(self, value: bool) -> None:
            calls.append(("blocking", value))

        def settimeout(self, value: float | None) -> None:
            calls.append(("timeout", value))

    def _socket_factory(family: int, kind: int) -> _Socket:
        calls.append(("socket", (family, kind)))
        return _Socket()

    def _connection_type(handle: int):
        calls.append(("connection", handle))
        return connection

    result = daemon_client._connect_unix_socket(  # noqa: SLF001 - transport deadline test
        "daemon.sock",
        deadline=10.25,
        clock=lambda: 10.0,
        socket_factory=_socket_factory,
        connection_type=_connection_type,
        socket_family=socket_family,
    )

    assert result is connection
    assert calls == [
        ("socket", (socket_family, socket.SOCK_STREAM)),
        ("timeout", pytest.approx(0.25)),
        ("connect", "daemon.sock"),
        ("blocking", True),
        ("connection", 42),
        ("detach", None),
        ("close", None),
    ]


def test_unix_socket_connect_closes_socket_after_timeout() -> None:
    closed: list[bool] = []

    class _Socket:
        def close(self) -> None:
            closed.append(True)

        def connect(self, _address: str) -> None:
            raise TimeoutError

        def settimeout(self, value: float | None) -> None:
            assert value == pytest.approx(0.25)

    with pytest.raises(DaemonClientError, match="Timed out connecting"):
        daemon_client._connect_unix_socket(  # noqa: SLF001 - transport timeout test
            "daemon.sock",
            deadline=10.25,
            clock=lambda: 10.0,
            socket_factory=lambda _family, _kind: _Socket(),
            connection_type=lambda handle: handle,
            socket_family=1,
        )

    assert closed == [True]


def test_unix_socket_client_closes_connection_when_authentication_stalls(monkeypatch) -> None:
    polls: list[float | None] = []
    closed: list[bool] = []
    connection = SimpleNamespace(
        close=lambda: closed.append(True),
        poll=lambda timeout: polls.append(timeout) or False,
    )
    monkeypatch.setattr(daemon_client, "_connect_unix_socket", lambda address, deadline: connection)

    with pytest.raises(DaemonClientError, match="Timed out connecting"):
        daemon_client._unix_socket_client(  # noqa: SLF001 - authentication deadline test
            "daemon.sock",
            authkey=b"secret",
            deadline=daemon_client.time.monotonic() + 1.0,
        )

    assert len(polls) == 1
    assert polls[0] is not None
    assert 0 < polls[0] <= 1.0
    assert closed == [True]


def test_daemon_request_subtracts_windows_connection_time_from_response_deadline(monkeypatch) -> None:
    current_time = {"value": 20.0}
    polls: list[float | None] = []

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def poll(self, timeout) -> bool:
            polls.append(timeout)
            return True

        def recv_bytes(self, maxlength=None) -> bytes:
            assert maxlength == daemon_client._MAX_DAEMON_RESPONSE_BYTES  # noqa: SLF001
            return b'{"ok":true}'

        def send_bytes(self, _payload: bytes) -> None:
            return None

    connection = _Connection()
    monkeypatch.setattr(daemon_client, "endpoint_family", lambda _paths: "AF_PIPE")
    monkeypatch.setattr(daemon_client, "endpoint_address", lambda _paths: r"\\.\pipe\daemon")
    monkeypatch.setattr(daemon_client, "daemon_authkey", lambda _paths: b"secret")
    monkeypatch.setattr(daemon_client.time, "monotonic", lambda: current_time["value"])

    def _windows_pipe_client(_address, *, authkey, deadline):
        assert authkey == b"secret"
        assert deadline == pytest.approx(21.0)
        current_time["value"] = 20.6
        return connection

    monkeypatch.setattr(daemon_client, "_windows_pipe_client", _windows_pipe_client)

    response = daemon_client.daemon_request(object(), {"command": "daemon_ping"}, timeout=1.0)

    assert response == {"ok": True}
    assert polls == [pytest.approx(0.4)]


def test_daemon_request_subtracts_unix_connection_time_from_response_deadline(monkeypatch) -> None:
    current_time = {"value": 30.0}
    polls: list[float | None] = []

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def poll(self, timeout) -> bool:
            polls.append(timeout)
            return True

        def recv_bytes(self, maxlength=None) -> bytes:
            assert maxlength == daemon_client._MAX_DAEMON_RESPONSE_BYTES  # noqa: SLF001
            return b'{"ok":true}'

        def send_bytes(self, _payload: bytes) -> None:
            current_time["value"] = 30.75

    def _unix_socket_client(_address, *, authkey, deadline):
        assert authkey == b"secret"
        assert deadline == pytest.approx(31.0)
        current_time["value"] = 30.5
        return _Connection()

    monkeypatch.setattr(daemon_client, "endpoint_family", lambda _paths: "AF_UNIX")
    monkeypatch.setattr(daemon_client, "endpoint_address", lambda _paths: "daemon.sock")
    monkeypatch.setattr(daemon_client, "daemon_authkey", lambda _paths: b"secret")
    monkeypatch.setattr(daemon_client.time, "monotonic", lambda: current_time["value"])
    monkeypatch.setattr(daemon_client, "_unix_socket_client", _unix_socket_client)

    response = daemon_client.daemon_request(object(), {"command": "daemon_ping"}, timeout=1.0)

    assert response == {"ok": True}
    assert polls == [pytest.approx(0.25)]
