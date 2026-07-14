from __future__ import annotations

from types import SimpleNamespace

import pytest

import data_engine.hosts.daemon.client as daemon_client
from data_engine.hosts.daemon.client import DaemonClientError


_ERROR_FILE_NOT_FOUND = 2
_ERROR_SEM_TIMEOUT = 121
_ERROR_PIPE_BUSY = 231


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

        def recv_bytes(self) -> bytes:
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


def test_daemon_request_preserves_unix_response_only_timeout(monkeypatch) -> None:
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

        def recv_bytes(self) -> bytes:
            return b'{"ok":true}'

        def send_bytes(self, _payload: bytes) -> None:
            current_time["value"] = 30.75

    def _client(*_args, **_kwargs):
        current_time["value"] = 30.5
        return _Connection()

    monkeypatch.setattr(daemon_client, "endpoint_family", lambda _paths: "AF_UNIX")
    monkeypatch.setattr(daemon_client, "endpoint_address", lambda _paths: "daemon.sock")
    monkeypatch.setattr(daemon_client, "daemon_authkey", lambda _paths: b"secret")
    monkeypatch.setattr(daemon_client.time, "monotonic", lambda: current_time["value"])
    monkeypatch.setattr(daemon_client, "Client", _client)

    response = daemon_client.daemon_request(object(), {"command": "daemon_ping"}, timeout=1.0)

    assert response == {"ok": True}
    assert polls == [pytest.approx(1.0)]
