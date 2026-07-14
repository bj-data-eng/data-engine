"""Listener loop and host serving helpers for the daemon process."""

from __future__ import annotations

from multiprocessing import AuthenticationError
from multiprocessing.connection import Listener, answer_challenge, deliver_challenge
from pathlib import Path
from contextlib import nullcontext
import os
import threading
import time
import traceback
from typing import TYPE_CHECKING

from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.client import (
    DaemonClientError,
    _DeadlineBoundConnection,
    _MAX_DAEMON_REQUEST_BYTES,
    _MAX_DAEMON_RESPONSE_BYTES,
    _decode_message,
    _encode_message,
    _remove_stale_unix_endpoint,
    daemon_authkey,
    endpoint_address,
    endpoint_family,
)
from data_engine.hosts.daemon.runtime_control import stop_active_work
from data_engine.platform.posix_watchdog import arm_posix_process_group_watchdog
from data_engine.platform.processes import (
    ProcessIdentity,
    ProcessInspectionError,
    inspect_process_identity,
    open_verified_windows_kill_on_close_job,
)
from data_engine.services import WorkspaceService

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService


_COMMAND_RECEIVE_TIMEOUT_SECONDS = 5.0
_MAX_CONNECTION_WORKERS = 32


def _require_current_process_containment(
    containment_nonce: str,
    *,
    expected_process_identity: ProcessIdentity | None = None,
) -> ProcessIdentity:
    """Verify that the current process owns its recorded containment boundary."""
    current_pid = os.getpid()
    actual_identity = inspect_process_identity(current_pid)
    if actual_identity is None:
        raise ProcessInspectionError(
            f"Unable to inspect the current daemon process identity for PID {current_pid}."
        )
    if (
        expected_process_identity is not None
        and actual_identity != expected_process_identity
    ):
        raise ProcessInspectionError(
            f"Current daemon process {current_pid} does not match its recorded identity."
        )
    if os.name == "posix":
        arm_posix_process_group_watchdog(
            containment_nonce=containment_nonce,
        )
    elif os.name == "nt":
        job = open_verified_windows_kill_on_close_job(
            actual_identity,
            nonce=containment_nonce,
        )
        job.close()
    else:
        raise ProcessInspectionError(
            f"Daemon process containment is unsupported on platform {os.name!r}."
        )
    return actual_identity


def _serve_connection(
    service: "DataEngineDaemonService",
    connection,
    *,
    authkey: bytes | None = None,
    family: str | None = None,
) -> None:
    """Handle one accepted daemon connection without blocking the listener loop."""
    shutdown_after_response = False
    try:
        with connection:
            payload = None
            command = ""
            selected_family = family or endpoint_family(service.paths)
            command_connection = _DeadlineBoundConnection(
                connection,
                deadline=time.monotonic() + _COMMAND_RECEIVE_TIMEOUT_SECONDS,
                timeout_message="Timed out waiting for a daemon command payload.",
                family=selected_family,
            )
            if authkey is not None:
                try:
                    deliver_challenge(command_connection, authkey)
                    answer_challenge(command_connection, authkey)
                except (AuthenticationError, DaemonClientError, EOFError, OSError) as exc:
                    service._debug_log(f"daemon authentication failed: {exc!r}")
                    return
            try:
                payload = _decode_message(
                    command_connection.recv_bytes(_MAX_DAEMON_REQUEST_BYTES)
                )
                request_id = (
                    str(payload.get("request_id", "")).strip()
                    if isinstance(payload, dict)
                    else ""
                )
                command = (
                    str(payload.get("command", "")).strip()
                    if isinstance(payload, dict)
                    else ""
                )
                timed_context = getattr(service, "_timed_operation", None)
                context = (
                    timed_context(
                        "daemon.ipc",
                        command or "unknown",
                        fields={"request_id": request_id or None},
                    )
                    if callable(timed_context)
                    else nullcontext()
                )
                with context:
                    response = service._handle_command(payload)
                shutdown_after_response = (
                    command == "shutdown_daemon"
                    and response.get("ok") is True
                    and response.get("draining") is False
                )
            except Exception as exc:  # pragma: no cover - defensive daemon boundary
                service._debug_log(f"command handling error: {exc!r}")
                response = {"ok": False, "error": str(exc)}
            try:
                response_connection = _DeadlineBoundConnection(
                    connection,
                    deadline=time.monotonic() + _COMMAND_RECEIVE_TIMEOUT_SECONDS,
                    timeout_message="Timed out sending a daemon command response.",
                    family=selected_family,
                )
                encoded_response = _encode_message(response)
                if len(encoded_response) > _MAX_DAEMON_RESPONSE_BYTES:
                    encoded_response = _encode_message(
                        {
                            "ok": False,
                            "error": "Daemon response exceeds the transport limit.",
                        }
                    )
                response_connection.send_bytes(encoded_response)
            except (BrokenPipeError, DaemonClientError, EOFError, OSError) as exc:
                service._debug_log(
                    f"connection closed before response could be delivered: {exc!r}"
                )
    finally:
        if shutdown_after_response:
            service.host.shutdown_event.set()
            try:
                threading.Thread(
                    target=service._wake_listener,
                    daemon=True,
                ).start()
            except Exception:
                service._wake_listener()


def serve_forever(service: "DataEngineDaemonService") -> None:
    """Run the workspace daemon listener loop until shutdown."""
    _require_current_process_containment(
        service.containment_nonce,
        expected_process_identity=service.process_identity,
    )
    worker_threads: set[threading.Thread] = set()
    active_connections: dict[int, object] = {}
    connection_lock = threading.Lock()
    connection_worker_slots = threading.BoundedSemaphore(_MAX_CONNECTION_WORKERS)
    listener_family = endpoint_family(service.paths)
    listener_authkey: bytes | None = None

    def _serve_tracked_connection(connection) -> None:
        try:
            _serve_connection(
                service,
                connection,
                authkey=listener_authkey,
                family=listener_family,
            )
        finally:
            with connection_lock:
                active_connections.pop(id(connection), None)
            connection_worker_slots.release()

    try:
        service.initialize()
        service.state.checkpoint_thread = threading.Thread(target=service._checkpoint_loop, daemon=True)
        service.state.checkpoint_thread.start()
        _remove_stale_unix_endpoint(service.paths)
        listener_authkey = daemon_authkey(service.paths)
        listener = Listener(
            endpoint_address(service.paths),
            family=listener_family,
        )
        service.host.listener = listener
        service._debug_log(f"listener ready endpoint={service.paths.daemon_endpoint_path}")
        while not service.host.shutdown_event.is_set():
            try:
                connection = listener.accept()
            except (AuthenticationError, OSError, EOFError):
                if service.host.shutdown_event.is_set():
                    break
                service._debug_log("listener accept failed but daemon remains alive")
                continue
            if service.host.shutdown_event.is_set():
                _close_connection(connection)
                break
            if not connection_worker_slots.acquire(blocking=False):
                service._debug_log(
                    "connection rejected because the daemon IPC worker limit is full"
                )
                _close_connection(connection)
                continue
            thread = threading.Thread(target=_serve_tracked_connection, args=(connection,), daemon=True)
            with connection_lock:
                active_connections[id(connection)] = connection
            worker_threads.add(thread)
            try:
                thread.start()
            except Exception:
                worker_threads.discard(thread)
                with connection_lock:
                    active_connections.pop(id(connection), None)
                _close_connection(connection)
                connection_worker_slots.release()
                raise
            worker_threads = {worker for worker in worker_threads if worker.is_alive()}
    except Exception as exc:
        service._debug_log(f"serve_forever fatal error: {exc!r}")
        service._debug_log(traceback.format_exc().rstrip())
        raise
    finally:
        service.host.shutdown_event.set()
        if service.host.listener is not None:
            try:
                service.host.listener.close()
            except Exception:
                pass
        with connection_lock:
            connections = tuple(active_connections.values())
        for connection in connections:
            _close_connection(connection)
        stop_active_work(service)
        current_thread = threading.current_thread()
        for thread in list(worker_threads):
            if thread is not current_thread:
                thread.join()
        service._shutdown()


def _close_connection(connection) -> None:
    close = getattr(connection, "close", None)
    if not callable(close):
        return
    try:
        close()
    except (EOFError, OSError):
        pass


def serve_workspace_daemon(
    service_type: type["DataEngineDaemonService"],
    *,
    workspace_root: Path | None = None,
    workspace_id: str | None = None,
    containment_nonce: str,
    lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT,
    workspace_service: WorkspaceService | None = None,
    resolve_paths_func=None,
) -> int:
    """Start serving one workspace daemon in the current process."""
    _require_current_process_containment(containment_nonce)
    if resolve_paths_func is None:
        workspace_service = workspace_service or WorkspaceService()
        resolve_paths_func = workspace_service.resolve_paths
    paths = resolve_paths_func(workspace_root=workspace_root, workspace_id=workspace_id)
    service = service_type(
        paths,
        containment_nonce=containment_nonce,
        lifecycle_policy=lifecycle_policy,
    )
    service.serve_forever()
    return 0


__all__ = ["serve_forever", "serve_workspace_daemon"]
