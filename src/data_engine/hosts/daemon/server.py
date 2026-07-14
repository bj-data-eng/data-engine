"""Listener loop and host serving helpers for the daemon process."""

from __future__ import annotations

from multiprocessing import AuthenticationError
from multiprocessing.connection import Listener
from pathlib import Path
from contextlib import nullcontext
import threading
import traceback
from typing import TYPE_CHECKING

from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.client import (
    _decode_message,
    _encode_message,
    _remove_stale_unix_endpoint,
    daemon_authkey,
    endpoint_address,
    endpoint_family,
)
from data_engine.hosts.daemon.runtime_control import stop_active_work
from data_engine.services import WorkspaceService

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService


_COMMAND_RECEIVE_TIMEOUT_SECONDS = 5.0


def _serve_connection(service: "DataEngineDaemonService", connection) -> None:
    """Handle one accepted daemon connection without blocking the listener loop."""
    with connection:
        payload = None
        try:
            poll = getattr(connection, "poll", None)
            if callable(poll) and not poll(_COMMAND_RECEIVE_TIMEOUT_SECONDS):
                raise TimeoutError("Timed out waiting for a daemon command payload.")
            payload = _decode_message(connection.recv_bytes())
            request_id = str(payload.get("request_id", "")).strip() if isinstance(payload, dict) else ""
            command = str(payload.get("command", "")).strip() if isinstance(payload, dict) else ""
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
        except Exception as exc:  # pragma: no cover - defensive daemon boundary
            service._debug_log(f"command handling error: {exc!r}")
            response = {"ok": False, "error": str(exc)}
        try:
            connection.send_bytes(_encode_message(response))
        except (BrokenPipeError, EOFError, OSError) as exc:
            service._debug_log(f"connection closed before response could be delivered: {exc!r}")


def serve_forever(service: "DataEngineDaemonService") -> None:
    """Run the workspace daemon listener loop until shutdown."""
    worker_threads: set[threading.Thread] = set()
    active_connections: dict[int, object] = {}
    connection_lock = threading.Lock()

    def _serve_tracked_connection(connection) -> None:
        try:
            _serve_connection(service, connection)
        finally:
            with connection_lock:
                active_connections.pop(id(connection), None)

    try:
        service.initialize()
        service.state.checkpoint_thread = threading.Thread(target=service._checkpoint_loop, daemon=True)
        service.state.checkpoint_thread.start()
        _remove_stale_unix_endpoint(service.paths)
        listener = Listener(
            endpoint_address(service.paths),
            family=endpoint_family(service.paths),
            authkey=daemon_authkey(service.paths),
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
    lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT,
    workspace_service: WorkspaceService | None = None,
    resolve_paths_func=None,
) -> int:
    """Start serving one workspace daemon in the current process."""
    if resolve_paths_func is None:
        workspace_service = workspace_service or WorkspaceService()
        resolve_paths_func = workspace_service.resolve_paths
    paths = resolve_paths_func(workspace_root=workspace_root, workspace_id=workspace_id)
    service = service_type(paths, lifecycle_policy=lifecycle_policy)
    service.serve_forever()
    return 0


__all__ = ["serve_forever", "serve_workspace_daemon"]
