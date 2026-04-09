"""Daemon IPC and lifecycle services."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.app import (
    DaemonClientError,
    daemon_request,
    force_shutdown_daemon_process,
    is_daemon_live,
    spawn_daemon_process,
)
from data_engine.platform.workspace_models import WorkspacePaths


class DaemonService:
    """Thin injectable wrapper around daemon lifecycle and IPC helpers."""

    def __init__(
        self,
        *,
        spawn_process_func: Callable[..., object] = spawn_daemon_process,
        request_func: Callable[..., dict[str, Any]] = daemon_request,
        is_live_func: Callable[[WorkspacePaths], bool] = is_daemon_live,
        force_shutdown_func: Callable[..., None] = force_shutdown_daemon_process,
        client_error_type: type[Exception] = DaemonClientError,
    ) -> None:
        self._spawn_process = spawn_process_func
        self._request = request_func
        self._is_live = is_live_func
        self._force_shutdown = force_shutdown_func
        self._client_error_type = client_error_type

    def spawn(
        self,
        paths: WorkspacePaths,
        *,
        lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT,
    ) -> object:
        """Start the local workspace daemon process for the given paths."""
        return self._spawn_process(paths, lifecycle_policy=lifecycle_policy)

    def request(self, paths: WorkspacePaths, payload: dict[str, Any], *, timeout: float = 0.0) -> dict[str, Any]:
        """Send one request to the local workspace daemon."""
        return self._request(paths, payload, timeout=timeout)

    def is_live(self, paths: WorkspacePaths) -> bool:
        """Return whether the local workspace daemon is reachable."""
        return self._is_live(paths)

    def force_shutdown(self, paths: WorkspacePaths, *, timeout: float = 0.5) -> None:
        """Force-stop the local workspace daemon for the given paths."""
        self._force_shutdown(paths, timeout=timeout)

    @property
    def client_error_type(self) -> type[Exception]:
        """Return the daemon client exception type."""
        return self._client_error_type


__all__ = ["DaemonService"]
