"""Daemon host object."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import threading
from typing import TYPE_CHECKING, Any

from data_engine.domain import DaemonLifecyclePolicy
from data_engine.domain.time import parse_utc_text, utcnow_text
from data_engine.hosts.daemon.client import (
    DaemonClientError,
    force_shutdown_daemon_process,
    WorkspaceLeaseError,
    _remove_stale_unix_endpoint,
    daemon_request,
    is_daemon_live,
    spawn_daemon_process,
)
from data_engine.hosts.daemon.composition import (
    DaemonHostDependencies,
    DaemonHostFacade,
    DaemonHostIdentity,
    DaemonHostState,
)
from data_engine.hosts.daemon.bootstrap import initialize_service
from data_engine.hosts.daemon.entrypoints import (
    default_workspace_service_factory,
    main as run_daemon_module,
    serve_workspace_daemon as serve_daemon_entrypoint,
)
from data_engine.hosts.daemon.commands import (
    DaemonCommandHandler,
)
from data_engine.hosts.daemon.constants import (
    APP_VERSION,
    CHECKPOINT_INTERVAL_SECONDS,
    CONTROL_REQUEST_POLL_INTERVAL_SECONDS,
    STALE_AFTER_SECONDS,
)
from data_engine.hosts.daemon.lifecycle import (
    checkpoint_loop,
    relinquish_workspace_after_checkpoint_failures,
    relinquish_workspace_for_control_request,
    relinquish_workspace_for_missing_root,
    shutdown,
    shutdown_if_unowned_and_idle,
)
from data_engine.hosts.daemon.server import serve_forever as serve_daemon_forever
from data_engine.platform.workspace_models import (
    WorkspacePaths,
    authored_workspace_is_available,
)
from data_engine.views.models import QtFlowCard, load_qt_flow_cards

if TYPE_CHECKING:
    from multiprocessing.connection import Listener


DAEMON_LOG_RETENTION_DAYS = 30

class DataEngineDaemonService:
    """Own one workspace daemon instance and its runtime state."""

    def __init__(
        self,
        paths: WorkspacePaths,
        *,
        dependencies: DaemonHostDependencies | None = None,
        identity: DaemonHostIdentity | None = None,
        lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT,
    ) -> None:
        self.paths = paths
        dependencies = dependencies or DaemonHostDependencies.build_default(paths)
        daemon_identity = identity or DaemonHostIdentity.current_process()
        self.lifecycle_policy = DaemonLifecyclePolicy.coerce(lifecycle_policy)
        self.started_at_utc = utcnow_text()
        self.state = DaemonHostState.build(started_at_utc=self.started_at_utc)
        self.host = DaemonHostFacade(self.state)

        self.runtime_cache_ledger = dependencies.runtime_cache_ledger
        self.runtime_control_ledger = dependencies.runtime_control_ledger
        self.flow_catalog_service = dependencies.flow_catalog_service
        self.flow_execution_service = dependencies.flow_execution_service
        self.runtime_execution_service = dependencies.runtime_execution_service
        self.shared_state_adapter = dependencies.shared_state_adapter
        self.machine_id = daemon_identity.machine_id
        self.daemon_id = daemon_identity.daemon_id
        self.pid = daemon_identity.pid
        self._state_lock = threading.RLock()
        self.command_handler = DaemonCommandHandler(self)

    @property
    def runtime_ledger(self):
        """Compatibility alias for cache-backed runtime history."""
        return self.runtime_cache_ledger

    def _workspace_root_is_available(self) -> bool:
        """Return whether the authored workspace still exists at the configured root."""
        return authored_workspace_is_available(self.paths)

    def _retained_daemon_log_lines(self, lines: list[str], *, now: datetime | None = None) -> list[str]:
        """Return daemon-log lines that still fall within the retention window."""
        cutoff = (now or datetime.now(UTC)) - timedelta(days=DAEMON_LOG_RETENTION_DAYS)
        retained: list[str] = []
        for line in lines:
            timestamp_text = line.split(" ", 1)[0].strip()
            try:
                parsed = parse_utc_text(timestamp_text)
            except Exception:
                parsed = None
            if parsed is None or parsed >= cutoff:
                retained.append(line)
        return retained

    def _debug_log(self, message: str) -> None:
        """Append one daemon diagnostic line and keep only the last retention window."""
        try:
            self.paths.runtime_state_dir.mkdir(parents=True, exist_ok=True)
            existing_lines: list[str] = []
            if self.paths.daemon_log_path.exists():
                existing_lines = self.paths.daemon_log_path.read_text(encoding="utf-8").splitlines(keepends=True)
            retained_lines = self._retained_daemon_log_lines(existing_lines)
            retained_lines.append(f"{utcnow_text()} pid={self.pid} workspace={self.paths.workspace_id} {message}\n")
            self.paths.daemon_log_path.write_text("".join(retained_lines), encoding="utf-8")
        except Exception:
            pass

    def initialize(self) -> None:
        initialize_service(self)

    def serve_forever(self) -> None:
        serve_daemon_forever(self)

    def _handle_command(self, payload: Any) -> dict[str, Any]:
        return self.command_handler.handle(payload)

    def _load_flow_cards(self, *, force: bool = False) -> tuple[QtFlowCard, ...]:
        del force
        return load_qt_flow_cards(self.flow_catalog_service, workspace_root=self.paths.workspace_root)

    def _checkpoint_loop(self) -> None:
        checkpoint_loop(self)

    def _checkpoint_once(self, *, status: str) -> None:
        self.command_handler.checkpoint_once(status=status)

    def _refresh_observer_snapshot(self) -> None:
        self.command_handler.refresh_observer_snapshot()

    def _update_daemon_state(self, *, status: str) -> None:
        self.command_handler.update_daemon_state(status=status)

    def _relinquish_workspace_after_checkpoint_failures(self) -> None:
        relinquish_workspace_after_checkpoint_failures(self)

    def _relinquish_workspace_for_control_request(self, requester_machine_id: str) -> None:
        relinquish_workspace_for_control_request(self, requester_machine_id)

    def _relinquish_workspace_for_missing_root(self) -> None:
        relinquish_workspace_for_missing_root(self)

    def _shutdown_if_unowned_and_idle(self, *, reason: str) -> None:
        shutdown_if_unowned_and_idle(self, reason=reason)

    def _wake_listener(self) -> None:
        try:
            daemon_request(self.paths, {"command": "daemon_ping"}, timeout=0.5)
        except Exception:
            pass

    def _shutdown(self) -> None:
        shutdown(self)
def main(
    argv: list[str] | None = None,
    *,
    workspace_service=None,
    workspace_service_factory=None,
    resolve_paths_func=None,
) -> int:
    """Module entrypoint for launching one workspace daemon process."""
    return run_daemon_module(
        DataEngineDaemonService,
        argv,
        workspace_service=workspace_service,
        workspace_service_factory=workspace_service_factory,
        resolve_paths_func=resolve_paths_func,
        serve_workspace_daemon_func=lambda service_type, **kwargs: serve_workspace_daemon(**kwargs),
    )


def serve_workspace_daemon(
    *,
    workspace_root=None,
    workspace_id=None,
    lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT,
    workspace_service=None,
    resolve_paths_func=None,
) -> int:
    return serve_daemon_entrypoint(
        DataEngineDaemonService,
        workspace_root=workspace_root,
        workspace_id=workspace_id,
        lifecycle_policy=lifecycle_policy,
        workspace_service=workspace_service,
        resolve_paths_func=resolve_paths_func,
    )


__all__ = [
    "CHECKPOINT_INTERVAL_SECONDS",
    "DaemonClientError",
    "DataEngineDaemonService",
    "STALE_AFTER_SECONDS",
    "WorkspaceLeaseError",
    "force_shutdown_daemon_process",
    "_remove_stale_unix_endpoint",
    "daemon_request",
    "default_workspace_service_factory",
    "is_daemon_live",
    "serve_workspace_daemon",
    "spawn_daemon_process",
]


if __name__ == "__main__":
    raise SystemExit(main())
