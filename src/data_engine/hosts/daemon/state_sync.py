"""Daemon state publication and observer-sync helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from data_engine.domain.time import utcnow_text
from data_engine.hosts.daemon.constants import APP_VERSION
from data_engine.views.models import QtFlowCard

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService


class DaemonStateSyncHandler:
    """Own daemon status payloads, checkpoint publication, and observer sync."""

    def __init__(self, service: "DataEngineDaemonService") -> None:
        self.service = service

    def load_flow_cards(self, *, force: bool = False) -> tuple[QtFlowCard, ...]:
        return self.service._load_flow_cards(force=force)

    def status_payload(self) -> dict[str, Any]:
        service = self.service
        with service._state_lock:
            state = service.state
            status = state.status
            workspace_owned = state.workspace_owned
            leased_by_machine_id = state.leased_by_machine_id
            runtime_active = state.runtime_active
            runtime_stopping = state.runtime_stopping
            manual_runs = sorted(state.manual_run_threads)
            last_checkpoint_at_utc = state.last_checkpoint_at_utc
        return {
            "workspace_id": service.paths.workspace_id,
            "workspace_root": str(service.paths.workspace_root),
            "machine_id": service.machine_id,
            "daemon_id": service.daemon_id,
            "pid": service.pid,
            "status": status,
            "workspace_owned": workspace_owned,
            "leased_by_machine_id": leased_by_machine_id,
            "engine_active": runtime_active,
            "engine_stopping": runtime_stopping,
            "manual_runs": manual_runs,
            "last_checkpoint_at_utc": last_checkpoint_at_utc,
        }

    def checkpoint_once(self, *, status: str) -> None:
        service = self.service
        checkpoint_time = utcnow_text()
        service.shared_state_adapter.checkpoint_workspace_state(
            service.paths,
            service.runtime_cache_ledger,
            workspace_id=service.paths.workspace_id,
            machine_id=service.machine_id,
            daemon_id=service.daemon_id,
            pid=service.pid,
            status=status,
            started_at_utc=service.started_at_utc,
            last_checkpoint_at_utc=checkpoint_time,
            app_version=APP_VERSION,
        )
        with service._state_lock:
            service.state.set_checkpoint_time(checkpoint_time)
        self.update_daemon_state(status=status)

    def refresh_observer_snapshot(self) -> None:
        service = self.service
        service.shared_state_adapter.hydrate_local_runtime(service.paths, service.runtime_cache_ledger)
        metadata = service.shared_state_adapter.read_lease_metadata(service.paths)
        with service._state_lock:
            service.state.set_leased_by_machine_id(
                str(metadata.get("machine_id"))
                if metadata is not None and metadata.get("machine_id") is not None
                else None
            )
        if metadata is None:
            self.update_daemon_state(status="available")
            service._shutdown_if_unowned_and_idle(reason="lease released")
            return
        self.update_daemon_state(status="leased")

    def update_daemon_state(self, *, status: str) -> None:
        service = self.service
        service.runtime_control_ledger.daemon_state.upsert(
            workspace_id=service.paths.workspace_id,
            pid=service.pid,
            endpoint_kind=service.paths.daemon_endpoint_kind,
            endpoint_path=service.paths.daemon_endpoint_path,
            started_at_utc=service.started_at_utc,
            last_checkpoint_at_utc=service.state.last_checkpoint_at_utc,
            status=status,
            app_root=str(service.paths.app_root),
            workspace_root=str(service.paths.workspace_root),
            version_text=APP_VERSION,
        )


__all__ = ["DaemonStateSyncHandler"]
