"""Daemon state publication and observer-sync helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from data_engine.domain.time import utcnow_text
from data_engine.hosts.daemon.constants import APP_VERSION, CHECKPOINT_INTERVAL_SECONDS
from data_engine.runtime.shared_state import WorkspaceLeaseLostError
from data_engine.views.models import QtFlowCard

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService


class DaemonStateSyncHandler:
    """Own daemon status payloads, checkpoint publication, and observer sync."""

    def __init__(self, service: "DataEngineDaemonService") -> None:
        self.service = service

    def load_flow_cards(self, *, force: bool = False) -> tuple[QtFlowCard, ...]:
        return self.service._load_flow_cards(force=force)

    def status_payload(
        self,
        *,
        since_version: int | None = None,
        since_event_sequence: int | None = None,
    ) -> dict[str, Any]:
        service = self.service
        projection = service.runtime_projector.snapshot()
        recent_events = ()
        events_truncated = False
        if since_event_sequence is not None:
            recent_events, events_truncated = service.runtime_projector.events_since(since_event_sequence)
        if (
            since_version is not None
            and since_version == projection.version
            and since_event_sequence is not None
            and since_event_sequence == projection.event_sequence
        ):
            return {
                "workspace_id": service.paths.workspace_id,
                "daemon_id": service.daemon_id,
                "projection_version": projection.version,
                "event_sequence": projection.event_sequence,
                "unchanged": True,
            }
        return {
            "workspace_id": service.paths.workspace_id,
            "workspace_root": str(service.paths.workspace_root),
            "machine_id": service.machine_id,
            "host_name": service.host_name,
            "daemon_id": service.daemon_id,
            "pid": service.pid,
            "status": projection.status,
            "workspace_owned": projection.workspace_owned,
            "leased_by_machine_id": projection.leased_by_machine_id,
            "leased_by_host_name": projection.leased_by_host_name,
            "engine_active": projection.runtime_active,
            "engine_stopping": projection.runtime_stopping,
            "engine_starting": projection.engine_starting,
            "active_engine_flow_names": list(projection.active_engine_flow_names),
            "active_runs": list(projection.active_runs),
            "flow_activity": list(projection.flow_activity),
            "manual_runs": list(projection.manual_runs),
            "last_checkpoint_at_utc": projection.last_checkpoint_at_utc,
            "projection_version": projection.version,
            "event_sequence": projection.event_sequence,
            "recent_events": list(recent_events),
            "events_truncated": events_truncated,
        }

    def wait_for_status_payload(
        self,
        *,
        since_version: int,
        since_event_sequence: int,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        """Wait for one projection change and return the resulting status payload."""
        projection, recent_events, events_truncated = self.service.runtime_projector.wait_for_change(
            since_version=since_version,
            since_event_sequence=since_event_sequence,
            timeout_seconds=timeout_seconds,
        )
        if projection.version == since_version and projection.event_sequence == since_event_sequence:
            return {
                "workspace_id": self.service.paths.workspace_id,
                "daemon_id": self.service.daemon_id,
                "projection_version": projection.version,
                "event_sequence": projection.event_sequence,
                "unchanged": True,
            }
        return {
            "workspace_id": self.service.paths.workspace_id,
            "workspace_root": str(self.service.paths.workspace_root),
            "machine_id": self.service.machine_id,
            "host_name": self.service.host_name,
            "daemon_id": self.service.daemon_id,
            "pid": self.service.pid,
            "status": projection.status,
            "workspace_owned": projection.workspace_owned,
            "leased_by_machine_id": projection.leased_by_machine_id,
            "leased_by_host_name": projection.leased_by_host_name,
            "engine_active": projection.runtime_active,
            "engine_stopping": projection.runtime_stopping,
            "engine_starting": projection.engine_starting,
            "active_engine_flow_names": list(projection.active_engine_flow_names),
            "active_runs": list(projection.active_runs),
            "flow_activity": list(projection.flow_activity),
            "manual_runs": list(projection.manual_runs),
            "last_checkpoint_at_utc": projection.last_checkpoint_at_utc,
            "projection_version": projection.version,
            "event_sequence": projection.event_sequence,
            "recent_events": list(recent_events),
            "events_truncated": events_truncated,
        }

    def checkpoint_once(self, *, status: str) -> None:
        service = self.service
        with service._checkpoint_operation_lock:
            checkpoint_time = utcnow_text()
            with service._state_lock:
                lease_token = service.state.lease_token
            if lease_token is None:
                raise WorkspaceLeaseLostError(
                    f"Workspace {service.paths.workspace_id!r} has no retained lease token."
                )
            service.shared_state_adapter.checkpoint_workspace_state(
                service.paths,
                service.runtime_cache_ledger,
                lease_token=lease_token,
                workspace_id=service.paths.workspace_id,
                machine_id=service.machine_id,
                host_name=service.host_name,
                daemon_id=service.daemon_id,
                pid=service.pid,
                process_identity=service.process_identity,
                containment_nonce=service.containment_nonce,
                status=status,
                started_at_utc=service.started_at_utc,
                last_checkpoint_at_utc=checkpoint_time,
                app_version=APP_VERSION,
                heartbeat_interval_seconds=CHECKPOINT_INTERVAL_SECONDS,
            )
            completed_at_utc = utcnow_text()
            with service._state_lock:
                service.state.set_checkpoint_time(completed_at_utc)
        service._publish_runtime_event("checkpoint.recorded")
        self.update_daemon_state(status=status)

    def refresh_observer_snapshot(self) -> None:
        service = self.service
        service.shared_state_adapter.hydrate_local_runtime(service.paths, service.runtime_cache_ledger)
        metadata = service.shared_state_adapter.read_lease_metadata(service.paths)
        with service._state_lock:
            service.state.set_lease_owner(
                (
                    str(metadata.get("machine_id"))
                    if metadata is not None and metadata.get("machine_id") is not None
                    else None
                ),
                (
                    str(metadata.get("host_name"))
                    if metadata is not None and metadata.get("host_name") is not None
                    else None
                ),
            )
        service._publish_runtime_event("observer.refreshed")
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
        with service._state_lock:
            service.state.status = status
        service._publish_runtime_event("daemon.state_updated")


__all__ = ["DaemonStateSyncHandler"]
