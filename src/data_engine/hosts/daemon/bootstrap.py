"""Bootstrap and claim-state initialization for the daemon host."""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_engine.domain.time import utcnow_text
from data_engine.hosts.daemon.client import _recover_broken_local_lease, _should_force_recover_local_lease
from data_engine.hosts.daemon.constants import APP_VERSION, STALE_AFTER_SECONDS
from data_engine.hosts.daemon.ownership import release_workspace_claim

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService


def initialize_service(service: "DataEngineDaemonService") -> None:
    """Claim the workspace when possible and hydrate local state."""
    service._debug_log("initialize starting")
    shared_state = service.shared_state_adapter
    shared_state.initialize_workspace(service.paths)
    claimed = shared_state.claim_workspace(service.paths)
    if not claimed:
        if _should_force_recover_local_lease(service.paths):
            service._debug_log("attempting local stale recovery before claim")
            _recover_broken_local_lease(service.paths)
            claimed = shared_state.claim_workspace(service.paths)
        if not shared_state.recover_stale_workspace(
            service.paths,
            machine_id=service.machine_id,
            stale_after_seconds=STALE_AFTER_SECONDS,
        ):
            metadata = shared_state.read_lease_metadata(service.paths)
            owner = str(metadata.get("machine_id")) if metadata is not None and metadata.get("machine_id") is not None else "another machine"
            if owner == service.machine_id:
                service._debug_log("initialize refused: already leased locally")
                from data_engine.hosts.daemon.client import WorkspaceLeaseError

                raise WorkspaceLeaseError(f"Workspace {service.paths.workspace_id!r} is already leased locally.")
            release_workspace_claim(service, leased_by_machine_id=owner, status="leased")
            shared_state.hydrate_local_runtime(service.paths, service.runtime_cache_ledger)
            service._update_daemon_state(status="leased")
            service._debug_log(f"initialize observer mode owner={owner}")
            return
        claimed = True
    with service._state_lock:
        if claimed:
            service.state.claim_workspace()
        else:
            service.state.release_workspace()
    service._publish_runtime_event("workspace.claimed" if claimed else "workspace.released")
    shared_state.write_lease_metadata(
        service.paths,
        workspace_id=service.paths.workspace_id,
        machine_id=service.machine_id,
        daemon_id=service.daemon_id,
        pid=service.pid,
        status="starting",
        started_at_utc=service.started_at_utc,
        last_checkpoint_at_utc=service.state.last_checkpoint_at_utc,
        app_version=APP_VERSION,
    )
    shared_state.hydrate_local_runtime(service.paths, service.runtime_cache_ledger)
    orphaned_run_count, orphaned_step_count = service.runtime_cache_ledger.reconcile_orphaned_activity(
        status="stopped",
        finished_at_utc=utcnow_text(),
        error_text="Recovered after daemon restart.",
    )
    if orphaned_run_count or orphaned_step_count:
        service._debug_log(
            "reconciled orphaned runtime rows"
            f" runs={orphaned_run_count} steps={orphaned_step_count}"
        )
    service._update_daemon_state(status="starting")
    service._checkpoint_once(status="idle")
    with service._state_lock:
        service.state.status = "idle"
    service._publish_runtime_event("daemon.ready")
    service._update_daemon_state(status="idle")
    service._debug_log("initialize complete workspace claimed")


__all__ = ["initialize_service"]
