"""Workspace ownership helpers for the daemon host."""

from __future__ import annotations

from typing import TYPE_CHECKING

from data_engine.hosts.daemon.constants import APP_VERSION
from data_engine.hosts.daemon.runtime_control import stop_active_work
from data_engine.runtime.shared_state import WorkspaceLeaseLostError, WorkspaceStateCorruptError

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService


def control_request_metadata(service: "DataEngineDaemonService") -> dict[str, object] | None:
    metadata = service.shared_state_adapter.read_control_request(service.paths)
    return metadata if isinstance(metadata, dict) else None


def honor_control_request_if_needed(service: "DataEngineDaemonService") -> bool:
    """Relinquish ownership when another workstation requests control."""
    with service._state_lock:
        if not service.host.workspace_owned:
            return False
    metadata = control_request_metadata(service)
    if metadata is None:
        return False
    requester = str(metadata.get("requester_machine_id", "")).strip()
    if not requester or requester == service.machine_id:
        return False
    requester_host_name = str(metadata.get("requester_host_name", "")).strip() or None
    service._debug_log(f"control request received requester={requester}")
    return service._relinquish_workspace_for_control_request(requester, requester_host_name)


def try_claim_requested_control(service: "DataEngineDaemonService") -> bool:
    """Claim released ownership when this workstation requested control."""
    with service._state_lock:
        if service.host.workspace_owned:
            return True
    metadata = control_request_metadata(service)
    if metadata is None:
        return False
    requester = str(metadata.get("requester_machine_id", "")).strip()
    if requester != service.machine_id:
        return False
    claimed = try_claim_released_workspace(service)
    if not claimed:
        return False
    service.shared_state_adapter.remove_control_request(service.paths)
    service._debug_log("control request fulfilled workspace claimed")
    return True


def lease_error_text(service: "DataEngineDaemonService") -> str:
    with service._state_lock:
        owner = service.host.leased_by_host_name or service.host.leased_by_machine_id or "another machine"
    return f"Workspace {service.paths.workspace_id!r} is leased by {owner}."


def handle_workspace_lease_lost(service: "DataEngineDaemonService", *, reason: str) -> None:
    """Close admission and stop this daemon without mutating a successor lease."""
    with service._state_lock:
        terminal_status = (
            service.state.status
            if service.state.status in {"failed", "workspace missing"}
            else "lease lost"
        )
        service.state.begin_work_drain()
        service.state.stop_runtime(status=terminal_status)
        service.state.release_workspace(status=terminal_status)
    stop_active_work(service)
    with service._state_lock:
        service.state.status = terminal_status
    service._debug_log(f"workspace lease lost reason={reason}")
    service._publish_runtime_event("workspace.lease_lost")
    service.host.shutdown_event.set()
    service._wake_listener()


def ensure_workspace_lease_current(service: "DataEngineDaemonService") -> bool:
    """Validate the retained token and drain immediately if it has been fenced."""
    with service._state_lock:
        if not service.state.workspace_owned or service.state.lease_token is None:
            return False
        lease_token = service.state.lease_token
    try:
        service.shared_state_adapter.assert_workspace_lease(
            service.paths,
            lease_token=lease_token,
        )
    except (WorkspaceLeaseLostError, WorkspaceStateCorruptError):
        handle_workspace_lease_lost(service, reason="token no longer current")
        return False
    return True


def try_claim_released_workspace(service: "DataEngineDaemonService") -> bool:
    """Try to reclaim an available workspace for this daemon."""
    with service._state_lock:
        if service.state.work_draining:
            return False
        if service.host.workspace_owned:
            return ensure_workspace_lease_current(service)
        shared_state = service.shared_state_adapter
        metadata = shared_state.read_lease_metadata(service.paths)
        if metadata is not None:
            owner = metadata.get("machine_id")
            owner_host = metadata.get("host_name")
            service.state.set_lease_owner(
                str(owner) if isinstance(owner, str) and owner.strip() else None,
                str(owner_host).strip()
                if isinstance(owner_host, str) and owner_host.strip()
                else None,
            )
            service._publish_runtime_event("workspace.lease_observed")
            return False
        try:
            lease_token = shared_state.claim_daemon_workspace(
                service.paths,
                workspace_id=service.paths.workspace_id,
                machine_id=service.machine_id,
                host_name=service.host_name,
                daemon_id=service.daemon_id,
                pid=service.pid,
                process_identity=service.process_identity,
                containment_nonce=service.containment_nonce,
                status="idle",
                started_at_utc=service.started_at_utc,
                last_checkpoint_at_utc=service.state.last_checkpoint_at_utc,
                app_version=APP_VERSION,
            )
        except Exception:
            return False
        if lease_token is None:
            metadata = shared_state.read_lease_metadata(service.paths)
            owner = metadata.get("machine_id") if isinstance(metadata, dict) else None
            owner_host = metadata.get("host_name") if isinstance(metadata, dict) else None
            service.state.set_lease_owner(
                str(owner) if isinstance(owner, str) and owner.strip() else None,
                str(owner_host).strip()
                if isinstance(owner_host, str) and owner_host.strip()
                else None,
            )
            service._publish_runtime_event("workspace.lease_observed")
            return False
        service.state.claim_workspace(lease_token)
        service._publish_runtime_event("workspace.claimed")
        try:
            service._checkpoint_once(status="idle")
            service.state.reset_checkpoint_failures()
        except Exception:
            release_workspace_claim(service)
            return False
        return True


def release_workspace_claim(
    service: "DataEngineDaemonService",
    *,
    leased_by_machine_id: str | None = None,
    leased_by_host_name: str | None = None,
    status: str | None = None,
    update_state: bool = False,
) -> None:
    """Release shared ownership and mark the daemon as no longer owning the workspace."""
    with service._state_lock:
        workspace_owned = service.host.workspace_owned
        lease_token = service.state.lease_token
    released_exact_lease = False
    if workspace_owned and lease_token is not None:
        try:
            service.shared_state_adapter.release_workspace(
                service.paths,
                lease_token=lease_token,
            )
        except (WorkspaceLeaseLostError, WorkspaceStateCorruptError):
            service._debug_log("workspace release skipped because lease token was fenced")
            handle_workspace_lease_lost(service, reason="release token no longer current")
            return
        released_exact_lease = True
    with service._state_lock:
        service.state.release_workspace(
            leased_by_machine_id=leased_by_machine_id,
            leased_by_host_name=leased_by_host_name,
            status=status,
        )
    if released_exact_lease:
        service._publish_runtime_event("workspace.released")
    elif leased_by_machine_id is not None or leased_by_host_name is not None:
        service._publish_runtime_event("workspace.lease_observed")
    if update_state and status is not None:
        service._update_daemon_state(status=status)


__all__ = [
    "control_request_metadata",
    "ensure_workspace_lease_current",
    "handle_workspace_lease_lost",
    "honor_control_request_if_needed",
    "lease_error_text",
    "release_workspace_claim",
    "try_claim_released_workspace",
    "try_claim_requested_control",
]
