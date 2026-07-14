"""Workspace ownership helpers for the daemon host."""

from __future__ import annotations

from typing import TYPE_CHECKING

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


def try_claim_released_workspace(service: "DataEngineDaemonService") -> bool:
    """Try to reclaim an available workspace for this daemon."""
    with service._state_lock:
        if service.state.work_draining:
            return False
        if service.host.workspace_owned:
            return True
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
            claimed = shared_state.claim_workspace(service.paths)
        except Exception:
            return False
        if not claimed:
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
        service.state.claim_workspace()
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
    if workspace_owned:
        try:
            service.shared_state_adapter.remove_lease_metadata(service.paths)
        except Exception:
            pass
        try:
            service.shared_state_adapter.release_workspace(service.paths)
        except Exception:
            pass
    with service._state_lock:
        service.state.release_workspace(
            leased_by_machine_id=leased_by_machine_id,
            leased_by_host_name=leased_by_host_name,
            status=status,
        )
    service._publish_runtime_event("workspace.released")
    if update_state and status is not None:
        service._update_daemon_state(status=status)


__all__ = [
    "control_request_metadata",
    "honor_control_request_if_needed",
    "lease_error_text",
    "release_workspace_claim",
    "try_claim_released_workspace",
    "try_claim_requested_control",
]
