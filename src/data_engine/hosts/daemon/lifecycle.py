"""Lifecycle and checkpoint policy for the daemon host."""

from __future__ import annotations

from pathlib import Path
import time
import traceback
from typing import TYPE_CHECKING

from data_engine.domain import DaemonLifecyclePolicy
from data_engine.hosts.daemon.ownership import (
    honor_control_request_if_needed,
    release_workspace_claim,
    try_claim_requested_control,
)
from data_engine.hosts.daemon.constants import (
    CHECKPOINT_INTERVAL_SECONDS,
    CONTROL_REQUEST_POLL_INTERVAL_SECONDS,
)
from data_engine.hosts.daemon.runtime_control import stop_active_work

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService


def checkpoint_loop(service: "DataEngineDaemonService") -> None:
    next_checkpoint_at = time.monotonic() + CHECKPOINT_INTERVAL_SECONDS
    while not service.host.shutdown_event.wait(CONTROL_REQUEST_POLL_INTERVAL_SECONDS):
        if _should_shutdown_for_missing_clients(service):
            service._debug_log("no live local clients remain; shutting down ephemeral daemon")
            relinquish_workspace_for_missing_clients(service)
            break
        if not service._workspace_root_is_available():
            service._debug_log("workspace root no longer available; shutting down daemon")
            relinquish_workspace_for_missing_root(service)
            break
        with service._state_lock:
            workspace_owned = service.host.workspace_owned
        if not workspace_owned:
            try:
                if try_claim_requested_control(service):
                    next_checkpoint_at = time.monotonic() + CHECKPOINT_INTERVAL_SECONDS
                    continue
                service._refresh_observer_snapshot()
            except Exception:
                pass
            continue
        try:
            if honor_control_request_if_needed(service):
                next_checkpoint_at = time.monotonic() + CHECKPOINT_INTERVAL_SECONDS
                continue
            if time.monotonic() < next_checkpoint_at:
                continue
            with service._state_lock:
                status = "degraded" if service.state.consecutive_checkpoint_failures >= 1 else service.host.status
            service._checkpoint_once(status=status)
            with service._state_lock:
                service.state.consecutive_checkpoint_failures = 0
            next_checkpoint_at = time.monotonic() + CHECKPOINT_INTERVAL_SECONDS
        except Exception:
            service._debug_log("checkpoint failed")
            service._debug_log(traceback.format_exc().rstrip())
            with service._state_lock:
                failure_count = service.state.increment_checkpoint_failures()
            if failure_count == 2:
                with service._state_lock:
                    service.state.status = "degraded"
                service._update_daemon_state(status="degraded")
                service._debug_log("daemon marked degraded after repeated checkpoint failures")
            if failure_count >= 3:
                with service._state_lock:
                    service.state.status = "failed"
                service._debug_log("relinquishing workspace after repeated checkpoint failures")
                relinquish_workspace_after_checkpoint_failures(service)
                next_checkpoint_at = time.monotonic() + CHECKPOINT_INTERVAL_SECONDS


def relinquish_workspace_after_checkpoint_failures(service: "DataEngineDaemonService") -> None:
    """Stop active work, release shared ownership, and stop the daemon."""
    with service._state_lock:
        service.state.stop_runtime(status="failed")
    service._debug_log("relinquish workspace starting")
    stop_active_work(service)
    release_workspace_claim(service, status="failed", update_state=True)
    service._debug_log("relinquish workspace complete")
    shutdown_if_unowned_and_idle(service, reason="checkpoint failures")


def relinquish_workspace_for_control_request(service: "DataEngineDaemonService", requester_machine_id: str) -> None:
    """Stop active work, hand ownership off, and stop this daemon."""
    with service._state_lock:
        service.state.stop_runtime(status="stopping flow")
    service._debug_log(f"relinquish for control request requester={requester_machine_id}")
    stop_active_work(service)
    release_workspace_claim(
        service,
        leased_by_machine_id=requester_machine_id,
        status="leased",
        update_state=True,
    )
    service._debug_log("relinquish for control request complete")
    shutdown_if_unowned_and_idle(service, reason="control request handoff")


def relinquish_workspace_for_missing_root(service: "DataEngineDaemonService") -> None:
    """Stop active work and exit when the authored workspace root disappears."""
    with service._state_lock:
        service.state.stop_runtime(status="workspace missing")
    stop_active_work(service)
    release_workspace_claim(service, status="workspace missing")
    service.host.shutdown_event.set()
    service._wake_listener()


def relinquish_workspace_for_missing_clients(service: "DataEngineDaemonService") -> None:
    """Stop active work and exit when an ephemeral daemon has no live local clients."""
    with service._state_lock:
        service.state.stop_runtime(status="client disconnected")
    stop_active_work(service)
    release_workspace_claim(service, status="client disconnected")
    service.host.shutdown_event.set()
    service._wake_listener()


def _should_shutdown_for_missing_clients(service: "DataEngineDaemonService") -> bool:
    """Return whether an ephemeral daemon should stop because no live clients remain."""
    if service.lifecycle_policy is not DaemonLifecyclePolicy.EPHEMERAL:
        return False
    with service._state_lock:
        if service.host.runtime_active or service.host.runtime_stopping:
            return False
        if service.state.manual_run_threads:
            return False
    try:
        return service.runtime_control_ledger.client_sessions.count_live(service.paths.workspace_id) == 0
    except Exception:
        return False


def shutdown_if_unowned_and_idle(service: "DataEngineDaemonService", *, reason: str) -> None:
    """Exit when this daemon no longer owns the workspace and has no active work."""
    with service._state_lock:
        if service.host.workspace_owned:
            return
        if service.host.runtime_active or service.host.runtime_stopping:
            return
        if service.state.manual_run_threads:
            return
    service._debug_log(f"shutdown requested reason={reason}")
    service.host.shutdown_event.set()
    service._wake_listener()


def shutdown(service: "DataEngineDaemonService") -> None:
    service._debug_log("shutdown starting")
    stop_active_work(service)
    if service.state.checkpoint_thread is not None and service.state.checkpoint_thread.is_alive():
        service.state.checkpoint_thread.join(timeout=5.0)
    with service._state_lock:
        workspace_owned = service.host.workspace_owned
        status = service.host.status
    if workspace_owned and status not in {"failed", "workspace missing"}:
        try:
            service._checkpoint_once(status="stopping")
        except Exception:
            pass
    release_workspace_claim(service)
    try:
        service.runtime_control_ledger.daemon_state.clear(service.paths.workspace_id)
    except Exception:
        pass
    service.runtime_cache_ledger.close()
    service.runtime_control_ledger.close()
    if service.host.listener is not None:
        try:
            service.host.listener.close()
        except Exception:
            pass
    if service.paths.daemon_endpoint_kind == "unix":
        try:
            Path(service.paths.daemon_endpoint_path).unlink()
        except FileNotFoundError:
            pass
    service._debug_log("shutdown complete")


__all__ = [
    "checkpoint_loop",
    "relinquish_workspace_after_checkpoint_failures",
    "relinquish_workspace_for_control_request",
    "relinquish_workspace_for_missing_root",
    "shutdown",
    "shutdown_if_unowned_and_idle",
]
