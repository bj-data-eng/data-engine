"""Window support helpers for the terminal UI surface."""

from __future__ import annotations

import os
from time import monotonic
from typing import TYPE_CHECKING

from data_engine.platform.workspace_models import authored_workspace_is_available
from data_engine.services import DaemonUpdateBatch
from data_engine.services.daemon_state import merge_update_batches

if TYPE_CHECKING:
    from data_engine.ui.tui.app import DataEngineTui


class TuiWindowSupportMixin:
    """Session, workspace, and daemon plumbing separated from the main TUI shell."""

    _SUBSCRIPTION_HEALTH_WINDOW_SECONDS = 15.0

    def _has_authored_workspace(self: "DataEngineTui") -> bool:
        """Return whether the selected workspace currently has authored flow modules."""
        return authored_workspace_is_available(self.workspace_paths)

    def _daemon_request(self: "DataEngineTui", paths, payload, *, timeout: float = 0.0):
        return self.daemon_service.request(paths, payload, timeout=timeout)

    def _is_daemon_live(self: "DataEngineTui", paths) -> bool:
        return self.daemon_service.is_live(paths)

    def _resolve_workspace_paths(self: "DataEngineTui", *, workspace_id: str | None = None):
        return self.services.workspace_service.resolve_paths(
            workspace_id=workspace_id,
            workspace_collection_root=self.workspace_collection_root_override,
        )

    def _monotonic(self: "DataEngineTui") -> float:
        return monotonic()

    def _register_client_session(self: "DataEngineTui") -> None:
        """Register this TUI process as one active local client for the workspace."""
        self.runtime_binding_service.register_client_session(
            self.runtime_binding,
            client_id=self.client_session_id,
            client_kind="tui",
            pid=os.getpid(),
        )

    def _unregister_client_session_and_check_for_shutdown(self: "DataEngineTui") -> bool:
        """Remove this TUI session and return whether no local clients remain."""
        try:
            self.runtime_binding_service.remove_client_session(self.runtime_binding, self.client_session_id)
            remaining = self.runtime_binding_service.count_live_client_sessions(self.runtime_binding)
            return remaining == 0
        except Exception:
            return False

    def _shutdown_daemon_on_close(self: "DataEngineTui") -> None:
        """Best-effort local daemon shutdown when the last local client closes."""
        try:
            if not self._is_daemon_live(self.workspace_paths):
                return
            workspace_snapshot = getattr(self, "workspace_snapshot", None)
            runtime_session = getattr(self, "runtime_session", None)
            engine_state = (
                str(getattr(getattr(workspace_snapshot, "engine", None), "state", "") or "").strip().lower()
                if workspace_snapshot is not None
                else ""
            )
            if not engine_state:
                runtime_active = bool(getattr(runtime_session, "runtime_active", False))
                runtime_stopping = bool(getattr(runtime_session, "runtime_stopping", False))
                engine_state = "stopping" if runtime_stopping else "running" if runtime_active else "idle"
            manual_run_active = bool(getattr(runtime_session, "manual_run_active", False))
            if workspace_snapshot is not None:
                engine_flow_names = set(getattr(workspace_snapshot.engine, "active_flow_names", ()))
                manual_run_active = any(
                    run.flow_name not in engine_flow_names
                    and run.state in {"starting", "running", "stopping"}
                    for run in getattr(workspace_snapshot, "active_runs", {}).values()
                )
            if engine_state in {"starting", "running", "stopping"}:
                self._daemon_request(
                    self.workspace_paths,
                    {"command": "stop_engine", "shutdown_when_idle": True},
                    timeout=1.5,
                )
                return
            if manual_run_active:
                return
            self._daemon_request(self.workspace_paths, {"command": "shutdown_daemon"}, timeout=1.5)
        except Exception:
            pass

    def _schedule_daemon_update_sync(self: "DataEngineTui") -> None:
        """Queue one daemon-driven full sync back onto the Textual app thread."""
        if not getattr(self, "is_mounted", False):
            return
        self.daemon_subscription.mark_subscription(self._monotonic())
        try:
            self.call_from_thread(self._sync_daemon_state)
        except Exception:
            return

    def _schedule_daemon_update_batch(self: "DataEngineTui", batch: DaemonUpdateBatch) -> None:
        """Queue one lane-based daemon update back onto the Textual app thread."""
        if not getattr(self, "is_mounted", False):
            return
        self.daemon_subscription.mark_subscription(self._monotonic())
        self._pending_daemon_update_batch = merge_update_batches(
            getattr(self, "_pending_daemon_update_batch", None),
            batch,
        )
        try:
            self.call_from_thread(self._apply_daemon_update_batch)
        except Exception:
            return

    def _ensure_daemon_wait_worker(self: "DataEngineTui") -> None:
        """Ensure the daemon wait worker is running for the current workspace."""
        if not getattr(self, "is_mounted", False):
            return
        if not self._has_authored_workspace():
            return
        import threading

        def _start_daemon_wait_thread(target):
            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            return thread

        self.daemon_subscription.ensure_started(
            workspace_available=lambda: self._has_authored_workspace(),
            on_update=lambda batch: self._schedule_daemon_update_batch(batch),
            start_worker=lambda target: _start_daemon_wait_thread(target),
        )

    def _should_run_daemon_heartbeat(self: "DataEngineTui") -> bool:
        """Return whether the TUI heartbeat should perform a daemon sync right now."""
        return self.daemon_subscription.should_run_heartbeat(getattr(self, "workspace_snapshot", None))
__all__ = ["TuiWindowSupportMixin"]
