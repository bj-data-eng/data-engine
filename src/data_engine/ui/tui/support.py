"""Window support helpers for the terminal UI surface."""

from __future__ import annotations

import os
from time import monotonic
from typing import TYPE_CHECKING

from data_engine.platform.workspace_models import authored_workspace_is_available

if TYPE_CHECKING:
    from data_engine.ui.tui.app import DataEngineTui


class TuiWindowSupportMixin:
    """Session, workspace, and daemon plumbing separated from the main TUI shell."""

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
            self._daemon_request(self.workspace_paths, {"command": "shutdown_daemon"}, timeout=1.5)
        except Exception:
            pass

    def _schedule_daemon_update_sync(self: "DataEngineTui") -> None:
        """Queue one daemon-driven sync back onto the Textual app thread."""
        if not getattr(self, "is_mounted", False):
            return
        try:
            self.call_from_thread(self._sync_daemon_state)
        except Exception:
            return
__all__ = ["TuiWindowSupportMixin"]
