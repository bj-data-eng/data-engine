"""Workspace daemon state and control services."""

from __future__ import annotations

from data_engine.hosts.daemon.manager import WorkspaceDaemonManager, WorkspaceDaemonSnapshot
from data_engine.hosts.daemon.shared_state import DaemonSharedStateAdapter
from data_engine.domain import WorkspaceControlState
from data_engine.platform.workspace_models import WorkspacePaths


class DaemonStateService:
    """Own workspace daemon-manager construction and normalized snapshot access."""

    def __init__(self, *, shared_state_adapter: DaemonSharedStateAdapter | None = None) -> None:
        self.shared_state_adapter = shared_state_adapter or DaemonSharedStateAdapter()

    def create_manager(self, paths: WorkspacePaths) -> WorkspaceDaemonManager:
        """Create one daemon-state manager for a workspace."""
        return WorkspaceDaemonManager(paths, shared_state_adapter=self.shared_state_adapter)

    def sync(self, manager: WorkspaceDaemonManager) -> WorkspaceDaemonSnapshot:
        """Fetch one normalized daemon snapshot."""
        return manager.sync()

    def control_state(
        self,
        manager: WorkspaceDaemonManager,
        snapshot: WorkspaceDaemonSnapshot,
        *,
        daemon_startup_in_progress: bool = False,
    ) -> WorkspaceControlState:
        """Build structured workspace control state from one daemon snapshot."""
        return manager.control_state(snapshot, daemon_startup_in_progress=daemon_startup_in_progress)

    def request_control(self, manager: WorkspaceDaemonManager) -> str:
        """Request workspace control through one daemon-state manager."""
        return manager.request_control()


__all__ = ["DaemonStateService"]
