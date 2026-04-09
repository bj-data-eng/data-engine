"""Host-agnostic workspace session use cases."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from data_engine.domain import OperatorSessionState, WorkspaceSessionState
from data_engine.platform.workspace_models import WorkspacePaths
from data_engine.services import WorkspaceService


@dataclass(frozen=True)
class WorkspaceBinding:
    """Normalized workspace/session binding for one resolved workspace target."""

    operator_session: OperatorSessionState
    workspace_session: WorkspaceSessionState


class WorkspaceSessionApplication:
    """Own workspace discovery and session-state derivation for hosts."""

    def __init__(self, *, workspace_service: WorkspaceService) -> None:
        self.workspace_service = workspace_service

    def refresh_session(
        self,
        *,
        workspace_paths: WorkspacePaths,
        override_root: Path | None,
    ) -> WorkspaceSessionState:
        """Return workspace session state rebound to paths plus current discovery."""
        discovered = self.workspace_service.discover(
            app_root=workspace_paths.app_root,
            workspace_collection_root=override_root,
        )
        return WorkspaceSessionState.from_paths(
            workspace_paths,
            override_root=override_root,
            discovered_workspace_ids=(item.workspace_id for item in discovered),
        )

    def bind_workspace(
        self,
        *,
        workspace_paths: WorkspacePaths,
        override_root: Path | None,
    ) -> WorkspaceBinding:
        """Return a fresh operator/session binding for one resolved workspace target."""
        workspace_session = self.refresh_session(
            workspace_paths=workspace_paths,
            override_root=override_root,
        )
        operator_session = OperatorSessionState.from_paths(
            workspace_paths,
            override_root=override_root,
        ).with_workspace(workspace_session)
        return WorkspaceBinding(
            operator_session=operator_session,
            workspace_session=workspace_session,
        )
