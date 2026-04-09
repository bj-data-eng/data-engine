"""Domain models for top-level operator session state."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING

from data_engine.domain.catalog import FlowCatalogState
from data_engine.domain.operations import OperationSessionState
from data_engine.domain.runtime import RuntimeSessionState, WorkspaceControlState
from data_engine.domain.support import WorkspaceSupportState
from data_engine.domain.workspace import WorkspaceSessionState

if TYPE_CHECKING:
    from data_engine.platform.workspace_models import WorkspacePaths


@dataclass(frozen=True)
class OperatorSessionState:
    """Top-level operator state shared by one surface shell."""

    workspace: WorkspaceSessionState
    workspace_control: WorkspaceControlState
    runtime: RuntimeSessionState
    catalog: FlowCatalogState
    operations: OperationSessionState
    support: WorkspaceSupportState

    @classmethod
    def from_paths(
        cls,
        workspace_paths: "WorkspacePaths",
        *,
        override_root: Path | None = None,
    ) -> "OperatorSessionState":
        """Return the default operator state for one resolved workspace binding."""
        return cls(
            workspace=WorkspaceSessionState.from_paths(workspace_paths, override_root=override_root),
            workspace_control=WorkspaceControlState.empty(),
            runtime=RuntimeSessionState.empty(),
            catalog=FlowCatalogState.empty(),
            operations=OperationSessionState.empty(),
            support=WorkspaceSupportState.empty(),
        )

    def with_workspace(self, workspace: WorkspaceSessionState) -> "OperatorSessionState":
        """Return a copy with workspace session state replaced."""
        return replace(self, workspace=workspace)

    def with_workspace_control(self, workspace_control: WorkspaceControlState) -> "OperatorSessionState":
        """Return a copy with workspace control state replaced."""
        return replace(self, workspace_control=workspace_control)

    def with_runtime(self, runtime: RuntimeSessionState) -> "OperatorSessionState":
        """Return a copy with runtime session state replaced."""
        return replace(self, runtime=runtime)

    def with_catalog(self, catalog: FlowCatalogState) -> "OperatorSessionState":
        """Return a copy with flow catalog state replaced."""
        return replace(self, catalog=catalog)

    def with_operations(self, operations: OperationSessionState) -> "OperatorSessionState":
        """Return a copy with operation session state replaced."""
        return replace(self, operations=operations)

    def with_support(self, support: WorkspaceSupportState) -> "OperatorSessionState":
        """Return a copy with support state replaced."""
        return replace(self, support=support)


__all__ = ["OperatorSessionState"]
