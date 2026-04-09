from __future__ import annotations

from pathlib import Path

from data_engine.domain import FlowCatalogState, OperatorSessionState, OperationSessionState, RuntimeSessionState


def _paths():
    workspace_root = Path("/tmp/workspaces/example_workspace")
    collection_root = workspace_root.parent
    return type(
        "Paths",
        (),
        {
            "workspace_collection_root": collection_root,
            "workspace_id": "example_workspace",
        },
    )()


def test_operator_session_state_groups_subdomain_state_together():
    session = OperatorSessionState.from_paths(_paths(), override_root=Path("/tmp/workspaces"))
    session = session.with_runtime(RuntimeSessionState.empty().with_runtime_flags(active=True, stopping=False))
    session = session.with_catalog(FlowCatalogState.empty(empty_message="No flow modules discovered."))
    session = session.with_operations(OperationSessionState.empty())

    assert session.workspace.current_workspace_id == "example_workspace"
    assert session.workspace.workspace_collection_root_override == Path("/tmp/workspaces").resolve()
    assert session.runtime.runtime_active is True
    assert session.catalog.empty_message == "No flow modules discovered."
