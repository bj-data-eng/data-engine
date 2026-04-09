from __future__ import annotations

from pathlib import Path

from data_engine.domain import WorkspaceRootState, WorkspaceSelectionState, WorkspaceSessionState
from data_engine.platform.workspace_models import WorkspacePaths


def _sample_paths() -> WorkspacePaths:
    root = Path("/tmp/workspaces/example_workspace")
    app_root = Path("/tmp/data_engine")
    state_root = root / ".workspace_state"
    artifacts = app_root / "artifacts"
    cache = artifacts / "workspace_cache" / "example_workspace"
    runtime = artifacts / "runtime_state" / "example_workspace"
    return WorkspacePaths(
        app_root=app_root,
        workspace_collection_root=Path("/tmp/workspaces"),
        workspace_id="example_workspace",
        workspace_root=root,
        config_dir=root / "config",
        flow_modules_dir=root / "flow_modules",
        databases_dir=root / "databases",
        workspace_state_dir=state_root,
        available_markers_dir=state_root / "available",
        leased_markers_dir=state_root / "leased",
        stale_markers_dir=state_root / "stale",
        lease_metadata_dir=state_root / "lease_metadata",
        lease_metadata_path=state_root / "lease_metadata" / "lease.json",
        control_requests_dir=state_root / "control_requests",
        control_request_path=state_root / "control_requests" / "request.json",
        shared_state_dir=state_root / "shared_state",
        shared_runs_path=state_root / "shared_state" / "runs.parquet",
        shared_step_runs_path=state_root / "shared_state" / "step_runs.parquet",
        shared_logs_path=state_root / "shared_state" / "logs.parquet",
        shared_file_state_path=state_root / "shared_state" / "file_state.parquet",
        artifacts_dir=artifacts,
        workspace_cache_dir=cache,
        compiled_flow_modules_dir=cache / "compiled_flow_modules",
        runtime_state_dir=runtime,
        runtime_db_path=runtime / "runtime_ledger.sqlite",
        daemon_log_path=runtime / "daemon.log",
        documentation_dir=root / "documentation",
        daemon_endpoint_kind="unix",
        daemon_endpoint_path=str(runtime / "daemon.sock"),
        sphinx_source_dir=app_root / "src" / "data_engine" / "sphinx_source",
    )


def test_workspace_root_state_reports_default_vs_override_text():
    root = WorkspaceRootState.from_paths(_sample_paths())
    overridden = root.with_override_root(Path("/tmp/custom_workspaces"))

    assert root.input_text == "/tmp/workspaces"
    assert root.status_text == "Workspace folder: /tmp/workspaces"
    assert overridden.input_text == str(Path("/tmp/custom_workspaces").resolve())
    assert overridden.status_text == f"Workspace folder: {Path('/tmp/custom_workspaces').resolve()}"


def test_workspace_root_state_reports_unconfigured_workspace_collection_root():
    root = WorkspaceRootState(effective_root=None, configured=False)

    assert root.input_text == ""
    assert root.status_text == "Workspace folder is not configured."


def test_workspace_selection_state_tracks_current_and_discovered_workspaces():
    selection = WorkspaceSelectionState.from_paths(
        _sample_paths(),
        discovered_workspace_ids=("example_workspace", "claims2"),
    )

    assert selection.current_workspace_id == "example_workspace"
    assert selection.selector_enabled is True
    assert selection.selector_options == ("example_workspace", "claims2")


def test_workspace_session_state_rebinds_paths_and_keeps_override():
    session = WorkspaceSessionState.from_paths(
        _sample_paths(),
        override_root=Path("/tmp/custom_workspaces"),
        discovered_workspace_ids=("example_workspace",),
    )
    rebound_paths = _sample_paths().__class__(**{**_sample_paths().__dict__, "workspace_id": "claims2"})

    rebound = session.with_paths(rebound_paths).with_discovered_workspace_ids(("claims2",))

    assert rebound.workspace_collection_root_override == Path("/tmp/custom_workspaces").resolve()
    assert rebound.current_workspace_id == "claims2"
    assert rebound.discovered_workspace_ids == ("claims2",)
