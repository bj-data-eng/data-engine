from __future__ import annotations

from pathlib import Path

from data_engine.application import WorkspaceSessionApplication
from data_engine.platform.workspace_models import DiscoveredWorkspace
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


class _FakeWorkspaceService:
    def __init__(self, discovered: tuple[DiscoveredWorkspace, ...]) -> None:
        self.discovered = discovered
        self.calls: list[tuple[Path, Path | None]] = []

    def discover(
        self,
        *,
        app_root: Path | None = None,
        workspace_collection_root: Path | None = None,
    ) -> tuple[DiscoveredWorkspace, ...]:
        assert app_root is not None
        self.calls.append((app_root, workspace_collection_root))
        return self.discovered


def test_workspace_session_application_refreshes_discovery(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    collection_root = tmp_path / "workspaces"
    workspace_root = collection_root / "alpha"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    discovered = (
        DiscoveredWorkspace(workspace_id="alpha", workspace_root=workspace_root),
        DiscoveredWorkspace(workspace_id="beta", workspace_root=collection_root / "beta"),
    )
    service = _FakeWorkspaceService(discovered)

    session = WorkspaceSessionApplication(workspace_service=service).refresh_session(
        workspace_paths=paths,
        override_root=collection_root,
    )

    assert service.calls == [(paths.app_root, collection_root)]
    assert session.current_workspace_id == "alpha"
    assert session.discovered_workspace_ids == ("alpha", "beta")
    assert session.workspace_collection_root_override == collection_root


def test_workspace_session_application_binds_fresh_operator_state(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    collection_root = tmp_path / "workspaces"
    workspace_root = collection_root / "alpha"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    discovered = (DiscoveredWorkspace(workspace_id="alpha", workspace_root=workspace_root),)
    service = _FakeWorkspaceService(discovered)

    binding = WorkspaceSessionApplication(workspace_service=service).bind_workspace(
        workspace_paths=paths,
        override_root=collection_root,
    )

    assert binding.workspace_session.current_workspace_id == "alpha"
    assert binding.workspace_session.discovered_workspace_ids == ("alpha",)
    assert binding.operator_session.workspace == binding.workspace_session
    assert binding.operator_session.runtime.runtime_active is False
    assert binding.operator_session.catalog.entries == ()
