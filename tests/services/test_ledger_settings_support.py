from __future__ import annotations

from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.theme import GITHUB_DARK, GITHUB_LIGHT
from data_engine.platform.workspace_models import DiscoveredWorkspace
from data_engine.services.ledger import LedgerService
from data_engine.services.settings import SettingsService
from data_engine.services.shared_state import SharedStateService
from data_engine.services.workspace_io import WorkspaceIoLayer
from data_engine.services.theme import ThemeService
from data_engine.services.workspaces import WorkspaceService

from tests.services.support import resolve_workspace_paths


def test_ledger_service_delegates_to_runtime_ledger(tmp_path):
    class _Ledger:
        def __init__(self):
            self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
            self.client_sessions = self

        def close(self):
            self.calls.append(("close", (), {}))

        def upsert(self, **kwargs):
            self.calls.append(("upsert", (), kwargs))

        def remove(self, client_id):
            self.calls.append(("remove", (client_id,), {}))

        def remove_for_process(self, **kwargs):
            self.calls.append(("purge", (), kwargs))

        def count_live(self, workspace_id, *, exclude_client_id=None):
            self.calls.append(("count", (workspace_id,), {"exclude_client_id": exclude_client_id}))
            return 3 if exclude_client_id is None else 2

    service = LedgerService()
    ledger = _Ledger()

    service.close(ledger)
    service.register_client_session(
        ledger,
        client_id="abc",
        workspace_id="workspace",
        client_kind="ui",
        pid=123,
    )
    service.remove_client_session(ledger, "abc")
    service.purge_process_client_sessions(
        ledger,
        workspace_id="workspace",
        client_kind="ui",
        pid=123,
    )

    assert service.count_live_client_sessions(ledger, "workspace") == 3
    assert service.count_live_client_sessions(ledger, "workspace", exclude_client_id="abc") == 2
    assert ledger.calls == [
        ("close", (), {}),
        ("upsert", (), {"client_id": "abc", "workspace_id": "workspace", "client_kind": "ui", "pid": 123}),
        ("remove", ("abc",), {}),
        ("purge", (), {"workspace_id": "workspace", "client_kind": "ui", "pid": 123}),
        ("count", ("workspace",), {"exclude_client_id": None}),
        ("count", ("workspace",), {"exclude_client_id": "abc"}),
    ]


def test_ledger_service_opens_workspace_ledgers_through_injected_collaborator(tmp_path):
    workspace_root = tmp_path / "workspace"
    calls: list[object] = []
    ledger = object()
    service = LedgerService(open_ledger_func=lambda root: calls.append(root) or ledger)

    opened = service.open_for_workspace(workspace_root)

    assert opened is ledger
    assert calls == [workspace_root.resolve()]


def test_ledger_service_default_open_uses_runtime_layout_policy(tmp_path):
    expected_workspace_root = tmp_path / "workspace"
    expected_db_path = tmp_path / "runtime" / "runtime_control.sqlite"

    class _Policy:
        def resolve_paths(self, *, workspace_root=None, **kwargs):
            assert kwargs == {}
            assert workspace_root == expected_workspace_root.resolve()

            class _Paths:
                runtime_control_db_path = expected_db_path

            return _Paths()

    service = LedgerService(runtime_layout_policy=_Policy())

    ledger = service.open_for_workspace(expected_workspace_root)
    try:
        assert ledger.db_path == expected_db_path.resolve()
    finally:
        ledger.close()


def test_settings_service_reads_and_persists_workspace_collection_root(tmp_path):
    store = LocalSettingsStore(tmp_path / "app_settings.sqlite")
    service = SettingsService(store)

    assert service.workspace_collection_root() is None

    target = tmp_path / "workspaces"
    service.set_workspace_collection_root(target)
    assert service.workspace_collection_root() == target

    reopened = SettingsService.open_default(app_root=tmp_path / "app")
    assert isinstance(reopened, SettingsService)


def test_settings_service_open_default_uses_injected_store_factory(tmp_path):
    calls: list[object | None] = []
    store = LocalSettingsStore(tmp_path / "settings.sqlite")

    opened = SettingsService.open_default(
        app_root=tmp_path / "app",
        store_factory=lambda app_root: calls.append(app_root) or store,
    )

    assert isinstance(opened, SettingsService)
    assert opened.workspace_collection_root() is None
    assert calls == [tmp_path / "app"]


def test_shared_state_service_hydrates_local_runtime(monkeypatch, tmp_path):
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    calls: list[tuple[object, object]] = []
    ledger = object()
    workspace_io = WorkspaceIoLayer()
    monkeypatch.setattr(
        workspace_io,
        "hydrate_local_runtime",
        lambda paths_arg, ledger_arg: calls.append((paths_arg, ledger_arg)),
    )

    SharedStateService(workspace_io=workspace_io).hydrate_local_runtime(paths, ledger)

    assert calls == [(paths, ledger)]


def test_theme_service_resolves_palette_and_labels():
    service = ThemeService(
        themes={"light": GITHUB_LIGHT, "dark": GITHUB_DARK},
        resolve_theme_name_func=lambda name: "dark" if name == "system" else name,
        system_theme_name_func=lambda: "dark",
        toggle_theme_name_func=lambda name: "light" if name == "dark" else "dark",
        theme_button_text_func=lambda name: f"toggle {name}",
    )

    assert service.resolve_name("system") == "dark"
    assert service.system_name() == "dark"
    assert service.toggle_name("dark") == "light"
    assert service.button_text("dark") == "toggle dark"
    assert service.palette("system") is GITHUB_DARK


def test_workspace_service_forwards_discovery_and_resolution(tmp_path):
    calls: list[tuple[str, object]] = []
    expected_paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    discovered = (DiscoveredWorkspace(workspace_id="alpha", workspace_root=tmp_path / "workspace"),)

    service = WorkspaceService(
        discover_workspaces_func=lambda **kwargs: calls.append(("discover", kwargs)) or discovered,
        resolve_workspace_paths_func=lambda **kwargs: calls.append(("resolve", kwargs)) or expected_paths,
    )

    assert service.discover(app_root=tmp_path / "app", workspace_collection_root=tmp_path / "collection") == discovered
    assert service.resolve_paths(workspace_id="alpha", workspace_root=tmp_path / "workspace") == expected_paths
    assert calls == [
        ("discover", {"app_root": tmp_path / "app", "workspace_collection_root": tmp_path / "collection"}),
        ("resolve", {"workspace_id": "alpha", "workspace_root": tmp_path / "workspace", "data_root": None, "workspace_collection_root": None}),
    ]
