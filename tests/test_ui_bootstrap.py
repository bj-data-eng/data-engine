from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from data_engine.authoring.flow import Flow
from data_engine.domain import DaemonLifecyclePolicy
from data_engine.services import DaemonService, SettingsService, ThemeService, WorkspaceService
from data_engine.ui.gui.bootstrap import (
    GuiDependencyFactories,
    GuiServices,
    build_default_gui_services,
    build_gui_service_kwargs,
    build_gui_services,
    default_gui_service_kwargs,
)
from data_engine.ui.gui.app_binding import initial_window_size_for_screen
from data_engine.ui.tui.bootstrap import (
    TuiDependencyFactories,
    TuiServices,
    build_default_tui_services,
    build_tui_service_kwargs,
    build_tui_services,
    default_tui_service_kwargs,
)
from data_engine.platform.theme import GITHUB_DARK, GITHUB_LIGHT


@dataclass
class FakeSettingsStore:
    value: Path | str | None = None

    def workspace_collection_root(self) -> Path | None:
        return Path("/tmp/workspaces")

    def set_workspace_collection_root(self, value: Path | str | None) -> None:
        self.value = value


def _bundle_inputs():
    discovered = (SimpleNamespace(name="workspace-a"),)
    resolved = SimpleNamespace(workspace_id="example", workspace_root=Path("/tmp/workspace"))
    definition_flow = Flow(name="alpha", group="Claims")
    definition = SimpleNamespace(name="alpha", description="Example flow", build=lambda: definition_flow)
    loaded_flow = Flow(name="beta", group="Claims")
    requests: list[dict[str, object]] = []

    def discover_workspaces_func(*, app_root=None, workspace_collection_root=None):
        return discovered

    def resolve_workspace_paths_func(*, workspace_id=None, workspace_root=None, data_root=None, workspace_collection_root=None):
        return resolved

    def discover_definitions_func(*, data_root=None):
        return (definition,)

    def load_flow_func(name: str, *, data_root=None):
        return loaded_flow

    def spawn_process_func(paths, *, lifecycle_policy=DaemonLifecyclePolicy.PERSISTENT):
        return {"paths": paths, "policy": lifecycle_policy}

    def request_func(paths, payload, *, timeout=0.0):
        requests.append(payload)
        return {"ok": True, "payload": payload, "timeout": timeout}

    def is_live_func(paths):
        return True

    def resolve_theme_name_func(theme_name: str) -> str:
        return "light"

    def system_theme_name_func() -> str:
        return "dark"

    def toggle_theme_name_func(theme_name: str) -> str:
        return "dark" if theme_name == "light" else "light"

    def theme_button_text_func(theme_name: str) -> str:
        return f"Switch from {theme_name}"

    themes = {
        "dark": GITHUB_DARK,
        "light": GITHUB_LIGHT,
    }

    return {
        "discovered": discovered,
        "settings_store": FakeSettingsStore(),
        "workspace": SimpleNamespace(name="workspace"),
        "resolved": resolved,
        "definition": definition,
        "loaded_flow": loaded_flow,
        "discover_workspaces_func": discover_workspaces_func,
        "resolve_workspace_paths_func": resolve_workspace_paths_func,
        "discover_definitions_func": discover_definitions_func,
        "load_flow_func": load_flow_func,
        "spawn_process_func": spawn_process_func,
        "request_func": request_func,
        "is_live_func": is_live_func,
        "resolve_theme_name_func": resolve_theme_name_func,
        "system_theme_name_func": system_theme_name_func,
        "toggle_theme_name_func": toggle_theme_name_func,
        "theme_button_text_func": theme_button_text_func,
        "themes": themes,
        "requests": requests,
    }


def test_default_gui_service_kwargs_uses_gui_collaborators():
    kwargs = default_gui_service_kwargs("dark")

    assert "discover_workspaces_func" not in kwargs
    assert "resolve_workspace_paths_func" not in kwargs
    assert kwargs["discover_definitions_func"].__name__ == "discover_flow_module_definitions"
    assert kwargs["load_flow_func"].__name__ == "load_flow"
    assert kwargs["spawn_process_func"].__name__ == "spawn_daemon_process"
    assert kwargs["request_func"].__name__ == "daemon_request"
    assert kwargs["is_live_func"].__name__ == "is_daemon_live"
    assert kwargs["resolve_theme_name_func"].__name__ == "resolve_theme_name"


def test_default_tui_service_kwargs_uses_tui_collaborators():
    kwargs = default_tui_service_kwargs("dark")

    assert "discover_workspaces_func" not in kwargs
    assert "resolve_workspace_paths_func" not in kwargs
    assert kwargs["discover_definitions_func"].__name__ == "discover_flow_module_definitions"
    assert kwargs["load_flow_func"].__name__ == "load_flow"
    assert kwargs["spawn_process_func"].__name__ == "spawn_daemon_process"
    assert kwargs["request_func"].__name__ == "daemon_request"
    assert kwargs["is_live_func"].__name__ == "is_daemon_live"
    assert kwargs["resolve_theme_name_func"].__name__ == "resolve_theme_name"


def test_build_gui_service_kwargs_wires_real_service_objects():
    bundle = _bundle_inputs()

    services = build_gui_service_kwargs(
        settings_store=bundle["settings_store"],
        workspace_service=None,
        discover_workspaces_func=bundle["discover_workspaces_func"],
        resolve_workspace_paths_func=bundle["resolve_workspace_paths_func"],
        discover_definitions_func=bundle["discover_definitions_func"],
        load_flow_func=bundle["load_flow_func"],
        spawn_process_func=bundle["spawn_process_func"],
        request_func=bundle["request_func"],
        is_live_func=bundle["is_live_func"],
        resolve_theme_name_func=bundle["resolve_theme_name_func"],
        system_theme_name_func=bundle["system_theme_name_func"],
        toggle_theme_name_func=bundle["toggle_theme_name_func"],
        theme_button_text_func=bundle["theme_button_text_func"],
        themes=bundle["themes"],
        default_theme_name="light",
    )

    assert isinstance(services["settings_service"], SettingsService)
    assert isinstance(services["workspace_service"], WorkspaceService)
    assert isinstance(services["daemon_service"], DaemonService)
    assert isinstance(services["theme_service"], ThemeService)

    settings_service = services["settings_service"]
    assert settings_service.workspace_collection_root() == Path("/tmp/workspaces")
    settings_service.set_workspace_collection_root(Path("/tmp/override"))
    assert bundle["settings_store"].value == Path("/tmp/override")

    workspace_service = services["workspace_service"]
    assert workspace_service.discover(app_root=Path("/tmp/app")) == bundle["discovered"]
    assert workspace_service.resolve_paths(workspace_id="example") is bundle["resolved"]

    flow_catalog_service = services["flow_catalog_service"]
    entries = flow_catalog_service.load_entries(workspace_root=Path("/tmp/workspace"))
    assert entries[0].name == "alpha"
    assert entries[0].title == "Alpha"

    flow_execution_service = services["flow_execution_service"]
    assert flow_execution_service.load_flow("beta", workspace_root=Path("/tmp/workspace")) is bundle["loaded_flow"]

    daemon_service = services["daemon_service"]
    assert daemon_service.spawn(bundle["resolved"], lifecycle_policy=DaemonLifecyclePolicy.EPHEMERAL)["policy"] == DaemonLifecyclePolicy.EPHEMERAL
    assert daemon_service.request(bundle["resolved"], {"command": "ping"}, timeout=2.5)["timeout"] == 2.5
    assert daemon_service.is_live(bundle["resolved"]) is True

    theme_service = services["theme_service"]
    assert theme_service.resolve_name("system") == "light"
    assert theme_service.system_name() == "dark"
    assert theme_service.toggle_name("light") == "dark"
    assert theme_service.button_text("light") == "Switch from light"
    assert theme_service.palette("light").name == "light"


def test_build_tui_service_kwargs_wires_real_service_objects():
    bundle = _bundle_inputs()

    services = build_tui_service_kwargs(
        settings_store=bundle["settings_store"],
        discover_workspaces_func=bundle["discover_workspaces_func"],
        resolve_workspace_paths_func=bundle["resolve_workspace_paths_func"],
        discover_definitions_func=bundle["discover_definitions_func"],
        load_flow_func=bundle["load_flow_func"],
        spawn_process_func=bundle["spawn_process_func"],
        request_func=bundle["request_func"],
        is_live_func=bundle["is_live_func"],
        resolve_theme_name_func=bundle["resolve_theme_name_func"],
        system_theme_name_func=bundle["system_theme_name_func"],
        toggle_theme_name_func=bundle["toggle_theme_name_func"],
        theme_button_text_func=bundle["theme_button_text_func"],
        themes=bundle["themes"],
        default_theme_name="light",
    )

    assert isinstance(services["settings_service"], SettingsService)
    assert isinstance(services["workspace_service"], WorkspaceService)
    assert isinstance(services["daemon_service"], DaemonService)
    assert isinstance(services["theme_service"], ThemeService)
    assert services["workspace_service"].resolve_paths(workspace_id="example") is bundle["resolved"]


def test_build_default_gui_services_uses_injected_factories():
    calls: list[str] = []

    class _Store:
        def workspace_collection_root(self):
            return Path("/tmp/workspaces")

        def set_workspace_collection_root(self, value):
            del value

    factories = GuiDependencyFactories(
        settings_store_factory=lambda app_root: calls.append("store") or _Store(),
        settings_service_factory=lambda store: calls.append("settings") or ("settings-service", store),
        workspace_service_factory=lambda discover, resolve: calls.append("workspace") or ("workspace-service", discover, resolve),
        workspace_session_application_factory=lambda workspace_service: ("workspace-session", workspace_service),
        action_state_application_factory=lambda: "action-state",
        detail_application_factory=lambda: "detail",
        flow_catalog_service_factory=lambda discover_definitions: ("flow-catalog-service", discover_definitions),
        flow_catalog_application_factory=lambda flow_catalog_service: ("flow-catalog-app", flow_catalog_service),
        flow_execution_service_factory=lambda load_flow_func: ("flow-exec", load_flow_func),
        daemon_service_factory=lambda spawn, request, is_live, error_type: ("daemon", spawn, request, is_live, error_type),
        daemon_state_service_factory=lambda: "daemon-state",
        ledger_service_factory=lambda: "ledger",
        log_service_factory=lambda: "log",
        runtime_binding_service_factory=lambda ledger_service, log_service, daemon_state_service, runtime_history_service: (
            "runtime-binding",
            ledger_service,
            log_service,
            daemon_state_service,
            runtime_history_service,
        ),
        runtime_state_service_factory=lambda runtime_binding_service, log_service: (
            "runtime-state",
            runtime_binding_service,
            log_service,
        ),
        runtime_history_service_factory=lambda: "runtime-history",
        reset_service_factory=lambda shared_state_service: ("reset-service", shared_state_service),
        shared_state_service_factory=lambda: "shared-state",
        runtime_application_factory=lambda daemon_service, daemon_state_service, shared_state_service: (
            "runtime-app",
            daemon_service,
            daemon_state_service,
            shared_state_service,
        ),
        control_application_factory=lambda runtime_application, daemon_state_service: (
            "control-app",
            runtime_application,
            daemon_state_service,
        ),
        theme_service_factory=lambda themes, default_theme_name, resolve, system, toggle, button_text: (
            "theme",
            default_theme_name,
        ),
    )

    services = build_default_gui_services(factories=factories)

    assert calls == ["store", "settings", "workspace"]
    assert services.settings_service[0] == "settings-service"
    assert services.workspace_service[0] == "workspace-service"
    assert services.ledger_service == "ledger"
    assert services.runtime_binding_service[0] == "runtime-binding"
    assert services.runtime_application[0] == "runtime-app"


def test_build_default_tui_services_uses_injected_factories():
    calls: list[str] = []

    class _Store:
        def workspace_collection_root(self):
            return Path("/tmp/workspaces")

        def set_workspace_collection_root(self, value):
            del value

    factories = TuiDependencyFactories(
        settings_store_factory=lambda app_root: calls.append("store") or _Store(),
        settings_service_factory=lambda store: calls.append("settings") or ("settings-service", store),
        workspace_service_factory=lambda discover, resolve: calls.append("workspace") or ("workspace-service", discover, resolve),
        workspace_session_application_factory=lambda workspace_service: ("workspace-session", workspace_service),
        action_state_application_factory=lambda: "action-state",
        detail_application_factory=lambda: "detail",
        flow_catalog_service_factory=lambda discover_definitions: ("flow-catalog-service", discover_definitions),
        flow_catalog_application_factory=lambda flow_catalog_service: ("flow-catalog-app", flow_catalog_service),
        flow_execution_service_factory=lambda load_flow_func: ("flow-exec", load_flow_func),
        daemon_service_factory=lambda spawn, request, is_live, error_type: ("daemon", spawn, request, is_live, error_type),
        daemon_state_service_factory=lambda: "daemon-state",
        ledger_service_factory=lambda: "ledger",
        log_service_factory=lambda: "log",
        runtime_binding_service_factory=lambda ledger_service, log_service, daemon_state_service, runtime_history_service: (
            "runtime-binding",
            ledger_service,
            log_service,
            daemon_state_service,
            runtime_history_service,
        ),
        runtime_state_service_factory=lambda runtime_binding_service, log_service: (
            "runtime-state",
            runtime_binding_service,
            log_service,
        ),
        runtime_history_service_factory=lambda: "runtime-history",
        reset_service_factory=lambda shared_state_service: ("reset-service", shared_state_service),
        shared_state_service_factory=lambda: "shared-state",
        runtime_application_factory=lambda daemon_service, daemon_state_service, shared_state_service: (
            "runtime-app",
            daemon_service,
            daemon_state_service,
            shared_state_service,
        ),
        control_application_factory=lambda runtime_application, daemon_state_service: (
            "control-app",
            runtime_application,
            daemon_state_service,
        ),
        theme_service_factory=lambda themes, default_theme_name, resolve, system, toggle, button_text: (
            "theme",
            default_theme_name,
        ),
    )

    services = build_default_tui_services(factories=factories)

    assert calls == ["store", "settings", "workspace"]
    assert services.settings_service[0] == "settings-service"
    assert services.workspace_service[0] == "workspace-service"
    assert services.ledger_service == "ledger"
    assert services.runtime_binding_service[0] == "runtime-binding"
    assert services.runtime_application[0] == "runtime-app"


def test_build_default_gui_and_tui_services_use_separate_surface_roots():
    bundle = _bundle_inputs()

    gui_services = build_default_gui_services(
        settings_store=bundle["settings_store"],
        discover_workspaces_func=bundle["discover_workspaces_func"],
        resolve_workspace_paths_func=bundle["resolve_workspace_paths_func"],
        discover_definitions_func=bundle["discover_definitions_func"],
        load_flow_func=bundle["load_flow_func"],
        spawn_process_func=bundle["spawn_process_func"],
        request_func=bundle["request_func"],
        is_live_func=bundle["is_live_func"],
        resolve_theme_name_func=bundle["resolve_theme_name_func"],
        system_theme_name_func=bundle["system_theme_name_func"],
        toggle_theme_name_func=bundle["toggle_theme_name_func"],
        theme_button_text_func=bundle["theme_button_text_func"],
        themes=bundle["themes"],
        default_theme_name="dark",
    )
    tui_services = build_default_tui_services(
        settings_store=bundle["settings_store"],
        discover_workspaces_func=bundle["discover_workspaces_func"],
        resolve_workspace_paths_func=bundle["resolve_workspace_paths_func"],
        discover_definitions_func=bundle["discover_definitions_func"],
        load_flow_func=bundle["load_flow_func"],
        spawn_process_func=bundle["spawn_process_func"],
        request_func=bundle["request_func"],
        is_live_func=bundle["is_live_func"],
        resolve_theme_name_func=bundle["resolve_theme_name_func"],
        system_theme_name_func=bundle["system_theme_name_func"],
        toggle_theme_name_func=bundle["toggle_theme_name_func"],
        theme_button_text_func=bundle["theme_button_text_func"],
        themes=bundle["themes"],
        default_theme_name="light",
    )

    assert isinstance(gui_services, GuiServices)
    assert isinstance(tui_services, TuiServices)
    assert gui_services.theme_service.default_theme_name == "dark"
    assert tui_services.theme_service.default_theme_name == "light"
    assert gui_services.settings_service.workspace_collection_root() == tui_services.settings_service.workspace_collection_root()
    assert gui_services.daemon_service.spawn(bundle["resolved"])["policy"] == DaemonLifecyclePolicy.PERSISTENT


def test_build_gui_and_tui_services_wrap_the_separate_default_roots():
    bundle = _bundle_inputs()

    gui_services = build_gui_services(
        settings_store=bundle["settings_store"],
        discover_workspaces_func=bundle["discover_workspaces_func"],
        resolve_workspace_paths_func=bundle["resolve_workspace_paths_func"],
        discover_definitions_func=bundle["discover_definitions_func"],
        load_flow_func=bundle["load_flow_func"],
        spawn_process_func=bundle["spawn_process_func"],
        request_func=bundle["request_func"],
        is_live_func=bundle["is_live_func"],
        resolve_theme_name_func=bundle["resolve_theme_name_func"],
        system_theme_name_func=bundle["system_theme_name_func"],
        toggle_theme_name_func=bundle["toggle_theme_name_func"],
        theme_button_text_func=bundle["theme_button_text_func"],
        themes=bundle["themes"],
        default_theme_name="dark",
    )
    tui_services = build_tui_services(
        settings_store=bundle["settings_store"],
        discover_workspaces_func=bundle["discover_workspaces_func"],
        resolve_workspace_paths_func=bundle["resolve_workspace_paths_func"],
        discover_definitions_func=bundle["discover_definitions_func"],
        load_flow_func=bundle["load_flow_func"],
        spawn_process_func=bundle["spawn_process_func"],
        request_func=bundle["request_func"],
        is_live_func=bundle["is_live_func"],
        resolve_theme_name_func=bundle["resolve_theme_name_func"],
        system_theme_name_func=bundle["system_theme_name_func"],
        toggle_theme_name_func=bundle["toggle_theme_name_func"],
        theme_button_text_func=bundle["theme_button_text_func"],
        themes=bundle["themes"],
        default_theme_name="light",
    )

    assert isinstance(gui_services, GuiServices)
    assert isinstance(tui_services, TuiServices)
    assert gui_services.theme_service.default_theme_name == "dark"
    assert tui_services.theme_service.default_theme_name == "light"


class _FakeGeometry:
    def __init__(self, width: int, height: int) -> None:
        self._width = width
        self._height = height

    def width(self) -> int:
        return self._width

    def height(self) -> int:
        return self._height


class _FakeScreen:
    def __init__(self, width: int, height: int) -> None:
        self._geometry = _FakeGeometry(width, height)

    def availableGeometry(self) -> _FakeGeometry:
        return self._geometry


def test_initial_window_size_for_screen_uses_screen_percentages():
    assert initial_window_size_for_screen(_FakeScreen(1920, 1080)) == (1497, 907)


def test_initial_window_size_for_screen_clamps_to_screen_bounds():
    assert initial_window_size_for_screen(_FakeScreen(1280, 800)) == (1180, 760)


def test_initial_window_size_for_screen_falls_back_without_geometry():
    assert initial_window_size_for_screen(None) == (1480, 920)
