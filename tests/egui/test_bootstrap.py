from __future__ import annotations

from data_engine.ui.egui import (
    EguiDependencyFactories,
    EguiServices,
    build_default_egui_services,
    build_egui_service_kwargs,
    build_egui_services,
    default_egui_service_kwargs,
)
from data_engine.ui.gui.bootstrap import GuiDependencyFactories, GuiServices


def test_egui_bootstrap_exports_parallel_gui_bootstrap_contract():
    assert EguiServices is GuiServices
    assert EguiDependencyFactories is GuiDependencyFactories


def test_default_egui_service_kwargs_match_gui_surface_contract():
    kwargs = default_egui_service_kwargs("dark")

    assert kwargs["discover_definitions_func"].__name__ == "discover_flow_module_definitions"
    assert kwargs["load_flow_func"].__name__ == "load_flow"
    assert kwargs["spawn_process_func"].__name__ == "spawn_daemon_process"
    assert kwargs["request_func"].__name__ == "daemon_request"
    assert kwargs["is_live_func"].__name__ == "is_daemon_live"


def test_egui_bootstrap_builders_delegate_to_gui_bootstrap():
    gui_kwargs = build_egui_service_kwargs()
    gui_services = build_default_egui_services()
    explicit_services = build_egui_services()

    assert isinstance(gui_kwargs, dict)
    assert isinstance(gui_services, GuiServices)
    assert isinstance(explicit_services, GuiServices)
