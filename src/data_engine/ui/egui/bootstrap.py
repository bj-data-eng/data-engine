"""egui surface composition helpers.

This package currently shares the existing desktop service graph so we can swap
the UI package path independently from the runtime/control architecture.
"""

from data_engine.ui.gui.bootstrap import (
    GuiDependencyFactories,
    GuiServices,
    build_default_gui_services,
    build_gui_service_kwargs,
    build_gui_services,
    default_gui_service_kwargs,
)

EguiServices = GuiServices
EguiDependencyFactories = GuiDependencyFactories


def default_egui_service_kwargs(theme_name: str) -> dict[str, object]:
    """Return the shared default seam kwargs used by the egui surface."""
    return default_gui_service_kwargs(theme_name)


def build_egui_service_kwargs(**kwargs):
    """Build the common service bundle used by the egui bootstrap module."""
    return build_gui_service_kwargs(**kwargs)


def build_default_egui_services(**kwargs) -> EguiServices:
    """Build the default egui service set."""
    return build_default_gui_services(**kwargs)


def build_egui_services(**kwargs) -> EguiServices:
    """Build the concrete egui service set."""
    return build_gui_services(**kwargs)


__all__ = [
    "EguiDependencyFactories",
    "EguiServices",
    "build_default_egui_services",
    "build_egui_service_kwargs",
    "build_egui_services",
    "default_egui_service_kwargs",
]
