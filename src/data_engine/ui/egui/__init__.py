"""Rust-backed egui surface package for Data Engine."""

from data_engine.domain import format_log_line
from data_engine.views import QtFlowCard, flow_category

__all__ = [
    "EguiDependencyFactories",
    "EguiServices",
    "QtFlowCard",
    "build_default_egui_services",
    "build_egui_service_kwargs",
    "build_egui_services",
    "default_egui_service_kwargs",
    "flow_category",
    "format_log_line",
    "launch",
    "main",
]


def __getattr__(name: str):
    if name in {
        "EguiDependencyFactories",
        "EguiServices",
        "build_default_egui_services",
        "build_egui_service_kwargs",
        "build_egui_services",
        "default_egui_service_kwargs",
    }:
        from data_engine.ui.egui.bootstrap import (
            EguiDependencyFactories,
            EguiServices,
            build_default_egui_services,
            build_egui_service_kwargs,
            build_egui_services,
            default_egui_service_kwargs,
        )

        return {
            "EguiDependencyFactories": EguiDependencyFactories,
            "EguiServices": EguiServices,
            "build_default_egui_services": build_default_egui_services,
            "build_egui_service_kwargs": build_egui_service_kwargs,
            "build_egui_services": build_egui_services,
            "default_egui_service_kwargs": default_egui_service_kwargs,
        }[name]
    if name in {"launch", "main"}:
        from data_engine.ui.egui.launcher import launch, main

        return {"launch": launch, "main": main}[name]
    raise AttributeError(name)
