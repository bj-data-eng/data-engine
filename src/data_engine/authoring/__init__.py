"""Authoring DSL and core flow model primitives."""

from __future__ import annotations

from importlib import import_module

__all__ = ["Batch", "FileRef", "Flow", "FlowContext", "discover_flows", "load_flow", "run"]


def __getattr__(name: str):
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    builder = import_module("data_engine.authoring.builder")
    return getattr(builder, name)
