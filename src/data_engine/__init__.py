"""Top-level package for the Data Engine workbook runtime."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_engine.authoring.builder import Batch
    from data_engine.authoring.builder import FileRef
    from data_engine.authoring.builder import Flow
    from data_engine.authoring.builder import FlowContext
    from data_engine.authoring.builder import discover_flows, load_flow, run

__all__ = ["Batch", "FileRef", "Flow", "FlowContext", "discover_flows", "load_flow", "run"]


def __getattr__(name: str):
    """Lazy-load runtime symbols so lightweight helpers can import package submodules safely."""
    if name in {"Batch", "FileRef", "Flow", "FlowContext", "discover_flows", "load_flow", "run"}:
        from data_engine.authoring.builder import Batch
        from data_engine.authoring.builder import FileRef
        from data_engine.authoring.builder import Flow
        from data_engine.authoring.builder import FlowContext
        from data_engine.authoring.builder import discover_flows
        from data_engine.authoring.builder import load_flow
        from data_engine.authoring.builder import run

        return {
            "Batch": Batch,
            "FileRef": FileRef,
            "Flow": Flow,
            "FlowContext": FlowContext,
            "discover_flows": discover_flows,
            "load_flow": load_flow,
            "run": run,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
