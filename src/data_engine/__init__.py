"""Top-level package for the Data Engine workbook runtime."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_engine.authoring.flow import Flow
    from data_engine.authoring.flow import discover_flows, load_flow, run
    from data_engine.authoring.primitives import Batch
    from data_engine.authoring.primitives import FileRef
    from data_engine.authoring.primitives import FlowContext

__all__ = ["Batch", "FileRef", "Flow", "FlowContext", "discover_flows", "load_flow", "run"]


def __getattr__(name: str):
    """Lazy-load runtime symbols so lightweight helpers can import package submodules safely."""
    if name in {"Batch", "FileRef", "Flow", "FlowContext", "discover_flows", "load_flow", "run"}:
        from data_engine.authoring.flow import Flow
        from data_engine.authoring.flow import discover_flows
        from data_engine.authoring.flow import load_flow
        from data_engine.authoring.flow import run
        from data_engine.authoring.primitives import Batch
        from data_engine.authoring.primitives import FileRef
        from data_engine.authoring.primitives import FlowContext

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
