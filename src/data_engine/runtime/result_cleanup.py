"""Helpers for releasing completed runtime results."""

from __future__ import annotations

import gc

from data_engine.core.primitives import FlowContext, WorkspaceConfigContext


def release_context_values(context: FlowContext) -> None:
    """Drop run-owned references once a completed context is no longer needed."""
    context.source = None
    context.mirror = None
    context.current = None
    context.objects.clear()
    context.metadata.clear()
    context.config = WorkspaceConfigContext()
    context.debug = None
    gc.collect()


def release_completed_results(result: object) -> object:
    """Drop bulky references from completed flow results when callers do not need them."""
    if not isinstance(result, list) or not all(isinstance(item, FlowContext) for item in result):
        return result
    for context in result:
        release_context_values(context)
    return result


__all__ = ["release_context_values", "release_completed_results"]
