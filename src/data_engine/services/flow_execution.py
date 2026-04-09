"""Executable flow loading services."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from data_engine.flow_modules.flow_module_loader import discover_flow_module_definitions, load_flow_module_definition

if TYPE_CHECKING:
    from data_engine.authoring.flow import Flow


def _default_load_flow(name: str, *, data_root: Path | None = None) -> "Flow":
    return load_flow_module_definition(name, data_root=data_root).build()


def _default_discover_flows(*, data_root: Path | None = None) -> tuple["Flow", ...]:
    return tuple(definition.build() for definition in discover_flow_module_definitions(data_root=data_root))


class FlowExecutionService:
    """Own executable flow loading through an explicit loader dependency."""

    def __init__(
        self,
        *,
        load_flow_func: Callable[..., "Flow"] = _default_load_flow,
        discover_flows_func: Callable[..., tuple["Flow", ...]] = _default_discover_flows,
    ) -> None:
        self._load_flow = load_flow_func
        self._discover_flows = discover_flows_func

    def load_flow(self, name: str, *, workspace_root: Path | None = None) -> "Flow":
        """Return one executable flow definition by name."""
        return self._load_flow(name, data_root=workspace_root)

    def load_flows(self, names: tuple[str, ...], *, workspace_root: Path | None = None) -> tuple["Flow", ...]:
        """Return executable flow definitions for the requested names."""
        return tuple(self.load_flow(name, workspace_root=workspace_root) for name in names)

    def discover_flows(self, *, workspace_root: Path | None = None) -> tuple["Flow", ...]:
        """Return all executable flow definitions for the requested workspace root."""
        return self._discover_flows(data_root=workspace_root)


__all__ = ["FlowExecutionService"]
