"""Flow catalog loading services."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from data_engine.core.flow import Flow
from data_engine.core.helpers import _title_case_words
from data_engine.domain import FlowCatalogEntry, default_flow_state, flow_category
from data_engine.core.model import FlowValidationError
from data_engine.flow_modules.flow_module_loader import FlowModuleDefinition, discover_flow_module_definitions
from data_engine.platform.paths import path_display


def _flow_paths(flow: Flow) -> tuple[str, str]:
    trigger = flow.trigger
    source = getattr(trigger, "source", None) if trigger is not None else None
    target = getattr(flow.mirror_spec, "root", None)
    return path_display(source), path_display(target)


def _flow_interval(flow: Flow) -> str:
    trigger = flow.trigger
    if trigger is None:
        return "-"
    if getattr(trigger, "interval", None) is not None:
        return str(trigger.interval)
    times = getattr(trigger, "times", ())
    if times:
        return ", ".join(str(value) for value in times)
    if getattr(trigger, "time", None) is not None:
        return str(trigger.time)
    return "-"


def flow_catalog_entry_from_flow(flow: Flow, *, description: str | None) -> FlowCatalogEntry:
    source_root, target_root = _flow_paths(flow)
    operation_items = tuple(step.label for step in flow.steps)
    operations = " -> ".join(operation_items) or "(no steps)"
    mode = flow.mode
    derived_title = flow.label or _title_case_words(flow.name or "", empty="Flow")
    return FlowCatalogEntry(
        name=flow.name,
        group=flow.group,
        title=derived_title,
        description=description or "",
        source_root=source_root,
        target_root=target_root,
        mode=mode,
        interval=_flow_interval(flow),
        operations=operations,
        operation_items=operation_items,
        state=default_flow_state(mode),
        valid=True,
        category=flow_category(mode),
    )


def _invalid_entry(definition: FlowModuleDefinition, exc: Exception) -> FlowCatalogEntry:
    return FlowCatalogEntry(
        name=definition.name,
        group=None,
        title=_title_case_words(definition.name, empty="Flow"),
        description=definition.description or "",
        source_root="(not set)",
        target_root="(not set)",
        mode="manual",
        interval="-",
        operations="Unavailable",
        operation_items=(),
        state="invalid",
        valid=False,
        category="manual",
        error=str(exc),
    )


class FlowCatalogService:
    """Own flow catalog loading through an explicit discovery dependency."""

    def __init__(
        self,
        *,
        discover_definitions_func: Callable[..., tuple[FlowModuleDefinition, ...]] = discover_flow_module_definitions,
    ) -> None:
        self._discover_definitions = discover_definitions_func

    def load_entries(self, *, workspace_root: Path | None = None) -> tuple[FlowCatalogEntry, ...]:
        """Return discovered flow catalog entries for the requested workspace root."""
        entries: list[FlowCatalogEntry] = []
        definitions = self._discover_definitions(data_root=workspace_root)
        if not definitions:
            raise FlowValidationError("No flow modules discovered.")
        for definition in definitions:
            try:
                entries.append(flow_catalog_entry_from_flow(definition.build(), description=definition.description))
            except Exception as exc:
                entries.append(_invalid_entry(definition, exc))
        return tuple(sorted(entries, key=lambda entry: entry.name))


__all__ = ["FlowCatalogService", "flow_catalog_entry_from_flow"]
