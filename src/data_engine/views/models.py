"""Shared UI-facing card models and small display helpers across Data Engine surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from data_engine.domain import FlowCatalogEntry, default_flow_state, flow_category

if TYPE_CHECKING:
    from data_engine.services.flow_catalog import FlowCatalogService


@dataclass(frozen=True)
class QtFlowCard:
    """Display model for one discovered flow."""

    name: str
    group: str | None
    title: str
    description: str
    source_root: str
    target_root: str
    mode: str
    interval: str
    operations: str
    operation_items: tuple[str, ...]
    state: str
    valid: bool
    category: str
    error: str = ""

def qt_flow_card_from_entry(entry: FlowCatalogEntry) -> QtFlowCard:
    """Map one catalog entry into a shared surface card."""
    return QtFlowCard(
        name=entry.name,
        group=entry.group,
        title=entry.title,
        description=entry.description,
        source_root=entry.source_root,
        target_root=entry.target_root,
        mode=entry.mode,
        interval=entry.interval,
        operations=entry.operations,
        operation_items=entry.operation_items,
        state=entry.state,
        valid=entry.valid,
        category=entry.category,
        error=entry.error,
    )


def flow_catalog_entry_from_qt_card(card: QtFlowCard) -> FlowCatalogEntry:
    """Map one shared surface card back into a catalog entry."""
    return FlowCatalogEntry(
        name=card.name,
        group=card.group,
        title=card.title,
        description=card.description,
        source_root=card.source_root,
        target_root=card.target_root,
        mode=card.mode,
        interval=card.interval,
        operations=card.operations,
        operation_items=card.operation_items,
        state=card.state,
        valid=card.valid,
        category=card.category,
        error=card.error,
    )


def qt_flow_cards_from_entries(entries: tuple[FlowCatalogEntry, ...] | list[FlowCatalogEntry]) -> tuple[QtFlowCard, ...]:
    """Map discovered catalog entries into shared surface cards."""
    return tuple(qt_flow_card_from_entry(entry) for entry in entries)


def load_qt_flow_cards(
    flow_catalog_service: "FlowCatalogService",
    *,
    workspace_root: Path | None = None,
) -> tuple[QtFlowCard, ...]:
    """Load discovered catalog entries and map them into shared surface cards."""
    return qt_flow_cards_from_entries(flow_catalog_service.load_entries(workspace_root=workspace_root))


__all__ = [
    "QtFlowCard",
    "default_flow_state",
    "flow_category",
    "flow_catalog_entry_from_qt_card",
    "load_qt_flow_cards",
    "qt_flow_card_from_entry",
    "qt_flow_cards_from_entries",
]
