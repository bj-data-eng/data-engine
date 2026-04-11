"""State helpers for the terminal UI surface."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_engine.domain import (
    FlowCatalogState,
    OperationSessionState,
    OperatorSessionState,
    RuntimeSessionState,
    WorkspaceControlState,
    WorkspaceSessionState,
)
from data_engine.views.models import QtFlowCard, flow_catalog_entry_from_qt_card, qt_flow_cards_from_entries

if TYPE_CHECKING:
    from data_engine.ui.tui.app import DataEngineTui


class TuiStateMixin:
    """Session-state helpers separated from the main TUI shell."""

    @property
    def log_store(self: "DataEngineTui"):
        """Expose the current runtime binding log store."""
        return self.runtime_binding.log_store

    @property
    def _daemon_manager(self: "DataEngineTui"):
        """Expose the current runtime binding daemon manager."""
        return self.runtime_binding.daemon_manager

    @property
    def runtime_session(self: "DataEngineTui") -> RuntimeSessionState:
        """Return the current terminal runtime/control session state."""
        return self._operator_session_state.runtime

    @runtime_session.setter
    def runtime_session(self: "DataEngineTui", value: RuntimeSessionState) -> None:
        self._operator_session_state = self._operator_session_state.with_runtime(value)

    @property
    def flow_catalog_state(self: "DataEngineTui") -> FlowCatalogState:
        """Return the current discovered flow catalog state."""
        return self._operator_session_state.catalog

    @flow_catalog_state.setter
    def flow_catalog_state(self: "DataEngineTui", value: FlowCatalogState) -> None:
        self._operator_session_state = self._operator_session_state.with_catalog(value)

    @property
    def flow_cards(self: "DataEngineTui") -> tuple[QtFlowCard, ...]:
        return qt_flow_cards_from_entries(self.flow_catalog_state.entries)

    @flow_cards.setter
    def flow_cards(self: "DataEngineTui", value: tuple[QtFlowCard, ...]) -> None:
        self.flow_catalog_state = self.flow_catalog_state.with_entries(
            tuple(flow_catalog_entry_from_qt_card(card) for card in value)
        )

    @property
    def flow_states(self: "DataEngineTui") -> dict[str, str]:
        return self.flow_catalog_state.flow_states or {}

    @flow_states.setter
    def flow_states(self: "DataEngineTui", value: dict[str, str]) -> None:
        self.flow_catalog_state = self.flow_catalog_state.with_flow_states(value)

    @property
    def selected_flow_name(self: "DataEngineTui") -> str | None:
        return self.flow_catalog_state.selected_flow_name

    @selected_flow_name.setter
    def selected_flow_name(self: "DataEngineTui", value: str | None) -> None:
        self.flow_catalog_state = self.flow_catalog_state.with_selected_flow_name(value)

    @property
    def workspace_control_state(self: "DataEngineTui") -> WorkspaceControlState:
        """Return the current structured workspace control state."""
        return self._operator_session_state.workspace_control

    @workspace_control_state.setter
    def workspace_control_state(self: "DataEngineTui", value: WorkspaceControlState) -> None:
        self._operator_session_state = self._operator_session_state.with_workspace_control(value)

    @property
    def workspace_session_state(self: "DataEngineTui") -> WorkspaceSessionState:
        """Return the current workspace selection/root session state."""
        return self._operator_session_state.workspace

    @workspace_session_state.setter
    def workspace_session_state(self: "DataEngineTui", value: WorkspaceSessionState) -> None:
        self._operator_session_state = self._operator_session_state.with_workspace(value)

    @property
    def operator_session_state(self: "DataEngineTui") -> OperatorSessionState:
        """Return the top-level operator session state for this surface."""
        return self._operator_session_state

    @property
    def operation_tracker(self: "DataEngineTui") -> OperationSessionState:
        """Return the current operation/step session state."""
        return self._operator_session_state.operations

    @operation_tracker.setter
    def operation_tracker(self: "DataEngineTui", value: OperationSessionState) -> None:
        self._operator_session_state = self._operator_session_state.with_operations(value)

    @property
    def workspace_collection_root_override(self: "DataEngineTui") -> Path | None:
        return self.workspace_session_state.workspace_collection_root_override

    @workspace_collection_root_override.setter
    def workspace_collection_root_override(self: "DataEngineTui", value: Path | None) -> None:
        self.workspace_session_state = self.workspace_session_state.with_override_root(value)

    @property
    def discovered_workspace_ids(self: "DataEngineTui") -> tuple[str, ...]:
        return self.workspace_session_state.discovered_workspace_ids

    @discovered_workspace_ids.setter
    def discovered_workspace_ids(self: "DataEngineTui", value: tuple[str, ...]) -> None:
        self.workspace_session_state = self.workspace_session_state.with_discovered_workspace_ids(value)

    def _selected_card(self: "DataEngineTui") -> QtFlowCard | None:
        if self.selected_flow_name is None:
            return None
        for card in self.flow_cards:
            if card.name == self.selected_flow_name:
                return card
        return None


__all__ = ["TuiStateMixin"]
