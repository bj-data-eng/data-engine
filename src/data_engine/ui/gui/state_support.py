"""Operator-session and log plumbing helpers for the GUI shell."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from data_engine.domain import (
    DaemonStatusState,
    FlowLogEntry,
    FlowCatalogState,
    OperationSessionState,
    OperatorSessionState,
    RuntimeSessionState,
    WorkspaceControlState,
    WorkspaceSessionState,
)
from data_engine.ui.gui.surface import (
    append_log_entry as surface_append_log_entry,
    append_log_line as surface_append_log_line,
    flush_deferred_ui_updates as surface_flush_deferred_ui_updates,
    log_matches_selection as surface_log_matches_selection,
    schedule_ui_refresh as surface_schedule_ui_refresh,
)
from data_engine.views.models import QtFlowCard, flow_catalog_entry_from_qt_card, qt_flow_cards_from_entries

if TYPE_CHECKING:
    from data_engine.ui.gui.app import DataEngineWindow


class GuiStateMixin:
    """Session-state and log-queue helpers separated from the main GUI shell."""

    @property
    def log_store(self: "DataEngineWindow"):
        """Expose the current runtime binding log store."""
        return self.runtime_binding.log_store

    @property
    def _daemon_manager(self: "DataEngineWindow"):
        """Expose the current runtime binding daemon manager."""
        return self.runtime_binding.daemon_manager

    @property
    def runtime_session(self: "DataEngineWindow") -> RuntimeSessionState:
        """Return the current operator runtime/control session state."""
        return self._operator_session_state.runtime

    @runtime_session.setter
    def runtime_session(self: "DataEngineWindow", value: RuntimeSessionState) -> None:
        self._operator_session_state = self._operator_session_state.with_runtime(value)

    @property
    def flow_catalog_state(self: "DataEngineWindow") -> FlowCatalogState:
        """Return the current discovered flow catalog state."""
        return self._operator_session_state.catalog

    @flow_catalog_state.setter
    def flow_catalog_state(self: "DataEngineWindow", value: FlowCatalogState) -> None:
        self._operator_session_state = self._operator_session_state.with_catalog(value)

    @property
    def flow_cards(self: "DataEngineWindow") -> dict[str, QtFlowCard]:
        return {card.name: card for card in qt_flow_cards_from_entries(self.flow_catalog_state.entries)}

    @flow_cards.setter
    def flow_cards(self: "DataEngineWindow", value: dict[str, QtFlowCard] | tuple[QtFlowCard, ...]) -> None:
        cards = tuple(value.values()) if isinstance(value, dict) else tuple(value)
        self.flow_catalog_state = self.flow_catalog_state.with_entries(
            tuple(flow_catalog_entry_from_qt_card(card) for card in cards)
        )

    @property
    def flow_states(self: "DataEngineWindow") -> dict[str, str]:
        return self.flow_catalog_state.flow_states or {}

    @flow_states.setter
    def flow_states(self: "DataEngineWindow", value: dict[str, str]) -> None:
        self.flow_catalog_state = self.flow_catalog_state.with_flow_states(value)

    @property
    def selected_flow_name(self: "DataEngineWindow") -> str | None:
        return self.flow_catalog_state.selected_flow_name

    @selected_flow_name.setter
    def selected_flow_name(self: "DataEngineWindow", value: str | None) -> None:
        self.flow_catalog_state = self.flow_catalog_state.with_selected_flow_name(value)

    @property
    def empty_flow_message(self: "DataEngineWindow") -> str:
        return self.flow_catalog_state.empty_message

    @empty_flow_message.setter
    def empty_flow_message(self: "DataEngineWindow", value: str) -> None:
        self.flow_catalog_state = self.flow_catalog_state.with_empty_message(value)

    @property
    def daemon_status(self: "DataEngineWindow") -> DaemonStatusState:
        """Return the current daemon status domain model."""
        return self._daemon_status

    @daemon_status.setter
    def daemon_status(self: "DataEngineWindow", value: DaemonStatusState) -> None:
        self._daemon_status = value

    @property
    def workspace_control_state(self: "DataEngineWindow") -> WorkspaceControlState:
        """Return the current structured workspace control state."""
        return self._operator_session_state.workspace_control

    @workspace_control_state.setter
    def workspace_control_state(self: "DataEngineWindow", value: WorkspaceControlState) -> None:
        self._operator_session_state = self._operator_session_state.with_workspace_control(value)

    @property
    def workspace_session_state(self: "DataEngineWindow") -> WorkspaceSessionState:
        """Return the current workspace selection/root session state."""
        return self._operator_session_state.workspace

    @workspace_session_state.setter
    def workspace_session_state(self: "DataEngineWindow", value: WorkspaceSessionState) -> None:
        self._operator_session_state = self._operator_session_state.with_workspace(value)

    @property
    def operator_session_state(self: "DataEngineWindow") -> OperatorSessionState:
        """Return the top-level operator session state for this surface."""
        return self._operator_session_state

    @property
    def operation_tracker(self: "DataEngineWindow") -> OperationSessionState:
        """Return the current operation/step session state."""
        return self._operator_session_state.operations

    @operation_tracker.setter
    def operation_tracker(self: "DataEngineWindow", value: OperationSessionState) -> None:
        self._operator_session_state = self._operator_session_state.with_operations(value)

    @property
    def docs_root_dir(self: "DataEngineWindow") -> Path | None:
        """Return the current built-docs root directory for this GUI window, if available."""
        return self._docs_root_dir

    @docs_root_dir.setter
    def docs_root_dir(self: "DataEngineWindow", value: Path | None) -> None:
        self._docs_root_dir = value

    @property
    def workspace_collection_root_override(self: "DataEngineWindow") -> Path | None:
        return self.workspace_session_state.workspace_collection_root_override

    @workspace_collection_root_override.setter
    def workspace_collection_root_override(self: "DataEngineWindow", value: Path | None) -> None:
        self.workspace_session_state = self.workspace_session_state.with_override_root(value)

    @property
    def discovered_workspace_ids(self: "DataEngineWindow") -> tuple[str, ...]:
        return self.workspace_session_state.discovered_workspace_ids

    @discovered_workspace_ids.setter
    def discovered_workspace_ids(self: "DataEngineWindow", value: tuple[str, ...]) -> None:
        self.workspace_session_state = self.workspace_session_state.with_discovered_workspace_ids(value)

    def _log_matches_selection(self: "DataEngineWindow", entry: FlowLogEntry) -> bool:
        return surface_log_matches_selection(self, entry)

    def _append_log_entry(self: "DataEngineWindow", entry: FlowLogEntry) -> None:
        surface_append_log_entry(self, entry)

    def _schedule_ui_refresh(self: "DataEngineWindow", *, log_view: bool = False, action_buttons: bool = False) -> None:
        surface_schedule_ui_refresh(self, log_view=log_view, action_buttons=action_buttons)

    def _flush_deferred_ui_updates(self: "DataEngineWindow") -> None:
        surface_flush_deferred_ui_updates(self)

    def _append_log_line(self: "DataEngineWindow", line: str, *, flow_name: str | None = None) -> None:
        surface_append_log_line(self, line, flow_name=flow_name)


__all__ = ["GuiStateMixin"]
