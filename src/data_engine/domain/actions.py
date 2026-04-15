"""Domain models for operator action availability and selected-flow state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Container, Mapping

from data_engine.domain.catalog import FlowCatalogLike
from data_engine.domain.runtime import RuntimeSessionState


@dataclass(frozen=True)
class SelectedFlowState:
    """Resolved state for one selected flow."""

    card: FlowCatalogLike | None
    state: str = ""
    has_logs: bool = False
    group_active: bool = False

    @property
    def present(self) -> bool:
        return self.card is not None

    @property
    def valid(self) -> bool:
        return bool(self.card is not None and self.card.valid)

    @property
    def running(self) -> bool:
        return bool(self.state)

    @classmethod
    def from_runtime(
        cls,
        *,
        card: FlowCatalogLike | None,
        flow_states: Mapping[str, str],
        runtime_session: RuntimeSessionState,
        flow_groups_by_name: Mapping[str, str],
        active_flow_states: Container[str],
        has_logs: bool,
    ) -> "SelectedFlowState":
        """Build one selected-flow state from current runtime and selection inputs."""
        if card is None:
            return cls(card=None)
        state = flow_states.get(card.name, card.state)
        return cls(
            card=card,
            state=state if state in active_flow_states else "",
            has_logs=has_logs,
            group_active=runtime_session.is_group_active(card.group, flow_groups_by_name),
        )


@dataclass(frozen=True)
class OperatorActionContext:
    """All state required to derive operator action availability."""

    runtime_session: RuntimeSessionState
    selected_flow: SelectedFlowState
    has_automated_flows: bool
    workspace_available: bool = True
    selected_run_group_present: bool = False
    local_request_pending: bool = False

__all__ = [
    "OperatorActionContext",
    "SelectedFlowState",
]
