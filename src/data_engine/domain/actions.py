"""Domain models for operator action availability and selected-flow state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Container, Mapping

from data_engine.domain.catalog import FlowCatalogLike
from data_engine.domain.runtime import RuntimeSessionState

@dataclass(frozen=True)
class PendingWorkspaceActionOverlay:
    """Local transient operator intent layered over streamed workspace truth."""

    control_actions: frozenset[str] = field(default_factory=frozenset)
    pending_manual_run_groups: frozenset[str | None] = field(default_factory=frozenset)
    stopping_manual_run_groups: frozenset[str | None] = field(default_factory=frozenset)

    @property
    def request_control_pending(self) -> bool:
        return "request_control" in self.control_actions

    @property
    def run_selected_flow_pending(self) -> bool:
        return "run_selected_flow" in self.control_actions

    @property
    def start_engine_pending(self) -> bool:
        return "start_runtime" in self.control_actions

    @property
    def stop_engine_pending(self) -> bool:
        return "stop_runtime" in self.control_actions

    @property
    def stop_pipeline_pending(self) -> bool:
        return "stop_pipeline" in self.control_actions

    @property
    def refresh_flows_pending(self) -> bool:
        return "refresh_flows" in self.control_actions

    @property
    def reset_flow_pending(self) -> bool:
        return "reset_flow" in self.control_actions

    @property
    def engine_transition_pending(self) -> bool:
        return self.start_engine_pending or self.stop_engine_pending or self.stop_pipeline_pending

    def manual_group_starting(
        self,
        group_name: str | None,
        *,
        selected_manual_running: bool,
        selected_manual_stopping: bool,
    ) -> bool:
        return (
            group_name in self.pending_manual_run_groups
            and not selected_manual_running
            and not selected_manual_stopping
        )

    def manual_group_stopping(self, group_name: str | None, *, selected_manual_running: bool) -> bool:
        return group_name in self.stopping_manual_run_groups and selected_manual_running


@dataclass(frozen=True)
class SelectedFlowState:
    """Resolved state for one selected flow."""

    card: FlowCatalogLike | None
    state: str = ""
    live_state: str = ""
    live_truth_known: bool = False
    live_manual_running: bool = False
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
        if self.live_truth_known:
            return bool(self.live_state)
        return self.group_active

    @property
    def stopping(self) -> bool:
        return self.live_state == "stopping"

    @property
    def automated(self) -> bool:
        card = self.card
        if card is None:
            return False
        return str(getattr(card, "mode", "") or "").strip().lower() in {"poll", "schedule"}

    @staticmethod
    def _live_state_for_card(
        card: FlowCatalogLike | None,
        live_runs: Mapping[str, Any] | None,
    ) -> str:
        if card is None or not live_runs:
            return ""
        matched_states = {
            str(getattr(run, "state", "") or "").strip().lower()
            for run in live_runs.values()
            if str(getattr(run, "flow_name", "") or "").strip() == card.name
            and str(getattr(run, "group_name", "") or "").strip() == str(card.group or "").strip()
        }
        for state in ("stopping", "running", "starting"):
            if state in matched_states:
                return state
        return ""

    @staticmethod
    def _live_manual_running_for_card(
        card: FlowCatalogLike | None,
        live_runs: Mapping[str, Any] | None,
        *,
        engine_active_flow_names: Container[str] = (),
    ) -> bool:
        if card is None or not live_runs:
            return False
        for run in live_runs.values():
            if str(getattr(run, "flow_name", "") or "").strip() != card.name:
                continue
            if card.name in engine_active_flow_names:
                continue
            if str(getattr(run, "group_name", "") or "").strip() != str(card.group or "").strip():
                continue
            state = str(getattr(run, "state", "") or "").strip().lower()
            if state in {"starting", "running", "stopping"}:
                return True
        return False

    @staticmethod
    def _live_group_active_for_card(
        card: FlowCatalogLike | None,
        live_runs: Mapping[str, Any] | None,
    ) -> bool:
        if card is None or not live_runs:
            return False
        target_group = str(card.group or "").strip()
        for run in live_runs.values():
            state = str(getattr(run, "state", "") or "").strip().lower()
            if state not in {"starting", "running", "stopping"}:
                continue
            if str(getattr(run, "group_name", "") or "").strip() != target_group:
                continue
            return True
        return False

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
        live_runs: Mapping[str, Any] | None = None,
        engine_active_flow_names: Container[str] = (),
    ) -> "SelectedFlowState":
        """Build one selected-flow state from current runtime and selection inputs."""
        if card is None:
            return cls(card=None)
        state = flow_states.get(card.name, card.state)
        live_state = cls._live_state_for_card(card, live_runs)
        live_manual_running = cls._live_manual_running_for_card(
            card,
            live_runs,
            engine_active_flow_names=engine_active_flow_names,
        )
        live_group_active = cls._live_group_active_for_card(card, live_runs)
        return cls(
            card=card,
            state=state if state in active_flow_states else "",
            live_state=live_state,
            live_truth_known=live_runs is not None,
            live_manual_running=live_manual_running,
            has_logs=has_logs,
            group_active=(
                live_group_active
                or runtime_session.is_group_active(card.group, flow_groups_by_name)
                or live_manual_running
                or bool(live_state)
                if live_runs is not None
                else runtime_session.is_group_active(card.group, flow_groups_by_name) or live_manual_running or bool(live_state)
            ),
        )


@dataclass(frozen=True)
class OperatorActionContext:
    """All state required to derive operator action availability."""

    runtime_session: RuntimeSessionState
    selected_flow: SelectedFlowState
    has_automated_flows: bool
    engine_state: str = "idle"
    engine_truth_known: bool = False
    live_truth_known: bool = False
    live_manual_run_active: bool = False
    workspace_available: bool = True
    selected_run_group_present: bool = False
    local_request_pending: bool = False
    overlay: PendingWorkspaceActionOverlay = field(default_factory=PendingWorkspaceActionOverlay)

    @property
    def control_available(self) -> bool:
        return self.runtime_session.control_available

    @property
    def normalized_engine_state(self) -> str:
        if self.engine_state in {"idle", "starting", "running", "stopping"}:
            if self.engine_truth_known or self.engine_state != "idle":
                return self.engine_state
        return (
            "stopping"
            if self.runtime_session.runtime_stopping
            else "running"
            if self.runtime_session.runtime_active
            else "idle"
        )

    @property
    def engine_starting(self) -> bool:
        return self.normalized_engine_state == "starting"

    @property
    def engine_running(self) -> bool:
        return self.normalized_engine_state in {"running", "stopping"}

    @property
    def engine_busy(self) -> bool:
        return self.normalized_engine_state in {"starting", "running", "stopping"}

    @property
    def manual_run_active(self) -> bool:
        return (
            self.live_manual_run_active
            if self.live_truth_known
            else self.runtime_session.manual_run_active
        )

    @property
    def selected_manual_running(self) -> bool:
        if self.live_truth_known or self.selected_flow.live_truth_known:
            return self.selected_flow.live_manual_running
        card = self.selected_flow.card
        if card is None:
            return False
        return card.name == self.runtime_session.manual_flow_name_for_group(card.group)

    @property
    def request_control_enabled(self) -> bool:
        return (
            not self.runtime_session.workspace_owned
            and not self.control_available
            and not (self.local_request_pending or self.overlay.request_control_pending)
            and self.workspace_available
        )

__all__ = [
    "OperatorActionContext",
    "PendingWorkspaceActionOverlay",
    "SelectedFlowState",
]
