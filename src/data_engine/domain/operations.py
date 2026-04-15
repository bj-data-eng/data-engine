"""Domain models for selected-flow operation and step session state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_engine.domain.logs import RuntimeStepEvent


@dataclass(frozen=True)
class OperationRowState:
    """Display-ready state for one step row."""

    status: str = "idle"
    started_at: float | None = None
    elapsed_seconds: float | None = None

    def started(self, *, now: float) -> "OperationRowState":
        """Return the running state for one started step."""
        return type(self)(status="running", started_at=now, elapsed_seconds=None)

    def finished(self, *, status: str, elapsed_seconds: float | None) -> "OperationRowState":
        """Return the completed state for one finished step."""
        return type(self)(status=status, started_at=None, elapsed_seconds=elapsed_seconds)

    def normalized(self) -> "OperationRowState":
        """Return the normalized post-success idle state."""
        if self.status != "success":
            return self
        return type(self)(status="idle", started_at=None, elapsed_seconds=self.elapsed_seconds)

    def duration_text(self, *, now: float, formatter) -> str:
        """Return the formatted visible duration for this row."""
        if self.status == "running" and isinstance(self.started_at, (int, float)):
            return formatter(now - float(self.started_at))
        if isinstance(self.elapsed_seconds, (int, float)):
            return formatter(float(self.elapsed_seconds))
        return ""


@dataclass(frozen=True)
class OperationFlowState:
    """Tracked step state for one flow."""

    current_index: int | None = None
    rows: dict[str, OperationRowState] = field(default_factory=dict)

    @classmethod
    def from_operation_names(cls, operation_names: tuple[str, ...]) -> "OperationFlowState":
        """Return the reset operation state for one flow."""
        return cls(
            current_index=None,
            rows={operation_name: OperationRowState() for operation_name in operation_names},
        )

    def row_state(self, operation_name: str) -> OperationRowState | None:
        """Return one row state by operation name."""
        return self.rows.get(operation_name)

    @property
    def has_running_rows(self) -> bool:
        """Return whether any tracked step row is currently running."""
        return any(row.status == "running" for row in self.rows.values())

    @property
    def has_observed_activity(self) -> bool:
        """Return whether any tracked step row has observed runtime activity."""
        return any(
            row.status != "idle" or row.elapsed_seconds is not None or row.started_at is not None
            for row in self.rows.values()
        )

    def apply_event(
        self,
        operation_names: tuple[str, ...],
        event: "RuntimeStepEvent",
        *,
        now: float,
    ) -> tuple["OperationFlowState", int | None]:
        """Return updated flow step state after one runtime step event."""
        if event.step_name is None or event.step_name not in operation_names:
            return self, None
        current = self.row_state(event.step_name) or OperationRowState()
        rows = dict(self.rows)
        if event.status == "started":
            rows[event.step_name] = current.started(now=now)
            return type(self)(current_index=operation_names.index(event.step_name), rows=rows), None
        if event.status == "success":
            rows[event.step_name] = current.finished(status="success", elapsed_seconds=event.elapsed_seconds)
            index = operation_names.index(event.step_name)
            return type(self)(current_index=index, rows=rows), index
        if event.status == "failed":
            rows[event.step_name] = current.finished(status="failed", elapsed_seconds=event.elapsed_seconds)
            return type(self)(current_index=self.current_index, rows=rows), None
        return self, None

    def normalized_completed(self) -> "OperationFlowState":
        """Return a copy with completed success rows reset to idle."""
        return type(self)(
            current_index=self.current_index,
            rows={name: row.normalized() for name, row in self.rows.items()},
        )


@dataclass(frozen=True)
class OperationSessionState:
    """Tracked step state for all loaded flows."""

    flow_states: dict[str, OperationFlowState] = field(default_factory=dict)

    @classmethod
    def empty(cls) -> "OperationSessionState":
        """Return the empty operation session state."""
        return cls()

    def state_for(self, flow_name: str) -> OperationFlowState | None:
        """Return one flow operation state by flow name."""
        return self.flow_states.get(flow_name)

    def reset_flow(self, flow_name: str, operation_names: tuple[str, ...]) -> "OperationSessionState":
        """Return a copy with one flow reset to idle operation state."""
        states = dict(self.flow_states)
        states[flow_name] = OperationFlowState.from_operation_names(operation_names)
        return type(self)(flow_states=states)

    def ensure_flow(self, flow_name: str, operation_names: tuple[str, ...]) -> "OperationSessionState":
        """Return a copy guaranteed to contain one flow state."""
        state = self.state_for(flow_name)
        if state is None or not state.rows:
            return self.reset_flow(flow_name, operation_names)
        return self

    def row_state(self, flow_name: str, operation_name: str) -> OperationRowState | None:
        """Return one operation row state from the current session."""
        state = self.state_for(flow_name)
        if state is None:
            return None
        return state.row_state(operation_name)

    def apply_event(
        self,
        flow_name: str,
        operation_names: tuple[str, ...],
        event: "RuntimeStepEvent",
        *,
        now: float,
    ) -> tuple["OperationSessionState", int | None]:
        """Return updated session state after one runtime step event."""
        if event.step_name is None or event.step_name not in operation_names:
            return self, None
        ensured = self.ensure_flow(flow_name, operation_names)
        flow_state = ensured.state_for(flow_name) or OperationFlowState.from_operation_names(operation_names)
        updated_flow_state, flash_index = flow_state.apply_event(operation_names, event, now=now)
        states = dict(ensured.flow_states)
        states[flow_name] = updated_flow_state
        return type(self)(flow_states=states), flash_index

    def duration_text(self, flow_name: str, operation_name: str, *, now: float, formatter) -> str:
        """Return one formatted duration string for the selected step row."""
        row_state = self.row_state(flow_name, operation_name)
        if row_state is None:
            return ""
        return row_state.duration_text(now=now, formatter=formatter)

    def normalize_completed(self, flow_name: str) -> "OperationSessionState":
        """Return a copy with completed success rows normalized for one flow."""
        state = self.state_for(flow_name)
        if state is None:
            return self
        states = dict(self.flow_states)
        states[flow_name] = state.normalized_completed()
        return type(self)(flow_states=states)

    def reset(self) -> "OperationSessionState":
        """Return the empty operation session state."""
        return type(self).empty()


__all__ = [
    "OperationFlowState",
    "OperationRowState",
    "OperationSessionState",
]
