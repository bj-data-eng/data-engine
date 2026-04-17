"""Domain models for selected-flow and run-detail state."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from data_engine.domain.catalog import FlowCatalogLike

if TYPE_CHECKING:
    from data_engine.domain.runs import FlowRunState
    from data_engine.domain.operations import OperationSessionState


@dataclass(frozen=True)
class FlowSummaryRow:
    """One labeled row in a flow summary/config display."""

    label: str
    value: str

    @classmethod
    def rows_for_flow(
        cls,
        card: FlowCatalogLike | None,
        flow_states: dict[str, str],
    ) -> tuple["FlowSummaryRow", ...]:
        """Build summary/config rows for one flow card."""
        if card is None:
            return (
                cls("Flow", "-"),
                cls("Mode", "-"),
                cls("Interval", "-"),
                cls("Settle", "-"),
                cls("Max Parallel", "-"),
                cls("State", "-"),
                cls("Source", "-"),
                cls("Target", "-"),
            )
        state = flow_states.get(card.name, card.state)
        return (
            cls("Flow", card.name),
            cls("Mode", card.mode),
            cls("Interval", card.interval),
            cls("Settle", card.settle),
            cls("Max Parallel", card.parallelism),
            cls("State", state),
            cls("Source", card.source_root),
            cls("Target", card.target_root),
        )

    @classmethod
    def pairs_for_flow(
        cls,
        card: FlowCatalogLike | None,
        flow_states: dict[str, str],
    ) -> tuple[tuple[str, str], ...]:
        """Build tuple pairs for display surfaces that only need labels and values."""
        return tuple((row.label, row.value) for row in cls.rows_for_flow(card, flow_states))


@dataclass(frozen=True)
class FlowSummaryState:
    """Explicit summary state for one selected flow."""

    rows: tuple[FlowSummaryRow, ...]

    @classmethod
    def from_flow(
        cls,
        card: FlowCatalogLike | None,
        flow_states: dict[str, str],
    ) -> "FlowSummaryState":
        """Build one summary-state bundle for a selected flow."""
        return cls(rows=FlowSummaryRow.rows_for_flow(card, flow_states))

    @property
    def pairs(self) -> tuple[tuple[str, str], ...]:
        """Return the legacy label/value pair projection for simple surfaces."""
        return tuple((row.label, row.value) for row in self.rows)


@dataclass(frozen=True)
class OperationArtifactState:
    """Artifact/inspection rules for one operation row."""

    operation_name: str

    @property
    def inspectable(self) -> bool:
        """Return whether the operation can surface an inspectable artifact."""
        return bool(self.operation_name)

    @property
    def artifact_key(self) -> str | None:
        """Return the runtime metadata key produced by this operation."""
        return self.operation_name or None

    @classmethod
    def capture_outputs(
        cls,
        card: FlowCatalogLike,
        existing: dict[str, Path],
        results: object,
    ) -> dict[str, Path]:
        """Return updated output-path mappings extracted from completed flow results."""
        if not isinstance(results, list):
            return existing.copy()
        captured = existing.copy()
        for context in results:
            metadata = getattr(context, "metadata", None)
            if not isinstance(metadata, dict):
                continue
            step_outputs = metadata.get("step_outputs")
            if not isinstance(step_outputs, dict):
                continue
            for operation_name in card.operation_items:
                value = step_outputs.get(cls(operation_name).artifact_key)
                if isinstance(value, Path):
                    captured[operation_name] = value
        return captured


@dataclass(frozen=True)
class OperationDetailRow:
    """One operation row in selected-flow detail state."""

    name: str
    status: str
    elapsed_seconds: float | None


@dataclass(frozen=True)
class SelectedFlowDetailState:
    """Surface-agnostic detail state for one selected flow."""

    title: str
    description: str
    error: str
    summary_rows: tuple[FlowSummaryRow, ...]
    operation_rows: tuple[OperationDetailRow, ...]

    @classmethod
    def from_flow(
        cls,
        card: FlowCatalogLike,
        tracker: "OperationSessionState",
        *,
        flow_states: dict[str, str] | None = None,
    ) -> "SelectedFlowDetailState":
        """Build the selected-flow detail state for one card."""
        summary_rows = FlowSummaryRow.rows_for_flow(card, flow_states or {})
        operation_rows = tuple(
            OperationDetailRow(
                name=operation_name,
                status=(row_state.status if row_state is not None else "idle"),
                elapsed_seconds=(row_state.elapsed_seconds if row_state is not None else None),
            )
            for operation_name in card.operation_items
            for row_state in (tracker.row_state(card.name, operation_name),)
        )
        return cls(
            title=card.title,
            description=card.description or "",
            error=card.error or "",
            summary_rows=summary_rows,
            operation_rows=operation_rows,
        )


@dataclass(frozen=True)
class RunStepDetailRow:
    """One step row inside a grouped run detail."""

    step_name: str
    status: str
    elapsed_seconds: float | None


@dataclass(frozen=True)
class RunDetailState:
    """Surface-agnostic detail state for one grouped run."""

    display_label: str
    source_label: str
    status: str
    elapsed_seconds: float | None
    step_rows: tuple[RunStepDetailRow, ...]

    @classmethod
    def from_run(cls, run_state: "FlowRunState") -> "RunDetailState":
        """Build the grouped-run detail state used by operator surfaces."""
        step_rows = tuple(
            RunStepDetailRow(
                step_name=step.step_name,
                status=step.status,
                elapsed_seconds=step.elapsed_seconds,
            )
            for step in run_state.steps
        )
        return cls(
            display_label=run_state.display_label,
            source_label=run_state.source_label,
            status=run_state.status,
            elapsed_seconds=run_state.elapsed_seconds,
            step_rows=step_rows,
        )

__all__ = [
    "FlowSummaryState",
    "FlowSummaryRow",
    "OperationArtifactState",
    "OperationDetailRow",
    "RunDetailState",
    "RunStepDetailRow",
    "SelectedFlowDetailState",
]
