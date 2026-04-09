"""Inspection and preview state models shared across operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from data_engine.domain.catalog import FlowCatalogLike
from data_engine.domain.details import FlowSummaryState


@dataclass(frozen=True)
class FlowStepOutputsState:
    """Latest known inspectable outputs for one flow."""

    outputs: dict[str, Path] = field(default_factory=dict)

    def get(self, operation_name: str) -> Path | None:
        """Return the last known output path for one operation."""
        return self.outputs.get(operation_name)

    def has(self, operation_name: str) -> bool:
        """Return whether one operation currently has an inspectable output."""
        return operation_name in self.outputs


@dataclass(frozen=True)
class StepOutputIndex:
    """Latest known inspectable outputs keyed by flow and operation."""

    flow_outputs: dict[str, FlowStepOutputsState] = field(default_factory=dict)

    @classmethod
    def empty(cls) -> "StepOutputIndex":
        """Return an empty output index."""
        return cls()

    @classmethod
    def from_mapping(cls, mapping: dict[str, dict[str, Path]]) -> "StepOutputIndex":
        """Build one output index from legacy nested flow/output mappings."""
        return cls(
            flow_outputs={
                flow_name: FlowStepOutputsState(outputs=dict(outputs))
                for flow_name, outputs in mapping.items()
            }
        )

    def outputs_for(self, flow_name: str) -> FlowStepOutputsState:
        """Return output state for one flow."""
        return self.flow_outputs.get(flow_name, FlowStepOutputsState())

    def output_path(self, flow_name: str, operation_name: str) -> Path | None:
        """Return the last known output path for one flow operation."""
        return self.outputs_for(flow_name).get(operation_name)

    def has_output(self, flow_name: str, operation_name: str) -> bool:
        """Return whether one flow operation has an inspectable output."""
        return self.outputs_for(flow_name).has(operation_name)

    def with_flow_outputs(self, flow_name: str, outputs: dict[str, Path]) -> "StepOutputIndex":
        """Return a copy with one flow's outputs replaced."""
        next_flow_outputs = dict(self.flow_outputs)
        next_flow_outputs[flow_name] = FlowStepOutputsState(outputs=dict(outputs))
        return type(self)(flow_outputs=next_flow_outputs)


@dataclass(frozen=True)
class ConfigPreviewState:
    """Surface-agnostic state for one flow config/summary preview."""

    title: str
    description: str
    summary: FlowSummaryState

    @classmethod
    def from_flow(
        cls,
        card: FlowCatalogLike | None,
        flow_states: dict[str, str],
    ) -> "ConfigPreviewState":
        """Build one config-preview state bundle for a selected flow."""
        if card is None:
            return cls(
                title="No flow selected",
                description="",
                summary=FlowSummaryState.from_flow(None, flow_states),
            )
        return cls(
            title=card.title,
            description=card.description or "No flow description provided.",
            summary=FlowSummaryState.from_flow(card, flow_states),
        )


__all__ = [
    "ConfigPreviewState",
    "FlowStepOutputsState",
    "StepOutputIndex",
]
