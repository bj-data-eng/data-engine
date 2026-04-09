"""Shared state and presentation helpers for Data Engine operator surfaces."""

from __future__ import annotations

from data_engine.domain.details import FlowSummaryState, OperationArtifactState
from data_engine.domain.operations import OperationFlowState, OperationRowState, OperationSessionState
from data_engine.views.models import QtFlowCard

def build_flow_summary(card: QtFlowCard | None, flow_states: dict[str, str]) -> FlowSummaryState:
    """Return summary rows for the selected flow."""
    return FlowSummaryState.from_flow(card, flow_states)


def is_inspectable_operation(operation_name: str) -> bool:
    """Return whether an operation can surface a previewable output path."""
    return OperationArtifactState(operation_name).inspectable


def artifact_key_for_operation(operation_name: str) -> str | None:
    """Return the runtime metadata key produced by one operation."""
    return OperationArtifactState(operation_name).artifact_key


def capture_step_outputs(flow_card: QtFlowCard, existing: dict[str, "Path"], results: object) -> dict[str, "Path"]:
    """Return updated output-path mappings extracted from completed flow results."""
    return OperationArtifactState.capture_outputs(flow_card, existing, results)


OperationDisplayState = OperationFlowState


__all__ = [
    "OperationDisplayState",
    "OperationRowState",
    "artifact_key_for_operation",
    "build_flow_summary",
    "capture_step_outputs",
    "is_inspectable_operation",
]
