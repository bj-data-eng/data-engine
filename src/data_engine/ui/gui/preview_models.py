"""Explicit request/state models for GUI preview dialogs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from data_engine.domain import ConfigPreviewState, FlowRunState, RunDetailState


@dataclass(frozen=True)
class OutputPreviewRequest:
    """Request state for one output-preview dialog."""

    operation_name: str
    output_path: Path


@dataclass(frozen=True)
class RunLogPreviewRequest:
    """Request state for one run-log preview dialog."""

    run_group: FlowRunState
    detail: RunDetailState
    source_path: str | None = None

    @classmethod
    def from_run(cls, run_group: FlowRunState, *, source_path: str | None = None) -> "RunLogPreviewRequest":
        """Build one run-log preview request from a grouped run."""
        return cls(run_group=run_group, detail=RunDetailState.from_run(run_group), source_path=source_path)


@dataclass(frozen=True)
class ConfigPreviewRequest:
    """Request state for one config-preview dialog."""

    preview: ConfigPreviewState

