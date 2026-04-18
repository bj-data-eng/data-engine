"""Shared selected-flow detail presentation helpers across operator surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from data_engine.domain import (
    FlowCatalogLike,
    FlowLogEntry,
    FlowRunState,
    RunStepState,
    RuntimeStepEvent,
    SelectedFlowDetailState,
    short_source_label,
)
from data_engine.domain.time import parse_utc_text


class LiveRunLike(Protocol):
    """Structural contract for daemon-owned live run snapshots."""

    run_id: str
    flow_name: str
    group_name: str
    source_path: str | None
    state: str
    current_step_name: str | None
    current_step_started_at_utc: str | None
    started_at_utc: str | None
    finished_at_utc: str | None
    elapsed_seconds: float | None
    error_text: str | None


@dataclass(frozen=True)
class SelectedFlowPresentation:
    """Normalized selected-flow detail state for operator surfaces."""

    detail_state: SelectedFlowDetailState | None
    run_groups: tuple[FlowRunState, ...]
    visible_run_groups: tuple[FlowRunState, ...]
    selected_run_key: tuple[str, str] | None
    empty_text: str

    @property
    def run_group_signature(self) -> tuple[tuple[str, str], ...]:
        """Return the stable visible run-list signature for diffing/render reuse."""
        return tuple(group.key for group in self.visible_run_groups)

    @property
    def selected_run_group(self) -> FlowRunState | None:
        """Return the normalized selected run group, if any."""
        if self.selected_run_key is not None:
            for run_group in self.run_groups:
                if run_group.key == self.selected_run_key:
                    return run_group
        return self.run_groups[0] if self.run_groups else None


def build_selected_flow_presentation(
    *,
    card: FlowCatalogLike | None,
    tracker,
    flow_states: dict[str, str],
    run_groups: tuple[FlowRunState, ...],
    selected_run_key: tuple[str, str] | None,
    max_visible_runs: int | None = None,
    live_runs: Mapping[str, LiveRunLike] | None = None,
    live_truth_authoritative: bool = False,
) -> SelectedFlowPresentation:
    """Return the selected-flow detail state and normalized run selection."""
    if card is None:
        return SelectedFlowPresentation(
            detail_state=None,
            run_groups=(),
            visible_run_groups=(),
            selected_run_key=None,
            empty_text="Select one flow to see details.",
        )
    detail_state = SelectedFlowDetailState.from_flow(
        card,
        tracker,
        flow_states=flow_states,
    )
    effective_run_groups = _effective_run_groups(
        flow_name=card.name,
        run_groups=run_groups,
        live_runs=live_runs,
        live_truth_authoritative=live_truth_authoritative,
    )
    normalized_key = (
        selected_run_key
        if any(group.key == selected_run_key for group in effective_run_groups)
        else (effective_run_groups[0].key if effective_run_groups else None)
    )
    visible_run_groups = (
        effective_run_groups[-max_visible_runs:]
        if max_visible_runs is not None and max_visible_runs >= 0
        else effective_run_groups
    )
    return SelectedFlowPresentation(
        detail_state=detail_state,
        run_groups=effective_run_groups,
        visible_run_groups=visible_run_groups,
        selected_run_key=normalized_key,
        empty_text="",
    )


def _effective_run_groups(
    *,
    flow_name: str,
    run_groups: tuple[FlowRunState, ...],
    live_runs: Mapping[str, LiveRunLike] | None,
    live_truth_authoritative: bool,
) -> tuple[FlowRunState, ...]:
    """Merge persisted run groups with daemon-owned live run truth for one flow."""
    if not live_runs:
        return run_groups

    live_flow_runs = tuple(run for run in live_runs.values() if run.flow_name == flow_name)
    if not live_flow_runs and not live_truth_authoritative:
        return run_groups

    active_by_run_id = {run.run_id: run for run in live_flow_runs}
    merged: list[FlowRunState] = []
    included_live_run_ids: set[str] = set()

    for group in run_groups:
        live_run = active_by_run_id.get(group.key[1])
        if live_run is not None:
            merged.append(group)
            included_live_run_ids.add(live_run.run_id)
            continue
        if live_truth_authoritative and group.status not in {"success", "failed", "stopped"}:
            continue
        merged.append(group)

    daemon_only_runs = sorted(
        (run for run in live_flow_runs if run.run_id not in included_live_run_ids),
        key=_live_run_sort_key,
    )
    merged.extend(_overlay_live_run(None, run) for run in daemon_only_runs)
    return tuple(merged)


def _overlay_live_run(existing: FlowRunState | None, live_run: LiveRunLike) -> FlowRunState:
    """Return one run-group state updated with daemon-owned live run truth."""
    live_entry = _live_run_entry(live_run)
    source_label = short_source_label(live_run.source_path)
    entries = existing.entries if existing is not None and existing.entries else (live_entry,)
    existing_steps = () if existing is None else existing.steps
    current_step_status = "stopping" if live_run.state == "stopping" else "started"
    live_steps = existing_steps
    if live_run.current_step_name is not None and not any(
        step.step_name == live_run.current_step_name and step.status == current_step_status
        for step in existing_steps
    ):
        live_steps = (
            *existing_steps,
            RunStepState(
                step_name=live_run.current_step_name,
                status=current_step_status,
                elapsed_seconds=live_run.elapsed_seconds,
                entry=live_entry,
            ),
        )
    return FlowRunState(
        key=(live_run.flow_name, live_run.run_id),
        display_label=_display_label_for_live_run(live_run),
        source_label=source_label,
        status=_run_group_status_from_live_state(live_run.state),
        elapsed_seconds=live_run.elapsed_seconds if live_run.elapsed_seconds is not None else (existing.elapsed_seconds if existing is not None else None),
        summary_entry=existing.summary_entry if existing is not None and existing.summary_entry is not None else live_entry,
        steps=tuple(live_steps),
        entries=tuple(entries),
    )


def _display_label_for_live_run(live_run: LiveRunLike) -> str:
    """Return the canonical local-time label for one live run."""
    for raw in (live_run.started_at_utc, live_run.current_step_started_at_utc, live_run.finished_at_utc):
        parsed = parse_utc_text(raw)
        if parsed is not None:
            return parsed.astimezone().strftime("%Y-%m-%d %I:%M:%S %p")
    return datetime.now(UTC).astimezone().strftime("%Y-%m-%d %I:%M:%S %p")


def _live_run_entry(live_run: LiveRunLike) -> FlowLogEntry:
    """Create one synthetic flow-log entry for a daemon-native live run."""
    timestamp = (
        parse_utc_text(live_run.current_step_started_at_utc)
        or parse_utc_text(live_run.started_at_utc)
        or parse_utc_text(live_run.finished_at_utc)
        or datetime.now(UTC)
    )
    event_status = "stopped" if live_run.state == "stopping" else _run_group_status_from_live_state(live_run.state)
    return FlowLogEntry(
        line=(
            live_run.error_text
            if live_run.state == "failed" and live_run.error_text
            else f"run={live_run.run_id} flow={live_run.flow_name} source={live_run.source_path or '-'} status={event_status}"
        ),
        kind="flow",
        flow_name=live_run.flow_name,
        created_at_utc=timestamp,
        event=RuntimeStepEvent(
            run_id=live_run.run_id,
            flow_name=live_run.flow_name,
            step_name=live_run.current_step_name,
            source_label=short_source_label(live_run.source_path),
            status=event_status,
            elapsed_seconds=live_run.elapsed_seconds,
        ),
    )


def _run_group_status_from_live_state(state: str) -> str:
    """Normalize one live-run state into the grouped-run status vocabulary."""
    normalized = str(state or "").strip().lower()
    if normalized in {"success", "failed", "stopped", "stopping"}:
        return normalized
    return "started"


def _live_run_sort_key(live_run: LiveRunLike) -> tuple[datetime, str]:
    """Return a stable chronological sort key for daemon-only live runs."""
    timestamp = (
        parse_utc_text(live_run.started_at_utc)
        or parse_utc_text(live_run.current_step_started_at_utc)
        or parse_utc_text(live_run.finished_at_utc)
        or datetime.now(UTC)
    )
    return (timestamp, live_run.run_id)


__all__ = ["SelectedFlowPresentation", "build_selected_flow_presentation"]
