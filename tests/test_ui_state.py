from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from data_engine.domain import OperationSessionState
from data_engine.domain import RuntimeStepEvent
from data_engine.views.models import QtFlowCard
from data_engine.views.state import artifact_key_for_operation, build_flow_summary, capture_step_outputs


def _sample_card() -> QtFlowCard:
    return QtFlowCard(
        name="manual_review",
        group="Manual",
        title="Manual Review",
        description="Runs a one-off validation pass.",
        source_root="/tmp/input",
        target_root="/tmp/output",
        mode="manual",
        interval="-",
        operations="Read -> Write",
        operation_items=("Read", "Write"),
        state="manual",
        valid=True,
        category="manual",
    )


def test_build_flow_summary_uses_selected_flow_state():
    summary = build_flow_summary(_sample_card(), {"manual_review": "running"})

    assert ("Flow", "manual_review") in summary.pairs
    assert ("State", "running") in summary.pairs
    assert ("Source", "/tmp/input") in summary.pairs


def test_operation_display_tracker_tracks_started_and_completed_steps():
    tracker = OperationSessionState.empty()
    operation_names = ("Read", "Write")

    tracker = tracker.reset_flow("manual_review", operation_names)
    tracker, flash_index = tracker.apply_event(
        "manual_review",
        operation_names,
        RuntimeStepEvent(flow_name="manual_review", step_name="Read", source_label="input.xlsx", status="started"),
        now=10.0,
    )

    assert flash_index is None
    assert tracker.row_state("manual_review", "Read").status == "running"
    assert tracker.duration_text("manual_review", "Read", now=10.5, formatter=lambda seconds: f"{seconds:.1f}s") == "0.5s"

    tracker, flash_index = tracker.apply_event(
        "manual_review",
        operation_names,
        RuntimeStepEvent(
            flow_name="manual_review",
            step_name="Read",
            source_label="input.xlsx",
            status="success",
            elapsed_seconds=0.25,
        ),
        now=11.0,
    )

    assert flash_index == 0
    assert tracker.row_state("manual_review", "Read").status == "success"
    tracker = tracker.normalize_completed("manual_review")
    assert tracker.row_state("manual_review", "Read").status == "idle"


def test_capture_step_outputs_maps_runtime_metadata_back_to_operation_names():
    card = _sample_card()
    latest = Path("/tmp/latest.parquet")

    captured = capture_step_outputs(
        card,
        {"Write": Path("/tmp/old.parquet")},
        [SimpleNamespace(metadata={"step_outputs": {artifact_key_for_operation("Write"): latest}})],
    )

    assert captured["Write"] == latest
