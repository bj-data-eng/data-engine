from __future__ import annotations

from data_engine.authoring.flow import Flow
from data_engine.domain import (
    FlowLogEntry,
    FlowRunState,
    FlowSummaryState,
    FlowSummaryRow,
    OperationSessionState,
    RunDetailState,
    RunStepState,
    SelectedFlowDetailState,
)
from data_engine.services.flow_catalog import flow_catalog_entry_from_flow
from data_engine.domain import RuntimeStepEvent
from data_engine.views.models import qt_flow_card_from_entry


def _sample_card():
    return qt_flow_card_from_entry(flow_catalog_entry_from_flow(
        Flow(name="claims_summary", label="Claims Summary", group="Claims")
        .step(lambda context: context.current, label="Read Excel")
        .step(lambda context: context.current, label="Write Parquet"),
        description="Review claims",
    ))


def test_selected_flow_detail_state_collects_summary_and_operation_rows():
    card = _sample_card()
    tracker = OperationSessionState.empty().ensure_flow(card.name, card.operation_items)
    tracker, _ = tracker.apply_event(
        card.name,
        card.operation_items,
        RuntimeStepEvent(
            run_id="run-1",
            flow_name=card.name,
            step_name=card.operation_items[0],
            source_label="input.xlsx",
            status="started",
            elapsed_seconds=None,
        ),
        now=10.0,
    )

    detail = SelectedFlowDetailState.from_flow(card, tracker, flow_states={card.name: "running"})

    assert detail.title == "Claims Summary"
    assert detail.summary_rows[0].label == "Flow"
    assert detail.summary_rows[0].value == "claims_summary"
    assert detail.operation_rows[0].status == "running"


def test_run_detail_state_collects_step_rows():
    step_entry = FlowLogEntry(
        line="step",
        kind="flow",
        flow_name="claims_summary",
        event=RuntimeStepEvent(
            run_id="run-1",
            flow_name="claims_summary",
            step_name="Read Excel",
            source_label="input.xlsx",
            status="success",
            elapsed_seconds=0.2,
        ),
    )
    run_group = FlowRunState(
        key=("claims_summary", "run-1"),
        display_label="2026-04-04 09:15:00 AM",
        source_label="input.xlsx",
        status="success",
        elapsed_seconds=1.2,
        summary_entry=None,
        steps=(
            RunStepState(
                step_name="Read Excel",
                status="success",
                elapsed_seconds=0.2,
                entry=step_entry,
            ),
        ),
        entries=(),
    )

    detail = RunDetailState.from_run(run_group)

    assert detail.source_label == "input.xlsx"
    assert detail.step_rows[0].step_name == "Read Excel"
    assert detail.step_rows[0].status == "success"


def test_flow_summary_rows_expose_core_flow_config_fields_in_display_order():
    rows = FlowSummaryRow.rows_for_flow(_sample_card(), {"claims_summary": "running"})

    assert [(row.label, row.value) for row in rows][:4] == [
        ("Flow", "claims_summary"),
        ("Mode", "manual"),
        ("Interval", "-"),
        ("Max Parallel", "1"),
    ]


def test_flow_summary_state_wraps_rows_and_pairs_together():
    summary = FlowSummaryState.from_flow(_sample_card(), {"claims_summary": "running"})

    assert summary.rows[0].label == "Flow"
    assert summary.rows[0].value == "claims_summary"
    assert summary.pairs[0] == ("Flow", "claims_summary")
