from __future__ import annotations

from data_engine.domain import FlowLogEntry, FlowRunState, RuntimeStepEvent


def test_flow_run_state_groups_entries_and_collapses_steps():
    entries = (
        FlowLogEntry(
            line="poller started",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name=None,
                source_label="input.xlsx",
                status="started",
            ),
        ),
        FlowLogEntry(
            line="poller read started",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name="Read",
                source_label="input.xlsx",
                status="started",
            ),
        ),
        FlowLogEntry(
            line="poller read success",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name="Read",
                source_label="input.xlsx",
                status="success",
                elapsed_seconds=0.25,
            ),
        ),
        FlowLogEntry(
            line="poller success",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name=None,
                source_label="input.xlsx",
                status="success",
                elapsed_seconds=0.4,
            ),
        ),
    )

    runs = FlowRunState.group_entries(entries)

    assert len(runs) == 1
    assert runs[0].key == ("poller", "run-1")
    assert runs[0].status == "success"
    assert len(runs[0].steps) == 1
    assert runs[0].steps[0].step_name == "Read"
    assert runs[0].steps[0].status == "success"
    assert len(runs[0].entries) == 4


def test_flow_run_state_preserves_failed_terminal_status():
    entries = (
        FlowLogEntry(
            line="poller started",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name=None,
                source_label="input.xlsx",
                status="started",
            ),
        ),
        FlowLogEntry(
            line="poller failed",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name=None,
                source_label="input.xlsx",
                status="failed",
            ),
        ),
        FlowLogEntry(
            line="poller success",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name=None,
                source_label="input.xlsx",
                status="success",
            ),
        ),
    )

    runs = FlowRunState.group_entries(entries)

    assert len(runs) == 1
    assert runs[0].status == "failed"
