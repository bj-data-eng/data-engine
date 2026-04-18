from __future__ import annotations

from data_engine.application import ActionStateApplication, DetailApplication
from data_engine.domain import FlowCatalogEntry, FlowLogEntry, FlowRunState, OperationSessionState, RuntimeStepEvent, RuntimeSessionState


def _entry() -> FlowCatalogEntry:
    return FlowCatalogEntry(
        name="example_manual",
        group="Examples",
        title="Example Manual",
        description="desc",
        source_root="/tmp/source",
        target_root="/tmp/target",
        mode="manual",
        interval="-",
        operations="Extract -> Write",
        operation_items=("Extract", "Write"),
        state="manual",
        valid=True,
        category="manual",
    )


def _run_group(run_id: str, flow_name: str) -> FlowRunState:
    entry = FlowLogEntry(
        line=f"{flow_name} success",
        kind="flow",
        flow_name=flow_name,
        event=RuntimeStepEvent(
            run_id=run_id,
            flow_name=flow_name,
            step_name=None,
            source_label="input.xlsx",
            status="success",
        ),
    )
    return FlowRunState.group_entries((entry,))[0]


def test_detail_application_normalizes_selected_run_key() -> None:
    card = _entry()
    run_groups = (
        _run_group("run-1", "example_manual"),
        _run_group("run-2", "example_manual"),
    )

    presentation = DetailApplication().build_selected_flow_presentation(
        card=card,
        tracker=OperationSessionState.empty(),
        flow_states={},
        run_groups=run_groups,
        selected_run_key=("example_manual", "missing"),
    )

    assert presentation.detail_state is not None
    assert presentation.selected_run_key == run_groups[0].key
    assert presentation.selected_run_group == run_groups[0]


def test_detail_application_returns_empty_state_without_selection() -> None:
    presentation = DetailApplication().build_selected_flow_presentation(
        card=None,
        tracker=OperationSessionState.empty(),
        flow_states={},
        run_groups=(),
        selected_run_key=None,
    )

    assert presentation.detail_state is None
    assert presentation.empty_text == "Select one flow to see details."
    assert presentation.selected_run_group is None


def test_detail_application_limits_visible_run_groups() -> None:
    card = _entry()
    run_groups = tuple(_run_group(f"run-{index}", "example_manual") for index in range(5))

    presentation = DetailApplication().build_selected_flow_presentation(
        card=card,
        tracker=OperationSessionState.empty(),
        flow_states={},
        run_groups=run_groups,
        selected_run_key=None,
        max_visible_runs=2,
    )

    assert tuple(group.key for group in presentation.visible_run_groups) == (
        run_groups[-2].key,
        run_groups[-1].key,
    )
    assert presentation.run_group_signature == (
        run_groups[-2].key,
        run_groups[-1].key,
    )


def test_action_state_application_builds_selected_flow_context() -> None:
    card = FlowCatalogEntry(
        name="poller",
        group="Examples",
        title="Poller",
        description="desc",
        source_root="/tmp/source",
        target_root="/tmp/target",
        mode="poll",
        interval="5m",
        operations="Read -> Write",
        operation_items=("Read", "Write"),
        state="poll ready",
        valid=True,
        category="automated",
    )

    context = ActionStateApplication().build_action_context(
        card=card,
        flow_states={"poller": "polling"},
        runtime_session=RuntimeSessionState.empty().with_runtime_flags(active=True, stopping=True).with_active_runtime_flow_names(("poller",)),
        flow_groups_by_name={"poller": "Examples"},
        active_flow_states={"running", "polling", "scheduled", "stopping flow", "stopping runtime"},
        has_logs=True,
        has_automated_flows=True,
        selected_run_group_present=True,
    )

    assert context.selected_flow.present is True
    assert context.selected_flow.running is True
    assert context.selected_flow.group_active is True
    assert context.selected_flow.has_logs is True
    assert context.has_automated_flows is True
    assert context.selected_run_group_present is True
