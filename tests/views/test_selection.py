from __future__ import annotations

from data_engine.domain import FlowCatalogEntry, FlowLogEntry, OperationSessionState, RuntimeStepEvent
from data_engine.services.runtime_state import RunLiveSnapshot
from data_engine.views import build_selected_flow_presentation


def _card() -> FlowCatalogEntry:
    return FlowCatalogEntry(
        name="claims_parallel_schedule",
        group="Claims",
        title="Claims Parallel Schedule",
        description="Parallel scheduled claims flow.",
        source_root="/tmp/source",
        target_root="/tmp/target",
        mode="schedule",
        interval="5s",
        settle="-",
        operations="Read -> Normalize -> Write",
        operation_items=("Read", "Normalize", "Write"),
        state="schedule ready",
        valid=True,
        category="automated",
    )


def _run_group(run_id: str, *, status: str = "started") -> object:
    entry = FlowLogEntry(
        line=f"{run_id} {status}",
        kind="flow",
        flow_name="claims_parallel_schedule",
        event=RuntimeStepEvent(
            run_id=run_id,
            flow_name="claims_parallel_schedule",
            step_name=None,
            source_label=f"{run_id}.xlsx",
            status=status,
            elapsed_seconds=1.0 if status != "started" else None,
        ),
    )
    return entry


def test_selected_flow_presentation_prefers_daemon_live_runs_for_nonterminal_history() -> None:
    card = _card()
    run_groups = tuple(
        _entry_to_group(_run_group(f"run-{index}"))
        for index in range(8)
    )
    live_runs = {
        f"run-{index}": RunLiveSnapshot(
            run_id=f"run-{index}",
            flow_name=card.name,
            group_name=card.group,
            source_path=f"run-{index}.xlsx",
            state="running",
            current_step_name="Normalize",
            current_step_started_at_utc="2026-04-18T12:00:00+00:00",
            started_at_utc="2026-04-18T11:59:00+00:00",
            elapsed_seconds=60.0,
        )
        for index in range(4)
    }

    presentation = build_selected_flow_presentation(
        card=card,
        tracker=OperationSessionState.empty(),
        flow_states={},
        run_groups=run_groups,
        selected_run_key=None,
        live_runs=live_runs,
        live_truth_authoritative=True,
    )

    assert tuple(group.key[1] for group in presentation.run_groups) == ("run-0", "run-1", "run-2", "run-3")
    assert all(group.status == "started" for group in presentation.run_groups)
    assert presentation.run_groups == run_groups[:4]


def test_selected_flow_presentation_keeps_terminal_history_and_adds_daemon_only_live_runs() -> None:
    card = _card()
    run_groups = (
        _entry_to_group(_run_group("finished-1", status="success")),
        _entry_to_group(_run_group("finished-2", status="failed")),
    )
    live_runs = {
        "live-1": RunLiveSnapshot(
            run_id="live-1",
            flow_name=card.name,
            group_name=card.group,
            source_path="live-1.xlsx",
            state="running",
            current_step_name="Write",
            current_step_started_at_utc="2026-04-18T12:00:00+00:00",
            started_at_utc="2026-04-18T11:59:00+00:00",
            elapsed_seconds=60.0,
        )
    }

    presentation = build_selected_flow_presentation(
        card=card,
        tracker=OperationSessionState.empty(),
        flow_states={},
        run_groups=run_groups,
        selected_run_key=("claims_parallel_schedule", "live-1"),
        live_runs=live_runs,
        live_truth_authoritative=True,
    )

    assert tuple(group.key[1] for group in presentation.run_groups) == ("finished-1", "finished-2", "live-1")
    assert presentation.selected_run_group is not None
    assert presentation.selected_run_group.key == ("claims_parallel_schedule", "live-1")
    assert presentation.selected_run_group.steps[-1].step_name == "Write"


def test_selected_flow_presentation_overlays_live_step_statuses_on_detail_rows() -> None:
    card = _card()
    live_runs = {
        "live-1": RunLiveSnapshot(
            run_id="live-1",
            flow_name=card.name,
            group_name=card.group,
            source_path="live-1.xlsx",
            state="running",
            current_step_name="Normalize",
            current_step_started_at_utc="2026-04-18T12:00:00+00:00",
            started_at_utc="2026-04-18T11:59:00+00:00",
            elapsed_seconds=60.0,
        )
    }

    presentation = build_selected_flow_presentation(
        card=card,
        tracker=OperationSessionState.empty(),
        flow_states={},
        run_groups=(),
        selected_run_key=None,
        live_runs=live_runs,
        live_truth_authoritative=True,
    )

    assert presentation.detail_state is not None
    assert [row.status for row in presentation.detail_state.operation_rows] == ["idle", "running", "idle"]


def _entry_to_group(entry: FlowLogEntry):
    from data_engine.domain import FlowRunState

    return FlowRunState.group_entries((entry,))[0]
