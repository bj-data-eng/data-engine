from __future__ import annotations

from datetime import UTC, datetime, timedelta

from data_engine.domain import FlowCatalogEntry, FlowLogEntry, OperationSessionState, RuntimeStepEvent
from data_engine.services.runtime_state import RunLiveSnapshot
from data_engine.views import build_selected_flow_presentation


def _card() -> FlowCatalogEntry:
    return FlowCatalogEntry(
        name="docs_parallel_schedule",
        group="Docs",
        title="Docs Parallel Schedule",
        description="Parallel scheduled docs flow.",
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
        flow_name="docs_parallel_schedule",
        event=RuntimeStepEvent(
            run_id=run_id,
            flow_name="docs_parallel_schedule",
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
    assert all(group.steps[-1].step_name == "Normalize" for group in presentation.run_groups)


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
        selected_run_key=("docs_parallel_schedule", "live-1"),
        live_runs=live_runs,
        live_truth_authoritative=True,
    )

    assert tuple(group.key[1] for group in presentation.run_groups) == ("finished-1", "finished-2", "live-1")
    assert presentation.selected_run_group is not None
    assert presentation.selected_run_group.key == ("docs_parallel_schedule", "live-1")
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


def test_selected_flow_presentation_represents_parallel_live_steps_without_serializing_them() -> None:
    card = FlowCatalogEntry(
        **{**_card().__dict__, "parallelism": "4"}
    )
    now = datetime.now(UTC)
    live_runs = {
        "live-1": RunLiveSnapshot(
            run_id="live-1",
            flow_name=card.name,
            group_name=card.group,
            source_path="live-1.xlsx",
            state="running",
            current_step_name="Read",
            current_step_started_at_utc=(now - timedelta(seconds=4)).isoformat(),
            started_at_utc=(now - timedelta(seconds=60)).isoformat(),
            elapsed_seconds=60.0,
        ),
        "live-2": RunLiveSnapshot(
            run_id="live-2",
            flow_name=card.name,
            group_name=card.group,
            source_path="live-2.xlsx",
            state="running",
            current_step_name="Normalize",
            current_step_started_at_utc=(now - timedelta(seconds=3)).isoformat(),
            started_at_utc=(now - timedelta(seconds=65)).isoformat(),
            elapsed_seconds=65.0,
        ),
        "live-3": RunLiveSnapshot(
            run_id="live-3",
            flow_name=card.name,
            group_name=card.group,
            source_path="live-3.xlsx",
            state="running",
            current_step_name="Normalize",
            current_step_started_at_utc=(now - timedelta(seconds=2)).isoformat(),
            started_at_utc=(now - timedelta(seconds=70)).isoformat(),
            elapsed_seconds=70.0,
        ),
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
    rows = {row.name: row for row in presentation.detail_state.operation_rows}
    assert rows["Read"].status == "running"
    assert rows["Read"].active_count == 1
    assert rows["Read"].live_started_at_utc == live_runs["live-1"].current_step_started_at_utc
    assert rows["Read"].live_elapsed_seconds is not None
    assert 0.0 <= rows["Read"].live_elapsed_seconds <= 10.0
    assert rows["Normalize"].status == "running"
    assert rows["Normalize"].active_count == 2
    assert rows["Normalize"].live_elapsed_seconds is None
    assert rows["Write"].status == "idle"


def test_selected_flow_presentation_keeps_finished_duration_for_parallel_flow_when_operation_is_no_longer_active() -> None:
    card = FlowCatalogEntry(
        **{**_card().__dict__, "parallelism": "4"}
    )
    tracker = OperationSessionState.empty().ensure_flow(card.name, card.operation_items)
    tracker, _ = tracker.apply_event(
        card.name,
        card.operation_items,
        RuntimeStepEvent(
            run_id="run-1",
            flow_name=card.name,
            step_name="Read",
            source_label="live-1.xlsx",
            status="success",
            elapsed_seconds=4.2,
        ),
        now=0.0,
    )

    presentation = build_selected_flow_presentation(
        card=card,
        tracker=tracker,
        flow_states={},
        run_groups=(),
        selected_run_key=None,
        live_runs={},
        live_truth_authoritative=True,
    )

    assert presentation.detail_state is not None
    rows = {row.name: row for row in presentation.detail_state.operation_rows}
    assert rows["Read"].status == "idle"
    assert rows["Read"].active_count == 0
    assert rows["Read"].elapsed_seconds == 4.2


def test_selected_flow_presentation_overlays_existing_nonterminal_group_with_live_run_data() -> None:
    card = _card()
    run_groups = (_entry_to_group(_run_group("run-1")),)
    live_runs = {
        "run-1": RunLiveSnapshot(
            run_id="run-1",
            flow_name=card.name,
            group_name=card.group,
            source_path="run-1.xlsx",
            state="running",
            current_step_name="Write",
            current_step_started_at_utc="2026-04-18T12:05:00+00:00",
            started_at_utc="2026-04-18T12:00:00+00:00",
            elapsed_seconds=300.0,
        )
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

    assert presentation.run_groups[0].key == (card.name, "run-1")
    assert presentation.run_groups[0].status == "started"
    assert presentation.run_groups[0].steps[-1].step_name == "Write"


def _entry_to_group(entry: FlowLogEntry):
    from data_engine.domain import FlowRunState

    return FlowRunState.group_entries((entry,))[0]

