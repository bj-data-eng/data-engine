from __future__ import annotations

from datetime import UTC, datetime, timedelta
from dataclasses import replace

import pytest

from data_engine.domain import FlowCatalogEntry, FlowLogEntry, OperationSessionState, RuntimeStepEvent
from data_engine.services.runtime_state import RunLiveSnapshot
from data_engine.views import build_selected_flow_presentation
from data_engine.views.runs import _display_duration_seconds


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
        _entry_to_group(replace(_run_group("finished-1", status="success"), created_at_utc=datetime(2026, 4, 18, 11, 57, tzinfo=UTC))),
        _entry_to_group(replace(_run_group("finished-2", status="failed"), created_at_utc=datetime(2026, 4, 18, 11, 58, tzinfo=UTC))),
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


def test_live_only_older_run_stays_before_newer_persisted_history():
    card = _card()
    now = datetime.now(UTC)
    saved = _entry_to_group(replace(_run_group("saved", status="success"), created_at_utc=now))
    live = RunLiveSnapshot(
        run_id="live", flow_name=card.name, group_name=card.group, source_path=None,
        state="running", started_at_utc=(now - timedelta(minutes=1)).isoformat(),
    )
    presentation = build_selected_flow_presentation(
        card=card, tracker=OperationSessionState.empty(), flow_states={}, run_groups=(saved,),
        selected_run_key=saved.key, live_runs={"live": live}, live_truth_authoritative=True,
    )
    assert tuple(run.key[1] for run in presentation.run_groups) == ("live", "saved")
    assert presentation.selected_run_key == saved.key


def test_confirmed_daemon_exit_stops_unfinished_history_without_hiding_it():
    card = _card()
    unfinished = _entry_to_group(_run_group("unfinished", status="started"))
    completed = _entry_to_group(_run_group("completed", status="success"))
    presentation = build_selected_flow_presentation(
        card=card, tracker=OperationSessionState.empty(), flow_states={},
        run_groups=(unfinished, completed), selected_run_key=unfinished.key,
        local_process_dead=True,
    )
    assert [run.status for run in presentation.run_groups] == ["stopped", "success"]
    assert presentation.run_groups[0].entries == unfinished.entries
    assert presentation.selected_run_key == unfinished.key


@pytest.mark.parametrize("authoritative", [False, True])
@pytest.mark.parametrize("live_runs", [None, {}])
def test_empty_live_truth_reconciles_only_when_available_and_authoritative(authoritative, live_runs):
    groups = tuple(_entry_to_group(_run_group(status, status=status)) for status in ("started", "stopping", "success", "failed", "stopped"))
    presentation = build_selected_flow_presentation(
        card=_card(), tracker=OperationSessionState.empty(), flow_states={},
        run_groups=groups, selected_run_key=groups[0].key,
        live_runs=live_runs, live_truth_authoritative=authoritative,
    )
    expected = groups[2:] if authoritative and live_runs is not None else groups
    assert presentation.run_groups == expected
    assert presentation.selected_run_key == expected[0].key


@pytest.mark.parametrize("state", ["running", "stopping"])
@pytest.mark.parametrize("step_only_history", [False, True])
def test_live_run_duration_uses_run_start_while_step_uses_step_start(state, step_only_history):
    card = _card()
    now = datetime.now(UTC)
    started = now - timedelta(seconds=60)
    step_started = now - timedelta(seconds=3)
    live = RunLiveSnapshot(
        run_id="live", flow_name=card.name, group_name=card.group, source_path=None,
        state=state, started_at_utc=started.isoformat(), elapsed_seconds=60.0,
        current_step_name="Write", current_step_started_at_utc=step_started.isoformat(),
    )
    history = ()
    if step_only_history:
        entry = FlowLogEntry(
            line="step-only history", kind="flow", flow_name=card.name,
            created_at_utc=step_started,
            event=RuntimeStepEvent(run_id="live", flow_name=card.name, step_name="Read", source_label="-", status="success"),
        )
        history = (_entry_to_group(entry),)
    presentation = build_selected_flow_presentation(
        card=card, tracker=OperationSessionState.empty(), flow_states={},
        run_groups=history, selected_run_key=None, max_visible_runs=1,
        live_runs={live.run_id: live}, live_truth_authoritative=True,
    )
    run, = presentation.visible_run_groups
    assert 60.0 <= _display_duration_seconds(run) < 65.0
    assert run.summary_entry.created_at_utc == started
    assert run.summary_entry.event.step_name is None
    step = run.steps[-1]
    assert step.entry.created_at_utc == step_started
    assert step.entry.event.step_name == "Write"
    assert 3.0 <= step.elapsed_seconds < 8.0
