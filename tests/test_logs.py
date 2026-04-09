from data_engine.domain import FlowLogEntry
from data_engine.runtime.runtime_db import RuntimeLedger, utcnow_text
from data_engine.services.logs import LogService
from data_engine.views.logs import FlowLogStore
from data_engine.domain import RuntimeStepEvent


def test_flow_log_store_filters_entries_by_selected_flow():
    store = FlowLogStore()

    poller_entry = store.append_line("poller started", kind="flow", flow_name="poller")
    store.append_entry(FlowLogEntry(line="manual started", kind="flow", flow_name="manual_review"))
    system_entry = store.append_line("global message", kind="system", flow_name=None)

    assert poller_entry.line == "poller started"
    assert poller_entry.kind == "flow"
    assert system_entry.kind == "system"
    assert tuple(entry.line for entry in store.entries_for_flow("poller")) == ("poller started",)
    assert tuple(entry.line for entry in store.entries_for_flow("manual_review")) == ("manual started",)
    assert store.entries_for_flow(None) == ()


def test_flow_log_store_clear_removes_all_entries():
    store = FlowLogStore()

    store.append_line("poller started", kind="flow", flow_name="poller")
    store.clear()

    assert store.entries_for_flow("poller") == ()


def test_flow_log_store_groups_runs_and_collapses_steps_within_run():
    store = FlowLogStore()
    run_started = FlowLogEntry(
        line="poller  started  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="started"),
    )
    started = FlowLogEntry(
        line="poller  read  started  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name="read", source_label="input.xlsx", status="started"),
    )
    success = FlowLogEntry(
        line="poller  read  success  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(
            run_id="run-1",
            flow_name="poller",
            step_name="read",
            source_label="input.xlsx",
            status="success",
            elapsed_seconds=0.25,
        ),
    )
    run_finished = FlowLogEntry(
        line="poller  success  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="success", elapsed_seconds=0.4),
    )

    store.append_entry(run_started)
    store.append_entry(started)
    store.append_entry(success)
    store.append_entry(run_finished)

    run_groups = store.runs_for_flow("poller")

    assert len(run_groups) == 1
    assert run_groups[0].source_label == "input.xlsx"
    assert run_groups[0].status == "success"
    assert len(run_groups[0].steps) == 1
    assert run_groups[0].steps[0].entry.event is not None
    assert run_groups[0].steps[0].entry.event.status == "success"
    assert len(run_groups[0].entries) == 4


def test_flow_log_store_separates_repeated_runs_for_same_source():
    store = FlowLogStore()
    first_run_started = FlowLogEntry(
        line="poller  started  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="started"),
    )
    first_run_finished = FlowLogEntry(
        line="poller  success  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="success"),
    )
    second_run_started = FlowLogEntry(
        line="poller  started  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-2", flow_name="poller", step_name=None, source_label="input.xlsx", status="started"),
    )

    store.append_entry(first_run_started)
    store.append_entry(first_run_finished)
    store.append_entry(second_run_started)

    run_groups = store.runs_for_flow("poller")

    assert len(run_groups) == 2
    assert run_groups[0].key != run_groups[1].key


def test_flow_log_store_keeps_failed_run_and_trailing_success_in_one_group():
    store = FlowLogStore()
    run_started = FlowLogEntry(
        line="poller  started  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="started"),
    )
    run_failed = FlowLogEntry(
        line="poller  failed  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="failed", elapsed_seconds=0.4),
    )
    run_finished = FlowLogEntry(
        line="poller  success  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="success", elapsed_seconds=0.4),
    )

    store.append_entry(run_started)
    store.append_entry(run_failed)
    store.append_entry(run_finished)

    run_groups = store.runs_for_flow("poller")

    assert len(run_groups) == 1
    assert run_groups[0].status == "failed"
    assert len(run_groups[0].entries) == 3


def test_flow_log_store_separates_different_sources_when_run_ids_differ():
    store = FlowLogStore()
    store.append_entry(
        FlowLogEntry(
            line="poller  started  input-a.xlsx",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input-a.xlsx", status="started"),
        )
    )
    store.append_entry(
        FlowLogEntry(
            line="poller  started  input-b.xlsx",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(run_id="run-2", flow_name="poller", step_name=None, source_label="input-b.xlsx", status="started"),
        )
    )
    store.append_entry(
        FlowLogEntry(
            line="poller  success  input-b.xlsx",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(run_id="run-2", flow_name="poller", step_name=None, source_label="input-b.xlsx", status="success", elapsed_seconds=0.4),
        )
    )

    run_groups = store.runs_for_flow("poller")

    assert len(run_groups) == 2
    assert {group.key for group in run_groups} == {("poller", "run-1"), ("poller", "run-2")}
    grouped_sizes = {group.key: len(group.entries) for group in run_groups}
    assert grouped_sizes[("poller", "run-1")] == 1
    assert grouped_sizes[("poller", "run-2")] == 2


def test_flow_log_store_clear_flow_only_removes_selected_flow():
    store = FlowLogStore()
    store.append_line("poller started", kind="flow", flow_name="poller")
    store.append_line("manual started", kind="flow", flow_name="manual_review")

    store.clear_flow("poller")

    assert tuple(entry.line for entry in store.entries_for_flow("poller")) == ()
    assert tuple(entry.line for entry in store.entries_for_flow("manual_review")) == ("manual started",)


def test_flow_log_store_ignores_unstructured_flow_entries_in_run_groups():
    store = FlowLogStore()
    store.append_line("plain flow log", kind="flow", flow_name="poller")

    assert store.runs_for_flow("poller") == ()


def test_flow_log_store_uses_latest_summary_status_unless_failed_or_stopped():
    store = FlowLogStore()
    started = FlowLogEntry(
        line="poller  started  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="started"),
    )
    failed = FlowLogEntry(
        line="poller  failed  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="failed"),
    )
    finished = FlowLogEntry(
        line="poller  success  input.xlsx",
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="success"),
    )

    store.append_entry(started)
    store.append_entry(failed)
    store.append_entry(finished)

    groups = store.runs_for_flow("poller")

    assert len(groups) == 1
    assert groups[0].status == "failed"


def test_flow_log_store_hydrates_persisted_runtime_logs(tmp_path):
    ledger = RuntimeLedger(tmp_path / "runtime_state" / "runtime_ledger.sqlite")
    created_at = utcnow_text()
    ledger.append_log(
        level="INFO",
        message="run=run-1 flow=poller source=/tmp/input.xlsx status=success elapsed=0.250000",
        created_at_utc=created_at,
        run_id="run-1",
        flow_name="poller",
    )

    store = LogService().create_store(ledger)
    groups = store.runs_for_flow("poller")

    assert len(groups) == 1
    assert groups[0].status == "success"
    assert groups[0].source_label == "input.xlsx"
