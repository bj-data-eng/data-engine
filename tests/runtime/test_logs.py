from datetime import datetime

from data_engine.domain import FlowLogEntry
from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.services.logs import LogService
from data_engine.views.logs import FlowLogStore
from data_engine.domain import RuntimeStepEvent
from data_engine.domain.runs import FlowRunState


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
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_ledger.sqlite")
    created_at = utcnow_text()
    ledger.logs.append(
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


def test_log_service_reload_appends_only_new_persisted_entries(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_ledger.sqlite")
    created_at = utcnow_text()
    ledger.logs.append(
        level="INFO",
        message="run=run-1 flow=poller source=/tmp/input-a.xlsx status=success elapsed=0.250000",
        created_at_utc=created_at,
        run_id="run-1",
        flow_name="poller",
    )
    service = LogService()
    store = service.create_store(ledger)
    first_ids = tuple(entry.persisted_id for entry in store.entries())

    ledger.logs.append(
        level="INFO",
        message="run=run-2 flow=poller source=/tmp/input-b.xlsx status=success elapsed=0.250000",
        created_at_utc=utcnow_text(),
        run_id="run-2",
        flow_name="poller",
    )
    service.reload(store, ledger)

    entries = store.entries()
    assert len(entries) == 2
    assert tuple(entry.persisted_id for entry in entries[:1]) == first_ids
    assert entries[-1].event is not None
    assert entries[-1].event.run_id == "run-2"


def test_log_service_reload_dedupes_incremental_persisted_copy_of_live_entry(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_ledger.sqlite")
    first_created_at = utcnow_text()
    ledger.logs.append(
        level="INFO",
        message="run=run-0 flow=poller source=/tmp/old.xlsx status=success elapsed=0.250000",
        created_at_utc=first_created_at,
        run_id="run-0",
        flow_name="poller",
    )
    service = LogService()
    store = service.create_store(ledger)
    created_at = utcnow_text()
    live_entry = FlowLogEntry(
        line=FlowLogEntry.format_runtime_message(
            "run=run-1 flow=poller source=/tmp/input.xlsx status=success elapsed=0.250000"
        ),
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(
            run_id="run-1",
            flow_name="poller",
            step_name=None,
            source_label="input.xlsx",
            status="success",
            elapsed_seconds=0.25,
        ),
        created_at_utc=datetime.fromisoformat(created_at),
    )
    store.append_entry(live_entry)
    ledger.logs.append(
        level="INFO",
        message="run=run-1 flow=poller source=/tmp/input.xlsx status=success elapsed=0.250000",
        created_at_utc=created_at,
        run_id="run-1",
        flow_name="poller",
    )

    service.reload(store, ledger)

    entries = store.entries()
    assert len(entries) == 2
    assert [entry.event.run_id if entry.event is not None else None for entry in entries] == ["run-0", "run-1"]


def test_flow_log_store_dedupes_duplicate_live_runtime_events_with_different_timestamps():
    store = FlowLogStore()
    first = FlowLogEntry(
        line=FlowLogEntry.format_runtime_message(
            "run=run-1 flow=poller source=/tmp/input.xlsx status=success elapsed=0.250000"
        ),
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(
            run_id="run-1",
            flow_name="poller",
            step_name=None,
            source_label="input.xlsx",
            status="success",
            elapsed_seconds=0.25,
        ),
        created_at_utc=datetime.fromisoformat("2026-04-18T12:00:00+00:00"),
    )
    second = FlowLogEntry(
        line=FlowLogEntry.format_runtime_message(
            "run=run-1 flow=poller source=/tmp/input.xlsx status=success elapsed=0.250000"
        ),
        kind="flow",
        flow_name="poller",
        event=RuntimeStepEvent(
            run_id="run-1",
            flow_name="poller",
            step_name=None,
            source_label="input.xlsx",
            status="success",
            elapsed_seconds=0.25,
        ),
        created_at_utc=datetime.fromisoformat("2026-04-18T12:00:01+00:00"),
    )

    store.append_entry(first)
    store.append_entry(second)

    assert len(store.entries()) == 1


def test_flow_log_store_caches_grouped_runs_per_flow(monkeypatch):
    store = FlowLogStore()
    store.append_entry(
        FlowLogEntry(
            line="poller  started  input.xlsx",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="started"),
        )
    )

    original = FlowRunState.group_entries
    calls: list[tuple[FlowLogEntry, ...]] = []

    def counting_group_entries(entries):
        calls.append(entries)
        return original(entries)

    monkeypatch.setattr(FlowRunState, "group_entries", counting_group_entries)

    first = store.runs_for_flow("poller")
    second = store.runs_for_flow("poller")

    assert len(first) == 1
    assert second == first
    assert len(calls) == 1

    store.append_entry(
        FlowLogEntry(
            line="poller  success  input.xlsx",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(run_id="run-1", flow_name="poller", step_name=None, source_label="input.xlsx", status="success"),
        )
    )

    refreshed = store.runs_for_flow("poller")

    assert len(refreshed) == 1
    assert refreshed[0].status == "success"
    assert len(calls) == 2


def test_flow_log_store_prunes_oldest_entries_when_window_is_exceeded():
    store = FlowLogStore(max_entries=2)
    store.append_line("first", kind="flow", flow_name="poller")
    store.append_line("second", kind="flow", flow_name="poller")
    store.append_line("third", kind="flow", flow_name="poller")

    assert [entry.line for entry in store.entries()] == ["second", "third"]


def test_log_service_create_store_hydrates_only_latest_visible_log_window(tmp_path, monkeypatch):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_ledger.sqlite")
    created_at = utcnow_text()
    for message in ("first", "second", "third"):
        ledger.logs.append(
            level="INFO",
            message=f"run=run-{message} flow=poller source=/tmp/{message}.xlsx status=success elapsed=0.250000",
            created_at_utc=created_at,
            run_id=f"run-{message}",
            flow_name="poller",
        )

    monkeypatch.setattr(LogService, "DEFAULT_VISIBLE_LOG_LIMIT", 2)

    store = LogService().create_store(ledger)

    assert [entry.event.run_id if entry.event is not None else None for entry in store.entries()] == [
        "run-second",
        "run-third",
    ]
