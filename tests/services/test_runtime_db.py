from __future__ import annotations

from datetime import UTC, datetime, timedelta

import data_engine.runtime.runtime_control_store as runtime_control_store_module
from data_engine.runtime.ledger_models import PersistedFileState, PersistedLogEntry, PersistedRun, PersistedStepRun
from data_engine.runtime.runtime_db import RuntimeCacheLedger, RuntimeControlLedger, utcnow_text


def test_runtime_ledger_initializes_schema_and_workspace_path(tmp_path):
    db_path = tmp_path / "runtime_state" / "runtime_cache.sqlite"

    ledger = RuntimeCacheLedger(db_path)

    assert db_path.exists()
    table_names = {
        row[0]
        for row in ledger._connection().execute(  # noqa: SLF001 - targeted schema verification
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    assert {"runs", "step_runs", "file_state", "logs"} <= table_names

    control = RuntimeControlLedger(tmp_path / "runtime_state" / "runtime_control.sqlite")
    control_table_names = {
        row[0]
        for row in control._connection().execute(  # noqa: SLF001 - targeted schema verification
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    assert {"daemon_state", "client_sessions"} <= control_table_names


def test_runtime_state_store_exposes_explicit_repositories(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_cache.sqlite")
    started_at = utcnow_text()

    ledger.runs.record_started(
        run_id="run-1",
        flow_name="docs_poll",
        group_name="Docs",
        source_path=None,
        started_at_utc=started_at,
    )
    step_run_id = ledger.step_outputs.record_started(
        run_id="run-1",
        flow_name="docs_poll",
        step_label="Read Excel",
        started_at_utc=started_at,
    )
    ledger.step_outputs.record_finished(
        step_run_id=step_run_id,
        status="success",
        finished_at_utc=started_at,
        elapsed_ms=1,
        output_path="/tmp/output.parquet",
    )
    ledger.runs.record_finished(run_id="run-1", status="success", finished_at_utc=started_at)
    ledger.logs.append(level="INFO", message="done", created_at_utc=started_at, run_id="run-1", flow_name="docs_poll")

    source = tmp_path / "input.xlsx"
    source.write_text("docs", encoding="utf-8")
    signature = ledger.source_signatures.signature_for_path(source)

    assert signature is not None
    ledger.source_signatures.upsert_file_state(
        flow_name="docs_poll",
        signature=signature,
        status="success",
        run_id="run-1",
        finished_at_utc=started_at,
    )

    assert ledger.runs.list(flow_name="docs_poll")[0].run_id == "run-1"
    assert ledger.step_outputs.list_for_run("run-1")[0].output_path == "/tmp/output.parquet"
    assert ledger.logs.list(flow_name="docs_poll")[0].message == "done"
    assert ledger.source_signatures.list_file_states(flow_name="docs_poll")[0].source_path == signature.source_path


def test_runtime_control_store_exposes_explicit_repositories(tmp_path, monkeypatch):
    ledger = RuntimeControlLedger(tmp_path / "runtime_state" / "runtime_control.sqlite")
    monkeypatch.setattr(runtime_control_store_module, "process_is_running", lambda pid, *, treat_defunct_as_dead: pid == 123)
    now = utcnow_text()

    ledger.daemon_state.upsert(
        workspace_id="default",
        pid=123,
        endpoint_kind="tcp",
        endpoint_path="127.0.0.1:0",
        started_at_utc=now,
        last_checkpoint_at_utc=now,
        status="running",
        app_root="/tmp/app",
        workspace_root="/tmp/workspace/default",
    )
    ledger.client_sessions.upsert(client_id="ui-1", workspace_id="default", client_kind="ui", pid=123)

    assert ledger.daemon_state.get("default") is not None
    assert ledger.client_sessions.count_live("default") == 1


def test_runtime_ledger_open_default_ignores_blank_env_override(tmp_path, monkeypatch):
    workspace = tmp_path / "collection" / "default"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(tmp_path / "data_engine"))
    monkeypatch.delenv("DATA_ENGINE_RUNTIME_CACHE_DB_PATH", raising=False)
    monkeypatch.delenv("DATA_ENGINE_RUNTIME_CONTROL_DB_PATH", raising=False)
    monkeypatch.setenv("DATA_ENGINE_RUNTIME_DB_PATH", "   ")

    ledger = RuntimeCacheLedger.open_default(data_root=workspace)

    from tests.services.support import resolve_workspace_paths

    expected = resolve_workspace_paths(workspace_root=workspace).runtime_db_path

    assert ledger.db_path == expected


def test_runtime_ledger_poll_staleness_uses_signature_and_last_status(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_cache.sqlite")
    source = tmp_path / "input.xlsx"
    source.write_text("docs", encoding="utf-8")

    signature = ledger.source_signatures.signature_for_path(source)

    assert signature is not None
    assert ledger.source_signatures.is_stale("docs_poll", signature) is True

    ledger.source_signatures.upsert_file_state(flow_name="docs_poll", signature=signature, status="success", run_id="run-1", finished_at_utc=utcnow_text())
    assert ledger.source_signatures.is_stale("docs_poll", signature) is False

    ledger.source_signatures.upsert_file_state(flow_name="docs_poll", signature=signature, status="failed", error_text="boom")
    assert ledger.source_signatures.is_stale("docs_poll", signature) is False

    source.write_text("docs changed", encoding="utf-8")
    changed_signature = ledger.source_signatures.signature_for_path(source)

    assert changed_signature is not None
    assert ledger.source_signatures.is_stale("docs_poll", changed_signature) is True


def test_runtime_ledger_persists_run_step_and_log_history(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_cache.sqlite")
    started_at = utcnow_text()
    ledger.runs.record_started(
        run_id="run-1",
        flow_name="docs_poll",
        group_name="Docs",
        source_path="/tmp/input.xlsx",
        started_at_utc=started_at,
    )
    step_run_id = ledger.step_outputs.record_started(
        run_id="run-1",
        flow_name="docs_poll",
        step_label="Read Excel",
        started_at_utc=started_at,
    )
    finished_at = utcnow_text()
    ledger.step_outputs.record_finished(
        step_run_id=step_run_id,
        status="success",
        finished_at_utc=finished_at,
        elapsed_ms=125,
        output_path="/tmp/output.parquet",
    )
    ledger.runs.record_finished(
        run_id="run-1",
        status="success",
        finished_at_utc=finished_at,
    )
    ledger.logs.append(
        level="INFO",
        message="run=run-1 flow=docs_poll source=/tmp/input.xlsx status=success elapsed=0.125000",
        created_at_utc=finished_at,
        run_id="run-1",
        flow_name="docs_poll",
    )

    runs = ledger.runs.list(flow_name="docs_poll")
    step_runs = ledger.step_outputs.list_for_run("run-1")
    logs = ledger.logs.list(flow_name="docs_poll")

    assert len(runs) == 1
    assert runs[0].status == "success"
    assert runs[0].elapsed_seconds is not None
    assert len(step_runs) == 1
    assert step_runs[0].status == "success"
    assert step_runs[0].elapsed_ms == 125
    assert step_runs[0].output_path == "/tmp/output.parquet"
    assert len(logs) == 1
    assert logs[0].run_id == "run-1"


def test_runtime_log_repository_append_many_persists_batched_rows(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_cache.sqlite")
    created_at = utcnow_text()
    ledger.logs.append_many(
        (
            PersistedLogEntry(
                id=1,
                run_id="run-1",
                flow_name="docs_poll",
                step_label="Read Excel",
                level="INFO",
                message="first",
                created_at_utc=created_at,
            ),
            PersistedLogEntry(
                id=2,
                run_id="run-1",
                flow_name="docs_poll",
                step_label="Write Parquet",
                level="INFO",
                message="second",
                created_at_utc=created_at,
            ),
        )
    )

    logs = ledger.logs.list(flow_name="docs_poll")

    assert [entry.message for entry in logs] == ["first", "second"]


def test_runtime_log_repository_list_after_id_returns_incremental_tail(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_cache.sqlite")
    created_at = utcnow_text()
    ledger.logs.append(level="INFO", message="first", created_at_utc=created_at, run_id="run-1", flow_name="docs_poll")
    ledger.logs.append(level="INFO", message="second", created_at_utc=created_at, run_id="run-1", flow_name="docs_poll")
    all_logs = ledger.logs.list(flow_name="docs_poll")

    tail = ledger.logs.list(flow_name="docs_poll", after_id=all_logs[0].id)

    assert [entry.message for entry in tail] == ["second"]


def test_runtime_log_repository_list_limit_returns_latest_rows_in_ascending_order(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_cache.sqlite")
    created_at = utcnow_text()
    ledger.logs.append(level="INFO", message="first", created_at_utc=created_at, run_id="run-1", flow_name="docs_poll")
    ledger.logs.append(level="INFO", message="second", created_at_utc=created_at, run_id="run-1", flow_name="docs_poll")
    ledger.logs.append(level="INFO", message="third", created_at_utc=created_at, run_id="run-1", flow_name="docs_poll")

    tail = ledger.logs.list(flow_name="docs_poll", limit=2)

    assert [entry.message for entry in tail] == ["second", "third"]


def test_runtime_ledger_prunes_history_older_than_30_days(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_cache.sqlite")
    old_started = (datetime.now(UTC) - timedelta(days=31)).isoformat()
    old_finished = (datetime.now(UTC) - timedelta(days=31, seconds=-1)).isoformat()
    new_started = utcnow_text()

    ledger.runs.record_started(run_id="old-run", flow_name="docs_poll", group_name="Docs", source_path="/tmp/old.xlsx", started_at_utc=old_started)
    old_step_id = ledger.step_outputs.record_started(run_id="old-run", flow_name="docs_poll", step_label="Read Excel", started_at_utc=old_started)
    ledger.step_outputs.record_finished(step_run_id=old_step_id, status="success", finished_at_utc=old_finished, elapsed_ms=10)
    ledger.logs.append(level="INFO", message="old log", created_at_utc=old_finished, run_id="old-run", flow_name="docs_poll")
    ledger.runs.record_started(run_id="new-run", flow_name="docs_poll", group_name="Docs", source_path="/tmp/new.xlsx", started_at_utc=new_started)
    new_step_id = ledger.step_outputs.record_started(run_id="new-run", flow_name="docs_poll", step_label="Read Excel", started_at_utc=new_started)
    ledger.step_outputs.record_finished(step_run_id=new_step_id, status="success", finished_at_utc=new_started, elapsed_ms=10)
    ledger.logs.append(level="INFO", message="new log", created_at_utc=new_started, run_id="new-run", flow_name="docs_poll")
    ledger.runs.record_finished(run_id="old-run", status="success", finished_at_utc=old_finished)
    ledger.runs.record_finished(run_id="new-run", status="success", finished_at_utc=new_started)

    runs = ledger.runs.list(flow_name="docs_poll")
    logs = ledger.logs.list(flow_name="docs_poll")

    assert [run.run_id for run in runs] == ["new-run"]
    assert [entry.run_id for entry in logs] == ["new-run"]
    assert ledger.step_outputs.list_for_run("old-run") == ()


def test_runtime_ledger_prunes_missing_file_state_rows(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_cache.sqlite")
    source_a = tmp_path / "a.xlsx"
    source_b = tmp_path / "b.xlsx"
    source_a.write_text("a", encoding="utf-8")
    source_b.write_text("b", encoding="utf-8")

    signature_a = ledger.source_signatures.signature_for_path(source_a)
    signature_b = ledger.source_signatures.signature_for_path(source_b)

    assert signature_a is not None
    assert signature_b is not None

    ledger.source_signatures.upsert_file_state(flow_name="docs_poll", signature=signature_a, status="success", run_id="run-a", finished_at_utc=utcnow_text())
    ledger.source_signatures.upsert_file_state(flow_name="docs_poll", signature=signature_b, status="success", run_id="run-b", finished_at_utc=utcnow_text())

    ledger.source_signatures.prune_missing(flow_name="docs_poll", current_source_paths={signature_a.source_path})

    states = ledger.source_signatures.list_file_states(flow_name="docs_poll")

    assert [state.source_path for state in states] == [signature_a.source_path]


def test_runtime_ledger_persists_daemon_state(tmp_path):
    ledger = RuntimeControlLedger(tmp_path / "runtime_state" / "runtime_control.sqlite")

    ledger.daemon_state.upsert(
        workspace_id="default",
        pid=123,
        endpoint_kind="unix",
        endpoint_path="/tmp/data_engine.sock",
        started_at_utc=utcnow_text(),
        last_checkpoint_at_utc=utcnow_text(),
        status="idle",
        app_root="/tmp/app",
        workspace_root="/tmp/workspace/default",
        version_text="0.1.0",
    )

    state = ledger.daemon_state.get("default")

    assert state is not None
    assert state.workspace_id == "default"
    assert state.pid == 123
    assert state.status == "idle"


def test_runtime_control_ledger_counts_live_windows_client_sessions(tmp_path, monkeypatch):
    ledger = RuntimeControlLedger(tmp_path / "runtime_state" / "runtime_control.sqlite")
    monkeypatch.setattr(runtime_control_store_module.os, "name", "nt")
    monkeypatch.setattr(
        runtime_control_store_module,
        "process_is_running",
        lambda pid, *, treat_defunct_as_dead: pid == 4321 and treat_defunct_as_dead is False,
    )

    ledger.client_sessions.upsert(client_id="live-ui", workspace_id="docs2", client_kind="ui", pid=4321)
    ledger.client_sessions.upsert(client_id="dead-ui", workspace_id="docs2", client_kind="ui", pid=9876)

    assert ledger.client_sessions.count_live("docs2") == 1
    remaining = ledger._connection().execute("SELECT client_id FROM client_sessions ORDER BY client_id").fetchall()  # noqa: SLF001
    assert [row[0] for row in remaining] == ["live-ui"]


def test_replace_runtime_snapshot_begins_immediate_transaction(tmp_path, monkeypatch):
    ledger = RuntimeCacheLedger(tmp_path / "runtime_state" / "runtime_cache.sqlite")
    statements: list[str] = []

    class _ConnectionProbe:
        def execute(self, sql: str, *args, **kwargs):
            statements.append(sql.strip())
            return self

        def executemany(self, sql: str, params):
            statements.append(sql.strip())
            return self

        def commit(self) -> None:
            statements.append("COMMIT")

        def rollback(self) -> None:
            statements.append("ROLLBACK")

    monkeypatch.setattr(ledger, "_connection", lambda: _ConnectionProbe())  # noqa: SLF001 - targeted transaction test

    ledger.snapshots.replace(
        runs=(PersistedRun(run_id="run-1", flow_name="demo", group_name="Demo", source_path=None, status="success", started_at_utc="2026-04-06T00:00:00+00:00", finished_at_utc="2026-04-06T00:00:01+00:00", error_text=None),),
        step_runs=(PersistedStepRun(id=1, run_id="run-1", flow_name="demo", step_label="Step 1", status="success", started_at_utc="2026-04-06T00:00:00+00:00", finished_at_utc="2026-04-06T00:00:01+00:00", elapsed_ms=1, error_text=None, output_path=None),),
        logs=(PersistedLogEntry(id=1, run_id="run-1", flow_name="demo", step_label=None, level="INFO", message="done", created_at_utc="2026-04-06T00:00:01+00:00"),),
        file_states=(PersistedFileState(flow_name="demo", source_path="/tmp/demo.xlsx", mtime_ns=1, size_bytes=1, last_success_run_id="run-1", last_success_at_utc="2026-04-06T00:00:01+00:00", last_status="success", last_error_text=None),),
    )

    assert statements[0] == "BEGIN IMMEDIATE"
    assert statements[-1] == "COMMIT"

