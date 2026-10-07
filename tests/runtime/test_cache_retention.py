from __future__ import annotations

from datetime import UTC, datetime, timedelta
import sqlite3

import pytest

from data_engine.domain.source_state import SourceSignature
from data_engine.domain.time import utcnow_text
from data_engine.runtime.runtime_db import RuntimeCacheLedger


def _start(ledger, run_id, *, started_at=None):
    ledger.runs.record_started(
        run_id=run_id, flow_name="flow", group_name="Group", source_path=None, started_at_utc=started_at,
    )
    return ledger.step_outputs.record_started(run_id=run_id, flow_name="flow", step_label="Step", started_at_utc=started_at)


@pytest.mark.parametrize("parameter_limit", [16, None], ids=["restricted", "build_limit"])
def test_completion_prunes_backlog_larger_than_sqlite_parameter_limit(tmp_path, parameter_limit):
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    connection = ledger._connection()
    if parameter_limit is not None:
        connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, parameter_limit)
    count = connection.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) + 1
    old = (datetime.now(UTC) - timedelta(days=30)).isoformat()
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.executemany(
            "INSERT INTO runs(run_id, flow_name, group_name, status, started_at_utc, finished_at_utc) VALUES (?, 'flow', 'Group', 'success', ?, ?)",
            ((f"expired-{index}", old, old) for index in range(count)),
        )
        connection.execute("COMMIT")
        connection.execute(
            "INSERT INTO step_runs(run_id, flow_name, step_label, status, started_at_utc) VALUES ('expired-0', 'flow', 'Step', 'success', ?)", (old,),
        )
        ledger.logs.append(level="info", message="old log", run_id="expired-0", created_at_utc=old)
        signature = SourceSignature(source_path="source.txt", mtime_ns=1, size_bytes=1)
        ledger.execution_state.upsert_file_state(
            flow_name="flow", signature=signature, status="success", run_id="expired-0", finished_at_utc=old,
        )
        _start(ledger, "current")

        ledger.runs.record_finished(run_id="current", status="success", finished_at_utc=utcnow_text())

        assert [run.run_id for run in ledger.runs.list()] == ["current"]
        assert ledger.step_outputs.list_for_run("expired-0") == ()
        assert ledger.logs.list(run_id="expired-0") == ()
        state, = ledger.source_signatures.list_file_states(flow_name="flow")
        assert state.last_success_run_id is None
        assert state.last_success_at_utc == old
        assert state.last_status == "success"
        assert not connection.in_transaction
    finally:
        ledger.close()


def test_retention_preserves_aged_active_run_and_step_until_they_finish(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    old = (datetime.now(UTC) - timedelta(days=30)).isoformat()
    try:
        step_id = _start(ledger, "long-running", started_at=old)
        recent_step = _start(ledger, "recent")
        ledger.step_outputs.record_finished(step_run_id=recent_step, status="success", finished_at_utc=utcnow_text(), elapsed_ms=1)
        ledger.runs.record_finished(run_id="recent", status="success", finished_at_utc=utcnow_text())

        run, = ledger.runs.list_active()
        step, = ledger.step_outputs.list_active()
        assert run.run_id == step.run_id == "long-running"
        assert step.id == step_id
        ledger.step_outputs.record_finished(step_run_id=step_id, status="success", finished_at_utc=utcnow_text(), elapsed_ms=1)
        ledger.runs.record_finished(run_id="long-running", status="success", finished_at_utc=utcnow_text())
        assert ledger.runs.get("long-running").status == "success"
        assert ledger.step_outputs.get(step_id).status == "success"
    finally:
        ledger.close()


def test_retention_failure_rolls_back_cleanup_without_failing_completed_run(tmp_path, caplog):
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    connection = ledger._connection()
    old = (datetime.now(UTC) - timedelta(days=30)).isoformat()
    try:
        old_step = _start(ledger, "expired", started_at=old)
        connection.execute("UPDATE runs SET status='success', finished_at_utc=? WHERE run_id='expired'", (old,))
        ledger.logs.append(level="info", message="retained on rollback", run_id="expired", created_at_utc=old)
        ledger.execution_state.upsert_file_state(
            flow_name="flow", signature=SourceSignature(source_path="source.txt", mtime_ns=1, size_bytes=1),
            status="success", run_id="expired", finished_at_utc=old,
        )
        connection.execute("CREATE TRIGGER refuse_pruning BEFORE DELETE ON runs BEGIN SELECT RAISE(ABORT, 'maintenance failure'); END")

        with pytest.raises(sqlite3.IntegrityError, match="maintenance failure"):
            ledger.runs.prune_history(retention_days=7)
        _start(ledger, "current")
        ledger.runs.record_finished(run_id="current", status="success", finished_at_utc=utcnow_text())

        assert ledger.runs.get("current").status == "success"
        assert ledger.runs.get("expired") is not None
        assert ledger.step_outputs.get(old_step) is not None
        assert ledger.logs.list(run_id="expired")
        assert ledger.source_signatures.list_file_states(flow_name="flow")[0].last_success_run_id == "expired"
        assert not connection.in_transaction
        assert "Unable to prune runtime history" in caplog.text
        connection.execute("DROP TRIGGER refuse_pruning")
        ledger.runs.prune_history(retention_days=7)
        assert ledger.runs.get("expired") is None
        assert ledger.runs.get("current").status == "success"
    finally:
        ledger.close()


def test_retention_inside_existing_transaction_preserves_outer_rollback(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    connection = ledger._connection()
    old = (datetime.now(UTC) - timedelta(days=30)).isoformat()
    try:
        step_id = _start(ledger, "expired", started_at=old)
        connection.execute("UPDATE runs SET status='success', finished_at_utc=? WHERE run_id='expired'", (old,))
        ledger.logs.append(level="info", message="history", run_id="expired", created_at_utc=old)
        connection.execute("BEGIN IMMEDIATE")

        ledger.runs.prune_history(retention_days=7)

        assert connection.in_transaction
        assert ledger.runs.get("expired") is None
        connection.rollback()
        assert ledger.runs.get("expired") is not None
        assert ledger.step_outputs.get(step_id) is not None
        assert ledger.logs.list(run_id="expired")
    finally:
        ledger.close()


@pytest.mark.parametrize("retention_days", [0, -1])
def test_invalid_retention_is_rejected_but_maintenance_cannot_fail_completion(tmp_path, retention_days):
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    try:
        with pytest.raises(ValueError, match="retention_days must be positive"):
            ledger.runs.prune_history(retention_days=retention_days)
        ledger.HISTORY_RETENTION_DAYS = retention_days
        step_id = _start(ledger, "current")
        ledger.step_outputs.record_finished(step_run_id=step_id, status="success", finished_at_utc=utcnow_text(), elapsed_ms=1)
        ledger.runs.record_finished(run_id="current", status="success", finished_at_utc=utcnow_text())
        assert ledger.runs.get("current").status == "success"
        assert ledger.step_outputs.get(step_id).status == "success"
        assert not ledger._connection().in_transaction
    finally:
        ledger.close()


def test_workspace_recovery_reconciles_only_nonterminal_rows(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    try:
        active_step = _start(ledger, "orphan")
        finished_step = _start(ledger, "finished")
        finished_at = utcnow_text()
        ledger.step_outputs.record_finished(
            step_run_id=finished_step, status="failed", finished_at_utc=finished_at,
            elapsed_ms=1, error_text="Original failure",
        )
        ledger.runs.record_finished(
            run_id="finished", status="failed", finished_at_utc=finished_at, error_text="Original failure",
        )
        counts = ledger.reconcile_orphaned_activity(
            status="stopped", finished_at_utc=utcnow_text(), error_text="Workspace recovery",
        )
        assert counts == (1, 1)
        assert ledger.runs.get("orphan").status == "stopped"
        assert ledger.step_outputs.get(active_step).status == "stopped"
        for row in (ledger.runs.get("finished"), ledger.step_outputs.get(finished_step)):
            assert row.status == "failed"
            assert row.finished_at_utc == finished_at
            assert row.error_text == "Original failure"
        assert ledger.reconcile_orphaned_activity(status="stopped", finished_at_utc=utcnow_text()) == (0, 0)
    finally:
        ledger.close()
