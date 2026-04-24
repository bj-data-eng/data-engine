from __future__ import annotations

from pathlib import Path

from data_engine.runtime.runtime_db import RuntimeCacheLedger, utcnow_text
from data_engine.services.ledger import RuntimeControlLedgerService
from data_engine.services.logs import LogService
from data_engine.services.daemon_state import DaemonStateService
from data_engine.services.runtime_history import RuntimeHistoryService
from data_engine.services.runtime_io import RuntimeIoLayer
from data_engine.services.runtime_binding import WorkspaceRuntimeBindingService

from tests.services.support import resolve_workspace_paths


def test_runtime_io_layer_serializes_execution_and_log_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state" / "runtime_cache.sqlite"
    store = RuntimeIoLayer(cache_ttl_seconds=1.0).open_cache_store(db_path)
    started_at = utcnow_text()

    store.execution_state.record_run_started(
        run_id="run-1",
        flow_name="docs_manual",
        group_name="Docs",
        source_path="docs.xlsx",
        started_at_utc=started_at,
    )
    step_run_id = store.execution_state.record_step_started(
        run_id="run-1",
        flow_name="docs_manual",
        step_label="Read Excel",
        started_at_utc=started_at,
    )
    store.execution_state.record_step_finished(
        step_run_id=step_run_id,
        status="success",
        finished_at_utc=started_at,
        elapsed_ms=123,
        output_path=None,
    )
    store.execution_state.record_run_finished(
        run_id="run-1",
        status="success",
        finished_at_utc=started_at,
    )
    store.logs.append(
        level="info",
        message="runtime:flow:docs_manual:status=success",
        created_at_utc=started_at,
        run_id="run-1",
        flow_name="docs_manual",
    )

    assert store.runs.list(flow_name="docs_manual")[0].status == "success"
    assert store.step_outputs.list_for_run("run-1")[0].elapsed_ms == 123
    assert store.logs.list(run_id="run-1")[0].message.endswith("status=success")
    store.close()


def test_runtime_io_layer_caches_reads_until_local_write_invalidates(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "runtime_state" / "runtime_cache.sqlite"
    ledger = RuntimeCacheLedger(db_path)
    started_at = utcnow_text()
    ledger.runs.record_started(
        run_id="run-1",
        flow_name="docs_manual",
        group_name="Docs",
        source_path="docs.xlsx",
        started_at_utc=started_at,
    )
    layer = RuntimeIoLayer(cache_ttl_seconds=60.0)
    store = layer.open_cache_store(db_path)

    delegate = store.runs._delegate  # type: ignore[attr-defined]
    original_list = delegate.list
    calls = {"count": 0}

    def _counted_list(*, flow_name: str | None = None):
        calls["count"] += 1
        return original_list(flow_name=flow_name)

    monkeypatch.setattr(delegate, "list", _counted_list)

    first = store.runs.list(flow_name="docs_manual")
    second = store.runs.list(flow_name="docs_manual")
    assert first == second
    assert calls["count"] == 1

    store.execution_state.record_run_finished(
        run_id="run-1",
        status="success",
        finished_at_utc=started_at,
    )
    refreshed = store.runs.list(flow_name="docs_manual")
    assert refreshed[0].status == "success"
    assert calls["count"] == 2

    store.close()
    ledger.close()


def test_runtime_binding_service_opens_runtime_io_cache_store(tmp_path: Path) -> None:
    paths = resolve_workspace_paths(workspace_root=tmp_path / "workspace")
    layer = RuntimeIoLayer(cache_ttl_seconds=1.0)
    service = WorkspaceRuntimeBindingService(
        ledger_service=RuntimeControlLedgerService(),
        log_service=LogService(),
        daemon_state_service=DaemonStateService(),
        runtime_history_service=RuntimeHistoryService(),
        runtime_io_layer=layer,
    )

    binding = service.open_binding(paths)

    try:
        binding.runtime_cache_ledger.execution_state.record_run_started(
            run_id="run-1",
            flow_name="docs_manual",
            group_name="Docs",
            source_path="docs.xlsx",
            started_at_utc=utcnow_text(),
        )
        assert binding.runtime_cache_ledger.runs.list(flow_name="docs_manual")
    finally:
        service.close_binding(binding)


def test_runtime_io_logs_proxy_supports_limited_tail_reads(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime_state" / "runtime_cache.sqlite"
    ledger = RuntimeCacheLedger(db_path)
    created_at = utcnow_text()
    ledger.logs.append(level="INFO", message="first", created_at_utc=created_at, run_id="run-1", flow_name="docs_manual")
    ledger.logs.append(level="INFO", message="second", created_at_utc=created_at, run_id="run-1", flow_name="docs_manual")
    ledger.logs.append(level="INFO", message="third", created_at_utc=created_at, run_id="run-1", flow_name="docs_manual")

    store = RuntimeIoLayer(cache_ttl_seconds=60.0).open_cache_store(db_path)
    try:
        tail = store.logs.list(flow_name="docs_manual", limit=2)
        assert [entry.message for entry in tail] == ["second", "third"]
    finally:
        store.close()
        ledger.close()


def test_runtime_io_read_cache_prunes_expired_entries_and_caps_size(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "runtime_state" / "runtime_cache.sqlite"
    ledger = RuntimeCacheLedger(db_path)
    started_at = utcnow_text()
    for index in range(1, 5):
        ledger.runs.record_started(
            run_id=f"run-{index}",
            flow_name="docs_manual",
            group_name="Docs",
            source_path=f"docs-{index}.xlsx",
            started_at_utc=started_at,
        )

    now = {"value": 0.0}
    monkeypatch.setattr("data_engine.services.runtime_io.monotonic", lambda: now["value"])
    store = RuntimeIoLayer(cache_ttl_seconds=1.0, max_read_cache_entries=2).open_cache_store(db_path)
    try:
        assert store.runs.get("run-1") is not None
        assert ("runs.get", "run-1") in store._handle._read_cache  # type: ignore[attr-defined]

        now["value"] = 2.0
        assert store.runs.get("run-2") is not None
        assert ("runs.get", "run-1") not in store._handle._read_cache  # type: ignore[attr-defined]

        assert store.runs.get("run-3") is not None
        assert store.runs.get("run-4") is not None
        assert len(store._handle._read_cache) <= 2  # type: ignore[attr-defined]
        assert ("runs.get", "run-2") not in store._handle._read_cache  # type: ignore[attr-defined]
    finally:
        store.close()
        ledger.close()

