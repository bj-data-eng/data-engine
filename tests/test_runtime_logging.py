from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import gc
import threading
import weakref

import pytest

import data_engine.runtime.execution.logging as runtime_logging
from data_engine.runtime.execution.logging import RuntimeLogEmitter, acquire_queued_runtime_log_sink
from data_engine.runtime.runtime_db import RuntimeCacheLedger


class _FakeLogSink:
    def __init__(self) -> None:
        self.rows = []
        self.append_calls = 0
        self.append_many_calls: list[int] = []

    def append(
        self,
        *,
        level: str,
        message: str,
        created_at_utc: str,
        run_id: str | None = None,
        flow_name: str | None = None,
        step_label: str | None = None,
    ) -> None:
        self.append_calls += 1
        self.rows.append((level, message, created_at_utc, run_id, flow_name, step_label))

    def append_many(self, rows) -> None:
        self.append_many_calls.append(len(rows))
        self.rows.extend(
            (row.level, row.message, row.created_at_utc, row.run_id, row.flow_name, row.step_label)
            for row in rows
        )


def test_queued_runtime_log_sink_flushes_shared_batches_on_last_close():
    sink = _FakeLogSink()
    first = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001, max_batch_size=100)
    second = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001, max_batch_size=100)
    first_emitter = RuntimeLogEmitter(first)
    second_emitter = RuntimeLogEmitter(second)

    first_emitter.log_runtime_message("first", level="info", run_id="run-1", flow_name="docs_poll", step_label="Read Excel")
    second_emitter.log_runtime_message("second", level="info", run_id="run-1", flow_name="docs_poll", step_label="Write Parquet")

    first.close()
    assert sink.rows == []

    second.close()

    assert [row[1] for row in sink.rows] == ["first", "second"]
    assert sink.append_calls == 0
    assert sum(sink.append_many_calls) == 2


def test_last_log_handle_release_allows_closed_ledger_collection(tmp_path):
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    ledger_ref = weakref.ref(ledger)
    sink_ref = weakref.ref(ledger.logs)
    handle = acquire_queued_runtime_log_sink(ledger.logs)
    RuntimeLogEmitter(handle).log_runtime_message("persisted", level="info", run_id=None, flow_name=None)
    handle.close()
    assert ledger.logs.list()[0].message == "persisted"
    ledger.close()
    del handle, ledger
    gc.collect()

    assert ledger_ref() is None
    assert sink_ref() is None


def test_reacquire_during_final_release_keeps_replacement_registered(monkeypatch):
    blocked = threading.Event()
    release = threading.Event()
    closing = threading.Event()

    class _BlockingSink(_FakeLogSink):
        def append_many(self, rows):
            if rows[0].message == "old":
                blocked.set()
                assert release.wait(5)
            super().append_many(rows)

    sink = _BlockingSink()
    first = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001)
    original_join = first._shared_sink._worker.join

    def join():
        closing.set()
        original_join()

    monkeypatch.setattr(first._shared_sink._worker, "join", join)
    RuntimeLogEmitter(first).log_runtime_message("old", level="info", run_id=None, flow_name=None)
    second = third = None
    try:
        assert blocked.wait(5)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(first.close)
            try:
                assert closing.wait(5)
                second = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001)
                release.set()
                future.result(timeout=5)
                third = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001)
                assert second._shared_sink is third._shared_sink
                assert runtime_logging._SHARED_QUEUED_SINKS[sink] is second._shared_sink
                RuntimeLogEmitter(third).log_runtime_message("new", level="info", run_id=None, flow_name=None)
            finally:
                release.set()
    finally:
        release.set()
        first.close()
        if second is not None:
            second.close()
        if third is not None:
            third.close()
    assert sink not in runtime_logging._SHARED_QUEUED_SINKS
    assert {row[1] for row in sink.rows} == {"old", "new"}


def test_concurrent_close_of_one_handle_preserves_other_handle():
    sink = _FakeLogSink()
    first = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001)
    second = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001)
    barrier = threading.Barrier(2)

    def close():
        barrier.wait(timeout=5)
        first.close()

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(close) for _ in range(2)]
            for future in futures:
                future.result(timeout=5)
        RuntimeLogEmitter(second).log_runtime_message("survived", level="info", run_id=None, flow_name=None)
    finally:
        first.close()
        second.close()
    assert [row[1] for row in sink.rows] == ["survived"]


def test_failed_sink_release_removes_registry_entry_and_allows_reacquire():
    class _FailingSink(_FakeLogSink):
        failing = True

        def append_many(self, rows):
            if self.failing:
                raise OSError("write failed")
            super().append_many(rows)

    sink = _FailingSink()
    first = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001)
    RuntimeLogEmitter(first).log_runtime_message("failed", level="info", run_id=None, flow_name=None)
    with pytest.raises(RuntimeError, match="Queued runtime log sink failed"):
        first.close()
    assert sink not in runtime_logging._SHARED_QUEUED_SINKS
    sink.failing = False
    second = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001)
    RuntimeLogEmitter(second).log_runtime_message("recovered", level="info", run_id=None, flow_name=None)
    second.close()
    assert [row[1] for row in sink.rows] == ["recovered"]


def test_queued_runtime_log_sink_can_be_reacquired_after_last_close():
    sink = _FakeLogSink()
    first = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001, max_batch_size=100)
    RuntimeLogEmitter(first).log_runtime_message("first", level="info", run_id="run-1", flow_name="docs_poll")
    first.close()

    second = acquire_queued_runtime_log_sink(sink, flush_interval_seconds=0.001, max_batch_size=100)
    RuntimeLogEmitter(second).log_runtime_message("second", level="info", run_id="run-2", flow_name="docs_poll")
    second.close()

    assert [row[1] for row in sink.rows] == ["first", "second"]

