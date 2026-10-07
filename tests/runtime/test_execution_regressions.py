from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
import os
from queue import Queue
import threading

import polars as pl
import pytest

from data_engine.authoring.flow import Flow
from data_engine.core.model import FlowExecutionError, FlowStoppedError
from data_engine.domain.time import utcnow_text
from data_engine.runtime.execution.single import FlowRuntime
from data_engine.runtime.execution.polling import RuntimePollingSupport
from data_engine.runtime.execution.grouped import GroupedFlowRuntime
import data_engine.runtime.execution.grouped as grouped_module
from data_engine.runtime.file_watch import PollingWatcher
from data_engine.runtime.runtime_db import RuntimeCacheLedger


@pytest.mark.parametrize("outcome", ["success", "failed", "stopped"])
def test_source_updates_coalesce_behind_active_run_without_blocking_other_sources(tmp_path, outcome):
    source_root = tmp_path / "input"
    source_root.mkdir()
    first = source_root / "a.txt"
    second = source_root / "b.txt"
    first.write_text("v1", encoding="utf-8")
    second.write_text("other", encoding="utf-8")
    alias_parent = source_root / "alias"
    alias_parent.mkdir()
    alias = alias_parent / ".." / first.name
    output = tmp_path / "result.txt"
    started = threading.Event()
    release = threading.Event()
    other_finished = threading.Event()
    calls = []

    def publish(context):
        source = context.source.path
        value = source.read_text(encoding="utf-8")
        calls.append((source.name, value))
        if source.name == first.name and value == "v1":
            started.set()
            assert release.wait(5)
            if outcome == "failed":
                raise ValueError("first attempt failed")
            if outcome == "stopped":
                raise FlowStoppedError("first attempt stopped")
        if source.name == first.name:
            output.write_text(value, encoding="utf-8")
        else:
            other_finished.set()
        return value

    flow = Flow(name="updates", group="Inputs").watch(
        mode="poll", source=source_root, interval="1s", max_parallel=2,
    ).step(publish)
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    runtime = FlowRuntime((flow,), continuous=True, runtime_ledger=ledger)
    queue = deque()
    queued_keys = set()
    pending = {}
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            try:
                runtime.polling.enqueue_job(queue, queued_keys, flow, first)
                runtime.dispatch_queued_jobs(queue, queued_keys, pending, executor, results=None)
                assert started.wait(5)
                for value, source in [("v2", alias), ("v3-latest", first)]:
                    first.write_text(value, encoding="utf-8")
                    runtime.polling.enqueue_job(queue, queued_keys, flow, source)
                assert len(queue) == len(queued_keys) == 1
                runtime.polling.enqueue_job(queue, queued_keys, flow, second)
                runtime.dispatch_queued_jobs(queue, queued_keys, pending, executor, results=None)
                assert other_finished.wait(5)
                for future, (job, _) in tuple(pending.items()):
                    if job.source_path == second:
                        future.result(timeout=5)
                runtime.dispatch_queued_jobs(queue, queued_keys, pending, executor, results=None)
                assert len(queue) == 1
                assert calls == [("a.txt", "v1"), ("b.txt", "other")]
                release.set()
                runtime.wait_for_dispatched_jobs(pending, results=None)
                runtime.dispatch_queued_jobs(queue, queued_keys, pending, executor, results=None)
                runtime.wait_for_dispatched_jobs(pending, results=None)
            finally:
                release.set()
        assert calls == [("a.txt", "v1"), ("b.txt", "other"), ("a.txt", "v3-latest")]
        assert output.read_text(encoding="utf-8") == "v3-latest"
        assert not runtime.polling.is_poll_source_stale(flow, first)
        assert len(ledger.runs.list(flow_name=flow.name)) == 3
        assert not queue and not queued_keys and not pending
    finally:
        runtime._close_runtime_resources()
        ledger.close()


def test_batch_polling_prunes_only_missing_sources_after_complete_enumeration(tmp_path):
    source_root = tmp_path / "input"
    source_root.mkdir()
    paths = [source_root / name for name in ("a.txt", "b.txt", "gone.txt")]
    flow = Flow(name="batch", group="Inputs").watch(
        mode="poll", run_as="batch", source=source_root, interval="1s",
    ).step(lambda context: None)
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    polling = RuntimePollingSupport(ledger.source_signatures)
    try:
        ledger.runs.record_started(run_id="history", flow_name=flow.name, group_name="Inputs", source_path=None, started_at_utc=None)
        ledger.runs.record_finished(run_id="history", status="success", finished_at_utc=utcnow_text())
        for path in paths:
            path.write_text("original", encoding="utf-8")
            ledger.execution_state.upsert_file_state(
                flow_name=flow.name, signature=polling.poll_source_signature(flow, path),
                status="success", run_id="history", finished_at_utc=utcnow_text(),
            )
        paths[0].write_text("changed-size", encoding="utf-8")
        paths[2].unlink()

        assert polling.stale_poll_sources(flow) == [None]
        states = ledger.source_signatures.list_file_states(flow_name=flow.name)
        assert {state.source_path for state in states} == {polling.normalized_source_path(path) for path in paths[:2]}
        assert all(state.last_success_run_id == "history" for state in states)
        assert not polling.is_poll_source_stale(flow, paths[1])
        assert polling.stale_batch_poll_signatures(flow) == (polling.poll_source_signature(flow, paths[0]),)
    finally:
        ledger.close()


@pytest.mark.skipif(os.name != "nt", reason="Windows source identity is case-insensitive")
def test_queue_coalesces_windows_source_spelling_variants(tmp_path):
    source = tmp_path / "Input.txt"
    source.write_text("input", encoding="utf-8")
    flow = Flow(name="identity", group="Inputs").step(lambda context: None)
    polling = RuntimePollingSupport(object())
    queue = deque()
    keys = set()
    for path in (source, source.with_name("INPUT.TXT"), source.with_name("input.txt")):
        polling.enqueue_job(queue, keys, flow, path)
    assert len(queue) == len(keys) == 1
    assert queue[0].source_path == source


@pytest.mark.parametrize("mode", ["direct", "parallel", "parallel_discard", "grouped", "grouped_continuous", "preview"])
def test_native_panic_is_reported_and_persisted_as_terminal_failure(tmp_path, mode):
    source_root = tmp_path / "input"
    source_root.mkdir()
    for name in ("a.txt", "b.txt"):
        (source_root / name).write_text(name, encoding="utf-8")
    panic = pl.exceptions.PanicException("native computation failed")

    def compute(context):
        if context.source.path.name == "a.txt":
            raise panic
        return "healthy"

    flow = Flow(name="panic", group="Compute").watch(
        mode="poll", source=source_root, interval="1s", max_parallel=2,
    ).step(compute, label="Compute")
    if mode == "grouped_continuous":
        flow = flow.watch(mode="manual", source=source_root, max_parallel=2)
    healthy = Flow(name="healthy", group="Other").step(lambda context: "healthy")
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    runtime = (GroupedFlowRuntime if mode.startswith("grouped") else FlowRuntime)(
        (flow, healthy) if mode.startswith("grouped") else (flow,), continuous=mode == "grouped_continuous", runtime_ledger=ledger,
    )
    try:
        with pytest.raises(FlowExecutionError) as caught:
            if mode == "direct":
                runtime.run_source(flow, source_root / "a.txt")
            elif mode == "parallel_discard":
                runtime.run_and_discard()
            elif mode == "preview":
                runtime.preview()
            else:
                runtime.run()
        assert caught.value.__cause__ is panic
        assert "PanicException" in caught.value.detail
        assert runtime.run_stop_controller.active_run_ids() == ()
        assert ledger.runs.list_active() == ()
        assert ledger.step_outputs.list_active() == ()
        if mode == "preview":
            assert ledger.runs.list() == ()
        else:
            failed = [run for run in ledger.runs.list() if run.status == "failed"]
            assert len(failed) == 1
            run = failed[0]
            step, = ledger.step_outputs.list_for_run(run.run_id)
            assert step.status == "failed"
            assert step.finished_at_utc and run.finished_at_utc
            assert step.error_text == run.error_text == str(caught.value)
            assert isinstance(step.elapsed_ms, int)
    finally:
        ledger.close()


@pytest.mark.parametrize("exception_type", [KeyboardInterrupt, SystemExit, GeneratorExit])
@pytest.mark.parametrize("mode", ["direct", "parallel", "grouped", "grouped_continuous", "preview"])
def test_execution_preserves_interrupt_identity_and_terminalizes_active_work(tmp_path, exception_type, mode):
    source_root = tmp_path / "input"
    source_root.mkdir()
    for name in ("a.txt", "b.txt"):
        (source_root / name).write_text(name, encoding="utf-8")
    interrupt = exception_type("operator interrupted")

    def interrupt_step(context):
        raise interrupt

    flow = Flow(name="interrupted", group="Compute").watch(
        mode="poll", source=source_root, interval="1s", max_parallel=2,
    ).step(interrupt_step)
    if mode == "grouped_continuous":
        flow = flow.watch(mode="manual", source=source_root, max_parallel=2)
    healthy = Flow(name="healthy", group="Other").step(lambda context: "healthy")
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    runtime = (GroupedFlowRuntime if mode.startswith("grouped") else FlowRuntime)(
        (flow, healthy) if mode.startswith("grouped") else (flow,), continuous=mode == "grouped_continuous", runtime_ledger=ledger,
    )
    try:
        with pytest.raises(exception_type) as caught:
            if mode == "direct":
                runtime.run_source(flow, source_root / "a.txt")
            elif mode == "preview":
                runtime.preview()
            else:
                runtime.run()
        assert caught.value is interrupt
        assert runtime.run_stop_controller.active_run_ids() == ()
        assert ledger.runs.list_active() == ()
        assert ledger.step_outputs.list_active() == ()
        for run in ledger.runs.list(flow_name=flow.name):
            assert run.status == "stopped" and run.finished_at_utc
            for step in ledger.step_outputs.list_for_run(run.run_id):
                assert step.status == "stopped" and step.finished_at_utc
    finally:
        ledger.close()


def test_cancelled_dispatch_preserves_coalesced_followup(tmp_path):
    source = tmp_path / "input.txt"
    source.write_text("first", encoding="utf-8")
    flow = Flow(name="cancelled", group="Inputs").watch(
        mode="poll", source=source, interval="1s", max_parallel=2,
    ).step(lambda context: context.source.path.read_text(encoding="utf-8"))
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    runtime = FlowRuntime((flow,), continuous=True, runtime_ledger=ledger)
    release = threading.Event()
    queue = deque()
    keys = set()
    pending = {}
    results = []
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            blocker = executor.submit(release.wait, 5)
            try:
                runtime.polling.enqueue_job(queue, keys, flow, source)
                runtime.dispatch_queued_jobs(queue, keys, pending, executor, results=results)
                cancelled, = pending
                assert cancelled.cancel()
                source.write_text("latest", encoding="utf-8")
                runtime.polling.enqueue_job(queue, keys, flow, source)
                runtime.dispatch_queued_jobs(queue, keys, pending, executor, results=results)
                release.set()
                runtime.wait_for_dispatched_jobs(pending, results=results)
            finally:
                release.set()
                blocker.result(timeout=5)
        assert [context.current for context in results] == ["latest"]
        assert not queue and not keys and not pending
        assert len(ledger.runs.list()) == 1
        assert not runtime.polling.is_poll_source_stale(flow, source)
    finally:
        runtime._close_runtime_resources()
        ledger.close()


def test_continuous_interrupt_drains_workers_and_always_stops_watcher(tmp_path, monkeypatch):
    source_root = tmp_path / "input"
    source_root.mkdir()
    for name in ("a.txt", "b.txt"):
        (source_root / name).write_text(name, encoding="utf-8")
    barrier = threading.Barrier(2)

    def interrupt(context):
        barrier.wait(timeout=5)
        raise KeyboardInterrupt("operator interrupted")

    flow = Flow(name="interrupted", group="Compute").watch(
        mode="poll", source=source_root, interval="1s", max_parallel=2,
    ).step(interrupt)
    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    runtime = FlowRuntime((flow,), continuous=True, runtime_ledger=ledger)
    watcher = PollingWatcher(source_root, settle=0)
    stopped = threading.Event()
    monkeypatch.setattr(watcher, "start", lambda: None)
    monkeypatch.setattr(watcher, "stop", stopped.set)
    monkeypatch.setattr(runtime.polling, "make_watcher", lambda trigger: watcher)
    try:
        with pytest.raises(KeyboardInterrupt, match="operator interrupted"):
            runtime.run()
        assert stopped.is_set()
        assert runtime.run_stop_controller.active_run_ids() == ()
        assert ledger.runs.list_active() == ()
        assert ledger.step_outputs.list_active() == ()
        assert len(ledger.runs.list()) == 2
    finally:
        ledger.close()


def test_grouped_failure_does_not_mask_interrupt_from_another_group(tmp_path, monkeypatch):
    barrier = threading.Barrier(2)
    failed = threading.Event()
    interrupt = SystemExit("shutdown requested")

    class _ErrorQueue(Queue):
        def put(self, item):
            super().put(item)
            if not isinstance(item[1], SystemExit):
                failed.set()

    def fail(context):
        barrier.wait(timeout=5)
        raise ValueError("ordinary failure")

    def interrupt_step(context):
        barrier.wait(timeout=5)
        assert failed.wait(5)
        raise interrupt

    ledger = RuntimeCacheLedger(tmp_path / "cache.sqlite")
    flows = (
        Flow(name="failure", group="Failure").step(fail),
        Flow(name="interrupt", group="Interrupt").step(interrupt_step),
    )
    runtime = GroupedFlowRuntime(flows, continuous=False, runtime_ledger=ledger)
    monkeypatch.setattr(grouped_module, "Queue", _ErrorQueue)
    try:
        with pytest.raises(SystemExit) as caught:
            runtime.run()
        assert caught.value is interrupt
        assert ledger.runs.list(flow_name="failure")[0].status == "failed"
        assert ledger.runs.list(flow_name="interrupt")[0].status == "stopped"
    finally:
        ledger.close()
