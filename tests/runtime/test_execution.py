from __future__ import annotations

from collections import deque
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from time import sleep

from data_engine.core.model import FlowStoppedError
from data_engine.core.primitives import FlowContext
from data_engine.platform.workspace_models import DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR
from data_engine.runtime.execution.context import QueuedRunJob
from data_engine.runtime.execution.single import FlowRuntime
from data_engine.runtime.execution.single import default_runtime_cache_ledger_service
from data_engine.runtime.execution.runner import FlowRunExecutionPorts, FlowRunExecutor
from data_engine.runtime.runtime_db import RuntimeCacheLedger


@dataclass(frozen=True)
class _Step:
    label: str
    fn: object
    function_name: str = "step_fn"
    save_as: str | None = None
    use: str | None = None


@dataclass(frozen=True)
class _Flow:
    name: str
    group: str
    steps: tuple[_Step, ...]
    trigger: object | None = None


class _ContextBuilder:
    def new_run_id(self) -> str:
        return "run-1"

    def build(self, flow: _Flow, source_path: Path | None, *, run_id: str) -> FlowContext:
        del source_path
        return FlowContext(
            flow_name=flow.name,
            group=flow.group,
            metadata={"started_at_utc": "2026-04-18T12:00:00+00:00", "run_id": run_id},
        )


class _Polling:
    def poll_source_signature(self, flow: _Flow, source_path: Path | None):
        del flow, source_path
        return None

    def normalized_source_path(self, source_path: Path | None) -> str | None:
        return None if source_path is None else str(source_path)


class _StateWriter:
    def __init__(self, calls: list[tuple[str, object]]) -> None:
        self.calls = calls

    def record_run_started(self, **kwargs) -> None:
        self.calls.append(("record_run_started", kwargs))

    def record_run_finished(self, **kwargs) -> None:
        self.calls.append(("record_run_finished", kwargs))

    def record_step_started(self, **kwargs) -> int:
        self.calls.append(("record_step_started", kwargs))
        return 1

    def record_step_finished(self, **kwargs) -> None:
        self.calls.append(("record_step_finished", kwargs))

    def upsert_file_state(self, **kwargs) -> None:
        self.calls.append(("upsert_file_state", kwargs))


class _LogEmitter:
    def __init__(self, calls: list[tuple[str, object]]) -> None:
        self.calls = calls

    def log_runtime_message(self, message: str, **kwargs) -> None:
        self.calls.append(("log_runtime_message", {"message": message, **kwargs}))

    def log_flow_event(self, run_id: str, flow_name: str, source_path: Path | None, **kwargs) -> None:
        self.calls.append(
            (
                "log_flow_event",
                {"run_id": run_id, "flow_name": flow_name, "source_path": source_path, **kwargs},
            )
        )

    def log_step_event(self, run_id: str, flow_name: str, step_label: str, source_path: Path | None, **kwargs) -> None:
        self.calls.append(
            (
                "log_step_event",
                {
                    "run_id": run_id,
                    "flow_name": flow_name,
                    "step_label": step_label,
                    "source_path": source_path,
                    **kwargs,
                },
            )
        )


class _StopController:
    def __init__(self) -> None:
        self.registered: list[str] = []
        self.unregistered: list[str] = []

    def register_run(self, run_id: str) -> None:
        self.registered.append(run_id)

    def unregister_run(self, run_id: str) -> None:
        self.unregistered.append(run_id)

    def check_run(self, run_id: str | None) -> None:
        del run_id


def _executor(calls: list[tuple[str, object]]) -> FlowRunExecutor:
    return FlowRunExecutor(
        FlowRunExecutionPorts(
            context_builder=_ContextBuilder(),
            polling=_Polling(),
            state_writer=_StateWriter(calls),
            log_emitter=_LogEmitter(calls),
            stop_controller=_StopController(),
        )
    )


def test_flow_run_executor_logs_success_before_publishing_run_finished_state() -> None:
    calls: list[tuple[str, object]] = []
    executor = _executor(calls)
    flow = _Flow(name="claims_summary", group="Claims", steps=(_Step("Emit", lambda context: "ok"),))

    executor.run_one(flow, None)

    log_index = next(index for index, call in enumerate(calls) if call[0] == "log_flow_event" and call[1]["status"] == "success")
    finish_index = next(index for index, call in enumerate(calls) if call[0] == "record_run_finished")

    assert log_index < finish_index


def test_flow_run_executor_logs_failure_before_publishing_run_finished_state() -> None:
    calls: list[tuple[str, object]] = []
    executor = _executor(calls)

    def _boom(context):
        del context
        raise FlowStoppedError("stop requested")

    flow = _Flow(name="claims_summary", group="Claims", steps=(_Step("Emit", _boom),))

    try:
        executor.run_one(flow, None)
    except FlowStoppedError:
        pass
    else:
        raise AssertionError("expected FlowStoppedError")

    log_index = next(index for index, call in enumerate(calls) if call[0] == "log_flow_event" and call[1]["status"] == "stopped")
    finish_index = next(index for index, call in enumerate(calls) if call[0] == "record_run_finished")

    assert log_index < finish_index


class _DelayedStateWriter(_StateWriter):
    def __init__(self, calls: list[tuple[str, object]], *, start_delay_seconds: float) -> None:
        super().__init__(calls)
        self.start_delay_seconds = start_delay_seconds

    def record_run_started(self, **kwargs) -> None:
        sleep(self.start_delay_seconds)
        super().record_run_started(**kwargs)

    def record_step_started(self, **kwargs) -> int:
        sleep(self.start_delay_seconds)
        return super().record_step_started(**kwargs)


def test_flow_run_executor_elapsed_excludes_start_write_delay() -> None:
    calls: list[tuple[str, object]] = []
    executor = FlowRunExecutor(
        FlowRunExecutionPorts(
            context_builder=_ContextBuilder(),
            polling=_Polling(),
            state_writer=_DelayedStateWriter(calls, start_delay_seconds=0.05),
            log_emitter=_LogEmitter(calls),
            stop_controller=_StopController(),
        )
    )
    flow = _Flow(name="claims_summary", group="Claims", steps=(_Step("Emit", lambda context: "ok"),))

    executor.run_one(flow, None)

    step_finished = next(call for call in calls if call[0] == "record_step_finished")
    run_finished = next(call for call in calls if call[0] == "record_run_finished")
    step_elapsed_ms = step_finished[1]["elapsed_ms"]
    assert isinstance(step_elapsed_ms, int)
    assert step_elapsed_ms < 25

    success_step_log = next(
        call for call in calls if call[0] == "log_step_event" and call[1]["status"] == "success"
    )
    success_flow_log = next(
        call for call in calls if call[0] == "log_flow_event" and call[1]["status"] == "success"
    )
    assert success_step_log[1]["elapsed"] < 0.025
    assert success_flow_log[1]["elapsed"] < 0.075
    assert run_finished[1]["status"] == "success"


def test_default_runtime_cache_ledger_service_opens_direct_runtime_cache_ledger(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "runtime_state" / "runtime_cache.sqlite"
    monkeypatch.setenv(DATA_ENGINE_RUNTIME_CACHE_DB_PATH_ENV_VAR, str(db_path))
    service = default_runtime_cache_ledger_service()

    ledger = service.open_runtime_cache_ledger()
    try:
        assert isinstance(ledger, RuntimeCacheLedger)
        assert ledger.db_path == db_path.resolve()
    finally:
        ledger.close()


def test_flow_runtime_discards_completed_contexts_when_results_collection_is_disabled() -> None:
    runtime = FlowRuntime(flows=(), continuous=True)
    try:
        future: Future[FlowContext] = Future()
        context = FlowContext(flow_name="demo", group="Demo")
        future.set_result(context)
        pending: dict[Future[FlowContext], tuple[object, int]] = {future: (object(), 0)}

        runtime._consume_completed_future(future, pending, results=None)

        assert pending == {}
    finally:
        runtime._close_runtime_resources()


def test_flow_runtime_dispatches_queued_jobs_when_results_collection_is_disabled() -> None:
    flow = _Flow(
        name="claims_poll",
        group="Claims",
        steps=(_Step("Emit", lambda context: context.current),),
    )
    runtime = FlowRuntime(flows=(flow,), continuous=True)
    try:
        queue = deque([QueuedRunJob(flow=flow, source_path=None, batch_signatures=())])
        queued_keys = {runtime.polling.job_key(flow, None)}
        pending: dict[Future[FlowContext], tuple[object, int]] = {}

        with ThreadPoolExecutor(max_workers=1) as executor:
            runtime.dispatch_queued_jobs(
                queue,
                queued_keys,
                pending,
                executor,
                results=None,
            )
            runtime.wait_for_dispatched_jobs(pending, results=None)

        assert queue == deque()
        assert queued_keys == set()
        assert pending == {}
    finally:
        runtime._close_runtime_resources()
