"""Single-runtime orchestration for authored flows."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from pathlib import Path
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from data_engine.core.model import FlowStoppedError, FlowValidationError
from data_engine.core.primitives import FlowContext, WatchSpec
from data_engine.runtime.execution.continuous import ContinuousRuntimeLoop
from data_engine.runtime.execution.context import QueuedRunJob, RuntimeContextBuilder
from data_engine.runtime.execution.logging import RuntimeLogEmitter, acquire_queued_runtime_log_sink
from data_engine.runtime.execution.polling import RuntimePollingSupport
from data_engine.runtime.execution.runner import FlowRunExecutionPorts, FlowRunExecutor
from data_engine.runtime.file_watch import PollingWatcher
from data_engine.runtime.runtime_db import RuntimeCacheLedger
from data_engine.runtime.stop import RuntimeStopController
from data_engine.services.runtime_io import default_runtime_io_layer
from data_engine.services.runtime_ports import RuntimeCacheStore

if TYPE_CHECKING:
    from data_engine.core.flow import Flow as CoreFlow


def _open_default_runtime_cache_ledger() -> RuntimeCacheStore:
    """Open the default runtime ledger for authored flow execution."""
    ledger = RuntimeCacheLedger.open_default()
    try:
        return default_runtime_io_layer().open_cache_store(ledger.db_path)
    finally:
        ledger.close()


@dataclass(frozen=True)
class RuntimeCacheLedgerService:
    """Own how authored flow execution opens its runtime ledger."""

    open_runtime_cache_ledger_func: Callable[[], RuntimeCacheStore]

    def open_runtime_cache_ledger(self) -> RuntimeCacheStore:
        """Open one runtime ledger for authored flow execution."""
        return self.open_runtime_cache_ledger_func()


def default_runtime_cache_ledger_service() -> RuntimeCacheLedgerService:
    """Build the default runtime-ledger service for authored flows."""
    return RuntimeCacheLedgerService(open_runtime_cache_ledger_func=_open_default_runtime_cache_ledger)


class FlowRuntime:
    """Sequential runtime that executes one or more configured flows."""

    def __init__(
        self,
        flows: tuple["CoreFlow", ...],
        *,
        continuous: bool,
        runtime_stop_event: threading.Event | None = None,
        flow_stop_event: threading.Event | None = None,
        status_callback: Callable[[str], None] | None = None,
        runtime_ledger: RuntimeCacheStore | None = None,
        runtime_ledger_service: RuntimeCacheLedgerService | None = None,
        runtime_ledger_factory: Callable[[], RuntimeCacheStore] | None = None,
        run_stop_controller: RuntimeStopController | None = None,
        workspace_id: str | None = None,
    ) -> None:
        self.flows = tuple(flows)
        self.continuous = continuous
        self.runtime_stop_event = runtime_stop_event
        self.flow_stop_event = flow_stop_event
        self.run_stop_controller = run_stop_controller or RuntimeStopController()
        self.status_callback = status_callback
        runtime_ledger_service = runtime_ledger_service or default_runtime_cache_ledger_service()
        self._runtime_ledger_factory = runtime_ledger_factory or runtime_ledger_service.open_runtime_cache_ledger
        self._owns_runtime_ledger = runtime_ledger is None
        self.runtime_ledger = runtime_ledger or self._runtime_ledger_factory()
        runtime_db_path = getattr(self.runtime_ledger, "db_path", None)
        debug_root = Path(runtime_db_path).expanduser().resolve().parent / "debug_artifacts" if runtime_db_path is not None else None
        self.context_builder = RuntimeContextBuilder(debug_root=debug_root, workspace_id=workspace_id)
        self._queued_log_sink = acquire_queued_runtime_log_sink(self.runtime_ledger.logs)
        self.log_emitter = RuntimeLogEmitter(self._queued_log_sink, workspace_id=workspace_id)
        self.polling = RuntimePollingSupport(self.runtime_ledger.source_signatures)
        self.run_executor = FlowRunExecutor(
            FlowRunExecutionPorts(
                context_builder=self.context_builder,
                polling=self.polling,
                state_writer=self.runtime_ledger.execution_state,
                log_emitter=self.log_emitter,
                stop_controller=self,
            )
        )
        self.continuous_loop = ContinuousRuntimeLoop(self)

    def run(self) -> list[FlowContext]:
        try:
            self._validate()
            if not self.continuous or all(flow.mode == "manual" for flow in self.flows):
                return self._run_once_all()
            return self.continuous_loop.run()
        finally:
            self._close_runtime_resources()

    def preview(self, *, use: str | None = None):
        """Run exactly one flow for notebook-style inspection and return one object."""
        try:
            self._validate()
            if len(self.flows) != 1:
                raise FlowValidationError("preview() requires exactly one flow.")
            flow = self.flows[0]
            startup_sources = self.polling.startup_sources(flow)
            if not startup_sources:
                raise FlowValidationError("preview() could not determine a startup source.")
            context = self.run_executor.preview_one(flow, startup_sources[0], use=use)
            if use is None or use == "current":
                return context.current
            if use not in context.objects:
                raise FlowValidationError(f"preview() could not find saved object {use!r}.")
            return context.objects[use]
        finally:
            self._close_runtime_resources()

    def run_source(self, flow: "CoreFlow", source_path: str | Path) -> FlowContext:
        """Run one flow for a specific source path."""
        try:
            self._validate()
            return self.run_executor.run_one(flow, Path(source_path))
        finally:
            self._close_runtime_resources()

    def run_batch(self, flow: "CoreFlow") -> FlowContext:
        """Run one flow once in batch mode using the configured source root."""
        try:
            self._validate()
            return self.run_executor.run_one(
                flow,
                None,
                batch_signatures=self.polling.stale_batch_poll_signatures(flow),
            )
        finally:
            self._close_runtime_resources()

    def _close_runtime_resources(self) -> None:
        """Drain queued log writes and close the runtime ledger when owned by this runtime."""
        self._queued_log_sink.close()
        if not self._owns_runtime_ledger:
            return
        self.runtime_ledger.close()

    def _validate(self) -> None:
        names = [flow.name for flow in self.flows]
        if any(name is None or not str(name).strip() for name in names):
            raise FlowValidationError("Flow names must be set before execution.")
        if len(set(names)) != len(names):
            raise FlowValidationError("Flow names must be unique within one runtime.")
        for flow in self.flows:
            if not flow.steps:
                raise FlowValidationError(f"Flow {flow.name!r} must define at least one step.")

    def _run_once_all(self) -> list[FlowContext]:
        results: list[FlowContext] = []
        for flow in self.flows:
            if self.runtime_stop_event is not None and self.runtime_stop_event.is_set():
                break
            jobs: list[QueuedRunJob] = []
            for source_path in self.polling.startup_sources(flow):
                batch_signatures = ()
                trigger = flow.trigger
                if (
                    source_path is None
                    and isinstance(trigger, WatchSpec)
                    and trigger.mode == "poll"
                    and trigger.run_as == "batch"
                    and trigger.source is not None
                    and trigger.source.is_dir()
                ):
                    batch_signatures = self.polling.stale_batch_poll_signatures(flow)
                jobs.append(QueuedRunJob(flow=flow, source_path=source_path, batch_signatures=batch_signatures))
            results.extend(self._run_jobs(jobs))
        return results

    def max_parallel_for_flow(self, flow: "CoreFlow") -> int:
        """Return the allowed per-flow source concurrency for one flow."""
        trigger = flow.trigger
        if not isinstance(trigger, WatchSpec):
            return 1
        if trigger.run_as != "individual":
            return 1
        return max(int(trigger.max_parallel), 1)

    def _run_jobs(self, jobs: list[QueuedRunJob]) -> list[FlowContext]:
        if not jobs:
            return []
        max_parallel = self.max_parallel_for_flow(jobs[0].flow)
        if max_parallel <= 1 or len(jobs) <= 1:
            results: list[FlowContext] = []
            for job in jobs:
                if self.runtime_stop_event is not None and self.runtime_stop_event.is_set():
                    break
                results.append(
                    self.run_executor.run_one(job.flow, job.source_path, batch_signatures=job.batch_signatures)
                )
            return results
        results_by_index: dict[int, FlowContext] = {}
        job_iter = iter(enumerate(jobs))
        with ThreadPoolExecutor(max_workers=min(max_parallel, len(jobs))) as executor:
            future_to_index: dict[Future[FlowContext], int] = {}

            def _submit_next_job() -> bool:
                if self.runtime_stop_event is not None and self.runtime_stop_event.is_set():
                    return False
                try:
                    index, job = next(job_iter)
                except StopIteration:
                    return False
                future = executor.submit(
                    self.run_executor.run_one,
                    job.flow,
                    job.source_path,
                    batch_signatures=job.batch_signatures,
                )
                future_to_index[future] = index
                return True

            for _ in range(min(max_parallel, len(jobs))):
                if not _submit_next_job():
                    break
            try:
                while future_to_index:
                    future = next(as_completed(tuple(future_to_index)))
                    index = future_to_index.pop(future)
                    results_by_index[index] = future.result()
                    _submit_next_job()
            except Exception:
                for future in future_to_index:
                    future.cancel()
                raise
        return [results_by_index[index] for index in range(len(results_by_index))]

    def dispatch_queued_jobs(
        self,
        queue,
        queued_keys: set[tuple[str, str | None]],
        pending_futures: dict[Future[FlowContext], tuple[QueuedRunJob, int]],
        executor: ThreadPoolExecutor,
        *,
        results: list[FlowContext],
    ) -> None:
        """Submit queued source jobs up to each flow's bounded concurrency and drain completions."""
        self._drain_completed_jobs(pending_futures, results=results)
        if self.runtime_stop_event is not None and self.runtime_stop_event.is_set():
            return
        if queue:
            queue_length = len(queue)
            for _ in range(queue_length):
                job = queue.popleft()
                key = self.polling.job_key(job.flow, job.source_path)
                flow_name = job.flow.name
                active_count = sum(1 for pending_job, _ in pending_futures.values() if pending_job.flow.name == flow_name)
                if active_count >= self.max_parallel_for_flow(job.flow):
                    queue.append(job)
                    continue
                queued_keys.discard(key)
                future = executor.submit(
                    self.run_executor.run_one,
                    job.flow,
                    job.source_path,
                    batch_signatures=job.batch_signatures,
                )
                pending_futures[future] = (job, len(results) + len(pending_futures))
        self._drain_completed_jobs(pending_futures, results=results)

    def wait_for_dispatched_jobs(
        self,
        pending_futures: dict[Future[FlowContext], tuple[QueuedRunJob, int]],
        *,
        results: list[FlowContext],
    ) -> None:
        """Wait for all pending queued jobs to complete."""
        while pending_futures:
            done, _ = wait(tuple(pending_futures), return_when=FIRST_COMPLETED)
            for future in done:
                self._consume_completed_future(future, pending_futures, results=results)

    def _drain_completed_jobs(
        self,
        pending_futures: dict[Future[FlowContext], tuple[QueuedRunJob, int]],
        *,
        results: list[FlowContext],
    ) -> None:
        done = [future for future in pending_futures if future.done()]
        for future in done:
            self._consume_completed_future(future, pending_futures, results=results)

    def _consume_completed_future(
        self,
        future: Future[FlowContext],
        pending_futures: dict[Future[FlowContext], tuple[QueuedRunJob, int]],
        *,
        results: list[FlowContext],
    ) -> None:
        pending_futures.pop(future, None)
        try:
            results.append(future.result())
        except FlowStoppedError:
            if self.flow_stop_event is not None:
                self.flow_stop_event.clear()
        except Exception:
            return

    def _preview_one(self, flow: "CoreFlow", source_path: "Path | None", *, use: str | None) -> FlowContext:
        return self.run_executor.preview_one(flow, source_path, use=use)

    def _make_watcher(self, trigger: WatchSpec) -> PollingWatcher:
        return self.polling.make_watcher(trigger)

    def _startup_sources(self, flow: "CoreFlow", *, allow_missing: bool = False):
        return self.polling.startup_sources(flow, allow_missing=allow_missing)

    def _stale_poll_sources(self, flow: "CoreFlow"):
        return self.polling.stale_poll_sources(flow)

    def _stale_batch_poll_signatures(self, flow: "CoreFlow"):
        return self.polling.stale_batch_poll_signatures(flow)

    def _is_poll_source_stale(self, flow: "CoreFlow", source_path: "Path | None") -> bool:
        return self.polling.is_poll_source_stale(flow, source_path)

    def _poll_source_signature(self, flow: "CoreFlow", source_path: "Path | None"):
        return self.polling.poll_source_signature(flow, source_path)

    def _normalized_source_path(self, source_path: "Path | None"):
        return self.polling.normalized_source_path(source_path)

    def register_run(self, run_id: str) -> None:
        """Mark one run id as active."""
        self.run_stop_controller.register_run(run_id)

    def unregister_run(self, run_id: str) -> None:
        """Clear active and requested state for one completed run id."""
        self.run_stop_controller.unregister_run(run_id)

    def check_run(self, run_id: str | None) -> None:
        """Raise when runtime-wide or run-id stop has been requested."""
        if self.flow_stop_event is not None and self.flow_stop_event.is_set():
            raise FlowStoppedError("Flow stop requested by operator.")
        self.run_stop_controller.check_run(run_id)

    def _emit_status(self, message: str) -> None:
        if self.status_callback is not None:
            self.status_callback(message)


__all__ = ["FlowRuntime", "RuntimeCacheLedgerService", "default_runtime_cache_ledger_service"]
