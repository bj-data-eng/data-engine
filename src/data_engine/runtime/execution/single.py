"""Single-runtime orchestration for authored flows."""

from __future__ import annotations

from pathlib import Path
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from data_engine.core.model import FlowStoppedError, FlowValidationError
from data_engine.core.primitives import FlowContext, WatchSpec
from data_engine.runtime.execution.continuous import ContinuousRuntimeLoop
from data_engine.runtime.execution.context import RuntimeContextBuilder
from data_engine.runtime.execution.logging import RuntimeLogEmitter
from data_engine.runtime.execution.polling import RuntimePollingSupport
from data_engine.runtime.execution.runner import FlowRunExecutionPorts, FlowRunExecutor
from data_engine.runtime.file_watch import PollingWatcher
from data_engine.runtime.runtime_db import RuntimeLedger
from data_engine.runtime.stop import RuntimeStopController

if TYPE_CHECKING:
    from data_engine.core.flow import Flow


def _open_default_runtime_ledger() -> RuntimeLedger:
    """Open the default runtime ledger for authored flow execution."""
    return RuntimeLedger.open_default()


@dataclass(frozen=True)
class RuntimeLedgerService:
    """Own how authored flow execution opens its runtime ledger."""

    open_runtime_ledger_func: Callable[[], RuntimeLedger]

    def open_runtime_ledger(self) -> RuntimeLedger:
        """Open one runtime ledger for authored flow execution."""
        return self.open_runtime_ledger_func()


def default_runtime_ledger_service() -> RuntimeLedgerService:
    """Build the default runtime-ledger service for authored flows."""
    return RuntimeLedgerService(open_runtime_ledger_func=_open_default_runtime_ledger)


class _FlowRuntime:
    """Sequential runtime that executes one or more configured flows."""

    def __init__(
        self,
        flows: tuple["Flow", ...],
        *,
        continuous: bool,
        runtime_stop_event: threading.Event | None = None,
        flow_stop_event: threading.Event | None = None,
        status_callback: Callable[[str], None] | None = None,
        runtime_ledger: RuntimeLedger | None = None,
        runtime_ledger_service: RuntimeLedgerService | None = None,
        runtime_ledger_factory: Callable[[], RuntimeLedger] | None = None,
        run_stop_controller: RuntimeStopController | None = None,
    ) -> None:
        self.flows = tuple(flows)
        self.continuous = continuous
        self.runtime_stop_event = runtime_stop_event
        self.flow_stop_event = flow_stop_event
        self.run_stop_controller = run_stop_controller or RuntimeStopController()
        self.status_callback = status_callback
        runtime_ledger_service = runtime_ledger_service or default_runtime_ledger_service()
        self._runtime_ledger_factory = runtime_ledger_factory or runtime_ledger_service.open_runtime_ledger
        self._owns_runtime_ledger = runtime_ledger is None
        self.runtime_ledger = runtime_ledger or self._runtime_ledger_factory()
        self.context_builder = RuntimeContextBuilder()
        self.log_emitter = RuntimeLogEmitter(self.runtime_ledger)
        self.polling = RuntimePollingSupport(self.runtime_ledger)
        self.run_executor = FlowRunExecutor(
            FlowRunExecutionPorts(
                context_builder=self.context_builder,
                polling=self.polling,
                runtime_ledger=self.runtime_ledger,
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
            self._close_owned_runtime_ledger()

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
            self._close_owned_runtime_ledger()

    def run_source(self, flow: "Flow", source_path: str | Path) -> FlowContext:
        """Run one flow for a specific source path."""
        try:
            self._validate()
            return self.run_executor.run_one(flow, Path(source_path))
        finally:
            self._close_owned_runtime_ledger()

    def run_batch(self, flow: "Flow") -> FlowContext:
        """Run one flow once in batch mode using the configured source root."""
        try:
            self._validate()
            return self.run_executor.run_one(
                flow,
                None,
                batch_signatures=self.polling.stale_batch_poll_signatures(flow),
            )
        finally:
            self._close_owned_runtime_ledger()

    def _close_owned_runtime_ledger(self) -> None:
        """Close the runtime ledger when this runtime opened it implicitly."""
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
                results.append(self.run_executor.run_one(flow, source_path, batch_signatures=batch_signatures))
        return results

    def _preview_one(self, flow: "Flow", source_path: "Path | None", *, use: str | None) -> FlowContext:
        return self.run_executor.preview_one(flow, source_path, use=use)

    def _make_watcher(self, trigger: WatchSpec) -> PollingWatcher:
        return self.polling.make_watcher(trigger)

    def _startup_sources(self, flow: "Flow", *, allow_missing: bool = False):
        return self.polling.startup_sources(flow, allow_missing=allow_missing)

    def _stale_poll_sources(self, flow: "Flow"):
        return self.polling.stale_poll_sources(flow)

    def _stale_batch_poll_signatures(self, flow: "Flow"):
        return self.polling.stale_batch_poll_signatures(flow)

    def _is_poll_source_stale(self, flow: "Flow", source_path: "Path | None") -> bool:
        return self.polling.is_poll_source_stale(flow, source_path)

    def _poll_source_signature(self, flow: "Flow", source_path: "Path | None"):
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


__all__ = ["RuntimeLedgerService", "_FlowRuntime", "default_runtime_ledger_service"]
