"""Command-shaped runtime engine for executing core flows."""

from __future__ import annotations

from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING, Callable

from data_engine.core.primitives import FlowContext
from data_engine.runtime.execution import FlowRuntime, GroupedFlowRuntime
from data_engine.runtime.runtime_db import RuntimeCacheLedger
from data_engine.runtime.stop import RuntimeStopController

if TYPE_CHECKING:
    from data_engine.core.flow import Flow as CoreFlow


class RuntimeEngine:
    """Execute flows through explicit runtime commands.

    The engine does not know about GUI, TUI, CLI, local settings, or daemon
    wiring. Hosts pass state/event collaborators in explicitly; the current
    implementation adapts the existing sequential and grouped runtimes while
    giving higher layers a command-shaped seam to target.
    """

    def __init__(
        self,
        *,
        runtime_ledger: RuntimeCacheLedger | None = None,
        runtime_stop_event: Event | None = None,
        flow_stop_event: Event | None = None,
        status_callback: Callable[[str], None] | None = None,
        flow_runtime_type: type[FlowRuntime] = FlowRuntime,
        grouped_runtime_type: type[GroupedFlowRuntime] = GroupedFlowRuntime,
        run_stop_controller: RuntimeStopController | None = None,
    ) -> None:
        self.runtime_ledger = runtime_ledger
        self.runtime_stop_event = runtime_stop_event
        self.flow_stop_event = flow_stop_event
        self.status_callback = status_callback
        self.flow_runtime_type = flow_runtime_type
        self.grouped_runtime_type = grouped_runtime_type
        self.run_stop_controller = run_stop_controller or RuntimeStopController()

    def run_once(self, flow: "CoreFlow") -> list[FlowContext]:
        """Run one flow once using its configured startup sources."""
        runtime = self._flow_runtime((flow,), continuous=False)
        return runtime.run()

    def run_source(self, flow: "CoreFlow", source_path: str | Path) -> FlowContext:
        """Run one flow for a specific source path."""
        runtime = self._flow_runtime((flow,), continuous=False)
        return runtime.run_source(flow, source_path)

    def run_batch(self, flow: "CoreFlow") -> FlowContext:
        """Run one flow once in batch mode using the configured source root."""
        runtime = self._flow_runtime((flow,), continuous=False)
        return runtime.run_batch(flow)

    def preview(self, flow: "CoreFlow", *, use: str | None = None) -> object:
        """Preview one flow through the one-shot runtime path."""
        runtime = self._flow_runtime((flow,), continuous=False)
        return runtime.preview(use=use)

    def run_continuous(self, flow: "CoreFlow") -> list[FlowContext]:
        """Run one flow continuously according to its trigger."""
        runtime = self._flow_runtime((flow,), continuous=True)
        return runtime.run()

    def run_grouped(
        self,
        flows: tuple["CoreFlow", ...],
        *,
        continuous: bool = True,
    ) -> list[FlowContext]:
        """Run flows grouped by flow group with sequential execution inside each group."""
        runtime = self.grouped_runtime_type(
            flows,
            continuous=continuous,
            **self._grouped_runtime_kwargs(),
        )
        return runtime.run()

    def stop(self, run_id: str) -> None:
        """Request that the active runtime stop a run by id."""
        self.run_stop_controller.request_stop(run_id)

    def _flow_runtime(self, flows: tuple["CoreFlow", ...], *, continuous: bool) -> FlowRuntime:
        return self.flow_runtime_type(
            flows,
            continuous=continuous,
            **self._flow_runtime_kwargs(),
        )

    def _flow_runtime_kwargs(self) -> dict[str, object]:
        kwargs: dict[str, object] = {}
        if self.runtime_stop_event is not None:
            kwargs["runtime_stop_event"] = self.runtime_stop_event
        if self.flow_stop_event is not None:
            kwargs["flow_stop_event"] = self.flow_stop_event
        if self.status_callback is not None:
            kwargs["status_callback"] = self.status_callback
        if self.runtime_ledger is not None:
            kwargs["runtime_ledger"] = self.runtime_ledger
        kwargs["run_stop_controller"] = self.run_stop_controller
        return kwargs

    def _grouped_runtime_kwargs(self) -> dict[str, object]:
        kwargs = self._flow_runtime_kwargs()
        if self.runtime_stop_event is None:
            kwargs.pop("runtime_stop_event", None)
        return kwargs


__all__ = ["RuntimeEngine"]
