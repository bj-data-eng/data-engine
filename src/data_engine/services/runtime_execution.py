"""Runtime execution services for flow runs and grouped engine runs."""

from __future__ import annotations

from threading import Event
from typing import TYPE_CHECKING

from data_engine.runtime.execution import _FlowRuntime, _GroupedFlowRuntime
from data_engine.runtime.engine import RuntimeEngine
from data_engine.runtime.runtime_db import RuntimeCacheLedger
from data_engine.runtime.stop import RuntimeStopController

if TYPE_CHECKING:
    from data_engine.core.flow import Flow as CoreFlow


class RuntimeExecutionService:
    """Own executable runtime construction for manual and grouped runs."""

    def __init__(
        self,
        *,
        flow_runtime_type: type[_FlowRuntime] = _FlowRuntime,
        grouped_runtime_type: type[_GroupedFlowRuntime] = _GroupedFlowRuntime,
        runtime_engine_type: type[RuntimeEngine] = RuntimeEngine,
        run_stop_controller: RuntimeStopController | None = None,
    ) -> None:
        self._flow_runtime_type = flow_runtime_type
        self._grouped_runtime_type = grouped_runtime_type
        self._runtime_engine_type = runtime_engine_type
        self._run_stop_controller = run_stop_controller or RuntimeStopController()

    def _engine(
        self,
        *,
        runtime_ledger: RuntimeCacheLedger | None = None,
        runtime_stop_event: Event | None = None,
        flow_stop_event: Event | None = None,
    ) -> RuntimeEngine:
        return self._runtime_engine_type(
            runtime_ledger=runtime_ledger,
            runtime_stop_event=runtime_stop_event,
            flow_stop_event=flow_stop_event,
            flow_runtime_type=self._flow_runtime_type,
            grouped_runtime_type=self._grouped_runtime_type,
            run_stop_controller=self._run_stop_controller,
        )

    def run_once(
        self,
        flow: "CoreFlow",
        *,
        runtime_ledger: RuntimeCacheLedger | None = None,
        flow_stop_event: Event | None = None,
    ) -> object:
        """Run one flow as a one-shot execution."""
        return self._engine(
            flow_stop_event=flow_stop_event,
            runtime_ledger=runtime_ledger,
        ).run_once(flow)

    def run_source(
        self,
        flow: "CoreFlow",
        source_path: str,
        *,
        runtime_ledger: RuntimeCacheLedger | None = None,
        flow_stop_event: Event | None = None,
    ) -> object:
        """Run one flow for a specific source path."""
        return self._engine(
            flow_stop_event=flow_stop_event,
            runtime_ledger=runtime_ledger,
        ).run_source(flow, source_path)

    def run_batch(
        self,
        flow: "CoreFlow",
        *,
        runtime_ledger: RuntimeCacheLedger | None = None,
        flow_stop_event: Event | None = None,
    ) -> object:
        """Run one flow once in batch mode."""
        return self._engine(
            flow_stop_event=flow_stop_event,
            runtime_ledger=runtime_ledger,
        ).run_batch(flow)

    def preview(
        self,
        flow: "CoreFlow",
        *,
        use: str | None = None,
        runtime_ledger: RuntimeCacheLedger | None = None,
    ) -> object:
        """Preview one flow through the one-shot runtime path."""
        return self._engine(
            runtime_ledger=runtime_ledger,
        ).preview(flow, use=use)

    def run_manual(
        self,
        flow: "CoreFlow",
        *,
        runtime_ledger: RuntimeCacheLedger,
        flow_stop_event: Event,
    ) -> object:
        """Run one flow as a manual one-shot execution."""
        return self.run_once(
            flow,
            runtime_ledger=runtime_ledger,
            flow_stop_event=flow_stop_event,
        )

    def run_continuous(
        self,
        flow: "CoreFlow",
        *,
        runtime_ledger: RuntimeCacheLedger | None = None,
        flow_stop_event: Event | None = None,
    ) -> object:
        """Run one flow continuously."""
        return self._engine(
            flow_stop_event=flow_stop_event,
            runtime_ledger=runtime_ledger,
        ).run_continuous(flow)

    def run_grouped(
        self,
        flows: tuple["CoreFlow", ...],
        *,
        runtime_ledger: RuntimeCacheLedger,
        runtime_stop_event: Event,
        flow_stop_event: Event,
    ) -> object:
        """Run grouped automated flows continuously."""
        return self._engine(
            runtime_stop_event=runtime_stop_event,
            flow_stop_event=flow_stop_event,
            runtime_ledger=runtime_ledger,
        ).run_grouped(flows, continuous=True)

    def run_grouped_continuous(
        self,
        flows: tuple["CoreFlow", ...],
        *,
        runtime_ledger: RuntimeCacheLedger | None = None,
        runtime_stop_event: Event | None = None,
        flow_stop_event: Event | None = None,
    ) -> object:
        """Run grouped automated flows continuously with optional runtime controls."""
        return self._engine(
            runtime_stop_event=runtime_stop_event,
            flow_stop_event=flow_stop_event,
            runtime_ledger=runtime_ledger,
        ).run_grouped(flows, continuous=True)

    def stop(self, run_id: str, *, flow_stop_event: Event | None = None) -> None:
        """Request that an active runtime stop a run by id."""
        self._engine(flow_stop_event=flow_stop_event).stop(run_id)


__all__ = ["RuntimeExecutionService"]
