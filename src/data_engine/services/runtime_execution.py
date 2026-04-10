"""Runtime execution services for flow runs and grouped engine runs."""

from __future__ import annotations

from collections.abc import Callable
from threading import Event
from typing import TYPE_CHECKING

from data_engine.authoring.execution import _FlowRuntime, _GroupedFlowRuntime
from data_engine.runtime.runtime_db import RuntimeCacheLedger

if TYPE_CHECKING:
    from data_engine.authoring.flow import Flow


class RuntimeExecutionService:
    """Own executable runtime construction for manual and grouped runs."""

    def __init__(
        self,
        *,
        flow_runtime_type: type[_FlowRuntime] = _FlowRuntime,
        grouped_runtime_type: type[_GroupedFlowRuntime] = _GroupedFlowRuntime,
    ) -> None:
        self._flow_runtime_type = flow_runtime_type
        self._grouped_runtime_type = grouped_runtime_type

    def run_once(
        self,
        flow: "Flow",
        *,
        runtime_ledger: RuntimeCacheLedger | None = None,
        flow_stop_event: Event | None = None,
    ) -> object:
        """Run one flow as a one-shot execution."""
        runtime = self._flow_runtime_type(
            (flow,),
            continuous=False,
            flow_stop_event=flow_stop_event,
            runtime_ledger=runtime_ledger,
        )
        return runtime.run()

    def preview(
        self,
        flow: "Flow",
        *,
        use: str | None = None,
        runtime_ledger: RuntimeCacheLedger | None = None,
    ) -> object:
        """Preview one flow through the one-shot runtime path."""
        runtime = self._flow_runtime_type(
            (flow,),
            continuous=False,
            runtime_ledger=runtime_ledger,
        )
        return runtime.preview(use=use)

    def run_manual(
        self,
        flow: "Flow",
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
        flow: "Flow",
        *,
        runtime_ledger: RuntimeCacheLedger | None = None,
        flow_stop_event: Event | None = None,
    ) -> object:
        """Run one flow continuously."""
        runtime = self._flow_runtime_type(
            (flow,),
            continuous=True,
            flow_stop_event=flow_stop_event,
            runtime_ledger=runtime_ledger,
        )
        return runtime.run()

    def run_grouped(
        self,
        flows: tuple["Flow", ...],
        *,
        runtime_ledger: RuntimeCacheLedger,
        runtime_stop_event: Event,
        flow_stop_event: Event,
    ) -> object:
        """Run grouped automated flows continuously."""
        runtime = self._grouped_runtime_type(
            flows,
            continuous=True,
            runtime_stop_event=runtime_stop_event,
            flow_stop_event=flow_stop_event,
            runtime_ledger=runtime_ledger,
        )
        return runtime.run()

    def run_grouped_continuous(
        self,
        flows: tuple["Flow", ...],
        *,
        runtime_ledger: RuntimeCacheLedger | None = None,
        runtime_stop_event: Event | None = None,
        flow_stop_event: Event | None = None,
    ) -> object:
        """Run grouped automated flows continuously with optional runtime controls."""
        runtime = self._grouped_runtime_type(
            flows,
            continuous=True,
            runtime_stop_event=runtime_stop_event,
            flow_stop_event=flow_stop_event,
            runtime_ledger=runtime_ledger,
        )
        return runtime.run()


__all__ = ["RuntimeExecutionService"]
