"""Grouped runtime orchestration for authored flows."""

from __future__ import annotations

from queue import Queue
import threading
from typing import TYPE_CHECKING, Callable

from data_engine.authoring.primitives import FlowContext
from data_engine.authoring.execution.single import _FlowRuntime, RuntimeLedgerService, default_runtime_ledger_service
from data_engine.runtime.runtime_db import RuntimeLedger

if TYPE_CHECKING:
    from data_engine.authoring.flow import Flow


class _GroupedFlowRuntime:
    """Grouped orchestrator: sequential within a group, parallel across groups."""

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
    ) -> None:
        self.flows = tuple(flows)
        self.continuous = continuous
        self.runtime_stop_event = runtime_stop_event
        self.flow_stop_event = flow_stop_event
        self.status_callback = status_callback
        self._runtime_ledger_service = runtime_ledger_service or default_runtime_ledger_service()
        self._runtime_ledger_factory = runtime_ledger_factory or self._runtime_ledger_service.open_runtime_ledger
        self._owns_runtime_ledger = runtime_ledger is None
        self.runtime_ledger = runtime_ledger or self._runtime_ledger_factory()

    def run(self) -> list[FlowContext]:
        grouped = self._grouped_flows()
        if len(grouped) <= 1:
            only = next(iter(grouped.values()), ())
            return _FlowRuntime(
                tuple(only),
                continuous=self.continuous,
                runtime_stop_event=self.runtime_stop_event,
                flow_stop_event=self.flow_stop_event,
                status_callback=self.status_callback,
                runtime_ledger=self.runtime_ledger,
                runtime_ledger_service=self._runtime_ledger_service,
            ).run()

        results_by_group: dict[str, list[FlowContext]] = {name: [] for name in grouped}
        errors: Queue[tuple[str, Exception]] = Queue()
        threads: list[threading.Thread] = []
        internal_runtime_stop = self.runtime_stop_event or threading.Event()
        internal_flow_stop = self.flow_stop_event or threading.Event()

        def run_group(group_name: str, group_flows: tuple["Flow", ...]) -> None:
            try:
                runtime = _FlowRuntime(
                    group_flows,
                    continuous=self.continuous,
                    runtime_stop_event=internal_runtime_stop,
                    flow_stop_event=internal_flow_stop,
                    status_callback=self.status_callback,
                    runtime_ledger=self.runtime_ledger,
                    runtime_ledger_service=self._runtime_ledger_service,
                    runtime_ledger_factory=self._runtime_ledger_factory,
                )
                results_by_group[group_name] = runtime.run()
            except Exception as exc:  # pragma: no cover
                errors.put((group_name, exc))
                if not self.continuous:
                    internal_runtime_stop.set()
        try:
            for group_name, group_flows in grouped.items():
                thread = threading.Thread(target=run_group, args=(group_name, group_flows), daemon=True)
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            if not self.continuous and not errors.empty():
                _, exc = errors.get()
                raise exc

            ordered_results: list[FlowContext] = []
            for group_name in grouped:
                ordered_results.extend(results_by_group[group_name])
            return ordered_results
        finally:
            if self._owns_runtime_ledger:
                self.runtime_ledger.close()

    def _grouped_flows(self) -> dict[str, tuple["Flow", ...]]:
        grouped: dict[str, list["Flow"]] = {}
        for index, flow in enumerate(self.flows):
            key = flow.group or f"group-{index}"
            grouped.setdefault(key, []).append(flow)
        return {name: tuple(group_flows) for name, group_flows in grouped.items()}


__all__ = ["_GroupedFlowRuntime"]
