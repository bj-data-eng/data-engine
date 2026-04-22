from __future__ import annotations

from pathlib import Path
from threading import Event
import pytest

from data_engine.authoring.flow import Flow
from data_engine.core.model import FlowStoppedError
from data_engine.core.primitives import FlowContext, FlowDebugContext, MirrorContext, SourceContext, WorkspaceConfigContext
from data_engine.runtime.stop import RuntimeStopController
from data_engine.services.flow_execution import FlowExecutionService
from data_engine.services.runtime_execution import RuntimeExecutionService


def test_flow_execution_service_uses_injected_loader_and_discovery(tmp_path):
    flow = Flow(name="docs", group="Docs")
    load_calls: list[tuple[str, object | None]] = []
    discover_calls: list[object | None] = []
    service = FlowExecutionService(
        load_flow_func=lambda name, *, data_root=None: load_calls.append((name, data_root)) or flow,
        discover_flows_func=lambda *, data_root=None: discover_calls.append(data_root) or (flow,),
    )

    assert service.load_flow("docs", workspace_root=tmp_path) is flow
    assert service.load_flows(("docs", "docs"), workspace_root=tmp_path) == (flow, flow)
    assert service.discover_flows(workspace_root=tmp_path) == (flow,)
    assert load_calls == [("docs", tmp_path), ("docs", tmp_path), ("docs", tmp_path)]
    assert discover_calls == [tmp_path]


def test_runtime_execution_service_constructs_runtime_objects():
    flow = Flow(name="docs", group="Docs")

    class _Runtime:
        instances: list["_Runtime"] = []

        def __init__(
            self,
            flows,
            *,
            continuous,
            runtime_stop_event=None,
            flow_stop_event=None,
            runtime_ledger=None,
            run_stop_controller=None,
        ):
            self.flows = flows
            self.continuous = continuous
            self.runtime_stop_event = runtime_stop_event
            self.flow_stop_event = flow_stop_event
            self.runtime_ledger = runtime_ledger
            self.run_stop_controller = run_stop_controller
            type(self).instances.append(self)

        def run(self):
            return {"flows": tuple(flow.name for flow in self.flows), "continuous": self.continuous}

        def preview(self, *, use=None):
            return {"preview": use, "continuous": self.continuous}

    class _GroupedRuntime:
        instances: list["_GroupedRuntime"] = []

        def __init__(
            self,
            flows,
            *,
            continuous,
            runtime_stop_event=None,
            flow_stop_event=None,
            runtime_ledger=None,
            run_stop_controller=None,
        ):
            self.flows = flows
            self.continuous = continuous
            self.runtime_stop_event = runtime_stop_event
            self.flow_stop_event = flow_stop_event
            self.runtime_ledger = runtime_ledger
            self.run_stop_controller = run_stop_controller
            type(self).instances.append(self)

        def run(self):
            return {"grouped": tuple(flow.name for flow in self.flows), "continuous": self.continuous}

    service = RuntimeExecutionService(flow_runtime_type=_Runtime, grouped_runtime_type=_GroupedRuntime)
    flow_stop = Event()
    runtime_stop = Event()
    ledger = object()

    assert service.run_once(flow, runtime_ledger=ledger, runtime_stop_event=runtime_stop, flow_stop_event=flow_stop) == {"flows": ("docs",), "continuous": False}
    assert service.preview(flow, use="csv", runtime_ledger=ledger) == {"preview": "csv", "continuous": False}
    assert service.run_manual(flow, runtime_ledger=ledger, runtime_stop_event=runtime_stop, flow_stop_event=flow_stop) == {"flows": ("docs",), "continuous": False}
    assert service.run_continuous(flow, runtime_ledger=ledger, flow_stop_event=flow_stop) == {"flows": ("docs",), "continuous": True}
    assert service.run_grouped((flow,), runtime_ledger=ledger, runtime_stop_event=runtime_stop, flow_stop_event=flow_stop) == {"grouped": ("docs",), "continuous": True}
    assert service.run_grouped_continuous((flow,), runtime_ledger=ledger, runtime_stop_event=runtime_stop, flow_stop_event=flow_stop) == {"grouped": ("docs",), "continuous": True}

    assert _Runtime.instances[0].continuous is False
    assert _Runtime.instances[1].continuous is False
    assert _Runtime.instances[2].continuous is False
    assert _Runtime.instances[3].continuous is True
    assert _Runtime.instances[0].runtime_stop_event is runtime_stop
    assert _Runtime.instances[2].runtime_stop_event is runtime_stop
    assert _GroupedRuntime.instances[0].runtime_stop_event is runtime_stop
    assert _GroupedRuntime.instances[1].flow_stop_event is flow_stop


def test_runtime_execution_service_run_manual_releases_completed_flow_contexts() -> None:
    flow = Flow(name="docs", group="Docs").step(lambda context: {"claim_id": 1}, save_as="result")
    runtime_stop = Event()
    flow_stop = Event()
    ledger = object()

    class _Runtime:
        def __init__(
            self,
            flows,
            *,
            continuous,
            runtime_stop_event=None,
            flow_stop_event=None,
            runtime_ledger=None,
            run_stop_controller=None,
        ):
            self.flows = flows
            self.continuous = continuous
            self.runtime_stop_event = runtime_stop_event
            self.flow_stop_event = flow_stop_event
            self.runtime_ledger = runtime_ledger
            self.run_stop_controller = run_stop_controller

        def run(self):
            return [
                FlowContext(
                    flow_name="docs",
                    group="Docs",
                    source=SourceContext(root=Path("/tmp/source"), path=Path("/tmp/source/docs.xlsx"), relative_path=Path("docs.xlsx")),
                    mirror=MirrorContext(root=Path("/tmp/output"), source_path=Path("/tmp/source/docs.xlsx"), relative_path=Path("docs.xlsx")),
                    current={"claim_id": 1},
                    objects={"result": {"claim_id": 1}},
                    metadata={"started_at_utc": "2026-04-21T00:00:00+00:00"},
                    config=WorkspaceConfigContext(workspace_root=Path("/tmp/workspace")),
                    debug=FlowDebugContext(
                        root=Path("/tmp/debug"),
                        workspace_id="docs",
                        flow_name="docs",
                        run_id="run-1",
                        source_path="/tmp/source/docs.xlsx",
                    ),
                )
            ]

    service = RuntimeExecutionService(flow_runtime_type=_Runtime)
    result = service.run_manual(
        flow,
        runtime_ledger=ledger,
        runtime_stop_event=runtime_stop,
        flow_stop_event=flow_stop,
    )

    assert isinstance(result, list)
    assert len(result) == 1
    context = result[0]
    assert context.source is None
    assert context.mirror is None
    assert context.current is None
    assert context.objects == {}
    assert context.metadata == {}
    assert context.config.workspace_root is None
    assert context.debug is None


def test_runtime_execution_service_run_manual_and_discard_uses_runtime_discard_hook_when_available() -> None:
    flow = Flow(name="docs", group="Docs")
    runtime_stop = Event()
    flow_stop = Event()
    ledger = object()
    calls: list[str] = []

    class _Runtime:
        def __init__(
            self,
            flows,
            *,
            continuous,
            runtime_stop_event=None,
            flow_stop_event=None,
            runtime_ledger=None,
            run_stop_controller=None,
        ):
            self.flows = flows
            self.continuous = continuous
            self.runtime_stop_event = runtime_stop_event
            self.flow_stop_event = flow_stop_event
            self.runtime_ledger = runtime_ledger
            self.run_stop_controller = run_stop_controller

        def run(self):
            calls.append("run")
            return ["result"]

        def run_and_discard(self):
            calls.append("run_and_discard")

    service = RuntimeExecutionService(flow_runtime_type=_Runtime)

    service.run_manual_and_discard(
        flow,
        runtime_ledger=ledger,
        runtime_stop_event=runtime_stop,
        flow_stop_event=flow_stop,
    )

    assert calls == ["run_and_discard"]


def test_runtime_execution_service_run_manual_and_discard_falls_back_for_legacy_runtime() -> None:
    flow = Flow(name="docs", group="Docs")
    runtime_stop = Event()
    flow_stop = Event()
    ledger = object()
    calls: list[str] = []

    class _Runtime:
        def __init__(
            self,
            flows,
            *,
            continuous,
            runtime_stop_event=None,
            flow_stop_event=None,
            runtime_ledger=None,
            run_stop_controller=None,
        ):
            self.flows = flows
            self.continuous = continuous
            self.runtime_stop_event = runtime_stop_event
            self.flow_stop_event = flow_stop_event
            self.runtime_ledger = runtime_ledger
            self.run_stop_controller = run_stop_controller

        def run(self):
            calls.append("run")
            return []

    service = RuntimeExecutionService(flow_runtime_type=_Runtime)

    service.run_manual_and_discard(
        flow,
        runtime_ledger=ledger,
        runtime_stop_event=runtime_stop,
        flow_stop_event=flow_stop,
    )

    assert calls == ["run"]


def test_runtime_execution_service_exposes_explicit_engine_commands(tmp_path):
    flow = Flow(name="docs", group="Docs")
    source = tmp_path / "docs.csv"
    source.write_text("claim_id\n1\n", encoding="utf-8")

    class _RunExecutor:
        def run_one(self, flow, source_path, *, batch_signatures=()):
            return {
                "flow": flow.name,
                "source_path": source_path,
                "batch_signatures": batch_signatures,
            }

    class _Polling:
        def stale_batch_poll_signatures(self, flow):
            return (f"{flow.name}:signature",)

    class _Runtime:
        def __init__(self, flows, *, continuous, flow_stop_event=None, runtime_ledger=None, run_stop_controller=None):
            self.flows = flows
            self.continuous = continuous
            self.flow_stop_event = flow_stop_event
            self.runtime_ledger = runtime_ledger
            self.run_stop_controller = run_stop_controller
            self.run_executor = _RunExecutor()
            self.polling = _Polling()
            self.closed = False

        def _validate(self):
            return None

        def _close_owned_runtime_ledger(self):
            self.closed = True

        def run_source(self, flow, source_path):
            self._validate()
            try:
                return self.run_executor.run_one(flow, source_path)
            finally:
                self._close_owned_runtime_ledger()

        def run_batch(self, flow):
            self._validate()
            try:
                return self.run_executor.run_one(
                    flow,
                    None,
                    batch_signatures=self.polling.stale_batch_poll_signatures(flow),
                )
            finally:
                self._close_owned_runtime_ledger()

    service = RuntimeExecutionService(flow_runtime_type=_Runtime)

    assert service.run_source(flow, source) == {
        "flow": "docs",
        "source_path": source,
        "batch_signatures": (),
    }
    assert service.run_batch(flow) == {
        "flow": "docs",
        "source_path": None,
        "batch_signatures": ("docs:signature",),
    }


def test_runtime_execution_service_stop_requests_run_id_on_controller():
    controller = RuntimeStopController()
    controller.register_run("run-123")
    RuntimeExecutionService(run_stop_controller=controller).stop("run-123")

    with pytest.raises(FlowStoppedError, match="run-123"):
        controller.check_run("run-123")


def test_runtime_execution_service_run_automated_splits_poll_and_schedule_flows():
    poll_flow = Flow(name="poll_docs", group="Docs").watch(mode="poll", source="/tmp/in", interval="5s").step(lambda context: context.current)
    schedule_flow = Flow(name="schedule_docs", group="Docs").watch(mode="schedule", interval="10m").step(lambda context: context.current)
    scheduler_calls: list[tuple[str, object]] = []

    class _GroupedRuntime:
        instances: list["_GroupedRuntime"] = []

        def __init__(self, flows, *, continuous, runtime_stop_event=None, flow_stop_event=None, runtime_ledger=None, run_stop_controller=None):
            self.flows = flows
            self.continuous = continuous
            self.runtime_stop_event = runtime_stop_event
            self.flow_stop_event = flow_stop_event
            self.runtime_ledger = runtime_ledger
            self.run_stop_controller = run_stop_controller
            type(self).instances.append(self)

        def run(self):
            self.runtime_stop_event.set()
            return tuple(flow.name for flow in self.flows)

    class _SchedulerHost:
        def __init__(self, *, runtime_engine):
            self.runtime_engine = runtime_engine
            scheduler_calls.append(("init", runtime_engine))

        def rebuild_jobs(self, flows):
            scheduler_calls.append(("rebuild", tuple(flow.name for flow in flows)))
            return tuple(flow.name for flow in flows)

        def start(self):
            scheduler_calls.append(("start", None))

        def shutdown(self):
            scheduler_calls.append(("shutdown", None))

    runtime_stop = Event()
    flow_stop = Event()
    ledger = object()
    service = RuntimeExecutionService(grouped_runtime_type=_GroupedRuntime, scheduler_host_factory=_SchedulerHost)

    result = service.run_automated(
        (poll_flow, schedule_flow),
        runtime_ledger=ledger,
        runtime_stop_event=runtime_stop,
        flow_stop_event=flow_stop,
    )

    assert result == ("poll_docs",)
    assert tuple(flow.name for flow in _GroupedRuntime.instances[0].flows) == ("poll_docs",)
    assert _GroupedRuntime.instances[0].runtime_stop_event is runtime_stop
    assert _GroupedRuntime.instances[0].runtime_ledger is ledger
    assert scheduler_calls[1:] == [
        ("rebuild", ("schedule_docs",)),
        ("start", None),
        ("shutdown", None),
    ]


def test_runtime_execution_service_run_automated_waits_for_schedule_only_flows():
    schedule_flow = Flow(name="schedule_docs", group="Docs").watch(mode="schedule", interval="10m").step(lambda context: context.current)
    runtime_stop = Event()
    scheduler_calls: list[str] = []

    class _SchedulerHost:
        def __init__(self, *, runtime_engine):
            self.runtime_engine = runtime_engine

        def rebuild_jobs(self, flows):
            scheduler_calls.append(f"rebuild:{','.join(flow.name for flow in flows)}")
            return tuple(flow.name for flow in flows)

        def start(self):
            scheduler_calls.append("start")
            runtime_stop.set()

        def shutdown(self):
            scheduler_calls.append("shutdown")

    result = RuntimeExecutionService(scheduler_host_factory=_SchedulerHost).run_automated(
        (schedule_flow,),
        runtime_stop_event=runtime_stop,
        flow_stop_event=Event(),
    )

    assert result == []
    assert scheduler_calls == ["rebuild:schedule_docs", "start", "shutdown"]

