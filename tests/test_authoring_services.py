from __future__ import annotations

from pathlib import Path

from data_engine.authoring.flow import discover_flows, load_flow, run, Flow
from data_engine.authoring.services import AuthoringServices, build_authoring_services, default_authoring_services


class _RuntimeExecutionService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def run_once(self, flow):
        self.calls.append(("run_once", flow))
        return ["once"]

    def preview(self, flow, *, use=None):
        self.calls.append(("preview", flow, use))
        return "preview"

    def run_continuous(self, flow):
        self.calls.append(("run_continuous", flow))
        return ["continuous"]

    def run_grouped_continuous(self, flows):
        self.calls.append(("run_grouped_continuous", tuple(flows)))
        return ["grouped"]


class _FlowExecutionService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def load_flow(self, name, *, workspace_root=None):
        self.calls.append(("load_flow", name, workspace_root))
        return Flow(name=name, group="Docs")

    def discover_flows(self, *, workspace_root=None):
        self.calls.append(("discover_flows", workspace_root))
        return (Flow(name="alpha", group="Docs"),)


def test_default_authoring_services_is_shared_and_cached():
    first = default_authoring_services()
    second = default_authoring_services()

    assert first is second
    assert isinstance(first, AuthoringServices)


def test_build_authoring_services_accepts_explicit_overrides():
    runtime_service = _RuntimeExecutionService()
    flow_service = _FlowExecutionService()

    bundle = build_authoring_services(
        runtime_execution_service=runtime_service,
        flow_execution_service=flow_service,
    )

    assert bundle.runtime_execution_service is runtime_service
    assert bundle.flow_execution_service is flow_service


def test_public_authoring_entrypoints_share_one_bundle():
    runtime_service = _RuntimeExecutionService()
    flow_service = _FlowExecutionService()
    bundle = AuthoringServices(
        runtime_execution_service=runtime_service,
        flow_execution_service=flow_service,
    )
    flow = Flow(name="docs", group="Docs").step(lambda context: context.current)

    assert flow.run_once(authoring_services=bundle) == ["once"]
    assert flow.preview(authoring_services=bundle) == "preview"
    assert flow.run(authoring_services=bundle) == ["continuous"]
    assert load_flow("docs", data_root=Path("/tmp/workspace"), authoring_services=bundle).name == "docs"
    assert discover_flows(data_root=Path("/tmp/workspace"), authoring_services=bundle)[0].name == "alpha"
    assert run(flow, authoring_services=bundle) == ["grouped"]

    assert runtime_service.calls == [
        ("run_once", flow),
        ("preview", flow, None),
        ("run_continuous", flow),
        ("run_grouped_continuous", (flow,)),
    ]
    assert flow_service.calls == [
        ("load_flow", "docs", Path("/tmp/workspace")),
        ("discover_flows", Path("/tmp/workspace")),
    ]

