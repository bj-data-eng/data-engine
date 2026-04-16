"""Flow DSL and public authoring entrypoints."""

from __future__ import annotations

import inspect
from pathlib import Path
from typing import TYPE_CHECKING

from data_engine.core.flow import Flow as _CoreFlow
from data_engine.core.helpers import _validate_slot_name
from data_engine.core.model import FlowValidationError
from data_engine.core.primitives import FlowContext

if TYPE_CHECKING:
    from data_engine.authoring.services import AuthoringServices
    from data_engine.services.flow_execution import FlowExecutionService
    from data_engine.services.runtime_execution import RuntimeExecutionService


def _resolve_authoring_services(
    *,
    authoring_services: AuthoringServices | None = None,
    runtime_execution_service: RuntimeExecutionService | None = None,
    flow_execution_service: FlowExecutionService | None = None,
) -> AuthoringServices:
    """Return one authoring collaborator bundle with explicit overrides applied."""
    from data_engine.authoring.services import build_authoring_services, default_authoring_services

    services = authoring_services or default_authoring_services()
    if runtime_execution_service is None and flow_execution_service is None:
        return services
    return build_authoring_services(
        runtime_execution_service=runtime_execution_service or services.runtime_execution_service,
        flow_execution_service=flow_execution_service or services.flow_execution_service,
    )


def _infer_authoring_flow_origin() -> tuple[str, Path] | None:
    """Return ``(flow_name, workspace_root)`` for direct authored module execution.

    This is a narrow authoring convenience for calls such as ``build().preview()``
    executed directly inside ``workspaces/<id>/flow_modules/*.py``. It should not
    affect compiled runtime execution or ad hoc inline flows outside a workspace
    ``flow_modules`` tree.
    """
    current = inspect.currentframe()
    if current is None:
        return None
    try:
        frame = current.f_back
        while frame is not None:
            filename = frame.f_code.co_filename
            try:
                path = Path(filename).resolve()
            except (OSError, RuntimeError):
                frame = frame.f_back
                continue
            if path.name == __file__:
                frame = frame.f_back
                continue
            if path.parent.name == "flow_modules":
                return path.stem, path.parent.parent.resolve()
            frame = frame.f_back
    finally:
        del current
    return None


def _with_inferred_authoring_metadata(flow: "Flow") -> "Flow":
    """Fill in missing direct-authoring metadata when executing from flow_modules."""
    if flow.name is not None and flow._workspace_root is not None:
        return flow
    inferred = _infer_authoring_flow_origin()
    if inferred is None:
        return flow
    inferred_name, inferred_workspace_root = inferred
    return flow._clone(
        name=flow.name or inferred_name,
        _workspace_root=flow._workspace_root or inferred_workspace_root,
    )


class Flow(_CoreFlow):
    """Public authoring flow with execution conveniences layered over core definitions."""

    def run_once(
        self,
        *,
        authoring_services: AuthoringServices | None = None,
        runtime_execution_service: RuntimeExecutionService | None = None,
    ) -> list[FlowContext]:
        """Run this flow once and return completed runtime contexts.

        Parameters
        ----------
        authoring_services : AuthoringServices | None
            Optional service bundle used by tests or embedded hosts.
        runtime_execution_service : RuntimeExecutionService | None
            Optional runtime execution service override.

        Returns
        -------
        list[FlowContext]
            One context per executed source.
        """
        flow = _with_inferred_authoring_metadata(self)
        service = _resolve_authoring_services(
            authoring_services=authoring_services,
            runtime_execution_service=runtime_execution_service,
        ).runtime_execution_service
        return service.run_once(flow)

    def preview(
        self,
        *,
        use: str | None = None,
        authoring_services: AuthoringServices | None = None,
        runtime_execution_service: RuntimeExecutionService | None = None,
    ) -> object:
        """Run this flow in preview mode and return one preview value.

        Parameters
        ----------
        use : str | None
            Optional named object slot to preview instead of the final current
            value.
        authoring_services : AuthoringServices | None
            Optional service bundle used by tests or embedded hosts.
        runtime_execution_service : RuntimeExecutionService | None
            Optional runtime execution service override.

        Returns
        -------
        object
            Preview value returned by the runtime execution service.

        Raises
        ------
        FlowValidationError
            If preview is requested from inside a compiled flow module.
        """
        from data_engine.flow_modules.flow_module_loader import in_compiled_flow_module_context

        if in_compiled_flow_module_context():
            raise FlowValidationError("preview() is not available inside compiled flow modules.")
        flow = _with_inferred_authoring_metadata(self)
        normalized_use = _validate_slot_name(method_name="preview", slot_name="use", value=use)
        service = _resolve_authoring_services(
            authoring_services=authoring_services,
            runtime_execution_service=runtime_execution_service,
        ).runtime_execution_service
        return service.preview(flow, use=normalized_use)

    def run(
        self,
        *,
        authoring_services: AuthoringServices | None = None,
        runtime_execution_service: RuntimeExecutionService | None = None,
    ) -> list[FlowContext]:
        """Run this flow continuously according to its trigger.

        Parameters
        ----------
        authoring_services : AuthoringServices | None
            Optional service bundle used by tests or embedded hosts.
        runtime_execution_service : RuntimeExecutionService | None
            Optional runtime execution service override.

        Returns
        -------
        list[FlowContext]
            Completed contexts collected before the runtime exits.
        """
        flow = _with_inferred_authoring_metadata(self)
        service = _resolve_authoring_services(
            authoring_services=authoring_services,
            runtime_execution_service=runtime_execution_service,
        ).runtime_execution_service
        return service.run_continuous(flow)


def load_flow(
    name: str,
    *,
    data_root: Path | None = None,
    authoring_services: AuthoringServices | None = None,
    flow_execution_service: FlowExecutionService | None = None,
) -> Flow:
    """Load one code-defined flow by flow-module name."""
    service = _resolve_authoring_services(
        authoring_services=authoring_services,
        flow_execution_service=flow_execution_service,
    ).flow_execution_service
    return service.load_flow(name, workspace_root=data_root)


def discover_flows(
    *,
    data_root: Path | None = None,
    authoring_services: AuthoringServices | None = None,
    flow_execution_service: FlowExecutionService | None = None,
) -> tuple[Flow, ...]:
    """Discover and build all code-defined flows from compiled flow modules."""
    service = _resolve_authoring_services(
        authoring_services=authoring_services,
        flow_execution_service=flow_execution_service,
    ).flow_execution_service
    return service.discover_flows(workspace_root=data_root)


def run(
    *flows: Flow,
    authoring_services: AuthoringServices | None = None,
    runtime_execution_service: RuntimeExecutionService | None = None,
) -> list[FlowContext]:
    """Run multiple flows with sequential execution per group and parallel groups."""
    service = _resolve_authoring_services(
        authoring_services=authoring_services,
        runtime_execution_service=runtime_execution_service,
    ).runtime_execution_service
    return service.run_grouped_continuous(tuple(flows))


__all__ = ["Flow", "discover_flows", "load_flow", "run"]
