"""Flow DSL and public authoring entrypoints."""

from __future__ import annotations

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
        service = _resolve_authoring_services(
            authoring_services=authoring_services,
            runtime_execution_service=runtime_execution_service,
        ).runtime_execution_service
        return service.run_once(self)

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
        normalized_use = _validate_slot_name(method_name="preview", slot_name="use", value=use)
        service = _resolve_authoring_services(
            authoring_services=authoring_services,
            runtime_execution_service=runtime_execution_service,
        ).runtime_execution_service
        return service.preview(self, use=normalized_use)

    def show(self) -> object:
        """Run this flow once and return the single final current value.

        Returns
        -------
        object
            Final ``context.current`` value.

        Raises
        ------
        FlowValidationError
            If called from a compiled flow module or the flow produces anything
            other than one result.
        """
        from data_engine.flow_modules.flow_module_loader import in_compiled_flow_module_context

        if in_compiled_flow_module_context():
            raise FlowValidationError("show() is not available inside compiled flow modules.")
        results = self.run_once()
        if len(results) != 1:
            raise FlowValidationError(f"show() requires exactly one result, found {len(results)}.")
        return results[0].current

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
        service = _resolve_authoring_services(
            authoring_services=authoring_services,
            runtime_execution_service=runtime_execution_service,
        ).runtime_execution_service
        return service.run_continuous(self)


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
