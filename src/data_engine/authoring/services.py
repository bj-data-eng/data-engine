"""Shared collaborator bundle for public authoring entrypoints."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from data_engine.services.flow_execution import FlowExecutionService
from data_engine.services.runtime_execution import RuntimeExecutionService


@dataclass(frozen=True)
class AuthoringServices:
    """Concrete collaborators shared by the public authoring API."""

    runtime_execution_service: RuntimeExecutionService
    flow_execution_service: FlowExecutionService


def build_authoring_services(
    *,
    runtime_execution_service: RuntimeExecutionService | None = None,
    flow_execution_service: FlowExecutionService | None = None,
) -> AuthoringServices:
    """Build one authoring collaborator bundle with optional overrides."""
    return AuthoringServices(
        runtime_execution_service=runtime_execution_service or RuntimeExecutionService(),
        flow_execution_service=flow_execution_service or FlowExecutionService(),
    )


@lru_cache(maxsize=1)
def default_authoring_services() -> AuthoringServices:
    """Return the shared default authoring collaborator bundle."""
    return build_authoring_services()


__all__ = [
    "AuthoringServices",
    "build_authoring_services",
    "default_authoring_services",
]
