"""Dependency wiring for the CLI surface."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from data_engine.platform.workspace_policy import AppStatePolicy
from data_engine.services import SharedStateService, WorkspaceService


@dataclass(frozen=True)
class CliDependencies:
    """Concrete collaborators used by the public CLI surface."""

    app_state_policy: AppStatePolicy
    shared_state_service: SharedStateService
    workspace_service: WorkspaceService


@dataclass(frozen=True)
class CliDependencyFactories:
    """Factories for the CLI's default concrete collaborators."""

    app_state_policy_factory: Callable[[], AppStatePolicy]
    shared_state_service_factory: Callable[[], SharedStateService]
    workspace_service_factory: Callable[[], WorkspaceService]


def default_cli_dependency_factories() -> CliDependencyFactories:
    return CliDependencyFactories(
        app_state_policy_factory=AppStatePolicy,
        shared_state_service_factory=SharedStateService,
        workspace_service_factory=WorkspaceService,
    )


def build_default_cli_dependencies(*, factories: CliDependencyFactories | None = None) -> CliDependencies:
    factories = factories or default_cli_dependency_factories()
    return CliDependencies(
        app_state_policy=factories.app_state_policy_factory(),
        shared_state_service=factories.shared_state_service_factory(),
        workspace_service=factories.workspace_service_factory(),
    )
