from __future__ import annotations

from data_engine.ui.cli.dependencies import CliDependencyFactories, build_default_cli_dependencies


def test_build_default_cli_dependencies_uses_factory_bundle():
    calls: list[str] = []

    class _Policy:
        pass

    class _SharedStateService:
        pass

    class _WorkspaceService:
        pass

    dependencies = build_default_cli_dependencies(
        factories=CliDependencyFactories(
            app_state_policy_factory=lambda: calls.append("policy") or _Policy(),
            shared_state_service_factory=lambda: calls.append("shared-state") or _SharedStateService(),
            workspace_service_factory=lambda: calls.append("workspace") or _WorkspaceService(),
        )
    )

    assert isinstance(dependencies.app_state_policy, _Policy)
    assert isinstance(dependencies.shared_state_service, _SharedStateService)
    assert isinstance(dependencies.workspace_service, _WorkspaceService)
    assert calls == ["policy", "shared-state", "workspace"]
