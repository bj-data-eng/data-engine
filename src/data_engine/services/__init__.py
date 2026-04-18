"""Injectable application service objects."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "DaemonService",
    "DaemonStateService",
    "DaemonUpdateSubscription",
    "CatalogPort",
    "CatalogQueryService",
    "CommandPort",
    "FlowCatalogService",
    "FlowExecutionService",
    "FlowConfigPreview",
    "FlowCatalogItem",
    "ForceShutdownCommandResult",
    "HistoryPort",
    "HistoryQueryService",
    "LedgerService",
    "LogService",
    "OperatorCommandResult",
    "OperatorCommandService",
    "ProvisionWorkspaceCommandResult",
    "RefreshFlowsCommandResult",
    "ResetFlowCommandResult",
    "ResetWorkspaceCommandResult",
    "RuntimeExecutionService",
    "ResetService",
    "RuntimeStatePort",
    "RuntimeStateService",
    "WorkspaceSnapshot",
    "WorkspaceRuntimeBinding",
    "WorkspaceRuntimeBindingService",
    "RuntimeHistoryService",
    "SettingsService",
    "SharedStateService",
    "ThemeService",
    "WorkspaceProvisioningService",
    "WorkspaceService",
]

_SERVICE_MODULES = {
    "DaemonService": "data_engine.services.daemon",
    "DaemonStateService": "data_engine.services.daemon_state",
    "DaemonUpdateSubscription": "data_engine.services.daemon_state",
    "CatalogPort": "data_engine.services.operator_queries",
    "CatalogQueryService": "data_engine.services.operator_queries",
    "CommandPort": "data_engine.services.operator_commands",
    "FlowCatalogService": "data_engine.services.flow_catalog",
    "FlowExecutionService": "data_engine.services.flow_execution",
    "FlowConfigPreview": "data_engine.services.operator_queries",
    "FlowCatalogItem": "data_engine.services.operator_queries",
    "ForceShutdownCommandResult": "data_engine.services.operator_commands",
    "HistoryPort": "data_engine.services.operator_queries",
    "HistoryQueryService": "data_engine.services.operator_queries",
    "LedgerService": "data_engine.services.ledger",
    "LogService": "data_engine.services.logs",
    "OperatorCommandResult": "data_engine.services.operator_commands",
    "OperatorCommandService": "data_engine.services.operator_commands",
    "ProvisionWorkspaceCommandResult": "data_engine.services.operator_commands",
    "RefreshFlowsCommandResult": "data_engine.services.operator_commands",
    "ResetFlowCommandResult": "data_engine.services.operator_commands",
    "ResetWorkspaceCommandResult": "data_engine.services.operator_commands",
    "RuntimeExecutionService": "data_engine.services.runtime_execution",
    "ResetService": "data_engine.services.reset",
    "RuntimeStatePort": "data_engine.services.runtime_state",
    "RuntimeStateService": "data_engine.services.runtime_state",
    "WorkspaceSnapshot": "data_engine.services.runtime_state",
    "WorkspaceRuntimeBinding": "data_engine.services.runtime_binding",
    "WorkspaceRuntimeBindingService": "data_engine.services.runtime_binding",
    "RuntimeHistoryService": "data_engine.services.runtime_history",
    "SettingsService": "data_engine.services.settings",
    "SharedStateService": "data_engine.services.shared_state",
    "ThemeService": "data_engine.services.theme",
    "WorkspaceProvisioningService": "data_engine.services.workspace_provisioning",
    "WorkspaceService": "data_engine.services.workspaces",
}


def __getattr__(name: str):
    module_name = _SERVICE_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    return getattr(module, name)
