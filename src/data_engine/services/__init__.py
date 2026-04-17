"""Injectable application service objects."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "DaemonService",
    "DaemonStateService",
    "CatalogPort",
    "CatalogQueryService",
    "FlowCatalogService",
    "FlowExecutionService",
    "FlowConfigPreview",
    "FlowCatalogItem",
    "HistoryPort",
    "HistoryQueryService",
    "LedgerService",
    "LogService",
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
    "CatalogPort": "data_engine.services.operator_queries",
    "CatalogQueryService": "data_engine.services.operator_queries",
    "FlowCatalogService": "data_engine.services.flow_catalog",
    "FlowExecutionService": "data_engine.services.flow_execution",
    "FlowConfigPreview": "data_engine.services.operator_queries",
    "FlowCatalogItem": "data_engine.services.operator_queries",
    "HistoryPort": "data_engine.services.operator_queries",
    "HistoryQueryService": "data_engine.services.operator_queries",
    "LedgerService": "data_engine.services.ledger",
    "LogService": "data_engine.services.logs",
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
