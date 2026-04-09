"""Injectable application service objects."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "DaemonService",
    "DaemonStateService",
    "FlowCatalogService",
    "FlowExecutionService",
    "LedgerService",
    "LogService",
    "RuntimeExecutionService",
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
    "FlowCatalogService": "data_engine.services.flow_catalog",
    "FlowExecutionService": "data_engine.services.flow_execution",
    "LedgerService": "data_engine.services.ledger",
    "LogService": "data_engine.services.logs",
    "RuntimeExecutionService": "data_engine.services.runtime_execution",
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
