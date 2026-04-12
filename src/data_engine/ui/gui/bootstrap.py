"""GUI composition helpers for default service wiring."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from data_engine.application import (
    ActionStateApplication,
    DetailApplication,
    FlowCatalogApplication,
    OperatorControlApplication,
    RuntimeApplication,
    WorkspaceSessionApplication,
)
from data_engine.authoring.flow import load_flow
from data_engine.flow_modules.flow_module_loader import discover_flow_module_definitions
from data_engine.hosts.daemon.app import DaemonClientError
from data_engine.hosts.daemon.app import daemon_request, is_daemon_live, spawn_daemon_process
from data_engine.platform.local_settings import LocalSettingsStore
from data_engine.platform.theme import (
    DEFAULT_THEME,
    THEMES,
    resolve_theme_name,
    system_theme_name,
    theme_button_text,
    toggle_theme_name,
)
from data_engine.services import (
    DaemonService,
    DaemonStateService,
    FlowCatalogService,
    FlowExecutionService,
    LedgerService,
    LogService,
    RuntimeHistoryService,
    WorkspaceRuntimeBindingService,
    SettingsService,
    SharedStateService,
    ThemeService,
    WorkspaceProvisioningService,
    WorkspaceService,
)


@dataclass(frozen=True)
class GuiServices:
    """Concrete service set for the desktop GUI surface."""

    settings_service: SettingsService
    workspace_service: WorkspaceService
    workspace_session_application: WorkspaceSessionApplication
    action_state_application: ActionStateApplication
    detail_application: DetailApplication
    flow_catalog_service: FlowCatalogService
    flow_catalog_application: FlowCatalogApplication
    flow_execution_service: FlowExecutionService
    daemon_service: DaemonService
    daemon_state_service: DaemonStateService
    runtime_application: RuntimeApplication
    control_application: OperatorControlApplication
    ledger_service: LedgerService
    log_service: LogService
    runtime_binding_service: WorkspaceRuntimeBindingService
    runtime_history_service: RuntimeHistoryService
    shared_state_service: SharedStateService
    theme_service: ThemeService
    workspace_provisioning_service: WorkspaceProvisioningService


@dataclass(frozen=True)
class GuiDependencyFactories:
    """Factories used to build the default GUI dependency bundle."""

    settings_store_factory: Callable[[Path | None], LocalSettingsStore]
    settings_service_factory: Callable[[LocalSettingsStore], SettingsService]
    workspace_service_factory: Callable[[object | None, object | None], WorkspaceService]
    workspace_session_application_factory: Callable[[WorkspaceService], WorkspaceSessionApplication]
    action_state_application_factory: Callable[[], ActionStateApplication]
    detail_application_factory: Callable[[], DetailApplication]
    flow_catalog_service_factory: Callable[[object], FlowCatalogService]
    flow_catalog_application_factory: Callable[[FlowCatalogService], FlowCatalogApplication]
    flow_execution_service_factory: Callable[[object], FlowExecutionService]
    daemon_service_factory: Callable[[object, object, object, type[Exception]], DaemonService]
    daemon_state_service_factory: Callable[[], DaemonStateService]
    ledger_service_factory: Callable[[], LedgerService]
    log_service_factory: Callable[[], LogService]
    runtime_binding_service_factory: Callable[[LedgerService, LogService, DaemonStateService, RuntimeHistoryService], WorkspaceRuntimeBindingService]
    runtime_history_service_factory: Callable[[], RuntimeHistoryService]
    shared_state_service_factory: Callable[[], SharedStateService]
    runtime_application_factory: Callable[[DaemonService, DaemonStateService, SharedStateService], RuntimeApplication]
    control_application_factory: Callable[[RuntimeApplication, DaemonStateService], OperatorControlApplication]
    theme_service_factory: Callable[[object, str, object, object, object, object], ThemeService]
    workspace_provisioning_service_factory: Callable[[], WorkspaceProvisioningService] = field(
        default=WorkspaceProvisioningService
    )


def default_gui_dependency_factories() -> GuiDependencyFactories:
    """Return the default constructor bundle for GUI bootstrap objects."""
    return GuiDependencyFactories(
        settings_store_factory=lambda app_root: LocalSettingsStore.open_default(app_root=app_root),
        settings_service_factory=SettingsService,
        workspace_service_factory=lambda discover, resolve: WorkspaceService(
            discover_workspaces_func=discover,
            resolve_workspace_paths_func=resolve,
        )
        if discover is not None or resolve is not None
        else WorkspaceService(),
        workspace_session_application_factory=lambda workspace_service: WorkspaceSessionApplication(
            workspace_service=workspace_service,
        ),
        action_state_application_factory=ActionStateApplication,
        detail_application_factory=DetailApplication,
        flow_catalog_service_factory=lambda discover_definitions: FlowCatalogService(
            discover_definitions_func=discover_definitions,
        ),
        flow_catalog_application_factory=lambda flow_catalog_service: FlowCatalogApplication(
            flow_catalog_service=flow_catalog_service,
        ),
        flow_execution_service_factory=lambda load_flow_func: FlowExecutionService(
            load_flow_func=load_flow_func,
        ),
        daemon_service_factory=lambda spawn, request, is_live, error_type: DaemonService(
            spawn_process_func=spawn,
            request_func=request,
            is_live_func=is_live,
            client_error_type=error_type,
        ),
        daemon_state_service_factory=DaemonStateService,
        ledger_service_factory=LedgerService,
        log_service_factory=LogService,
        runtime_binding_service_factory=lambda ledger_service, log_service, daemon_state_service, runtime_history_service: WorkspaceRuntimeBindingService(
            ledger_service=ledger_service,
            log_service=log_service,
            daemon_state_service=daemon_state_service,
            runtime_history_service=runtime_history_service,
        ),
        runtime_history_service_factory=RuntimeHistoryService,
        shared_state_service_factory=SharedStateService,
        runtime_application_factory=lambda daemon_service, daemon_state_service, shared_state_service: RuntimeApplication(
            daemon_service=daemon_service,
            daemon_state_service=daemon_state_service,
            shared_state_service=shared_state_service,
        ),
        control_application_factory=lambda runtime_application, daemon_state_service: OperatorControlApplication(
            runtime_application=runtime_application,
            daemon_state_service=daemon_state_service,
        ),
        theme_service_factory=lambda themes, default_theme_name, resolve, system, toggle, button_text: ThemeService(
            themes=themes,
            default_theme_name=default_theme_name,
            resolve_theme_name_func=resolve,
            system_theme_name_func=system,
            toggle_theme_name_func=toggle,
            theme_button_text_func=button_text,
        ),
        workspace_provisioning_service_factory=WorkspaceProvisioningService,
    )


def default_gui_service_kwargs(theme_name: str) -> dict[str, object]:
    """Return the shared default seam kwargs used by the GUI surface."""
    del theme_name
    return {
        "discover_definitions_func": discover_flow_module_definitions,
        "load_flow_func": load_flow,
        "spawn_process_func": spawn_daemon_process,
        "request_func": daemon_request,
        "is_live_func": is_daemon_live,
        "resolve_theme_name_func": resolve_theme_name,
    }


def _gui_services_from_kwargs(service_kwargs: dict[str, object]) -> GuiServices:
    """Convert shared surface service kwargs into GUI-specific services."""
    return GuiServices(
        settings_service=service_kwargs["settings_service"],
        workspace_service=service_kwargs["workspace_service"],
        workspace_session_application=service_kwargs["workspace_session_application"],
        action_state_application=service_kwargs["action_state_application"],
        detail_application=service_kwargs["detail_application"],
        flow_catalog_service=service_kwargs["flow_catalog_service"],
        flow_catalog_application=service_kwargs["flow_catalog_application"],
        flow_execution_service=service_kwargs["flow_execution_service"],
        daemon_service=service_kwargs["daemon_service"],
        daemon_state_service=service_kwargs["daemon_state_service"],
        runtime_application=service_kwargs["runtime_application"],
        control_application=service_kwargs["control_application"],
        ledger_service=service_kwargs["ledger_service"],
        log_service=service_kwargs["log_service"],
        runtime_binding_service=service_kwargs["runtime_binding_service"],
        runtime_history_service=service_kwargs["runtime_history_service"],
        shared_state_service=service_kwargs["shared_state_service"],
        theme_service=service_kwargs["theme_service"],
        workspace_provisioning_service=service_kwargs["workspace_provisioning_service"],
    )


def build_gui_service_kwargs(
    *,
    settings_service: SettingsService | None = None,
    workspace_service: WorkspaceService | None = None,
    workspace_session_application: WorkspaceSessionApplication | None = None,
    action_state_application: ActionStateApplication | None = None,
    detail_application: DetailApplication | None = None,
    flow_catalog_service: FlowCatalogService | None = None,
    flow_catalog_application: FlowCatalogApplication | None = None,
    flow_execution_service: FlowExecutionService | None = None,
    daemon_service: DaemonService | None = None,
    daemon_state_service: DaemonStateService | None = None,
    runtime_application: RuntimeApplication | None = None,
    control_application: OperatorControlApplication | None = None,
    ledger_service: LedgerService | None = None,
    log_service: LogService | None = None,
    runtime_binding_service: WorkspaceRuntimeBindingService | None = None,
    runtime_history_service: RuntimeHistoryService | None = None,
    shared_state_service: SharedStateService | None = None,
    theme_service: ThemeService | None = None,
    workspace_provisioning_service: WorkspaceProvisioningService | None = None,
    settings_store: LocalSettingsStore | None = None,
    factories: GuiDependencyFactories | None = None,
    app_root: Path | None = None,
    discover_workspaces_func=None,
    resolve_workspace_paths_func=None,
    discover_definitions_func=discover_flow_module_definitions,
    load_flow_func=load_flow,
    spawn_process_func=spawn_daemon_process,
    request_func=daemon_request,
    is_live_func=is_daemon_live,
    client_error_type: type[Exception] = DaemonClientError,
    resolve_theme_name_func=resolve_theme_name,
    system_theme_name_func=system_theme_name,
    toggle_theme_name_func=toggle_theme_name,
    theme_button_text_func=theme_button_text,
    themes=THEMES,
    default_theme_name: str | None = None,
) -> dict[str, Any]:
    """Build the common service bundle used by the GUI bootstrap module."""
    factories = factories or default_gui_dependency_factories()
    discover_definitions_func = discover_definitions_func or discover_flow_module_definitions
    load_flow_func = load_flow_func or load_flow
    spawn_process_func = spawn_process_func or spawn_daemon_process
    request_func = request_func or daemon_request
    is_live_func = is_live_func or is_daemon_live
    resolve_theme_name_func = resolve_theme_name_func or resolve_theme_name
    system_theme_name_func = system_theme_name_func or system_theme_name
    toggle_theme_name_func = toggle_theme_name_func or toggle_theme_name
    theme_button_text_func = theme_button_text_func or theme_button_text
    themes = themes or THEMES
    default_theme_name = default_theme_name or DEFAULT_THEME
    settings_store = settings_store or factories.settings_store_factory(app_root)
    settings_service = settings_service or factories.settings_service_factory(settings_store)
    workspace_service = workspace_service or factories.workspace_service_factory(
        discover_workspaces_func,
        resolve_workspace_paths_func,
    )
    workspace_session_application = workspace_session_application or factories.workspace_session_application_factory(
        workspace_service,
    )
    action_state_application = action_state_application or factories.action_state_application_factory()
    detail_application = detail_application or factories.detail_application_factory()
    flow_catalog_service = flow_catalog_service or factories.flow_catalog_service_factory(discover_definitions_func)
    flow_catalog_application = flow_catalog_application or factories.flow_catalog_application_factory(flow_catalog_service)
    flow_execution_service = flow_execution_service or factories.flow_execution_service_factory(load_flow_func)
    daemon_service = daemon_service or factories.daemon_service_factory(
        spawn_process_func,
        request_func,
        is_live_func,
        client_error_type,
    )
    daemon_state_service = daemon_state_service or factories.daemon_state_service_factory()
    ledger_service = ledger_service or factories.ledger_service_factory()
    log_service = log_service or factories.log_service_factory()
    runtime_history_service = runtime_history_service or factories.runtime_history_service_factory()
    runtime_binding_service = runtime_binding_service or factories.runtime_binding_service_factory(
        ledger_service,
        log_service,
        daemon_state_service,
        runtime_history_service,
    )
    shared_state_service = shared_state_service or factories.shared_state_service_factory()
    workspace_provisioning_service = workspace_provisioning_service or factories.workspace_provisioning_service_factory()
    runtime_application = runtime_application or factories.runtime_application_factory(
        daemon_service,
        daemon_state_service,
        shared_state_service,
    )
    control_application = control_application or factories.control_application_factory(
        runtime_application,
        daemon_state_service,
    )
    theme_service = theme_service or factories.theme_service_factory(
        themes,
        default_theme_name,
        resolve_theme_name_func,
        system_theme_name_func,
        toggle_theme_name_func,
        theme_button_text_func,
    )
    return {
        "settings_service": settings_service,
        "workspace_service": workspace_service,
        "workspace_session_application": workspace_session_application,
        "action_state_application": action_state_application,
        "detail_application": detail_application,
        "flow_catalog_service": flow_catalog_service,
        "flow_catalog_application": flow_catalog_application,
        "flow_execution_service": flow_execution_service,
        "daemon_service": daemon_service,
        "daemon_state_service": daemon_state_service,
        "runtime_application": runtime_application,
        "control_application": control_application,
        "ledger_service": ledger_service,
        "log_service": log_service,
        "runtime_binding_service": runtime_binding_service,
        "runtime_history_service": runtime_history_service,
        "shared_state_service": shared_state_service,
        "theme_service": theme_service,
        "workspace_provisioning_service": workspace_provisioning_service,
    }


def build_default_gui_services(
    *,
    settings_service: SettingsService | None = None,
    workspace_service: WorkspaceService | None = None,
    workspace_session_application: WorkspaceSessionApplication | None = None,
    action_state_application: ActionStateApplication | None = None,
    detail_application: DetailApplication | None = None,
    flow_catalog_service: FlowCatalogService | None = None,
    flow_catalog_application: FlowCatalogApplication | None = None,
    flow_execution_service: FlowExecutionService | None = None,
    daemon_service: DaemonService | None = None,
    daemon_state_service: DaemonStateService | None = None,
    runtime_application: RuntimeApplication | None = None,
    control_application: OperatorControlApplication | None = None,
    ledger_service: LedgerService | None = None,
    log_service: LogService | None = None,
    runtime_binding_service: WorkspaceRuntimeBindingService | None = None,
    runtime_history_service: RuntimeHistoryService | None = None,
    shared_state_service: SharedStateService | None = None,
    theme_service: ThemeService | None = None,
    workspace_provisioning_service: WorkspaceProvisioningService | None = None,
    settings_store: LocalSettingsStore | None = None,
    factories: GuiDependencyFactories | None = None,
    app_root: Path | None = None,
    discover_workspaces_func=None,
    resolve_workspace_paths_func=None,
    discover_definitions_func=None,
    load_flow_func=None,
    spawn_process_func=None,
    request_func=None,
    is_live_func=None,
    client_error_type: type[Exception] = DaemonClientError,
    resolve_theme_name_func=None,
    system_theme_name_func=None,
    toggle_theme_name_func=None,
    theme_button_text_func=None,
    themes=None,
    default_theme_name: str | None = None,
) -> GuiServices:
    """Build the default desktop GUI service set."""
    service_kwargs = build_gui_service_kwargs(
        settings_service=settings_service,
        workspace_service=workspace_service,
        workspace_session_application=workspace_session_application,
        action_state_application=action_state_application,
        detail_application=detail_application,
        flow_catalog_service=flow_catalog_service,
        flow_catalog_application=flow_catalog_application,
        flow_execution_service=flow_execution_service,
        daemon_service=daemon_service,
        daemon_state_service=daemon_state_service,
        runtime_application=runtime_application,
        control_application=control_application,
        ledger_service=ledger_service,
        log_service=log_service,
        runtime_binding_service=runtime_binding_service,
        runtime_history_service=runtime_history_service,
        shared_state_service=shared_state_service,
        theme_service=theme_service,
        workspace_provisioning_service=workspace_provisioning_service,
        settings_store=settings_store,
        factories=factories,
        app_root=app_root,
        discover_workspaces_func=discover_workspaces_func,
        resolve_workspace_paths_func=resolve_workspace_paths_func,
        discover_definitions_func=discover_definitions_func,
        load_flow_func=load_flow_func,
        spawn_process_func=spawn_process_func,
        request_func=request_func,
        is_live_func=is_live_func,
        client_error_type=client_error_type,
        resolve_theme_name_func=resolve_theme_name_func,
        system_theme_name_func=system_theme_name_func,
        toggle_theme_name_func=toggle_theme_name_func,
        theme_button_text_func=theme_button_text_func,
        themes=themes,
        default_theme_name=default_theme_name,
    )
    return _gui_services_from_kwargs(service_kwargs)


def build_gui_services(
    *,
    settings_service: SettingsService | None = None,
    workspace_service: WorkspaceService | None = None,
    workspace_session_application: WorkspaceSessionApplication | None = None,
    action_state_application: ActionStateApplication | None = None,
    detail_application: DetailApplication | None = None,
    flow_catalog_service: FlowCatalogService | None = None,
    flow_catalog_application: FlowCatalogApplication | None = None,
    flow_execution_service: FlowExecutionService | None = None,
    daemon_service: DaemonService | None = None,
    daemon_state_service: DaemonStateService | None = None,
    runtime_application: RuntimeApplication | None = None,
    control_application: OperatorControlApplication | None = None,
    ledger_service: LedgerService | None = None,
    log_service: LogService | None = None,
    runtime_history_service: RuntimeHistoryService | None = None,
    shared_state_service: SharedStateService | None = None,
    theme_service: ThemeService | None = None,
    workspace_provisioning_service: WorkspaceProvisioningService | None = None,
    settings_store: LocalSettingsStore | None = None,
    factories: GuiDependencyFactories | None = None,
    app_root: Path | None = None,
    discover_workspaces_func=None,
    resolve_workspace_paths_func=None,
    discover_definitions_func=None,
    load_flow_func=None,
    spawn_process_func=None,
    request_func=None,
    is_live_func=None,
    client_error_type: type[Exception] = DaemonClientError,
    resolve_theme_name_func=None,
    system_theme_name_func=None,
    toggle_theme_name_func=None,
    theme_button_text_func=None,
    themes=None,
    default_theme_name: str | None = None,
) -> GuiServices:
    """Compatibility wrapper around the GUI default composition root."""
    return build_default_gui_services(
        settings_service=settings_service,
        workspace_service=workspace_service,
        workspace_session_application=workspace_session_application,
        action_state_application=action_state_application,
        detail_application=detail_application,
        flow_catalog_service=flow_catalog_service,
        flow_catalog_application=flow_catalog_application,
        flow_execution_service=flow_execution_service,
        daemon_service=daemon_service,
        daemon_state_service=daemon_state_service,
        runtime_application=runtime_application,
        control_application=control_application,
        ledger_service=ledger_service,
        log_service=log_service,
        runtime_history_service=runtime_history_service,
        shared_state_service=shared_state_service,
        theme_service=theme_service,
        workspace_provisioning_service=workspace_provisioning_service,
        settings_store=settings_store,
        factories=factories,
        app_root=app_root,
        discover_workspaces_func=discover_workspaces_func,
        resolve_workspace_paths_func=resolve_workspace_paths_func,
        discover_definitions_func=discover_definitions_func,
        load_flow_func=load_flow_func,
        spawn_process_func=spawn_process_func,
        request_func=request_func,
        is_live_func=is_live_func,
        client_error_type=client_error_type,
        resolve_theme_name_func=resolve_theme_name_func,
        system_theme_name_func=system_theme_name_func,
        toggle_theme_name_func=toggle_theme_name_func,
        theme_button_text_func=theme_button_text_func,
        themes=themes,
        default_theme_name=default_theme_name,
    )

__all__ = [
    "GuiDependencyFactories",
    "GuiServices",
    "build_default_gui_services",
    "build_gui_service_kwargs",
    "build_gui_services",
    "default_gui_dependency_factories",
    "default_gui_service_kwargs",
]
