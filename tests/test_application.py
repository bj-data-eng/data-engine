from __future__ import annotations

from pathlib import Path

from data_engine.application import (
    ActionStateApplication,
    DetailApplication,
    FlowCatalogApplication,
    OperatorControlApplication,
    RuntimeApplication,
    WorkspaceSessionApplication,
)
from data_engine.authoring.model import FlowValidationError
from data_engine.domain import (
    DaemonLifecyclePolicy,
    FlowCatalogEntry,
    FlowLogEntry,
    OperationSessionState,
    RuntimeSessionState,
    RuntimeStepEvent,
)
from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot
from data_engine.platform.workspace_models import DiscoveredWorkspace
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


class _FakeFlowCatalogService:
    def __init__(self, entries: tuple[FlowCatalogEntry, ...]) -> None:
        self.entries = entries
        self.calls: list[Path] = []

    def load_entries(self, *, workspace_root: Path | None = None) -> tuple[FlowCatalogEntry, ...]:
        assert workspace_root is not None
        self.calls.append(workspace_root)
        return self.entries


class _FailingFlowCatalogService:
    def load_entries(self, *, workspace_root: Path | None = None) -> tuple[FlowCatalogEntry, ...]:
        assert workspace_root is not None
        raise FlowValidationError("invalid flow")


class _FakeWorkspaceService:
    def __init__(self, discovered: tuple[DiscoveredWorkspace, ...]) -> None:
        self.discovered = discovered
        self.calls: list[tuple[Path, Path | None]] = []

    def discover(
        self,
        *,
        app_root: Path | None = None,
        workspace_collection_root: Path | None = None,
    ) -> tuple[DiscoveredWorkspace, ...]:
        assert app_root is not None
        self.calls.append((app_root, workspace_collection_root))
        return self.discovered


class _FakeDaemonService:
    def __init__(self, *, response: dict | None = None, request_error: Exception | None = None) -> None:
        self.response = response or {"ok": True}
        self.request_error = request_error
        self.spawn_calls: list[tuple[Path, DaemonLifecyclePolicy]] = []
        self.request_calls: list[tuple[Path, dict, float]] = []
        self.force_shutdown_calls: list[tuple[Path, float]] = []

    def spawn(self, paths, *, lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT) -> object:
        self.spawn_calls.append((paths.workspace_root, lifecycle_policy))
        return {"ok": True}

    def request(self, paths, payload, *, timeout: float = 0.0):
        self.request_calls.append((paths.workspace_root, payload, timeout))
        if self.request_error is not None:
            raise self.request_error
        return self.response

    def force_shutdown(self, paths, *, timeout: float = 0.5) -> None:
        if self.request_error is not None:
            raise self.request_error
        self.force_shutdown_calls.append((paths.workspace_root, timeout))

    @property
    def client_error_type(self):
        return RuntimeError


class _FakeDaemonStateService:
    def __init__(self, snapshot: WorkspaceDaemonSnapshot, control_state) -> None:
        self.snapshot = snapshot
        self.control_state_value = control_state
        self.request_control_message = "Control request sent."
        self.request_control_error: Exception | None = None

    def sync(self, manager):
        del manager
        return self.snapshot

    def control_state(self, manager, snapshot, *, daemon_startup_in_progress: bool = False):
        del manager, snapshot, daemon_startup_in_progress
        return self.control_state_value

    def request_control(self, manager):
        del manager
        if self.request_control_error is not None:
            raise self.request_control_error
        return self.request_control_message


class _FakeSharedStateService:
    def __init__(self) -> None:
        self.hydrate_calls: list[tuple[Path, object]] = []

    def hydrate_local_runtime(self, paths, runtime_ledger) -> None:
        self.hydrate_calls.append((paths.workspace_root, runtime_ledger))


def test_flow_catalog_application_loads_state(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    entry = FlowCatalogEntry(
        name="example_manual",
        group="Examples",
        title="Example Manual",
        description="desc",
        source_root="/tmp/source",
        target_root="/tmp/target",
        mode="manual",
        interval="-",
        operations="Extract -> Write",
        operation_items=("Extract", "Write"),
        state="manual",
        valid=True,
        category="manual",
    )
    service = _FakeFlowCatalogService((entry,))

    state = FlowCatalogApplication(flow_catalog_service=service).load_state(workspace_root=workspace_root)

    assert service.calls == [workspace_root]
    assert state.entries == (entry,)
    assert state.selected_flow_name == "example_manual"
    assert state.empty_message == ""


def test_flow_catalog_application_reports_missing_workspace_catalog(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspaces" / "alpha"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    result = FlowCatalogApplication(flow_catalog_service=_FakeFlowCatalogService(())).load_workspace_catalog(
        workspace_paths=paths,
        missing_message="No flow modules discovered.",
    )

    assert result.loaded is False
    assert result.error_text is None
    assert result.catalog_state.empty_message == "No flow modules discovered."


def test_flow_catalog_application_reports_validation_errors(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspaces" / "alpha"
    (workspace_root / "flow_modules").mkdir(parents=True)
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    result = FlowCatalogApplication(flow_catalog_service=_FailingFlowCatalogService()).load_workspace_catalog(
        workspace_paths=paths,
    )

    assert result.loaded is False
    assert result.error_text == "invalid flow"
    assert result.catalog_state.empty_message == "invalid flow"


def test_flow_catalog_application_builds_grouped_presentation_and_selected_index(tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    entries = (
        FlowCatalogEntry(
            name="manual_review",
            group="Manual",
            title="Manual Review",
            description="desc",
            source_root="/tmp/source",
            target_root="/tmp/target",
            mode="manual",
            interval="-",
            operations="Review",
            operation_items=("Review",),
            state="manual",
            valid=True,
            category="manual",
        ),
        FlowCatalogEntry(
            name="poller",
            group="Imports",
            title="Poller",
            description="desc",
            source_root="/tmp/source",
            target_root="/tmp/target",
            mode="poll",
            interval="30s",
            operations="Read",
            operation_items=("Read",),
            state="poll ready",
            valid=True,
            category="automated",
        ),
    )
    app = FlowCatalogApplication(flow_catalog_service=_FakeFlowCatalogService(entries))
    state = app.load_state(workspace_root=workspace_root).with_selected_flow_name("poller")

    presentation = app.build_presentation(catalog_state=state)

    assert tuple(group_name for group_name, _entries in presentation.grouped_cards) == ("Imports", "Manual")
    assert presentation.selected_card is not None
    assert presentation.selected_card.name == "poller"
    assert presentation.selected_list_index == 1


def test_workspace_session_application_refreshes_discovery(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    collection_root = tmp_path / "workspaces"
    workspace_root = collection_root / "alpha"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    discovered = (
        DiscoveredWorkspace(workspace_id="alpha", workspace_root=workspace_root),
        DiscoveredWorkspace(workspace_id="beta", workspace_root=collection_root / "beta"),
    )
    service = _FakeWorkspaceService(discovered)

    session = WorkspaceSessionApplication(workspace_service=service).refresh_session(
        workspace_paths=paths,
        override_root=collection_root,
    )

    assert service.calls == [(paths.app_root, collection_root)]
    assert session.current_workspace_id == "alpha"
    assert session.discovered_workspace_ids == ("alpha", "beta")
    assert session.workspace_collection_root_override == collection_root


def test_workspace_session_application_binds_fresh_operator_state(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    collection_root = tmp_path / "workspaces"
    workspace_root = collection_root / "alpha"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    discovered = (DiscoveredWorkspace(workspace_id="alpha", workspace_root=workspace_root),)
    service = _FakeWorkspaceService(discovered)

    binding = WorkspaceSessionApplication(workspace_service=service).bind_workspace(
        workspace_paths=paths,
        override_root=collection_root,
    )

    assert binding.workspace_session.current_workspace_id == "alpha"
    assert binding.workspace_session.discovered_workspace_ids == ("alpha",)
    assert binding.operator_session.workspace == binding.workspace_session
    assert binding.operator_session.runtime.runtime_active is False
    assert binding.operator_session.catalog.entries == ()


def test_runtime_application_syncs_and_hydrates_lease_state(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    snapshot = WorkspaceDaemonSnapshot(
        live=False,
        workspace_owned=False,
        leased_by_machine_id="other-machine",
        runtime_active=True,
        runtime_stopping=False,
        manual_runs=("example_manual",),
        last_checkpoint_at_utc=None,
        source="lease",
    )
    daemon_state_service = _FakeDaemonStateService(snapshot, control_state=object())
    shared_state_service = _FakeSharedStateService()
    runtime_app = RuntimeApplication(
        daemon_service=_FakeDaemonService(),
        daemon_state_service=daemon_state_service,
        shared_state_service=shared_state_service,
    )

    sync_state = runtime_app.sync_state(
        paths=paths,
        daemon_manager=object(),
        flow_cards=(
            FlowCatalogEntry(
                name="example_manual",
                group="Examples",
                title="Example Manual",
                description="",
                source_root="-",
                target_root="-",
                mode="manual",
                interval="-",
                operations="Step",
                operation_items=("Step",),
                state="manual",
                valid=True,
                category="manual",
            ),
        ),
        runtime_ledger=object(),
        daemon_startup_in_progress=False,
    )

    assert sync_state.snapshot is snapshot
    assert sync_state.snapshot_source == "lease"
    assert sync_state.runtime_session.manual_run_active is True
    assert shared_state_service.hydrate_calls


def test_runtime_application_spawn_daemon_uses_ephemeral_policy_by_default(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_service = _FakeDaemonService()
    runtime_app = RuntimeApplication(
        daemon_service=daemon_service,
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
    )

    result = runtime_app.spawn_daemon(paths)

    assert result.ok is True
    assert daemon_service.spawn_calls == [(workspace_root, DaemonLifecyclePolicy.EPHEMERAL)]


def test_runtime_application_spawn_daemon_uses_configured_persistent_policy(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_service = _FakeDaemonService()
    runtime_app = RuntimeApplication(
        daemon_service=daemon_service,
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
        daemon_lifecycle_policy=DaemonLifecyclePolicy.PERSISTENT,
    )

    result = runtime_app.spawn_daemon(paths)

    assert result.ok is True
    assert daemon_service.spawn_calls == [(workspace_root, DaemonLifecyclePolicy.PERSISTENT)]


def test_runtime_application_force_shutdown_daemon_uses_daemon_service(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_service = _FakeDaemonService()
    runtime_app = RuntimeApplication(
        daemon_service=daemon_service,
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=True,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="daemon",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
    )

    result = runtime_app.force_shutdown_daemon(paths, timeout=0.75)

    assert result.ok is True
    assert daemon_service.force_shutdown_calls == [(workspace_root, 0.75)]


def test_runtime_application_normalizes_command_errors(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_service = _FakeDaemonService(request_error=RuntimeError("boom"))
    runtime_app = RuntimeApplication(
        daemon_service=daemon_service,
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
    )

    result = runtime_app.run_flow(paths, name="example_manual", wait=False, timeout=2.0)

    assert daemon_service.spawn_calls == [(workspace_root, DaemonLifecyclePolicy.EPHEMERAL)]
    assert result.ok is False
    assert result.error == "boom"


def test_runtime_application_command_failure_uses_verbose_fallback_when_daemon_returns_no_detail(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_service = _FakeDaemonService(response={"ok": False})
    runtime_app = RuntimeApplication(
        daemon_service=daemon_service,
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
    )

    result = runtime_app.start_engine(paths, timeout=2.0)

    assert daemon_service.spawn_calls == [(workspace_root, DaemonLifecyclePolicy.EPHEMERAL)]
    assert result.ok is False
    assert result.error == "Failed to start the automated engine. The daemon returned no additional detail."


def test_runtime_application_blocks_requests_when_workspace_root_is_missing(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_service = _FakeDaemonService()
    runtime_app = RuntimeApplication(
        daemon_service=daemon_service,
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
    )

    manual_result = runtime_app.run_flow(paths, name="example_manual", wait=False, timeout=2.0)
    engine_result = runtime_app.start_engine(paths, timeout=2.0)

    assert daemon_service.spawn_calls == []
    assert manual_result.ok is False
    assert manual_result.error == "Workspace root is no longer available."
    assert engine_result.ok is False
    assert engine_result.error == "Workspace root is no longer available."


def test_runtime_application_completes_manual_run_failure_without_manual_modal_for_automated_flow() -> None:
    runtime_app = RuntimeApplication(
        daemon_service=_FakeDaemonService(),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
    )

    completion = runtime_app.complete_manual_run(
        runtime_session=resolve_runtime_session_with_manual_run("Examples", "poller"),
        flow_name="poller",
        group_name="Examples",
        flow_mode="poll",
        results=None,
        error=RuntimeError("boom"),
        stop_requested=False,
    )

    assert completion.runtime_session.manual_run_active is False
    assert completion.state_updates == {"poller": "failed"}
    assert completion.show_error_text is None
    assert completion.log_messages[0].text == "Flow failed: poller: boom"


def test_runtime_application_manual_failure_uses_exception_type_when_message_is_blank() -> None:
    runtime_app = RuntimeApplication(
        daemon_service=_FakeDaemonService(),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
    )

    completion = runtime_app.complete_manual_run(
        runtime_session=resolve_runtime_session_with_manual_run("Examples", "example_completed"),
        flow_name="example_completed",
        group_name="Examples",
        flow_mode="manual",
        results=None,
        error=RuntimeError(),
        stop_requested=False,
    )

    assert completion.show_error_text == "example_completed failed.\n\nRuntimeError"
    assert completion.log_messages[0].text == "Flow failed: example_completed: RuntimeError"


def test_runtime_application_completes_engine_stop_as_normal_stop() -> None:
    runtime_app = RuntimeApplication(
        daemon_service=_FakeDaemonService(),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
    )

    completion = runtime_app.complete_engine_run(
        runtime_session=resolve_runtime_session_with_engine(("poller",)),
        flow_names=("poller",),
        flow_modes_by_name={"poller": "poll"},
        error=RuntimeError("background unwind"),
        runtime_stop_requested=True,
        flow_stop_requested=False,
    )

    assert completion.runtime_session.runtime_active is False
    assert completion.runtime_session.active_runtime_flow_names == ()
    assert completion.state_updates == {"poller": "poll ready"}
    assert completion.failed_flow_names == ()
    assert completion.log_messages[0].text == "Runtime flow stop."


def test_operator_control_application_blocks_run_when_workspace_is_leased(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_state_service = _FakeDaemonStateService(
        WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=False,
            leased_by_machine_id="other-machine",
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="lease",
        ),
        control_state=object(),
    )
    control_app = OperatorControlApplication(
        runtime_application=RuntimeApplication(
            daemon_service=_FakeDaemonService(),
            daemon_state_service=daemon_state_service,
            shared_state_service=_FakeSharedStateService(),
        ),
        daemon_state_service=daemon_state_service,
    )

    result = control_app.run_selected_flow(
        paths=paths,
        runtime_session=resolve_runtime_session_with_lease(),
        selected_flow_name="poller",
        selected_flow_valid=True,
        selected_flow_group="Examples",
        selected_flow_group_active=False,
        blocked_status_text="other-machine currently has control of this workspace.",
    )

    assert result.requested is False
    assert result.status_text == "other-machine currently has control of this workspace."


def test_operator_control_application_requests_control_and_marks_follow_up() -> None:
    daemon_state_service = _FakeDaemonStateService(
        WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=False,
            leased_by_machine_id="other-machine",
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="lease",
        ),
        control_state=object(),
    )
    control_app = OperatorControlApplication(
        runtime_application=RuntimeApplication(
            daemon_service=_FakeDaemonService(),
            daemon_state_service=daemon_state_service,
            shared_state_service=_FakeSharedStateService(),
        ),
        daemon_state_service=daemon_state_service,
    )

    result = control_app.request_control(object())

    assert result.requested is True
    assert result.sync_after is True
    assert result.ensure_daemon_started is True
    assert result.status_text == "Control request sent."


def test_operator_control_application_request_control_uses_verbose_fallback_when_exception_has_no_detail() -> None:
    daemon_state_service = _FakeDaemonStateService(
        WorkspaceDaemonSnapshot(
            live=False,
            workspace_owned=False,
            leased_by_machine_id="other-machine",
            runtime_active=False,
            runtime_stopping=False,
            manual_runs=(),
            last_checkpoint_at_utc=None,
            source="lease",
        ),
        control_state=object(),
    )
    daemon_state_service.request_control_error = RuntimeError()
    control_app = OperatorControlApplication(
        runtime_application=RuntimeApplication(
            daemon_service=_FakeDaemonService(),
            daemon_state_service=daemon_state_service,
            shared_state_service=_FakeSharedStateService(),
        ),
        daemon_state_service=daemon_state_service,
    )

    result = control_app.request_control(object())

    assert result.requested is False
    assert result.error_text == "Failed to request workspace control. The daemon returned no additional detail."


def test_operator_control_application_start_engine_uses_verbose_fallback_when_runtime_returns_blank_error(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    class _BlankErrorRuntimeApplication:
        def start_engine(self, paths, timeout: float = 2.0):
            del paths, timeout
            return type("Result", (), {"ok": False, "error": ""})()

    control_app = OperatorControlApplication(
        runtime_application=_BlankErrorRuntimeApplication(),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
    )

    result = control_app.start_engine(
        paths=paths,
        runtime_session=resolve_runtime_session_with_lease().__class__.empty(),
        has_automated_flows=True,
        blocked_status_text="blocked",
    )

    assert result.requested is False
    assert result.error_text == "Failed to start the automated engine. The daemon returned no additional detail."


def test_operator_control_application_run_selected_flow_uses_verbose_fallback_when_runtime_returns_blank_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    class _BlankErrorRuntimeApplication:
        def run_flow(self, paths, *, name: str, wait: bool = False, timeout: float = 2.0):
            del paths, name, wait, timeout
            return type("Result", (), {"ok": False, "error": ""})()

    control_app = OperatorControlApplication(
        runtime_application=_BlankErrorRuntimeApplication(),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
    )

    result = control_app.run_selected_flow(
        paths=paths,
        runtime_session=resolve_runtime_session_with_lease().__class__.empty(),
        selected_flow_name="poller",
        selected_flow_valid=True,
        selected_flow_group="Examples",
        selected_flow_group_active=False,
        blocked_status_text="blocked",
    )

    assert result.requested is False
    assert result.error_text == "Failed to run poller. The daemon returned no additional detail."


def test_operator_control_application_allows_manual_run_while_engine_is_active_for_other_group(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)

    class _RuntimeApplication:
        def run_flow(self, paths, *, name: str, wait: bool = False, timeout: float = 2.0):
            del paths, wait, timeout
            return type("Result", (), {"ok": True, "error": None, "name": name})()

    control_app = OperatorControlApplication(
        runtime_application=_RuntimeApplication(),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=True,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=True,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="runtime",
            ),
            control_state=object(),
        ),
    )

    result = control_app.run_selected_flow(
        paths=paths,
        runtime_session=RuntimeSessionState(runtime_active=True, active_runtime_flow_names=("poller",)),
        selected_flow_name="manual_claims",
        selected_flow_valid=True,
        selected_flow_group="Manual",
        selected_flow_group_active=False,
        blocked_status_text="blocked",
    )

    assert result.requested is True
    assert result.sync_after is True


def test_operator_control_application_blocks_refresh_while_active(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    control_app = OperatorControlApplication(
        runtime_application=RuntimeApplication(
            daemon_service=_FakeDaemonService(),
            daemon_state_service=_FakeDaemonStateService(
                WorkspaceDaemonSnapshot(
                    live=False,
                    workspace_owned=True,
                    leased_by_machine_id=None,
                    runtime_active=False,
                    runtime_stopping=False,
                    manual_runs=(),
                    last_checkpoint_at_utc=None,
                    source="none",
                ),
                control_state=object(),
            ),
            shared_state_service=_FakeSharedStateService(),
        ),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
    )

    result = control_app.refresh_flows(
        paths=paths,
        runtime_session=resolve_runtime_session_with_engine(("poller",)),
        has_authored_workspace=True,
    )

    assert result.reload_catalog is False
    assert result.error_text == "Stop active engine or manual runs before refreshing flows."


def test_operator_control_application_refreshes_locally_when_no_authored_workspace(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    control_app = OperatorControlApplication(
        runtime_application=RuntimeApplication(
            daemon_service=_FakeDaemonService(),
            daemon_state_service=_FakeDaemonStateService(
                WorkspaceDaemonSnapshot(
                    live=False,
                    workspace_owned=True,
                    leased_by_machine_id=None,
                    runtime_active=False,
                    runtime_stopping=False,
                    manual_runs=(),
                    last_checkpoint_at_utc=None,
                    source="none",
                ),
                control_state=object(),
            ),
            shared_state_service=_FakeSharedStateService(),
        ),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
    )

    result = control_app.refresh_flows(
        paths=paths,
        runtime_session=resolve_runtime_session_with_lease().__class__.empty(),
        has_authored_workspace=False,
    )

    assert result.reload_catalog is True
    assert result.sync_after is True
    assert result.status_text == "No flow modules discovered."


def test_operator_control_application_refresh_reports_daemon_warning_but_keeps_reload(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    control_app = OperatorControlApplication(
        runtime_application=RuntimeApplication(
            daemon_service=_FakeDaemonService(request_error=RuntimeError("unreachable")),
            daemon_state_service=_FakeDaemonStateService(
                WorkspaceDaemonSnapshot(
                    live=False,
                    workspace_owned=True,
                    leased_by_machine_id=None,
                    runtime_active=False,
                    runtime_stopping=False,
                    manual_runs=(),
                    last_checkpoint_at_utc=None,
                    source="none",
                ),
                control_state=object(),
            ),
            shared_state_service=_FakeSharedStateService(),
        ),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
    )

    result = control_app.refresh_flows(
        paths=paths,
        runtime_session=resolve_runtime_session_with_lease().__class__.empty(),
        has_authored_workspace=True,
    )

    assert result.reload_catalog is True
    assert result.sync_after is True
    assert result.status_text == "Reloaded flow definitions."
    assert result.warning_text == "unreachable"


def test_detail_application_normalizes_selected_run_key() -> None:
    card = FlowCatalogEntry(
        name="example_manual",
        group="Examples",
        title="Example Manual",
        description="desc",
        source_root="/tmp/source",
        target_root="/tmp/target",
        mode="manual",
        interval="-",
        operations="Extract -> Write",
        operation_items=("Extract", "Write"),
        state="manual",
        valid=True,
        category="manual",
    )
    run_groups = (
        _run_group("run-1", "example_manual"),
        _run_group("run-2", "example_manual"),
    )

    presentation = DetailApplication().build_selected_flow_presentation(
        card=card,
        tracker=OperationSessionState.empty(),
        flow_states={},
        run_groups=run_groups,
        selected_run_key=("example_manual", "missing"),
    )

    assert presentation.detail_state is not None
    assert presentation.selected_run_key == run_groups[0].key
    assert presentation.selected_run_group == run_groups[0]


def test_detail_application_returns_empty_state_without_selection() -> None:
    presentation = DetailApplication().build_selected_flow_presentation(
        card=None,
        tracker=OperationSessionState.empty(),
        flow_states={},
        run_groups=(),
        selected_run_key=None,
    )

    assert presentation.detail_state is None
    assert presentation.empty_text == "Select one flow to see details."
    assert presentation.selected_run_group is None


def test_detail_application_limits_visible_run_groups() -> None:
    card = FlowCatalogEntry(
        name="example_manual",
        group="Examples",
        title="Example Manual",
        description="desc",
        source_root="/tmp/source",
        target_root="/tmp/target",
        mode="manual",
        interval="-",
        operations="Extract -> Write",
        operation_items=("Extract", "Write"),
        state="manual",
        valid=True,
        category="manual",
    )
    run_groups = tuple(_run_group(f"run-{index}", "example_manual") for index in range(5))

    presentation = DetailApplication().build_selected_flow_presentation(
        card=card,
        tracker=OperationSessionState.empty(),
        flow_states={},
        run_groups=run_groups,
        selected_run_key=None,
        max_visible_runs=2,
    )

    assert tuple(group.key for group in presentation.visible_run_groups) == (
        run_groups[-2].key,
        run_groups[-1].key,
    )
    assert presentation.run_group_signature == (
        run_groups[-2].key,
        run_groups[-1].key,
    )


def test_action_state_application_builds_selected_flow_context() -> None:
    card = FlowCatalogEntry(
        name="poller",
        group="Examples",
        title="Poller",
        description="desc",
        source_root="/tmp/source",
        target_root="/tmp/target",
        mode="poll",
        interval="5m",
        operations="Read -> Write",
        operation_items=("Read", "Write"),
        state="poll ready",
        valid=True,
        category="automated",
    )

    context = ActionStateApplication().build_action_context(
        card=card,
        flow_states={"poller": "polling"},
        runtime_session=resolve_runtime_session_with_engine(("poller",)),
        flow_groups_by_name={"poller": "Examples"},
        active_flow_states={"running", "polling", "scheduled", "stopping flow", "stopping runtime"},
        has_logs=True,
        has_automated_flows=True,
        selected_run_group_present=True,
    )

    assert context.selected_flow.present is True
    assert context.selected_flow.running is True
    assert context.selected_flow.group_active is True
    assert context.selected_flow.has_logs is True
    assert context.has_automated_flows is True
    assert context.selected_run_group_present is True


def test_runtime_application_builds_runtime_snapshot_from_logs() -> None:
    card = FlowCatalogEntry(
        name="poller",
        group="Imports",
        title="Poller",
        description="desc",
        source_root="/tmp/source",
        target_root="/tmp/target",
        mode="poll",
        interval="30s",
        operations="Read -> Write",
        operation_items=("Read", "Write"),
        state="poll ready",
        valid=True,
        category="automated",
    )
    runtime_app = RuntimeApplication(
        daemon_service=_FakeDaemonService(),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
    )
    entries = (
        FlowLogEntry(
            line="run started",
            kind="flow",
            flow_name="poller",
            event=RuntimeStepEvent(
                run_id="run-1",
                flow_name="poller",
                step_name="Read",
                source_label="source.xlsx",
                status="started",
                elapsed_seconds=None,
            ),
        ),
    )

    snapshot = runtime_app.build_runtime_snapshot(
        flow_cards=(card,),
        log_entries=entries,
        runtime_session=resolve_runtime_session_with_engine(("poller",)),
        now=10.0,
    )

    assert snapshot.flow_states["poller"] == "stopping runtime"
    assert snapshot.operation_tracker.state_for("poller") is not None
    assert snapshot.signature_for(resolve_runtime_session_with_engine(("poller",)))


def test_runtime_application_plans_flow_state_refresh_diffs_and_signature() -> None:
    runtime_app = RuntimeApplication(
        daemon_service=_FakeDaemonService(),
        daemon_state_service=_FakeDaemonStateService(
            WorkspaceDaemonSnapshot(
                live=False,
                workspace_owned=True,
                leased_by_machine_id=None,
                runtime_active=False,
                runtime_stopping=False,
                manual_runs=(),
                last_checkpoint_at_utc=None,
                source="none",
            ),
            control_state=object(),
        ),
        shared_state_service=_FakeSharedStateService(),
    )
    runtime_session = resolve_runtime_session_with_engine(("poller",))

    plan = runtime_app.plan_flow_state_refresh(
        previous_states={"poller": "poll ready", "manual_review": "manual"},
        next_states={"poller": "stopping runtime", "manual_review": "manual"},
        runtime_session=runtime_session,
    )

    assert plan.flow_states["poller"] == "stopping runtime"
    assert plan.changed_flow_names == frozenset({"poller"})
    assert plan.states_changed is True
    assert plan.signature[1] == ("poller",)


def resolve_runtime_session_with_manual_run(group_name: str, flow_name: str):
    from data_engine.domain import RuntimeSessionState

    return RuntimeSessionState.empty().with_manual_runs_map({group_name: flow_name})


def resolve_runtime_session_with_engine(flow_names: tuple[str, ...]):
    from data_engine.domain import RuntimeSessionState

    return RuntimeSessionState.empty().with_runtime_flags(active=True, stopping=True).with_active_runtime_flow_names(flow_names)


def resolve_runtime_session_with_lease():
    from data_engine.domain import RuntimeSessionState

    return RuntimeSessionState.empty().__class__(
        workspace_owned=False,
        leased_by_machine_id="other-machine",
        runtime_active=False,
        runtime_stopping=False,
        active_runtime_flow_names=(),
        manual_runs=(),
    )


def _run_group(run_id: str, flow_name: str):
    from data_engine.domain import FlowLogEntry, FlowRunState, RuntimeStepEvent

    entry = FlowLogEntry(
        line=f"{flow_name} success",
        kind="flow",
        flow_name=flow_name,
        event=RuntimeStepEvent(
            run_id=run_id,
            flow_name=flow_name,
            step_name=None,
            source_label="input.xlsx",
            status="success",
        ),
    )
    return FlowRunState.group_entries((entry,))[0]
