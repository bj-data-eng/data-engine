from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from data_engine.application import OperatorControlApplication, RuntimeApplication
from data_engine.domain import DaemonLifecyclePolicy, OperatorActionContext, RuntimeSessionState, SelectedFlowState
from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


class _FakeDaemonService:
    def __init__(self, *, response: dict | None = None, request_error: Exception | None = None) -> None:
        self.response = response or {"ok": True}
        self.request_error = request_error
        self.spawn_calls: list[tuple[Path, DaemonLifecyclePolicy]] = []

    def spawn(self, paths, *, lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.PERSISTENT) -> object:
        self.spawn_calls.append((paths.workspace_root, lifecycle_policy))
        return {"ok": True}

    def request(self, paths, payload, *, timeout: float = 0.0):
        del paths, payload, timeout
        if self.request_error is not None:
            raise self.request_error
        return self.response

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
    def hydrate_local_runtime(self, paths, runtime_ledger) -> None:
        del paths, runtime_ledger


def _snapshot(
    *,
    live: bool = False,
    workspace_owned: bool = True,
    leased_by_machine_id: str | None = None,
    runtime_active: bool = False,
    runtime_stopping: bool = False,
    manual_runs: tuple[str, ...] = (),
    source: str = "none",
) -> WorkspaceDaemonSnapshot:
    return WorkspaceDaemonSnapshot(
        live=live,
        workspace_owned=workspace_owned,
        leased_by_machine_id=leased_by_machine_id,
        runtime_active=runtime_active,
        runtime_stopping=runtime_stopping,
        manual_runs=manual_runs,
        last_checkpoint_at_utc=None,
        source=source,
    )


def _runtime_app(*, daemon_service: _FakeDaemonService | None = None, snapshot: WorkspaceDaemonSnapshot | None = None):
    return RuntimeApplication(
        daemon_service=daemon_service or _FakeDaemonService(),
        daemon_state_service=_FakeDaemonStateService(snapshot or _snapshot(), control_state=object()),
        shared_state_service=_FakeSharedStateService(),
    )


def _lease_session() -> RuntimeSessionState:
    return RuntimeSessionState(
        workspace_owned=False,
        leased_by_machine_id="other-machine",
        runtime_active=False,
        runtime_stopping=False,
        active_runtime_flow_names=(),
        manual_runs=(),
    )


def _action_context(
    *,
    runtime_session: RuntimeSessionState,
    flow_name: str | None = None,
    group_name: str | None = None,
    flow_mode: str | None = None,
    group_active: bool = False,
    live_truth_known: bool = False,
    live_manual_running: bool = False,
    live_manual_run_active: bool = False,
    engine_state: str = "idle",
    has_automated_flows: bool = True,
    workspace_available: bool = True,
    local_request_pending: bool = False,
) -> OperatorActionContext:
    card = None
    if flow_name is not None:
        card = SimpleNamespace(name=flow_name, group=group_name, mode=flow_mode, valid=True)
    return OperatorActionContext(
        runtime_session=runtime_session,
        selected_flow=SelectedFlowState(
            card=card,
            live_truth_known=live_truth_known,
            live_manual_running=live_manual_running,
            group_active=group_active,
        ),
        has_automated_flows=has_automated_flows,
        engine_state=engine_state,
        engine_truth_known=live_truth_known,
        live_truth_known=live_truth_known,
        live_manual_run_active=live_manual_run_active,
        workspace_available=workspace_available,
        local_request_pending=local_request_pending,
    )


def test_operator_control_application_blocks_run_when_workspace_is_leased(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_state_service = _FakeDaemonStateService(
        _snapshot(workspace_owned=False, leased_by_machine_id="other-machine", source="lease"),
        control_state=object(),
    )
    control_app = OperatorControlApplication(
        runtime_application=_runtime_app(snapshot=daemon_state_service.snapshot),
        daemon_state_service=daemon_state_service,
    )

    result = control_app.run_selected_flow(
        paths=paths,
        action_context=_action_context(
            runtime_session=_lease_session(),
            flow_name="poller",
            group_name="Examples",
        ),
        selected_flow_name="poller",
        selected_flow_valid=True,
        blocked_status_text="other-machine currently has control of this workspace.",
    )

    assert result.requested is False
    assert result.status_text == "other-machine currently has control of this workspace."


def test_operator_control_application_requests_control_and_marks_follow_up() -> None:
    daemon_state_service = _FakeDaemonStateService(
        _snapshot(workspace_owned=False, leased_by_machine_id="other-machine", source="lease"),
        control_state=object(),
    )
    control_app = OperatorControlApplication(
        runtime_application=_runtime_app(snapshot=daemon_state_service.snapshot),
        daemon_state_service=daemon_state_service,
    )

    result = control_app.request_control(object())

    assert result.requested is True
    assert result.sync_after is True
    assert result.ensure_daemon_started is True
    assert result.status_text == "Control request sent."


def test_operator_control_application_request_control_uses_verbose_fallback_when_exception_has_no_detail() -> None:
    daemon_state_service = _FakeDaemonStateService(
        _snapshot(workspace_owned=False, leased_by_machine_id="other-machine", source="lease"),
        control_state=object(),
    )
    daemon_state_service.request_control_error = RuntimeError()
    control_app = OperatorControlApplication(
        runtime_application=_runtime_app(snapshot=daemon_state_service.snapshot),
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
        daemon_state_service=_FakeDaemonStateService(_snapshot(), control_state=object()),
    )

    result = control_app.start_engine(
        paths=paths,
        action_context=_action_context(runtime_session=RuntimeSessionState.empty()),
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
        daemon_state_service=_FakeDaemonStateService(_snapshot(), control_state=object()),
    )

    result = control_app.run_selected_flow(
        paths=paths,
        action_context=_action_context(
            runtime_session=RuntimeSessionState.empty(),
            flow_name="poller",
            group_name="Examples",
        ),
        selected_flow_name="poller",
        selected_flow_valid=True,
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
            _snapshot(live=True, runtime_active=True, source="runtime"),
            control_state=object(),
        ),
    )

    result = control_app.run_selected_flow(
        paths=paths,
        action_context=_action_context(
            runtime_session=RuntimeSessionState(runtime_active=True, active_runtime_flow_names=("poller",)),
            flow_name="manual_docs",
            group_name="Manual",
            engine_state="running",
            live_truth_known=True,
        ),
        selected_flow_name="manual_docs",
        selected_flow_valid=True,
        blocked_status_text="blocked",
    )

    assert result.requested is True
    assert result.sync_after is True


def test_operator_control_application_blocks_automated_run_while_engine_is_starting(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    run_calls: list[str] = []

    class _RuntimeApplication:
        def run_flow(self, paths, *, name: str, wait: bool = False, timeout: float = 2.0):
            del paths, wait, timeout
            run_calls.append(name)
            return type("Result", (), {"ok": True, "error": None, "name": name})()

    control_app = OperatorControlApplication(
        runtime_application=_RuntimeApplication(),
        daemon_state_service=_FakeDaemonStateService(
            _snapshot(live=True, runtime_active=False, source="runtime"),
            control_state=object(),
        ),
    )

    result = control_app.run_selected_flow(
        paths=paths,
        action_context=_action_context(
            runtime_session=RuntimeSessionState.empty(),
            flow_name="poller",
            group_name="Docs",
            flow_mode="poll",
            engine_state="starting",
            live_truth_known=True,
        ),
        selected_flow_name="poller",
        selected_flow_valid=True,
        blocked_status_text="blocked",
    )

    assert result.requested is False
    assert run_calls == []


def test_operator_control_application_allows_manual_run_while_other_group_manual_is_active(tmp_path: Path, monkeypatch) -> None:
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
            _snapshot(live=True, manual_runs=("docs2_parallel_poll",), source="runtime"),
            control_state=object(),
        ),
    )

    result = control_app.run_selected_flow(
        paths=paths,
        action_context=_action_context(
            runtime_session=RuntimeSessionState.empty().with_manual_runs_map({"Imports": "docs2_parallel_poll"}),
            flow_name="example_manual",
            group_name="Manual",
            live_truth_known=True,
            live_manual_run_active=True,
        ),
        selected_flow_name="example_manual",
        selected_flow_valid=True,
        blocked_status_text="blocked",
    )

    assert result.requested is True
    assert result.sync_after is True


def test_operator_control_application_blocks_manual_run_when_selected_group_is_already_active(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    run_calls: list[str] = []

    class _RuntimeApplication:
        def run_flow(self, paths, *, name: str, wait: bool = False, timeout: float = 2.0):
            del paths, wait, timeout
            run_calls.append(name)
            return type("Result", (), {"ok": True, "error": None, "name": name})()

    control_app = OperatorControlApplication(
        runtime_application=_RuntimeApplication(),
        daemon_state_service=_FakeDaemonStateService(
            _snapshot(live=True, manual_runs=("example_manual",), source="runtime"),
            control_state=object(),
        ),
    )

    result = control_app.run_selected_flow(
        paths=paths,
        action_context=_action_context(
            runtime_session=RuntimeSessionState.empty().with_manual_runs_map({"Manual": "example_manual"}),
            flow_name="other_manual_flow",
            group_name="Manual",
            group_active=True,
            live_truth_known=True,
            live_manual_running=True,
            live_manual_run_active=True,
        ),
        selected_flow_name="other_manual_flow",
        selected_flow_valid=True,
        blocked_status_text="blocked",
    )

    assert result.requested is False
    assert run_calls == []


def test_operator_control_application_stop_pipeline_targets_only_active_manual_flow_when_selection_differs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    stop_calls: list[tuple[str, float]] = []

    class _RuntimeApplication:
        def stop_flow(self, paths, *, name: str, timeout: float = 2.0):
            del paths
            stop_calls.append((name, timeout))
            return type("Result", (), {"ok": True, "error": None})()

    control_app = OperatorControlApplication(
        runtime_application=_RuntimeApplication(),
        daemon_state_service=_FakeDaemonStateService(
            _snapshot(live=True, manual_runs=("example_manual",), source="runtime"),
            control_state=object(),
        ),
    )

    result = control_app.stop_pipeline(
        paths=paths,
        action_context=_action_context(
            runtime_session=RuntimeSessionState.empty().with_manual_runs_map({"Manual": "example_manual"}),
            flow_name="different_manual",
            group_name="Different Group",
        ),
        selected_flow_name="different_manual",
        blocked_status_text="blocked",
    )

    assert result.requested is True
    assert result.sync_after is True
    assert result.status_text == "Stopping selected flow..."
    assert stop_calls == [("example_manual", 2.0)]


def test_operator_control_application_stop_pipeline_prefers_selected_manual_flow_over_engine_stop_when_engine_runs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    stop_flow_calls: list[tuple[str, float]] = []
    stop_engine_calls: list[float] = []

    class _RuntimeApplication:
        def stop_flow(self, paths, *, name: str, timeout: float = 2.0):
            del paths
            stop_flow_calls.append((name, timeout))
            return type("Result", (), {"ok": True, "error": None})()

        def stop_engine(self, paths, *, timeout: float = 2.0):
            del paths
            stop_engine_calls.append(timeout)
            return type("Result", (), {"ok": True, "error": None})()

    control_app = OperatorControlApplication(
        runtime_application=_RuntimeApplication(),
        daemon_state_service=_FakeDaemonStateService(
            _snapshot(live=True, runtime_active=True, manual_runs=("manual_review",), source="runtime"),
            control_state=object(),
        ),
    )

    result = control_app.stop_pipeline(
        paths=paths,
        action_context=_action_context(
            runtime_session=RuntimeSessionState(runtime_active=True, active_runtime_flow_names=("poller",)).with_manual_runs_map({"Manual": "manual_review"}),
            flow_name="manual_review",
            group_name="Manual",
            group_active=True,
            live_truth_known=True,
            live_manual_running=True,
            live_manual_run_active=True,
            engine_state="running",
        ),
        selected_flow_name="manual_review",
        blocked_status_text="blocked",
    )

    assert result.requested is True
    assert result.status_text == "Stopping selected flow..."
    assert stop_flow_calls == [("manual_review", 2.0)]
    assert stop_engine_calls == []


def test_operator_control_application_blocks_refresh_while_active(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    control_app = OperatorControlApplication(
        runtime_application=_runtime_app(),
        daemon_state_service=_FakeDaemonStateService(_snapshot(), control_state=object()),
    )

    result = control_app.refresh_flows(
        paths=paths,
        action_context=_action_context(
            runtime_session=RuntimeSessionState.empty().with_runtime_flags(active=True, stopping=True).with_active_runtime_flow_names(("poller",)),
            engine_state="stopping",
        ),
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
        runtime_application=_runtime_app(),
        daemon_state_service=_FakeDaemonStateService(_snapshot(), control_state=object()),
    )

    result = control_app.refresh_flows(
        paths=paths,
        action_context=_action_context(runtime_session=RuntimeSessionState.empty()),
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
        runtime_application=_runtime_app(daemon_service=_FakeDaemonService(request_error=RuntimeError("unreachable"))),
        daemon_state_service=_FakeDaemonStateService(_snapshot(), control_state=object()),
    )

    result = control_app.refresh_flows(
        paths=paths,
        action_context=_action_context(runtime_session=RuntimeSessionState.empty()),
        has_authored_workspace=True,
    )

    assert result.reload_catalog is True
    assert result.sync_after is True
    assert result.status_text == "Reloaded flow definitions."
    assert result.warning_text == "unreachable"

