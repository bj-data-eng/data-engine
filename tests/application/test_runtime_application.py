from __future__ import annotations

from pathlib import Path

from data_engine.application import RuntimeApplication
from data_engine.domain import (
    DaemonLifecyclePolicy,
    FlowCatalogEntry,
    FlowLogEntry,
    RuntimeSessionState,
    RuntimeStepEvent,
)
from data_engine.hosts.daemon.manager import WorkspaceDaemonSnapshot
from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


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

    def sync(self, manager):
        del manager
        return self.snapshot

    def control_state(self, manager, snapshot, *, daemon_startup_in_progress: bool = False):
        del manager, snapshot, daemon_startup_in_progress
        return self.control_state_value


class _FakeSharedStateService:
    def __init__(self) -> None:
        self.hydrate_calls: list[tuple[Path, object]] = []

    def hydrate_local_runtime(self, paths, runtime_ledger) -> None:
        self.hydrate_calls.append((paths.workspace_root, runtime_ledger))


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


def _runtime_app(
    *,
    daemon_service: _FakeDaemonService | None = None,
    snapshot: WorkspaceDaemonSnapshot | None = None,
    shared_state_service: _FakeSharedStateService | None = None,
    daemon_lifecycle_policy: DaemonLifecyclePolicy = DaemonLifecyclePolicy.EPHEMERAL,
) -> RuntimeApplication:
    return RuntimeApplication(
        daemon_service=daemon_service or _FakeDaemonService(),
        daemon_state_service=_FakeDaemonStateService(snapshot or _snapshot(), control_state=object()),
        shared_state_service=shared_state_service or _FakeSharedStateService(),
        daemon_lifecycle_policy=daemon_lifecycle_policy,
    )


def _entry(
    *,
    name: str,
    group: str = "Imports",
    title: str | None = None,
    mode: str = "poll",
    interval: str = "30s",
    operations: str = "Read -> Write",
    operation_items: tuple[str, ...] = ("Read", "Write"),
    state: str = "poll ready",
    category: str = "automated",
    source_root: str = "/tmp/source",
    target_root: str = "/tmp/target",
) -> FlowCatalogEntry:
    return FlowCatalogEntry(
        name=name,
        group=group,
        title=title or name.replace("_", " ").title(),
        description="desc",
        source_root=source_root,
        target_root=target_root,
        mode=mode,
        interval=interval,
        operations=operations,
        operation_items=operation_items,
        state=state,
        valid=True,
        category=category,
    )


def _engine_session(flow_names: tuple[str, ...]) -> RuntimeSessionState:
    return RuntimeSessionState.empty().with_runtime_flags(active=True, stopping=True).with_active_runtime_flow_names(flow_names)


def _manual_session(group_name: str, flow_name: str) -> RuntimeSessionState:
    return RuntimeSessionState.empty().with_manual_runs_map({group_name: flow_name})


def test_runtime_application_syncs_and_hydrates_lease_state(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    snapshot = _snapshot(
        workspace_owned=False,
        leased_by_machine_id="other-machine",
        runtime_active=True,
        manual_runs=("example_manual",),
        source="lease",
    )
    shared_state_service = _FakeSharedStateService()
    runtime_app = _runtime_app(snapshot=snapshot, shared_state_service=shared_state_service)

    sync_state = runtime_app.sync_state(
        paths=paths,
        daemon_manager=object(),
        flow_cards=(
            _entry(
                name="example_manual",
                group="Examples",
                title="Example Manual",
                mode="manual",
                interval="-",
                operations="Step",
                operation_items=("Step",),
                state="manual",
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

    result = _runtime_app(daemon_service=daemon_service).spawn_daemon(paths)

    assert result.ok is True
    assert daemon_service.spawn_calls == [(workspace_root, DaemonLifecyclePolicy.EPHEMERAL)]


def test_runtime_application_spawn_daemon_uses_configured_persistent_policy(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_service = _FakeDaemonService()

    result = _runtime_app(
        daemon_service=daemon_service,
        daemon_lifecycle_policy=DaemonLifecyclePolicy.PERSISTENT,
    ).spawn_daemon(paths)

    assert result.ok is True
    assert daemon_service.spawn_calls == [(workspace_root, DaemonLifecyclePolicy.PERSISTENT)]


def test_runtime_application_force_shutdown_daemon_uses_daemon_service(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_service = _FakeDaemonService()

    result = _runtime_app(
        daemon_service=daemon_service,
        snapshot=_snapshot(live=True, source="daemon"),
    ).force_shutdown_daemon(paths, timeout=0.75)

    assert result.ok is True
    assert daemon_service.force_shutdown_calls == [(workspace_root, 0.75)]


def test_runtime_application_normalizes_command_errors(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    (workspace_root / "flow_modules").mkdir(parents=True)
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_service = _FakeDaemonService(request_error=RuntimeError("boom"))

    result = _runtime_app(daemon_service=daemon_service).run_flow(paths, name="example_manual", wait=False, timeout=2.0)

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

    result = _runtime_app(daemon_service=daemon_service).start_engine(paths, timeout=2.0)

    assert daemon_service.spawn_calls == [(workspace_root, DaemonLifecyclePolicy.EPHEMERAL)]
    assert result.ok is False
    assert result.error == "Failed to start the automated engine. The daemon returned no additional detail."


def test_runtime_application_blocks_requests_when_workspace_root_is_missing(tmp_path: Path, monkeypatch) -> None:
    app_root = tmp_path / "app"
    workspace_root = tmp_path / "workspace"
    monkeypatch.setenv("DATA_ENGINE_APP_ROOT", str(app_root))
    paths = resolve_workspace_paths(workspace_root=workspace_root)
    daemon_service = _FakeDaemonService()
    runtime_app = _runtime_app(daemon_service=daemon_service)

    manual_result = runtime_app.run_flow(paths, name="example_manual", wait=False, timeout=2.0)
    engine_result = runtime_app.start_engine(paths, timeout=2.0)

    assert daemon_service.spawn_calls == []
    assert manual_result.ok is False
    assert manual_result.error == "Workspace root is no longer available."
    assert engine_result.ok is False
    assert engine_result.error == "Workspace root is no longer available."


def test_runtime_application_completes_manual_run_failure_without_manual_modal_for_automated_flow() -> None:
    completion = _runtime_app().complete_manual_run(
        runtime_session=_manual_session("Examples", "poller"),
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
    completion = _runtime_app().complete_manual_run(
        runtime_session=_manual_session("Examples", "example_completed"),
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
    completion = _runtime_app().complete_engine_run(
        runtime_session=_engine_session(("poller",)),
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


def test_runtime_application_builds_runtime_snapshot_from_logs() -> None:
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

    snapshot = _runtime_app().build_runtime_snapshot(
        flow_cards=(_entry(name="poller"),),
        log_entries=entries,
        runtime_session=_engine_session(("poller",)),
        now=10.0,
    )

    assert snapshot.flow_states["poller"] == "stopping runtime"
    assert snapshot.operation_tracker.state_for("poller") is not None
    assert snapshot.signature_for(_engine_session(("poller",)))


def test_runtime_application_stopping_snapshot_narrows_active_runtime_flows_to_running_work() -> None:
    entries = (
        FlowLogEntry(
            line="poller_a read started",
            kind="flow",
            flow_name="poller_a",
            event=RuntimeStepEvent(
                run_id="run-a",
                flow_name="poller_a",
                step_name="Read",
                source_label="a.xlsx",
                status="started",
                elapsed_seconds=None,
            ),
        ),
        FlowLogEntry(
            line="poller_b read success",
            kind="flow",
            flow_name="poller_b",
            event=RuntimeStepEvent(
                run_id="run-b",
                flow_name="poller_b",
                step_name="Read",
                source_label="b.xlsx",
                status="success",
                elapsed_seconds=0.2,
            ),
        ),
    )

    snapshot = _runtime_app().build_runtime_snapshot(
        flow_cards=(
            _entry(name="poller_a", title="Poller A", source_root="/tmp/source-a", target_root="/tmp/target-a"),
            _entry(
                name="poller_b",
                title="Poller B",
                mode="schedule",
                state="schedule ready",
                source_root="/tmp/source-b",
                target_root="/tmp/target-b",
            ),
        ),
        log_entries=entries,
        runtime_session=_engine_session(("poller_a", "poller_b")),
        now=10.0,
    )

    assert snapshot.active_runtime_flow_names == ("poller_a",)
    assert snapshot.flow_states["poller_a"] == "stopping runtime"
    assert snapshot.flow_states["poller_b"] == "schedule ready"


def test_runtime_application_plans_flow_state_refresh_diffs_and_signature() -> None:
    runtime_session = _engine_session(("poller",))

    plan = _runtime_app().plan_flow_state_refresh(
        previous_states={"poller": "poll ready", "manual_review": "manual"},
        next_states={"poller": "stopping runtime", "manual_review": "manual"},
        runtime_session=runtime_session,
    )

    assert plan.flow_states["poller"] == "stopping runtime"
    assert plan.changed_flow_names == frozenset({"poller"})
    assert plan.states_changed is True
    assert plan.signature[1] == ("poller",)
