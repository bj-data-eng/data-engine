from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from data_engine.ui.gui.control_support import GuiControlMixin
from data_engine.ui.gui.controllers import jobs
from data_engine.ui.gui.controllers import flows
from data_engine.ui.gui.controllers.flows import _GuiFlowPresentationController, _GuiWorkspaceCatalogController
from data_engine.ui.gui.controllers.runtime import GuiRuntimeController
from data_engine.ui.gui.presenters import workspace_settings
from data_engine.ui.gui.presenters.workspace_settings import (
    _settings_target_paths,
    _settings_provision_target_paths,
    refresh_workspace_provisioning_controls,
    refresh_workspace_visibility_panel,
)
from data_engine.ui.gui.support import GuiWindowSupportMixin
from data_engine.services.operator_commands import OperatorCommandService


class _Window(GuiWindowSupportMixin, GuiControlMixin):
    pass


@pytest.fixture
def window(monkeypatch, tmp_path):
    window = _Window()
    window.ui_closing = False
    window._ui_timing_log_path = None
    window._workspace_binding_generation = 1
    window.workspace_paths = SimpleNamespace(workspace_id="A", workspace_root=tmp_path / "A")
    window.runtime_binding = _binding(window.workspace_paths)
    window.closed = []
    window.opened = []

    def open_binding(paths):
        binding = _binding(paths)
        window.opened.append(binding)
        return binding

    window.runtime_binding_service = SimpleNamespace(close_binding=window.closed.append, open_binding=open_binding)
    window._pending_control_actions = set()
    window._pending_control_action_tokens = {}
    window.selected_flow_name = "shared_flow"
    window.clear_flow_log_button = _Button()
    window.provision_workspace_button = _Button()
    window.force_shutdown_daemon_button = _Button()
    window.reset_workspace_button = _Button()
    for name in ("workspace_target_label", "workspace_provision_status_label", "force_shutdown_daemon_status_label", "reset_workspace_status_label", "workspace_counts_footer_label"):
        setattr(window, name, _Label())
    window.errors = []
    window._show_message_box_later = lambda **kwargs: window.errors.append(kwargs)
    window.emitted = []
    window.sync_emitted = []
    window.signals = SimpleNamespace(
        control_action_finished=SimpleNamespace(emit=lambda *args: window.emitted.append(args)),
        daemon_sync_finished=SimpleNamespace(emit=window.sync_emitted.append),
    )
    window.queued = []
    monkeypatch.setattr(jobs, "start_worker_thread", lambda window, *, target: window.queued.append(target))
    monkeypatch.setattr(workspace_settings, "refresh_workspace_visibility_panel", lambda window: None)
    monkeypatch.setattr(workspace_settings, "_settings_target_paths", lambda window: window.settings_paths)
    monkeypatch.setattr(workspace_settings, "_settings_provision_target_paths", lambda window: window.settings_paths)
    window.settings_paths = window.workspace_paths
    return window


class _Button:
    def __init__(self):
        self.enabled = True

    def isEnabled(self):
        return self.enabled

    def setEnabled(self, enabled):
        self.enabled = enabled


class _Label:
    def __init__(self):
        self.value = ""

    def setText(self, value):
        self.value = value

    def text(self):
        return self.value


def _binding(paths):
    return SimpleNamespace(
        workspace_paths=paths,
        runtime_cache_ledger=object(),
        runtime_control_ledger=object(),
        daemon_manager=object(),
    )


def _switch(window):
    old = window.runtime_binding
    window._workspace_binding_generation += 1
    window.workspace_paths = SimpleNamespace(workspace_id="B", workspace_root=old.workspace_paths.workspace_root.parent / "B")
    window.runtime_binding = _binding(window.workspace_paths)
    window.settings_paths = window.workspace_paths
    window._pending_control_actions.clear()
    window._pending_control_action_tokens.clear()
    jobs.retire_workspace_binding(window, old)


def _presentation(monkeypatch, command):
    controller = _GuiFlowPresentationController(
        catalog_query_service=None, history_query_service=None, command_service=command,
    )
    monkeypatch.setattr(controller, "refresh_action_buttons", lambda window: None)
    return controller


@pytest.mark.parametrize("switch_during_command", [False, True])
def test_reset_uses_dispatch_workspace_and_rejects_stale_completion(window, monkeypatch, switch_during_command):
    original_paths = window.workspace_paths
    original_binding = window.runtime_binding
    token = window._workspace_binding_token()
    calls = []

    def reset(**kwargs):
        calls.append(kwargs)
        if switch_during_command:
            _switch(window)
        assert window.closed == []
        return SimpleNamespace(error_text=None)

    controller = _presentation(monkeypatch, SimpleNamespace(reset_flow=reset))
    controller.clear_logs(window)
    if not switch_during_command:
        _switch(window)
    assert window.closed == []
    window.queued.pop()()

    assert calls == [{"paths": original_paths, "runtime_cache_ledger": original_binding.runtime_cache_ledger, "flow_name": "shared_flow"}]
    assert window.closed == [original_binding]
    action, payload = window.emitted.pop()
    assert payload["workspace_token"] == token
    window.flow_controller = SimpleNamespace(finish_control_action=lambda *args: pytest.fail("stale result reached current workspace"))
    window._pending_control_actions.add("reset_flow")
    window._finish_control_action(action, payload)
    assert window._pending_control_actions == {"reset_flow"}


def test_request_control_retains_dispatch_manager_and_token(window, monkeypatch):
    binding = window.runtime_binding
    managers = []
    controller = _presentation(monkeypatch, SimpleNamespace(request_control=lambda manager: managers.append(manager)))
    controller.request_control(window)
    _switch(window)
    window.queued.pop()()
    assert managers == [binding.daemon_manager]
    assert window.emitted[0][1]["workspace_token"] == (1, "A")
    assert window.closed == [binding]


def test_refresh_flows_retains_dispatch_paths_and_token(window, monkeypatch):
    original_paths = window.workspace_paths
    calls = []
    window.command_service = SimpleNamespace(refresh_flows=lambda **kwargs: calls.append(kwargs))
    window._has_authored_workspace = lambda: True
    presentation = _presentation(monkeypatch, None)
    monkeypatch.setattr(presentation, "_action_context", lambda window: "context-A")
    controller = _GuiWorkspaceCatalogController(workspace_service=None, catalog_query_service=None)
    controller.refresh_flows_requested(window, presentation)
    _switch(window)
    window.queued.pop()()
    assert calls == [{"paths": original_paths, "action_context": "context-A", "has_authored_workspace": True}]
    assert window.emitted[0][1]["workspace_token"] == (1, "A")


@pytest.mark.parametrize("action, worker, method", [
    ("start_runtime", "_start_runtime_worker", "start_engine"),
    ("stop_runtime", "_stop_runtime_worker", "stop_pipeline"),
    ("stop_pipeline", "_stop_pipeline_worker", "stop_pipeline"),
    ("run_selected_flow", "_run_selected_flow_worker", "run_selected_flow"),
])
def test_runtime_controls_capture_completion_identity(window, action, worker, method):
    paths = window.workspace_paths
    calls = []
    controller = GuiRuntimeController(runtime_application=None, daemon_service=None, runtime_state_service=None, command_service=SimpleNamespace(**{method: lambda **kwargs: calls.append(kwargs)}))
    window.flow_controller = SimpleNamespace(refresh_action_buttons=lambda window: None)
    args = ({"paths": paths}, "shared_flow") if action == "run_selected_flow" else ({"paths": paths},)
    controller._begin_control_action(window, action, target=getattr(controller, worker), args=args)
    _switch(window)
    window._pending_control_action_tokens[action] = window._workspace_binding_token()
    window.queued.pop()()
    assert calls == [{"paths": paths}]
    assert window.emitted[0][1]["workspace_token"] == (1, "A")


@pytest.mark.parametrize("action", ["provision_workspace", "force_shutdown_daemon", "reset_workspace"])
@pytest.mark.parametrize("switch_operator", [False, True])
def test_settings_actions_capture_target_before_queue(window, action, switch_operator):
    target_paths = window.settings_paths
    binding = window.runtime_binding
    calls = []

    def command(*args, **kwargs):
        calls.append((args, kwargs))
        assert window.closed == []
        return SimpleNamespace(workspace_id=target_paths.workspace_id, error_text=None)

    window.command_service = SimpleNamespace(**{action: command})
    dispatch = {"provision_workspace": workspace_settings.provision_selected_workspace,
                "force_shutdown_daemon": workspace_settings.force_shutdown_daemon,
                "reset_workspace": workspace_settings.reset_workspace}[action]
    dispatch(window)
    if switch_operator:
        _switch(window)
    else:
        window.settings_paths = SimpleNamespace(workspace_id="B", workspace_root=Path("B"))
    window.queued.pop()()
    args, kwargs = calls[0]
    if action == "reset_workspace":
        owned, = window.opened
        assert owned is not binding
        assert kwargs == {"paths": target_paths, "runtime_cache_ledger": owned.runtime_cache_ledger, "runtime_control_ledger": owned.runtime_control_ledger}
    else:
        assert args == (target_paths,)
    payload = window.emitted[0][1]
    assert payload["workspace_token"] == (1, "A")
    assert payload["payload"]["settings_target"] == (0, target_paths.workspace_root, "A")
    if not switch_operator:
        # A settings-only selection change must reject the old target's UI result too.
        workspace_settings.finish_control_action(window, action, payload["payload"])
        assert action not in window._pending_control_actions
    assert window.closed == window.opened + ([binding] if switch_operator else [])


def test_other_settings_binding_is_owned_and_closed_once(window):
    paths = SimpleNamespace(workspace_id="settings-target", workspace_root=Path("settings-target"))
    window.settings_paths = paths
    other = _binding(paths)
    window.runtime_binding_service.open_binding = lambda target: other
    seen = []
    window.command_service = SimpleNamespace(reset_workspace=lambda **kwargs: (seen.append(kwargs), SimpleNamespace(error_text=None))[1])
    workspace_settings.reset_workspace(window)
    _switch(window)
    window.queued.pop()()
    assert seen[0]["paths"] is paths
    assert seen[0]["runtime_cache_ledger"] is other.runtime_cache_ledger
    assert sum(binding is other for binding in window.closed) == 1


def test_settings_reselection_rejects_earlier_completion_for_same_target(window):
    window.command_service = SimpleNamespace(force_shutdown_daemon=lambda *args, **kwargs: SimpleNamespace(error_text=None))
    workspace_settings.force_shutdown_daemon(window)
    controller = _GuiWorkspaceCatalogController(workspace_service=None, catalog_query_service=None)
    window.workspace_settings_selector = SimpleNamespace(itemData=lambda index: ("B", "A")[index])
    window._workspace_counts_footer_cache = {}
    window._refresh_workspace_root_controls = lambda: None
    controller.settings_workspace_target_changed(window, 0)
    controller.settings_workspace_target_changed(window, 1)
    window.queued.pop()()
    payload = window.emitted[0][1]["payload"]
    assert payload["workspace_id"] == "A"
    workspace_settings.finish_control_action(window, "force_shutdown_daemon", payload)
    assert "force_shutdown_daemon" not in window._pending_control_actions


@pytest.mark.parametrize("raise_error", [False, True])
def test_retired_binding_waits_for_last_worker_even_on_failure(window, raise_error):
    binding = window.runtime_binding

    def target(window, job):
        assert job.binding is binding
        assert window.closed == []
        if raise_error:
            raise ValueError("worker failure")

    jobs.start_workspace_job(window, target=target)
    jobs.start_workspace_job(window, target=target)
    jobs.retire_workspace_binding(window, binding)
    if raise_error:
        with pytest.raises(ValueError):
            window.queued.pop()()
        with pytest.raises(ValueError):
            window.queued.pop()()
    else:
        window.queued.pop()()
        assert window.closed == []
        window.queued.pop()()
    assert window.closed == [binding]
    assert window._binding_jobs.active == {}


def test_thread_start_failure_releases_binding(window, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("cannot start thread")

    monkeypatch.setattr(jobs, "start_worker_thread", fail)
    with pytest.raises(RuntimeError):
        jobs.start_workspace_job(window, target=lambda window, job: None)
    jobs.retire_workspace_binding(window, window.runtime_binding)
    assert window.closed == [window.runtime_binding]


def test_stale_daemon_sync_does_not_clear_current_sync_flags(window):
    controller = GuiRuntimeController(runtime_application=None, daemon_service=None, runtime_state_service=None, command_service=None)
    old_token = window._workspace_binding_token()
    _switch(window)
    window._daemon_sync_in_progress = True
    window._daemon_sync_pending = True
    controller.finish_daemon_sync(window, {"workspace_token": old_token, "error": ValueError("old workspace")})
    assert window._daemon_sync_in_progress is True
    assert window._daemon_sync_pending is True


def test_daemon_sync_uses_captured_binding_catalog_and_startup_state(window):
    binding = window.runtime_binding
    card = object()
    window.flow_cards = {"shared_flow": card}
    window._daemon_sync_in_progress = False
    window._daemon_startup_in_progress = False
    window._has_authored_workspace = lambda: True
    window._monotonic = lambda: 0.0
    seen = []

    def sync(captured, **kwargs):
        seen.append((captured, kwargs))
        _switch(window)
        window.flow_cards = {"shared_flow": object()}
        window._daemon_startup_in_progress = True
        assert window.closed == []
        return SimpleNamespace(runtime_session=object(), workspace_control_state=object(), snapshot=None)

    def projection(captured, **kwargs):
        assert captured is binding
        assert kwargs["flow_cards"] == (card,)
        return object()

    def snapshot(**kwargs):
        assert kwargs["binding"] is binding
        assert kwargs["daemon_startup_in_progress"] is False
        return object()

    window.runtime_binding_service.sync_runtime_state = sync
    controller = GuiRuntimeController(runtime_application=None, daemon_service=None, runtime_state_service=SimpleNamespace(rebuild_projection=projection, snapshot_from_projection=snapshot), command_service=None)
    controller.sync_from_daemon(window)
    window.queued.pop()()
    assert seen[0][0] is binding
    assert seen[0][1]["flow_cards"] == (card,)
    assert window.sync_emitted[0]["workspace_token"] == (1, "A")
    assert "error" not in window.sync_emitted[0]
    assert window.closed == [binding]


@pytest.mark.parametrize("switch_during_spawn", [False, True])
def test_daemon_startup_uses_dispatch_paths_and_rejects_old_result(window, switch_during_spawn):
    paths = window.workspace_paths
    spawned = []
    checked = []

    def spawn(target):
        spawned.append(target)
        if switch_during_spawn:
            _switch(window)
        return SimpleNamespace(ok=True)

    controller = GuiRuntimeController(
        runtime_application=SimpleNamespace(spawn_daemon=spawn),
        daemon_service=SimpleNamespace(is_live=lambda target: checked.append(target) or True),
        runtime_state_service=None, command_service=None,
    )
    jobs.start_workspace_job(window, target=controller.start_daemon_worker)
    if not switch_during_spawn:
        _switch(window)
    window._daemon_startup_in_progress = True
    window.queued.pop()()
    assert spawned == checked == [paths]
    window._finish_daemon_startup = lambda *args: pytest.fail("old startup result updated current workspace")
    window._finish_control_action(*window.emitted[0])
    assert window._daemon_startup_in_progress is True


def test_orphaned_startup_cleanup_keeps_captured_paths_after_close_and_switch(window):
    paths = window.workspace_paths
    requests = []

    def spawn(target):
        assert target is paths
        _switch(window)
        window.ui_closing = True
        return SimpleNamespace(ok=True)

    window._daemon_request = lambda target, payload, **kwargs: requests.append((target, payload))
    window.runtime_binding_service.count_live_client_sessions = lambda binding: 0
    controller = GuiRuntimeController(
        runtime_application=SimpleNamespace(spawn_daemon=spawn),
        daemon_service=None, runtime_state_service=None, command_service=None,
    )
    jobs.start_workspace_job(window, target=controller.start_daemon_worker)
    window.queued.pop()()
    assert requests == [(paths, {"command": "shutdown_daemon"})]
    assert window.opened[0].workspace_paths is paths
    assert window.emitted == []


def test_retained_real_sqlite_resource_remains_usable_through_reset(window, monkeypatch):
    connection = sqlite3.connect(":memory:")
    binding = window.runtime_binding
    refreshed = []

    def refresh():
        refreshed.append(connection.execute("SELECT 1").fetchone())

    binding.runtime_cache_ledger = SimpleNamespace(refresh_external_state=refresh)

    def close(target):
        window.closed.append(target)
        if target is binding:
            connection.close()

    window.runtime_binding_service.close_binding = close
    command = OperatorCommandService(
        control_application=None,
        runtime_application=SimpleNamespace(reset_flow=lambda paths, *, name: SimpleNamespace(ok=True)),
        reset_service=None, workspace_provisioning_service=None,
    )
    controller = _presentation(monkeypatch, command)
    controller.clear_logs(window)
    _switch(window)
    window.queued.pop()()
    assert refreshed == [(1,)]
    assert window.emitted[0][1]["payload"]["error_text"] is None
    with pytest.raises(sqlite3.ProgrammingError):
        connection.execute("SELECT 1")


def test_old_subscription_update_cannot_replace_new_workspace_batch(window):
    token = window._workspace_binding_token()
    _switch(window)
    window._pending_daemon_update_batch = object()
    current = window._pending_daemon_update_batch
    window._schedule_daemon_update_batch(object(), token=token)
    assert window._pending_daemon_update_batch is current


def _missing_settings_target(window, monkeypatch):
    requested = []
    window.workspace_paths.workspace_configured = True
    window.workspace_paths.app_root = Path("app")
    window.workspace_session_state = SimpleNamespace(discovered_workspace_ids=("A",))
    window.settings_workspace_target_id = "deleted"
    window.workspace_collection_root_override = Path("collection")

    def resolve(**kwargs):
        requested.append(kwargs["workspace_id"])
        raise FileNotFoundError("Workspace 'deleted' was not found.")

    window.services = SimpleNamespace(workspace_service=SimpleNamespace(resolve_paths=resolve, discover=lambda **kwargs: ()))
    monkeypatch.setattr(workspace_settings, "_settings_target_paths", _settings_target_paths)
    monkeypatch.setattr(workspace_settings, "_settings_provision_target_paths", _settings_provision_target_paths)
    return requested


@pytest.mark.parametrize("dispatch", [workspace_settings.provision_selected_workspace, workspace_settings.force_shutdown_daemon, workspace_settings.reset_workspace])
@pytest.mark.parametrize("empty_unconfigured_collection", [False, True])
def test_missing_explicit_settings_target_fails_before_dispatch(window, monkeypatch, dispatch, empty_unconfigured_collection):
    requested = _missing_settings_target(window, monkeypatch)
    if empty_unconfigured_collection:
        window.workspace_paths.workspace_configured = False
        window.workspace_session_state.discovered_workspace_ids = ()
    dispatch(window)
    assert requested == ([] if dispatch is workspace_settings.provision_selected_workspace else ["deleted"])
    assert window.settings_workspace_target_id == "deleted"
    assert window.queued == window.opened == []
    assert window._pending_control_actions == set()
    assert window._pending_control_action_tokens == {}
    assert not window.reset_workspace_button.isEnabled()
    assert not window.force_shutdown_daemon_button.isEnabled()
    assert not window.provision_workspace_button.isEnabled()
    assert "deleted" in window.errors[0]["text"]


@pytest.mark.parametrize("refresh", [refresh_workspace_provisioning_controls, refresh_workspace_visibility_panel])
def test_missing_settings_target_disables_controls_without_raising(window, monkeypatch, refresh):
    requested = _missing_settings_target(window, monkeypatch)
    refresh(window)
    assert requested == []
    assert window.settings_workspace_target_id == "deleted"
    assert "unavailable" in window.workspace_target_label.text()
    assert window.workspace_counts_footer_label.text() == "Workspace unavailable."
    assert not window.reset_workspace_button.isEnabled()
    assert not window.force_shutdown_daemon_button.isEnabled()
    assert not window.provision_workspace_button.isEnabled()
    assert window.errors == []


def test_missing_target_after_dispatch_does_not_apply_completion(window, monkeypatch):
    window.command_service = SimpleNamespace(force_shutdown_daemon=lambda *args, **kwargs: SimpleNamespace(error_text=None))
    workspace_settings.force_shutdown_daemon(window)
    window.queued.pop()()
    _missing_settings_target(window, monkeypatch)
    workspace_settings.finish_control_action(window, "force_shutdown_daemon", window.emitted[0][1]["payload"])
    assert window._pending_control_actions == set()
    assert window.force_shutdown_daemon_status_label.text() == ""


def test_gui_provisioning_passes_resolved_alias_not_directory_name(window, monkeypatch, tmp_path):
    target = SimpleNamespace(workspace_id="analytics", workspace_root=tmp_path / "docs")
    requested = []
    window.workspace_paths.workspace_configured = True
    window.workspace_paths.app_root = Path("app")
    window.workspace_session_state = SimpleNamespace(discovered_workspace_ids=("analytics",))
    window.settings_workspace_target_id = "analytics"
    window.workspace_collection_root_override = Path("collection")

    def resolve(**kwargs):
        if kwargs.get("workspace_root") != target.workspace_root:
            raise FileNotFoundError("New provisioning targets require their literal discovered root.")
        requested.append(kwargs["workspace_id"])
        return target

    window.services = SimpleNamespace(workspace_service=SimpleNamespace(resolve_paths=resolve, discover=lambda **kwargs: (target,)))
    monkeypatch.setattr(workspace_settings, "_settings_target_paths", _settings_target_paths)
    monkeypatch.setattr(workspace_settings, "_settings_provision_target_paths", _settings_provision_target_paths)
    seen = []
    window.command_service = SimpleNamespace(provision_workspace=lambda paths, **kwargs: (seen.append(paths), SimpleNamespace(error_text=None))[1])
    workspace_settings.provision_selected_workspace(window)
    window.settings_workspace_target_id = "other"
    window.queued.pop()()
    assert requested == ["analytics"]
    assert seen == [target]
    assert seen[0].workspace_id != seen[0].workspace_root.name
    assert window.emitted[0][1]["payload"]["workspace_id"] == "analytics"
    assert not target.workspace_root.exists()


@pytest.mark.parametrize("workspace_ids", [(), ("A",)])
def test_catalog_refresh_preserves_missing_pinned_settings_selection(window, monkeypatch, workspace_ids):
    class Selector:
        def __init__(self):
            self.items = []
            self.index = -1

        def blockSignals(self, block):
            pass

        def clear(self):
            self.items.clear()

        def addItem(self, text, data):
            self.items.append((text, data))

        def findData(self, data):
            return next((index for index, item in enumerate(self.items) if item[1] == data), -1)

        def count(self):
            return len(self.items)

        def setCurrentIndex(self, index):
            self.index = index

        def setEnabled(self, enabled):
            pass

    window.workspace_paths.app_root = Path("app")
    window.workspace_collection_root_override = Path("collection")
    window.settings_workspace_target_id = "deleted"
    window._settings_workspace_target_pinned = True
    window.workspace_settings_selector = Selector()
    state = SimpleNamespace(current_workspace_id="A", discovered_workspace_ids=workspace_ids)
    monkeypatch.setattr(flows, "WorkspaceSessionState", SimpleNamespace(from_paths=lambda *args, **kwargs: state))
    service = SimpleNamespace(discover=lambda **kwargs: tuple(SimpleNamespace(workspace_id=workspace_id) for workspace_id in workspace_ids))
    controller = _GuiWorkspaceCatalogController(workspace_service=service, catalog_query_service=None)
    controller.reload_workspace_options(window)
    selector = window.workspace_settings_selector
    assert window.settings_workspace_target_id == "deleted"
    assert window._settings_workspace_target_pinned is True
    assert selector.items[selector.index] == ("deleted (unavailable)", "deleted")
