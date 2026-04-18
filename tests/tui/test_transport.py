from __future__ import annotations

from data_engine.services.runtime_state import ControlSnapshot, EngineSnapshot, WorkspaceSnapshot

from tests.tui.support import make_tui


class _AliveThread:
    def is_alive(self) -> bool:
        return True


def test_tui_heartbeat_skips_sync_when_subscription_is_healthy(monkeypatch):
    app = make_tui()
    sync_calls: list[str] = []
    app.workspace_snapshot = WorkspaceSnapshot(
        workspace_id=app.workspace_paths.workspace_id,
        version=3,
        control=ControlSnapshot(state="available"),
        engine=EngineSnapshot(state="running", daemon_live=True, transport="subscription"),
        flows={},
        active_runs={},
    )
    app.daemon_subscription.last_sync_monotonic = 100.0
    app.daemon_subscription.last_subscription_monotonic = 109.0
    app.daemon_subscription.thread = _AliveThread()
    monkeypatch.setattr(app.daemon_subscription, "clock", lambda: 110.0)
    monkeypatch.setattr(app, "_ensure_daemon_wait_worker", lambda: None)
    monkeypatch.setattr(app, "_sync_daemon_state", lambda: sync_calls.append("sync"))

    app._heartbeat_daemon_state()

    assert sync_calls == []
    assert app.daemon_subscription.thread is not None


def test_tui_heartbeat_resyncs_when_subscription_is_stale(monkeypatch):
    app = make_tui()
    sync_calls: list[str] = []
    app.workspace_snapshot = WorkspaceSnapshot(
        workspace_id=app.workspace_paths.workspace_id,
        version=3,
        control=ControlSnapshot(state="available"),
        engine=EngineSnapshot(state="running", daemon_live=True, transport="subscription"),
        flows={},
        active_runs={},
    )
    app.daemon_subscription.last_sync_monotonic = 10.0
    app.daemon_subscription.last_subscription_monotonic = 10.0
    app.daemon_subscription.thread = _AliveThread()
    monkeypatch.setattr(app.daemon_subscription, "clock", lambda: 40.0)
    monkeypatch.setattr(app, "_ensure_daemon_wait_worker", lambda: None)
    monkeypatch.setattr(app, "_sync_daemon_state", lambda: sync_calls.append("sync"))

    app._heartbeat_daemon_state()

    assert sync_calls == ["sync"]
    assert app.daemon_subscription.thread is not None
