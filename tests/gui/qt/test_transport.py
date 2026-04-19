from __future__ import annotations

from data_engine.services.runtime_state import ControlSnapshot, EngineSnapshot, WorkspaceSnapshot

from tests.gui.qt.support import _dispose_window, _make_window


class _AliveThread:
    def is_alive(self) -> bool:
        return True


def test_gui_heartbeat_skips_sync_when_subscription_is_healthy(qapp, monkeypatch):
    window = _make_window()
    sync_calls: list[str] = []
    try:
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=3,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )
        window.daemon_subscription.last_sync_monotonic = 100.0
        window.daemon_subscription.last_subscription_monotonic = 109.0
        window.daemon_subscription.thread = _AliveThread()
        monkeypatch.setattr(window.daemon_subscription, "clock", lambda: 110.0)
        monkeypatch.setattr(window, "_ensure_daemon_wait_worker", lambda: None)
        monkeypatch.setattr(window, "_sync_from_daemon", lambda: sync_calls.append("sync"))

        window._heartbeat_daemon_sync()

        assert sync_calls == []
        assert window.daemon_subscription.thread is not None
        assert window.daemon_subscription.thread.is_alive()
    finally:
        _dispose_window(qapp, window)


def test_gui_heartbeat_resyncs_when_subscription_is_stale(qapp, monkeypatch):
    window = _make_window()
    sync_calls: list[str] = []
    try:
        window.workspace_snapshot = WorkspaceSnapshot(
            workspace_id=window.workspace_paths.workspace_id,
            version=3,
            control=ControlSnapshot(state="available"),
            engine=EngineSnapshot(state="running", daemon_live=True, transport="subscription"),
            flows={},
            active_runs={},
        )
        window.daemon_subscription.last_sync_monotonic = 10.0
        window.daemon_subscription.last_subscription_monotonic = 10.0
        window.daemon_subscription.thread = _AliveThread()
        monkeypatch.setattr(window.daemon_subscription, "clock", lambda: 40.0)
        monkeypatch.setattr(window, "_ensure_daemon_wait_worker", lambda: None)
        monkeypatch.setattr(window, "_sync_from_daemon", lambda: sync_calls.append("sync"))

        window._heartbeat_daemon_sync()

        assert sync_calls == ["sync"]
        assert window.daemon_subscription.thread is not None
        assert window.daemon_subscription.thread.is_alive()
    finally:
        _dispose_window(qapp, window)
