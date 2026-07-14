from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import threading

from data_engine.hosts.daemon.state_sync import DaemonStateSyncHandler
from data_engine.platform.processes import ProcessIdentity


class _State:
    def __init__(self) -> None:
        self.lease_token = "b" * 32
        self.last_checkpoint_at_utc = "2026-07-14T00:00:00+00:00"
        self.status = "starting"

    def set_checkpoint_time(self, value: str) -> None:
        self.last_checkpoint_at_utc = value


class _SharedStateAdapter:
    def __init__(self) -> None:
        self.checkpoint_kwargs: dict[str, object] | None = None

    def checkpoint_workspace_state(self, paths, ledger, **kwargs) -> None:
        del paths, ledger
        self.checkpoint_kwargs = kwargs


class _DaemonStateRepository:
    def __init__(self) -> None:
        self.upsert_kwargs: dict[str, object] | None = None

    def upsert(self, **kwargs) -> None:
        self.upsert_kwargs = kwargs


def test_checkpoint_forwards_verified_process_identity_and_containment_nonce() -> None:
    process_identity = ProcessIdentity(
        pid=101,
        start_key="process-start",
        executable_path="/test/python",
        process_group_id=101,
        process_session_id=101,
    )
    containment_nonce = "c" * 64
    shared_state_adapter = _SharedStateAdapter()
    daemon_state = _DaemonStateRepository()
    events: list[str] = []
    service = SimpleNamespace(
        _checkpoint_operation_lock=threading.RLock(),
        _publish_runtime_event=events.append,
        _state_lock=threading.RLock(),
        containment_nonce=containment_nonce,
        daemon_id="daemon-a",
        host_name="host-a",
        machine_id="machine-a",
        paths=SimpleNamespace(
            app_root=Path("/app"),
            daemon_endpoint_kind="unix",
            daemon_endpoint_path="/runtime/daemon.sock",
            workspace_id="workspace",
            workspace_root=Path("/workspace"),
        ),
        pid=101,
        process_identity=process_identity,
        runtime_cache_ledger=object(),
        runtime_control_ledger=SimpleNamespace(daemon_state=daemon_state),
        shared_state_adapter=shared_state_adapter,
        started_at_utc="2026-07-14T00:00:00+00:00",
        state=_State(),
    )

    DaemonStateSyncHandler(service).checkpoint_once(status="idle")

    assert shared_state_adapter.checkpoint_kwargs is not None
    assert shared_state_adapter.checkpoint_kwargs["process_identity"] is process_identity
    assert shared_state_adapter.checkpoint_kwargs["containment_nonce"] == containment_nonce
    assert daemon_state.upsert_kwargs is not None
    assert daemon_state.upsert_kwargs["status"] == "idle"
    assert events == ["checkpoint.recorded", "daemon.state_updated"]
