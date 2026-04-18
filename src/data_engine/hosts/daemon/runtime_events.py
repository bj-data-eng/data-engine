"""Internal daemon runtime event bus and live projection."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Condition, RLock
from typing import Any, Protocol


@dataclass(frozen=True)
class DaemonRuntimeEvent:
    """One internal daemon runtime event."""

    workspace_id: str
    event_type: str
    timestamp_utc: str
    correlation_id: str | None
    payload: dict[str, Any]


class DaemonRuntimeEventSubscriber(Protocol):
    """Callable subscriber contract for daemon runtime events."""

    def __call__(self, event: DaemonRuntimeEvent) -> None: ...


@dataclass(frozen=True)
class DaemonRuntimeProjectionSnapshot:
    """Authoritative daemon-owned live runtime projection."""

    workspace_id: str
    version: int
    status: str
    workspace_owned: bool
    leased_by_machine_id: str | None
    runtime_active: bool
    runtime_stopping: bool
    engine_starting: bool
    active_engine_flow_names: tuple[str, ...]
    active_runs: tuple[dict[str, Any], ...]
    flow_activity: tuple[dict[str, Any], ...]
    manual_runs: tuple[str, ...]
    last_checkpoint_at_utc: str | None


class DaemonRuntimeEventBus:
    """Simple in-process event bus for daemon runtime events."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._subscribers: dict[object, DaemonRuntimeEventSubscriber] = {}

    def subscribe(self, callback: DaemonRuntimeEventSubscriber) -> object:
        """Register one runtime-event subscriber."""
        token = object()
        with self._lock:
            self._subscribers[token] = callback
        return token

    def unsubscribe(self, token: object) -> None:
        """Remove one runtime-event subscriber."""
        with self._lock:
            self._subscribers.pop(token, None)

    def publish(self, event: DaemonRuntimeEvent) -> None:
        """Publish one runtime event to current subscribers."""
        with self._lock:
            subscribers = tuple(self._subscribers.values())
        for subscriber in subscribers:
            subscriber(event)


class DaemonRuntimeProjector:
    """Maintain the live daemon runtime projection from internal events."""

    def __init__(self, *, workspace_id: str, initial_state: dict[str, Any]) -> None:
        self._lock = RLock()
        self._condition = Condition(self._lock)
        self._snapshot = self._snapshot_from_state(workspace_id=workspace_id, state=initial_state, version=0)

    def handle(self, event: DaemonRuntimeEvent) -> None:
        """Apply one event to the live projection snapshot."""
        state = event.payload.get("state")
        if not isinstance(state, dict):
            return
        self.refresh(state)

    def refresh(self, state: dict[str, Any]) -> None:
        """Refresh the live projection from the current daemon-owned state."""
        with self._lock:
            previous = self._snapshot
            refreshed = self._snapshot_from_state(
                workspace_id=previous.workspace_id,
                state=state,
                version=previous.version,
            )
            if refreshed != previous:
                self._snapshot = DaemonRuntimeProjectionSnapshot(
                    workspace_id=refreshed.workspace_id,
                    version=previous.version + 1,
                    status=refreshed.status,
                    workspace_owned=refreshed.workspace_owned,
                    leased_by_machine_id=refreshed.leased_by_machine_id,
                    runtime_active=refreshed.runtime_active,
                    runtime_stopping=refreshed.runtime_stopping,
                    engine_starting=refreshed.engine_starting,
                    active_engine_flow_names=refreshed.active_engine_flow_names,
                    active_runs=refreshed.active_runs,
                    flow_activity=refreshed.flow_activity,
                    manual_runs=refreshed.manual_runs,
                    last_checkpoint_at_utc=refreshed.last_checkpoint_at_utc,
                )
                self._condition.notify_all()

    def snapshot(self) -> DaemonRuntimeProjectionSnapshot:
        """Return the current live projection snapshot."""
        with self._lock:
            return self._snapshot

    def wait_for_version_change(
        self,
        *,
        since_version: int,
        timeout_seconds: float,
    ) -> DaemonRuntimeProjectionSnapshot:
        """Wait until the projection version changes or the timeout expires."""
        with self._condition:
            if self._snapshot.version != since_version:
                return self._snapshot
            timeout = max(float(timeout_seconds), 0.0)
            self._condition.wait_for(lambda: self._snapshot.version != since_version, timeout=timeout)
            return self._snapshot

    @staticmethod
    def _snapshot_from_state(
        *,
        workspace_id: str,
        state: dict[str, Any],
        version: int,
    ) -> DaemonRuntimeProjectionSnapshot:
        return DaemonRuntimeProjectionSnapshot(
            workspace_id=workspace_id,
            version=version,
            status=str(state.get("status", "starting")),
            workspace_owned=bool(state.get("workspace_owned", False)),
            leased_by_machine_id=_coerce_optional_text(state.get("leased_by_machine_id")),
            runtime_active=bool(state.get("runtime_active", False)),
            runtime_stopping=bool(state.get("runtime_stopping", False)),
            engine_starting=bool(state.get("engine_starting", False)),
            active_engine_flow_names=tuple(
                str(name)
                for name in state.get("active_engine_flow_names", ())
                if isinstance(name, str) and name.strip()
            ),
            active_runs=tuple(
                item for item in state.get("active_runs", ()) if isinstance(item, dict)
            ),
            flow_activity=tuple(
                item for item in state.get("flow_activity", ()) if isinstance(item, dict)
            ),
            manual_runs=tuple(str(name) for name in state.get("manual_runs", ()) if str(name).strip()),
            last_checkpoint_at_utc=_coerce_optional_text(state.get("last_checkpoint_at_utc")),
        )


def _coerce_optional_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


__all__ = [
    "DaemonRuntimeEvent",
    "DaemonRuntimeEventBus",
    "DaemonRuntimeEventSubscriber",
    "DaemonRuntimeProjectionSnapshot",
    "DaemonRuntimeProjector",
]
