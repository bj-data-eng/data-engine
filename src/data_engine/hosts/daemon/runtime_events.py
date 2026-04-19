"""Internal daemon runtime event bus and live projection."""

from __future__ import annotations

from collections import deque
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
    event_sequence: int


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
        self._event_sequence = 0
        self._event_history: deque[tuple[int, dict[str, Any]]] = deque(maxlen=512)
        self._snapshot = self._snapshot_from_state(
            workspace_id=workspace_id,
            state=initial_state,
            version=0,
            event_sequence=0,
        )

    def handle(self, event: DaemonRuntimeEvent) -> None:
        """Apply one event to the live projection snapshot."""
        state = event.payload.get("state")
        if not isinstance(state, dict):
            return
        with self._condition:
            previous = self._snapshot
            self._event_sequence += 1
            self._event_history.append(
                (
                    self._event_sequence,
                    {
                        "sequence": self._event_sequence,
                        "workspace_id": event.workspace_id,
                        "event_type": event.event_type,
                        "timestamp_utc": event.timestamp_utc,
                        "correlation_id": event.correlation_id,
                        "payload": dict(event.payload),
                    },
                )
            )
            refreshed = self._snapshot_from_state(
                workspace_id=previous.workspace_id,
                state=state,
                version=previous.version,
                event_sequence=self._event_sequence,
            )
            if self._same_runtime_state(previous, refreshed):
                self._snapshot = refreshed
            else:
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
                    event_sequence=refreshed.event_sequence,
                )
            self._condition.notify_all()

    def refresh(self, state: dict[str, Any]) -> None:
        """Refresh the live projection from the current daemon-owned state."""
        with self._lock:
            previous = self._snapshot
            refreshed = self._snapshot_from_state(
                workspace_id=previous.workspace_id,
                state=state,
                version=previous.version,
                event_sequence=previous.event_sequence,
            )
            if not self._same_runtime_state(previous, refreshed):
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
                    event_sequence=refreshed.event_sequence,
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

    def events_since(self, since_event_sequence: int) -> tuple[tuple[dict[str, Any], ...], bool]:
        """Return buffered daemon events after one event sequence and whether history overflowed."""
        with self._lock:
            return self._events_since_locked(since_event_sequence)

    def wait_for_change(
        self,
        *,
        since_version: int,
        since_event_sequence: int,
        timeout_seconds: float,
    ) -> tuple[DaemonRuntimeProjectionSnapshot, tuple[dict[str, Any], ...], bool]:
        """Wait until projection or event sequence changes, then return snapshot plus new events."""
        with self._condition:
            if (
                self._snapshot.version == since_version
                and self._snapshot.event_sequence == since_event_sequence
            ):
                timeout = max(float(timeout_seconds), 0.0)
                self._condition.wait_for(
                    lambda: (
                        self._snapshot.version != since_version
                        or self._snapshot.event_sequence != since_event_sequence
                    ),
                    timeout=timeout,
                )
            events, truncated = self._events_since_locked(since_event_sequence)
            return self._snapshot, events, truncated

    def _events_since_locked(self, since_event_sequence: int) -> tuple[tuple[dict[str, Any], ...], bool]:
        history = tuple(self._event_history)
        if not history:
            return (), False
        oldest_sequence = history[0][0]
        truncated = since_event_sequence > 0 and since_event_sequence < oldest_sequence - 1
        return (
            tuple(payload for sequence, payload in history if sequence > since_event_sequence),
            truncated,
        )

    @staticmethod
    def _same_runtime_state(
        previous: DaemonRuntimeProjectionSnapshot,
        refreshed: DaemonRuntimeProjectionSnapshot,
    ) -> bool:
        return (
            previous.workspace_id == refreshed.workspace_id
            and previous.status == refreshed.status
            and previous.workspace_owned == refreshed.workspace_owned
            and previous.leased_by_machine_id == refreshed.leased_by_machine_id
            and previous.runtime_active == refreshed.runtime_active
            and previous.runtime_stopping == refreshed.runtime_stopping
            and previous.engine_starting == refreshed.engine_starting
            and previous.active_engine_flow_names == refreshed.active_engine_flow_names
            and previous.active_runs == refreshed.active_runs
            and previous.flow_activity == refreshed.flow_activity
            and previous.manual_runs == refreshed.manual_runs
            and previous.last_checkpoint_at_utc == refreshed.last_checkpoint_at_utc
        )

    @staticmethod
    def _snapshot_from_state(
        *,
        workspace_id: str,
        state: dict[str, Any],
        version: int,
        event_sequence: int,
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
            event_sequence=event_sequence,
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
