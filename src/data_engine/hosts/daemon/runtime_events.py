"""Internal daemon runtime event bus and live projection."""

from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
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
        self._snapshot = DaemonRuntimeProjectionSnapshot(
            workspace_id=workspace_id,
            version=0,
            status=str(initial_state.get("status", "starting")),
            workspace_owned=bool(initial_state.get("workspace_owned", False)),
            leased_by_machine_id=_coerce_optional_text(initial_state.get("leased_by_machine_id")),
            runtime_active=bool(initial_state.get("runtime_active", False)),
            runtime_stopping=bool(initial_state.get("runtime_stopping", False)),
            engine_starting=bool(initial_state.get("engine_starting", False)),
            manual_runs=tuple(str(name) for name in initial_state.get("manual_runs", ()) if str(name).strip()),
            last_checkpoint_at_utc=_coerce_optional_text(initial_state.get("last_checkpoint_at_utc")),
        )

    def handle(self, event: DaemonRuntimeEvent) -> None:
        """Apply one event to the live projection snapshot."""
        state = event.payload.get("state")
        if not isinstance(state, dict):
            return
        with self._lock:
            previous = self._snapshot
            self._snapshot = DaemonRuntimeProjectionSnapshot(
                workspace_id=previous.workspace_id,
                version=previous.version + 1,
                status=str(state.get("status", previous.status)),
                workspace_owned=bool(state.get("workspace_owned", previous.workspace_owned)),
                leased_by_machine_id=_coerce_optional_text(state.get("leased_by_machine_id", previous.leased_by_machine_id)),
                runtime_active=bool(state.get("runtime_active", previous.runtime_active)),
                runtime_stopping=bool(state.get("runtime_stopping", previous.runtime_stopping)),
                engine_starting=bool(state.get("engine_starting", previous.engine_starting)),
                manual_runs=tuple(
                    str(name)
                    for name in state.get("manual_runs", previous.manual_runs)
                    if isinstance(name, str) and name.strip()
                ),
                last_checkpoint_at_utc=_coerce_optional_text(
                    state.get("last_checkpoint_at_utc", previous.last_checkpoint_at_utc)
                ),
            )

    def snapshot(self) -> DaemonRuntimeProjectionSnapshot:
        """Return the current live projection snapshot."""
        with self._lock:
            return self._snapshot


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
