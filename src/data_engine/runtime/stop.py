"""Run-id-aware runtime stop control."""

from __future__ import annotations

from threading import Lock

from data_engine.core.model import FlowStoppedError


class RuntimeStopController:
    """Track stop requests for specific active runtime run ids."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._requested_run_ids: set[str] = set()
        self._active_run_ids: set[str] = set()

    def request_stop(self, run_id: str) -> None:
        """Request that one active or future run id stop."""
        normalized = str(run_id).strip()
        if not normalized:
            raise ValueError("run_id must be non-empty.")
        with self._lock:
            self._requested_run_ids.add(normalized)

    def register_run(self, run_id: str) -> None:
        """Mark one run id as active."""
        with self._lock:
            self._active_run_ids.add(run_id)

    def unregister_run(self, run_id: str) -> None:
        """Clear active and requested state for one completed run id."""
        with self._lock:
            self._active_run_ids.discard(run_id)
            self._requested_run_ids.discard(run_id)

    def check_run(self, run_id: str | None) -> None:
        """Raise when stop has been requested for ``run_id``."""
        if run_id is None:
            return
        with self._lock:
            stop_requested = run_id in self._requested_run_ids
        if stop_requested:
            raise FlowStoppedError(f"Run stop requested by operator: {run_id}.")

    def active_run_ids(self) -> tuple[str, ...]:
        """Return active run ids in stable order."""
        with self._lock:
            return tuple(sorted(self._active_run_ids))


__all__ = ["RuntimeStopController"]
