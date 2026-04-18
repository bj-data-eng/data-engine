"""Workspace daemon state and control services."""

from __future__ import annotations

from collections.abc import Callable
from threading import Event, Thread

from data_engine.hosts.daemon.manager import WorkspaceDaemonManager, WorkspaceDaemonSnapshot
from data_engine.hosts.daemon.shared_state import DaemonSharedStateAdapter
from data_engine.domain import WorkspaceControlState
from data_engine.platform.workspace_models import WorkspacePaths


class DaemonUpdateSubscription:
    """Own client-side daemon subscription state for one workspace manager."""

    def __init__(
        self,
        *,
        daemon_state_service: "DaemonStateService",
        manager: WorkspaceDaemonManager,
        clock: Callable[[], float],
        timeout_seconds: float = 1.5,
        stale_after_seconds: float = 15.0,
    ) -> None:
        self.daemon_state_service = daemon_state_service
        self.manager = manager
        self.clock = clock
        self.timeout_seconds = timeout_seconds
        self.stale_after_seconds = stale_after_seconds
        self.stop_event = Event()
        self.thread: Thread | None = None
        self.last_sync_monotonic = 0.0
        self.last_subscription_monotonic = 0.0

    def is_alive(self) -> bool:
        """Return whether the background subscription worker is alive."""
        thread = self.thread
        return bool(thread is not None and thread.is_alive())

    def mark_sync(self, now_monotonic: float | None = None) -> None:
        """Record one successful foreground sync timestamp."""
        self.last_sync_monotonic = float(self.clock() if now_monotonic is None else now_monotonic)

    def mark_subscription(self, now_monotonic: float | None = None) -> None:
        """Record one successful subscription update timestamp."""
        self.last_subscription_monotonic = float(self.clock() if now_monotonic is None else now_monotonic)

    def should_run_heartbeat(self, snapshot) -> bool:
        """Return whether fallback heartbeat sync should run for the current snapshot."""
        if snapshot is None:
            return True
        return self.daemon_state_service.should_run_heartbeat(
            daemon_live=snapshot.engine.daemon_live,
            transport_mode=snapshot.engine.transport,
            wait_worker_alive=self.is_alive(),
            now_monotonic=self.clock(),
            last_sync_monotonic=self.last_sync_monotonic,
            last_subscription_monotonic=self.last_subscription_monotonic,
            stale_after_seconds=self.stale_after_seconds,
        )

    def ensure_started(
        self,
        *,
        workspace_available: Callable[[], bool],
        on_update: Callable[[WorkspaceDaemonSnapshot], None],
        start_worker: Callable[[Callable[[], None]], Thread],
    ) -> Thread | None:
        """Start the background subscription worker if it is not already alive.

        `start_worker` must start the thread before returning it.
        """
        if self.is_alive():
            return self.thread
        self.stop_event.clear()

        def _run() -> None:
            def _handle_update(snapshot: WorkspaceDaemonSnapshot) -> None:
                self.mark_subscription()
                on_update(snapshot)

            self.daemon_state_service.run_subscription_loop(
                self.manager,
                stop_event=self.stop_event,
                workspace_available=workspace_available,
                on_update=_handle_update,
                timeout_seconds=self.timeout_seconds,
            )

        self.thread = start_worker(_run)
        return self.thread

    def stop(self) -> None:
        """Request the background subscription worker to stop."""
        self.stop_event.set()


class DaemonStateService:
    """Own workspace daemon-manager construction and normalized snapshot access."""

    def __init__(self, *, shared_state_adapter: DaemonSharedStateAdapter | None = None) -> None:
        self.shared_state_adapter = shared_state_adapter or DaemonSharedStateAdapter()

    def create_manager(self, paths: WorkspacePaths) -> WorkspaceDaemonManager:
        """Create one daemon-state manager for a workspace."""
        return WorkspaceDaemonManager(paths, shared_state_adapter=self.shared_state_adapter)

    def sync(self, manager: WorkspaceDaemonManager) -> WorkspaceDaemonSnapshot:
        """Fetch one normalized daemon snapshot."""
        return manager.sync()

    def wait_for_update(
        self,
        manager: WorkspaceDaemonManager,
        *,
        timeout_seconds: float = 5.0,
    ) -> WorkspaceDaemonSnapshot:
        """Wait for one daemon projection update and return the normalized snapshot."""
        return manager.wait_for_update(timeout_seconds=timeout_seconds)

    def run_subscription_loop(
        self,
        manager: WorkspaceDaemonManager,
        *,
        stop_event: Event,
        workspace_available: Callable[[], bool],
        on_update: Callable[[WorkspaceDaemonSnapshot], None],
        timeout_seconds: float = 1.5,
    ) -> None:
        """Drive one long-poll subscription loop until stopped.

        The daemon-state service owns the transport semantics here:
        authored-workspace gating, long-poll waiting, and unchanged-snapshot
        suppression. Surfaces provide only the stop signal and the update sink.
        """
        while not stop_event.is_set():
            if not workspace_available():
                if stop_event.wait(timeout_seconds):
                    return
                continue
            previous_snapshot = getattr(manager, "_last_snapshot", None)
            snapshot = self.wait_for_update(manager, timeout_seconds=timeout_seconds)
            if stop_event.is_set():
                return
            if previous_snapshot is not None and snapshot == previous_snapshot:
                continue
            on_update(snapshot)

    @staticmethod
    def should_run_heartbeat(
        *,
        daemon_live: bool,
        transport_mode: str,
        wait_worker_alive: bool,
        now_monotonic: float,
        last_sync_monotonic: float,
        last_subscription_monotonic: float,
        stale_after_seconds: float = 15.0,
    ) -> bool:
        """Return whether fallback heartbeat sync should run right now.

        Subscription is the primary transport. Heartbeat remains a recovery
        mechanism when the daemon is down, transport is not subscription-led,
        the wait worker is missing, or the subscription path has gone quiet for
        too long.
        """
        if not daemon_live:
            return True
        if transport_mode != "subscription":
            return True
        if not wait_worker_alive:
            return True
        freshest = max(float(last_sync_monotonic or 0.0), float(last_subscription_monotonic or 0.0))
        return (float(now_monotonic) - freshest) >= max(float(stale_after_seconds), 0.0)

    def control_state(
        self,
        manager: WorkspaceDaemonManager,
        snapshot: WorkspaceDaemonSnapshot,
        *,
        daemon_startup_in_progress: bool = False,
    ) -> WorkspaceControlState:
        """Build structured workspace control state from one daemon snapshot."""
        return manager.control_state(snapshot, daemon_startup_in_progress=daemon_startup_in_progress)

    def request_control(self, manager: WorkspaceDaemonManager) -> str:
        """Request workspace control through one daemon-state manager."""
        return manager.request_control()


__all__ = ["DaemonStateService", "DaemonUpdateSubscription"]
