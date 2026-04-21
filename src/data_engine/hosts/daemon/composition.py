"""Composition helpers for the daemon host."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
import threading
from uuid import uuid4
from typing import Callable

from data_engine.hosts.daemon.shared_state import DaemonSharedStateAdapter
from data_engine.platform.workspace_models import WorkspacePaths, machine_id_text
from data_engine.runtime.runtime_db import RuntimeCacheLedger, RuntimeControlLedger
from data_engine.services.flow_catalog import FlowCatalogService
from data_engine.services.flow_execution import FlowExecutionService
from data_engine.services.ledger import RuntimeControlLedgerService
from data_engine.services.runtime_ports import RuntimeCacheStore
from data_engine.services.runtime_execution import RuntimeExecutionService


@dataclass(frozen=True)
class DaemonHostDependencyFactories:
    """Constructor seam for daemon host collaborators."""

    flow_catalog_service_factory: Callable[[], FlowCatalogService]
    flow_execution_service_factory: Callable[[], FlowExecutionService]
    runtime_execution_service_factory: Callable[[], RuntimeExecutionService]
    shared_state_adapter_factory: Callable[[], DaemonSharedStateAdapter] = field(default=DaemonSharedStateAdapter)


def default_daemon_host_dependency_factories() -> DaemonHostDependencyFactories:
    """Build the default daemon-host constructor bundle."""
    return DaemonHostDependencyFactories(
        flow_catalog_service_factory=FlowCatalogService,
        flow_execution_service_factory=FlowExecutionService,
        runtime_execution_service_factory=RuntimeExecutionService,
        shared_state_adapter_factory=DaemonSharedStateAdapter,
    )


@dataclass(frozen=True)
class DaemonHostDependencies:
    """Concrete collaborators used by one daemon host instance."""

    runtime_cache_ledger: RuntimeCacheStore
    runtime_control_ledger: RuntimeControlLedger
    flow_catalog_service: FlowCatalogService
    flow_execution_service: FlowExecutionService
    runtime_execution_service: RuntimeExecutionService
    shared_state_adapter: DaemonSharedStateAdapter

    @classmethod
    def build_default(
        cls,
        paths: WorkspacePaths,
        *,
        ledger_service: RuntimeControlLedgerService | None = None,
        factories: DaemonHostDependencyFactories | None = None,
    ) -> "DaemonHostDependencies":
        """Build the default dependency bundle for one workspace host."""
        ledger_service = ledger_service or RuntimeControlLedgerService()
        factories = factories or default_daemon_host_dependency_factories()
        return cls(
            runtime_cache_ledger=RuntimeCacheLedger(paths.runtime_cache_db_path),
            runtime_control_ledger=ledger_service.open_for_workspace(paths.workspace_root),
            flow_catalog_service=factories.flow_catalog_service_factory(),
            flow_execution_service=factories.flow_execution_service_factory(),
            runtime_execution_service=factories.runtime_execution_service_factory(),
            shared_state_adapter=factories.shared_state_adapter_factory(),
        )


@dataclass(frozen=True)
class DaemonHostIdentity:
    """Process and machine identity for one daemon host instance."""

    machine_id: str
    daemon_id: str
    pid: int

    @classmethod
    def current_process(cls) -> "DaemonHostIdentity":
        """Build the current-process identity for one daemon host."""
        return cls(
            machine_id=machine_id_text(),
            daemon_id=uuid4().hex,
            pid=os.getpid(),
        )


@dataclass
class DaemonHostState:
    """Mutable state for a fresh daemon host instance."""

    status: str
    last_checkpoint_at_utc: str
    workspace_owned: bool
    leased_by_machine_id: str | None
    runtime_active: bool
    runtime_stopping: bool
    engine_starting: bool
    active_engine_flow_names: tuple[str, ...]
    engine_thread: threading.Thread | None
    engine_runtime_stop_event: threading.Event
    engine_flow_stop_event: threading.Event
    pending_manual_run_names: set[str]
    manual_run_threads: dict[str, threading.Thread]
    manual_runtime_stop_events: dict[str, threading.Event]
    manual_flow_stop_events: dict[str, threading.Event]
    shutdown_event: threading.Event
    checkpoint_thread: threading.Thread | None
    consecutive_checkpoint_failures: int
    listener: object | None
    shutdown_when_idle: bool
    missing_clients_since_monotonic: float | None

    @classmethod
    def build(cls, *, started_at_utc: str) -> "DaemonHostState":
        """Build the default mutable state for a fresh daemon host."""
        return cls(
            status="starting",
            last_checkpoint_at_utc=started_at_utc,
            workspace_owned=False,
            leased_by_machine_id=None,
            runtime_active=False,
            runtime_stopping=False,
            engine_starting=False,
            active_engine_flow_names=(),
            engine_thread=None,
            engine_runtime_stop_event=threading.Event(),
            engine_flow_stop_event=threading.Event(),
            pending_manual_run_names=set(),
            manual_run_threads={},
            manual_runtime_stop_events={},
            manual_flow_stop_events={},
            shutdown_event=threading.Event(),
            checkpoint_thread=None,
            consecutive_checkpoint_failures=0,
            listener=None,
            shutdown_when_idle=False,
            missing_clients_since_monotonic=None,
        )

    def claim_workspace(self) -> None:
        """Mark the current daemon as owning the workspace."""
        self.workspace_owned = True
        self.leased_by_machine_id = None
        self.status = "idle"

    def release_workspace(self, *, leased_by_machine_id: str | None = None, status: str | None = None) -> None:
        """Mark the current daemon as no longer owning the workspace."""
        self.workspace_owned = False
        self.leased_by_machine_id = leased_by_machine_id
        if status is not None:
            self.status = status

    def begin_runtime(self, *, status: str = "running", active_flow_names: tuple[str, ...] = ()) -> None:
        """Mark the engine runtime as active and running."""
        self.engine_starting = False
        self.runtime_active = True
        self.runtime_stopping = False
        self.active_engine_flow_names = tuple(active_flow_names)
        self.status = status

    def stop_runtime(self, *, status: str = "stopping") -> None:
        """Mark the engine runtime as stopping."""
        self.engine_starting = False
        self.runtime_stopping = True
        self.status = status

    def end_runtime(self, *, status: str = "idle") -> None:
        """Mark the engine runtime as inactive."""
        self.engine_starting = False
        self.runtime_active = False
        self.runtime_stopping = False
        self.active_engine_flow_names = ()
        self.engine_thread = None
        self.engine_runtime_stop_event = threading.Event()
        self.engine_flow_stop_event = threading.Event()
        if self.status != "failed":
            self.status = status

    def set_checkpoint_time(self, checkpoint_at_utc: str, *, status: str | None = None) -> None:
        """Update the last successful checkpoint timestamp."""
        self.last_checkpoint_at_utc = checkpoint_at_utc
        if status is not None:
            self.status = status

    def set_leased_by_machine_id(self, machine_id: str | None) -> None:
        """Update the current lease owner identifier."""
        self.leased_by_machine_id = machine_id

    def increment_checkpoint_failures(self) -> int:
        """Increment the repeated-checkpoint failure counter."""
        self.consecutive_checkpoint_failures += 1
        return self.consecutive_checkpoint_failures

    def reset_checkpoint_failures(self) -> None:
        """Reset the repeated-checkpoint failure counter."""
        self.consecutive_checkpoint_failures = 0

    def set_engine_threads(
        self,
        *,
        runtime_stop_event: threading.Event,
        flow_stop_event: threading.Event,
        engine_thread: threading.Thread | None = None,
    ) -> None:
        """Replace the active engine coordination objects."""
        self.engine_runtime_stop_event = runtime_stop_event
        self.engine_flow_stop_event = flow_stop_event
        self.engine_thread = engine_thread

    def reserve_engine_start(self) -> bool:
        """Reserve engine startup so concurrent start requests collapse to one attempt."""
        if self.runtime_active or self.engine_starting:
            return False
        self.engine_starting = True
        return True

    def clear_engine_start_reservation(self) -> None:
        """Clear any in-progress engine startup reservation."""
        self.engine_starting = False

    def reserve_manual_run(self, name: str) -> bool:
        """Reserve one manual run name before flow loading starts."""
        if name in self.pending_manual_run_names:
            return False
        self.pending_manual_run_names.add(name)
        return True

    def clear_manual_run_reservation(self, name: str) -> None:
        """Clear one in-progress manual run reservation."""
        self.pending_manual_run_names.discard(name)

    def register_manual_run(
        self,
        name: str,
        *,
        thread: threading.Thread,
        runtime_stop_event: threading.Event,
        flow_stop_event: threading.Event,
    ) -> None:
        """Register one manual run and its graceful and hard stop signals."""
        self.pending_manual_run_names.discard(name)
        self.manual_run_threads[name] = thread
        self.manual_runtime_stop_events[name] = runtime_stop_event
        self.manual_flow_stop_events[name] = flow_stop_event

    def unregister_manual_run(self, name: str) -> None:
        """Remove one completed manual run."""
        self.pending_manual_run_names.discard(name)
        self.manual_run_threads.pop(name, None)
        self.manual_runtime_stop_events.pop(name, None)
        self.manual_flow_stop_events.pop(name, None)

    def set_listener(self, listener: object | None) -> None:
        """Update the active listener object."""
        self.listener = listener

    def request_shutdown_when_idle(self) -> None:
        """Mark this daemon to exit once active work drains and no clients remain."""
        self.shutdown_when_idle = True

    def clear_shutdown_when_idle(self) -> None:
        """Clear any pending idle-shutdown request."""
        self.shutdown_when_idle = False

    def mark_clients_present(self) -> None:
        """Clear any transient no-client observation window."""
        self.missing_clients_since_monotonic = None

    def note_missing_clients(self, *, now_monotonic: float) -> float:
        """Record the start of one no-client window and return its start time."""
        if self.missing_clients_since_monotonic is None:
            self.missing_clients_since_monotonic = now_monotonic
        return self.missing_clients_since_monotonic


class DaemonHostFacade:
    """Explicit high-level host-state facade over the mutable daemon state object."""

    def __init__(self, state: DaemonHostState) -> None:
        self.state = state

    @property
    def status(self) -> str:
        return self.state.status

    @status.setter
    def status(self, value: str) -> None:
        self.state.status = value

    @property
    def workspace_owned(self) -> bool:
        return self.state.workspace_owned

    @workspace_owned.setter
    def workspace_owned(self, value: bool) -> None:
        self.state.workspace_owned = value

    @property
    def leased_by_machine_id(self) -> str | None:
        return self.state.leased_by_machine_id

    @leased_by_machine_id.setter
    def leased_by_machine_id(self, value: str | None) -> None:
        self.state.leased_by_machine_id = value

    @property
    def runtime_active(self) -> bool:
        return self.state.runtime_active

    @runtime_active.setter
    def runtime_active(self, value: bool) -> None:
        self.state.runtime_active = value

    @property
    def runtime_stopping(self) -> bool:
        return self.state.runtime_stopping

    @runtime_stopping.setter
    def runtime_stopping(self, value: bool) -> None:
        self.state.runtime_stopping = value

    @property
    def shutdown_event(self) -> threading.Event:
        return self.state.shutdown_event

    @shutdown_event.setter
    def shutdown_event(self, value: threading.Event) -> None:
        self.state.shutdown_event = value

    @property
    def listener(self) -> object | None:
        return self.state.listener

    @listener.setter
    def listener(self, value: object | None) -> None:
        self.state.listener = value


__all__ = [
    "DaemonHostFacade",
    "DaemonHostDependencyFactories",
    "DaemonHostDependencies",
    "DaemonHostIdentity",
    "DaemonHostState",
    "default_daemon_host_dependency_factories",
]
