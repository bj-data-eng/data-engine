"""Runtime stop and drain helpers for the daemon host."""

from __future__ import annotations

from dataclasses import dataclass
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService
    from data_engine.hosts.daemon.composition import DaemonHostState


ACTIVE_WORK_DRAIN_TIMEOUT_SECONDS = 1.5


@dataclass(frozen=True)
class ActiveWorkDrainResult:
    """Outcome of one bounded attempt to drain runtime workers."""

    remaining_workers: tuple[str, ...]

    @property
    def complete(self) -> bool:
        """Return whether no startup or runtime worker can still execute."""
        return not self.remaining_workers


@dataclass(frozen=True)
class _WorkerReference:
    label: str
    thread: threading.Thread


def stop_active_work(
    service: "DataEngineDaemonService",
    *,
    timeout_seconds: float | None = None,
) -> ActiveWorkDrainResult:
    """Close runtime admission, signal workers, and make one bounded drain attempt.

    The first attempt waits up to ``ACTIVE_WORK_DRAIN_TIMEOUT_SECONDS`` by
    default. Later retries are non-blocking because all stop signals have
    already been delivered. Ownership and storage callers must proceed only
    when the returned result is complete.
    """
    with service._state_lock:
        drain_started = service.state.begin_work_drain()
        if service.state.runtime_active or service.state.engine_starting:
            service.state.runtime_stopping = True
        engine_runtime_stop_event = service.state.engine_runtime_stop_event
        engine_flow_stop_event = service.state.engine_flow_stop_event
        manual_runtime_stop_events = tuple(service.state.manual_runtime_stop_events.values())
        manual_flow_stop_events = tuple(service.state.manual_flow_stop_events.values())
        worker_references = _worker_references_locked(service.state)

    engine_runtime_stop_event.set()
    engine_flow_stop_event.set()
    for stop_event in manual_runtime_stop_events:
        stop_event.set()
    for stop_event in manual_flow_stop_events:
        stop_event.set()

    wait_seconds = (
        ACTIVE_WORK_DRAIN_TIMEOUT_SECONDS if timeout_seconds is None and drain_started else timeout_seconds or 0.0
    )
    _join_workers(worker_references, timeout_seconds=max(float(wait_seconds), 0.0))

    with service._state_lock:
        remaining_workers = _reap_and_describe_remaining_work_locked(service.state)
        publish_stopped = not remaining_workers and not service.state.work_drain_complete
        if publish_stopped:
            service.state.work_drain_complete = True

    result = ActiveWorkDrainResult(remaining_workers=remaining_workers)
    if publish_stopped:
        service._publish_runtime_event("runtime.stopped")
    return result


def wait_for_active_work(service: "DataEngineDaemonService") -> None:
    """Wait without polling until every retained runtime worker has exited."""
    result = stop_active_work(service)
    current_thread = threading.current_thread()
    while not result.complete:
        with service._state_lock:
            worker_references = _worker_references_locked(service.state)
        joinable_threads = _unique_threads(
            reference.thread for reference in worker_references if reference.thread is not current_thread
        )
        if not joinable_threads:
            result = stop_active_work(service, timeout_seconds=0.0)
            if result.complete:
                break
            raise RuntimeError(
                "Runtime drain cannot complete because active work has no joinable worker thread: "
                + ", ".join(result.remaining_workers)
            )
        for thread in joinable_threads:
            thread.join()
        result = stop_active_work(service, timeout_seconds=0.0)


def _join_workers(worker_references: tuple[_WorkerReference, ...], *, timeout_seconds: float) -> None:
    if timeout_seconds <= 0.0:
        return
    deadline = time.monotonic() + timeout_seconds
    current_thread = threading.current_thread()
    for thread in _unique_threads(reference.thread for reference in worker_references):
        if thread is current_thread or not thread.is_alive():
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            break
        thread.join(timeout=remaining)


def _worker_references_locked(state: "DaemonHostState") -> tuple[_WorkerReference, ...]:
    references: list[_WorkerReference] = []
    if state.engine_start_thread is not None:
        references.append(_WorkerReference("engine startup", state.engine_start_thread))
    if state.engine_thread is not None:
        references.append(_WorkerReference("engine runtime", state.engine_thread))
    if state.finishing_engine_thread is not None:
        references.append(_WorkerReference("engine finalization", state.finishing_engine_thread))
    references.extend(
        _WorkerReference(f"manual startup:{name}", thread)
        for name, thread in sorted(state.pending_manual_run_threads.items())
    )
    references.extend(
        _WorkerReference(f"manual runtime:{name}", thread)
        for name, thread in sorted(state.manual_run_threads.items())
    )
    references.extend(
        _WorkerReference(f"manual finalization:{name}", thread)
        for name, thread in sorted(state.finishing_manual_run_threads.items())
    )
    return tuple(references)


def _reap_and_describe_remaining_work_locked(state: "DaemonHostState") -> tuple[str, ...]:
    if state.engine_start_thread is not None and not state.engine_start_thread.is_alive():
        state.clear_engine_start_reservation()
    if state.finishing_engine_thread is not None and not state.finishing_engine_thread.is_alive():
        state.finishing_engine_thread = None
    if state.engine_thread is not None and not state.engine_thread.is_alive():
        state.end_runtime()

    for name, thread in tuple(state.pending_manual_run_threads.items()):
        if not thread.is_alive():
            state.clear_manual_run_reservation(name)
    for name, thread in tuple(state.finishing_manual_run_threads.items()):
        if not thread.is_alive():
            state.finishing_manual_run_threads.pop(name, None)
    for name, thread in tuple(state.manual_run_threads.items()):
        if not thread.is_alive():
            state.unregister_manual_run(name)

    if (
        state.engine_thread is None
        and state.engine_start_thread is None
        and state.finishing_engine_thread is None
        and (state.runtime_active or state.runtime_stopping)
    ):
        state.end_runtime()

    remaining = [reference.label for reference in _worker_references_locked(state) if reference.thread.is_alive()]
    if state.engine_starting and state.engine_start_thread is None:
        remaining.append("engine startup")
    remaining.extend(
        f"manual startup:{name}"
        for name in sorted(state.pending_manual_run_names)
        if name not in state.pending_manual_run_threads
    )
    return tuple(dict.fromkeys(remaining))


def _unique_threads(threads) -> tuple[threading.Thread, ...]:
    unique: list[threading.Thread] = []
    seen: set[int] = set()
    for thread in threads:
        identity = id(thread)
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(thread)
    return tuple(unique)


__all__ = [
    "ACTIVE_WORK_DRAIN_TIMEOUT_SECONDS",
    "ActiveWorkDrainResult",
    "stop_active_work",
    "wait_for_active_work",
]
