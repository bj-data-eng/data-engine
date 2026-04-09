"""Runtime stop/join helpers for the daemon host."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService


def stop_active_work(service: "DataEngineDaemonService") -> None:
    """Signal all active runtime work to stop and wait briefly for it to exit."""
    with service._state_lock:
        engine_runtime_stop_event = service.state.engine_runtime_stop_event
        engine_flow_stop_event = service.state.engine_flow_stop_event
        engine_thread = service.state.engine_thread
        manual_stop_events = list(service.state.manual_stop_events.values())
        manual_threads = list(service.state.manual_run_threads.values())
    engine_runtime_stop_event.set()
    engine_flow_stop_event.set()
    for stop_event in manual_stop_events:
        stop_event.set()
    if engine_thread is not None:
        engine_thread.join(timeout=1.5)
    for thread in manual_threads:
        thread.join(timeout=1.5)
    with service._state_lock:
        service.state.end_runtime()


__all__ = ["stop_active_work"]
