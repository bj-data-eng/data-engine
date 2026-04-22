"""Runtime-control actions for the daemon host."""

from __future__ import annotations

import threading
import traceback
from typing import TYPE_CHECKING, Any

from data_engine.domain.time import utcnow_text
from data_engine.hosts.daemon.ownership import lease_error_text, try_claim_released_workspace

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService


class DaemonRuntimeCommandHandler:
    """Own daemon runtime-control actions."""

    def __init__(self, service: "DataEngineDaemonService") -> None:
        self.service = service

    def automated_flow_names(self, *, force: bool = False) -> tuple[str, ...]:
        return tuple(
            card.name
            for card in self.service._load_flow_cards(force=force)
            if card.valid and card.mode in {"poll", "schedule"}
        )

    def run_flow(self, *, name: str, wait: bool, request_id: str | None = None) -> dict[str, Any]:
        service = self.service
        with service._timed_operation(
            "daemon.runtime",
            "run_flow",
            fields={"flow": name, "wait": wait, "request_id": request_id},
        ):
            if not service.state.workspace_owned and not try_claim_released_workspace(service):
                return {"ok": False, "error": lease_error_text(service)}
            cards_by_name = {card.name: card for card in service._load_flow_cards()}
            card = cards_by_name.get(name)
            if card is None or not card.valid:
                cards_by_name = {card.name: card for card in service._load_flow_cards(force=True)}
                card = cards_by_name.get(name)
            if card is None:
                return {"ok": False, "error": f"Unknown flow: {name}"}
            if not card.valid:
                return {"ok": False, "error": card.error or f"Flow {name} is invalid."}
            with service._state_lock:
                existing_thread = service.state.manual_run_threads.get(name)
                if (existing_thread is not None and existing_thread.is_alive()) or name in service.state.pending_manual_run_names:
                    return {"ok": False, "error": f"Flow {name} is already running."}
                active_same_group = next(
                    (
                        flow_name
                        for flow_name, thread in service.state.manual_run_threads.items()
                        if flow_name != name
                        and thread.is_alive()
                        and cards_by_name.get(flow_name) is not None
                        and cards_by_name[flow_name].group == card.group
                    ),
                    None,
                )
                if active_same_group is None:
                    active_same_group = next(
                        (
                            flow_name
                            for flow_name in service.state.pending_manual_run_names
                            if flow_name != name
                            and cards_by_name.get(flow_name) is not None
                            and cards_by_name[flow_name].group == card.group
                        ),
                        None,
                    )
                if active_same_group is not None:
                    return {"ok": False, "error": f"Group {card.group} already has {active_same_group} running."}
                service.state.reserve_manual_run(name)
                service._publish_runtime_event("manual.run_reserved", correlation_id=request_id, payload={"flow_name": name})
            try:
                runtime_stop_event = threading.Event()
                flow_stop_event = threading.Event()

                def _target() -> None:
                    try:
                        with service._timed_operation(
                            "daemon.runtime",
                            "load_manual_flow",
                            fields={"flow": name, "request_id": request_id},
                        ):
                            flow = service.flow_execution_service.load_flow(name, workspace_root=service.paths.workspace_root)
                        with service._timed_operation(
                            "daemon.runtime",
                            "run_manual_flow",
                            fields={"flow": name, "request_id": request_id},
                        ):
                            service.runtime_execution_service.run_manual(
                                flow,
                                runtime_ledger=service.runtime_execution_ledger,
                                runtime_stop_event=runtime_stop_event,
                                flow_stop_event=flow_stop_event,
                                workspace_id=service.paths.workspace_id,
                            )
                        service._debug_log(f"manual flow completed name={name}")
                    except Exception as exc:
                        service._debug_log(f"manual flow crashed name={name} error={exc!r}")
                        service._debug_log(traceback.format_exc().rstrip())
                        service.runtime_cache_ledger.logs.append(
                            level="ERROR",
                            message=str(exc),
                            created_at_utc=utcnow_text(),
                            flow_name=name,
                        )
                    finally:
                        with service._state_lock:
                            service.state.unregister_manual_run(name)
                        service._publish_runtime_event(
                            "manual.run_unregistered",
                            correlation_id=request_id,
                            payload={"flow_name": name},
                        )
                        service.runtime_cache_ledger.close_current_thread_connection()

                thread = threading.Thread(target=_target, daemon=True)
                with service._state_lock:
                    service.state.register_manual_run(
                        name,
                        thread=thread,
                        runtime_stop_event=runtime_stop_event,
                        flow_stop_event=flow_stop_event,
                    )
                service._publish_runtime_event(
                    "manual.run_registered",
                    correlation_id=request_id,
                    payload={"flow_name": name},
                )
                thread.start()
                if wait:
                    thread.join()
                return {"ok": True}
            except Exception as exc:
                with service._state_lock:
                    service.state.clear_manual_run_reservation(name)
                service._publish_runtime_event(
                    "manual.run_reservation_cleared",
                    correlation_id=request_id,
                    payload={"flow_name": name},
                )
                return {"ok": False, "error": str(exc)}

    def start_engine(self, *, request_id: str | None = None) -> dict[str, Any]:
        service = self.service
        with service._timed_operation(
            "daemon.runtime",
            "start_engine",
            fields={"request_id": request_id},
        ):
            if not service.state.workspace_owned and not try_claim_released_workspace(service):
                return {"ok": False, "error": lease_error_text(service)}
            with service._state_lock:
                service.state.clear_shutdown_when_idle()
                if not service.state.reserve_engine_start():
                    return {"ok": True}
            service._publish_runtime_event("engine.start_reserved", correlation_id=request_id)
            flow_names = self.automated_flow_names(force=True)
            if not flow_names:
                flow_names = self.automated_flow_names(force=True)
            if not flow_names:
                with service._state_lock:
                    service.state.clear_engine_start_reservation()
                service._publish_runtime_event("engine.start_reservation_cleared", correlation_id=request_id)
                return {"ok": False, "error": "No automated flows are available."}
            try:
                with service._timed_operation(
                    "daemon.runtime",
                    "load_engine_flows",
                    fields={"flow_count": len(flow_names), "request_id": request_id},
                ):
                    flows = service.flow_execution_service.load_flows(flow_names, workspace_root=service.paths.workspace_root)
            except Exception as exc:
                with service._state_lock:
                    service.state.clear_engine_start_reservation()
                service._publish_runtime_event("engine.start_reservation_cleared", correlation_id=request_id)
                return {"ok": False, "error": str(exc)}
            with service._state_lock:
                runtime_stop_event = threading.Event()
                flow_stop_event = threading.Event()
                service.state.set_engine_threads(runtime_stop_event=runtime_stop_event, flow_stop_event=flow_stop_event)
                service.state.begin_runtime(status="running", active_flow_names=tuple(flow_names))
            service._publish_runtime_event("engine.started", correlation_id=request_id)

            def _target() -> None:
                try:
                    with service._timed_operation(
                        "daemon.runtime",
                        "run_engine",
                        fields={"flow_count": len(flows), "request_id": request_id},
                    ):
                        service.runtime_execution_service.run_automated(
                            flows,
                            runtime_ledger=service.runtime_execution_ledger,
                            runtime_stop_event=runtime_stop_event,
                            flow_stop_event=flow_stop_event,
                            workspace_id=service.paths.workspace_id,
                        )
                    service._debug_log("engine runtime exited normally")
                except Exception as exc:
                    service._debug_log(f"engine runtime crashed error={exc!r}")
                    service._debug_log(traceback.format_exc().rstrip())
                    raise
                finally:
                    with service._state_lock:
                        service.state.end_runtime(status="idle")
                    service._publish_runtime_event("engine.stopped", correlation_id=request_id)
                    service._shutdown_for_requested_idle_disconnect(reason="engine stopped after client disconnect")
                    service._debug_log(f"engine thread finished status={service.state.status}")

            engine_thread = threading.Thread(target=_target, daemon=True)
            with service._state_lock:
                service.state.engine_thread = engine_thread
            try:
                engine_thread.start()
            except Exception:
                with service._state_lock:
                    service.state.end_runtime(status="idle")
                raise
            return {"ok": True}

    def stop_engine(self, *, request_id: str | None = None, shutdown_when_idle: bool = False) -> dict[str, Any]:
        service = self.service
        with service._timed_operation(
            "daemon.runtime",
            "stop_engine",
            fields={"request_id": request_id},
        ):
            if not service.state.workspace_owned:
                return {"ok": False, "error": lease_error_text(service)}
            with service._state_lock:
                if shutdown_when_idle:
                    service.state.request_shutdown_when_idle()
                if not service.state.runtime_active:
                    if shutdown_when_idle:
                        service._shutdown_for_requested_idle_disconnect(reason="idle disconnect request")
                    return {"ok": True}
                service.state.stop_runtime(status="stopping")
                runtime_stop_event = service.state.engine_runtime_stop_event
            service._publish_runtime_event("engine.stop_requested", correlation_id=request_id)
            runtime_stop_event.set()
            return {"ok": True}

    def stop_flow(self, name: str, request_id: str | None = None) -> dict[str, Any]:
        service = self.service
        with service._timed_operation(
            "daemon.runtime",
            "stop_flow",
            fields={"flow": name, "request_id": request_id},
        ):
            if not service.state.workspace_owned:
                return {"ok": False, "error": lease_error_text(service)}
            with service._state_lock:
                stop_event = service.state.manual_runtime_stop_events.get(name)
            if stop_event is None:
                return {"ok": False, "error": f"Flow {name} is not running."}
            service._publish_runtime_event("manual.stop_requested", correlation_id=request_id, payload={"flow_name": name})
            stop_event.set()
            return {"ok": True}


__all__ = ["DaemonRuntimeCommandHandler"]
