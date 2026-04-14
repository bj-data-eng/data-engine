"""Daemon IPC command routing."""

from __future__ import annotations

from dataclasses import asdict
import threading
from typing import TYPE_CHECKING, Any

from data_engine.hosts.daemon.runtime_commands import DaemonRuntimeCommandHandler
from data_engine.hosts.daemon.state_sync import DaemonStateSyncHandler

if TYPE_CHECKING:
    from data_engine.hosts.daemon.app import DataEngineDaemonService


class DaemonCommandHandler:
    """Route daemon IPC commands onto narrower host collaborators."""

    def __init__(self, service: "DataEngineDaemonService") -> None:
        self.service = service
        self.runtime_commands = DaemonRuntimeCommandHandler(service)
        self.state_sync = DaemonStateSyncHandler(service)

    def handle(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {"ok": False, "error": "Invalid command payload."}
        command = str(payload.get("command", ""))
        if command == "daemon_ping":
            return {"ok": True, "workspace_id": self.service.paths.workspace_id}
        if command == "daemon_status":
            return {"ok": True, "status": self.state_sync.status_payload()}
        if command == "list_flows":
            return {"ok": True, "flows": [asdict(card) for card in self.state_sync.load_flow_cards()]}
        if command == "get_flow":
            name = str(payload.get("name", ""))
            flow = next((card for card in self.state_sync.load_flow_cards() if card.name == name), None)
            if flow is None:
                return {"ok": False, "error": f"Unknown flow: {name}"}
            return {"ok": True, "flow": asdict(flow)}
        if command == "refresh_flows":
            return {"ok": True, "flows": [asdict(card) for card in self.state_sync.load_flow_cards(force=True)]}
        if command == "run_flow":
            return self.runtime_commands.run_flow(name=str(payload.get("name", "")), wait=bool(payload.get("wait", False)))
        if command == "start_engine":
            return self.runtime_commands.start_engine()
        if command == "stop_engine":
            return self.runtime_commands.stop_engine()
        if command == "stop_flow":
            return self.runtime_commands.stop_flow(str(payload.get("name", "")))
        if command == "shutdown_daemon":
            self.service.host.shutdown_event.set()
            threading.Thread(target=self.service._wake_listener, daemon=True).start()
            return {"ok": True}
        return {"ok": False, "error": f"Unknown command: {command}"}

    def checkpoint_once(self, *, status: str) -> None:
        self.state_sync.checkpoint_once(status=status)

    def refresh_observer_snapshot(self) -> None:
        self.state_sync.refresh_observer_snapshot()

    def update_daemon_state(self, *, status: str) -> None:
        self.state_sync.update_daemon_state(status=status)


__all__ = ["DaemonCommandHandler"]
