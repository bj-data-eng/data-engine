from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

from data_engine.authoring.flow import Flow
from data_engine.domain import DaemonLifecyclePolicy
from data_engine.platform.theme import GITHUB_DARK, GITHUB_LIGHT


@dataclass
class FakeSettingsStore:
    value: Path | str | None = None

    def workspace_collection_root(self) -> Path | None:
        return Path("/tmp/workspaces")

    def set_workspace_collection_root(self, value: Path | str | None) -> None:
        self.value = value


def bundle_inputs():
    discovered = (SimpleNamespace(name="workspace-a"),)
    resolved = SimpleNamespace(workspace_id="example", workspace_root=Path("/tmp/workspace"))
    definition_flow = Flow(name="alpha", group="Docs")
    definition = SimpleNamespace(name="alpha", description="Example flow", build=lambda: definition_flow)
    loaded_flow = Flow(name="beta", group="Docs")
    requests: list[dict[str, object]] = []

    def discover_workspaces_func(*, app_root=None, workspace_collection_root=None):
        return discovered

    def resolve_workspace_paths_func(*, workspace_id=None, workspace_root=None, data_root=None, workspace_collection_root=None):
        return resolved

    def discover_definitions_func(*, data_root=None):
        return (definition,)

    def load_flow_func(name: str, *, data_root=None):
        return loaded_flow

    def spawn_process_func(paths, *, lifecycle_policy=DaemonLifecyclePolicy.PERSISTENT):
        return {"paths": paths, "policy": lifecycle_policy}

    def request_func(paths, payload, *, timeout=0.0):
        requests.append(payload)
        return {"ok": True, "payload": payload, "timeout": timeout}

    def is_live_func(paths):
        return True

    def resolve_theme_name_func(theme_name: str) -> str:
        return "light"

    def system_theme_name_func() -> str:
        return "dark"

    def toggle_theme_name_func(theme_name: str) -> str:
        return "dark" if theme_name == "light" else "light"

    def theme_button_text_func(theme_name: str) -> str:
        return f"Switch from {theme_name}"

    themes = {
        "dark": GITHUB_DARK,
        "light": GITHUB_LIGHT,
    }

    return {
        "discovered": discovered,
        "settings_store": FakeSettingsStore(),
        "workspace": SimpleNamespace(name="workspace"),
        "resolved": resolved,
        "definition": definition,
        "loaded_flow": loaded_flow,
        "discover_workspaces_func": discover_workspaces_func,
        "resolve_workspace_paths_func": resolve_workspace_paths_func,
        "discover_definitions_func": discover_definitions_func,
        "load_flow_func": load_flow_func,
        "spawn_process_func": spawn_process_func,
        "request_func": request_func,
        "is_live_func": is_live_func,
        "resolve_theme_name_func": resolve_theme_name_func,
        "system_theme_name_func": system_theme_name_func,
        "toggle_theme_name_func": toggle_theme_name_func,
        "theme_button_text_func": theme_button_text_func,
        "themes": themes,
        "requests": requests,
    }


class FakeGeometry:
    def __init__(self, width: int, height: int) -> None:
        self._width = width
        self._height = height

    def width(self) -> int:
        return self._width

    def height(self) -> int:
        return self._height


class FakeScreen:
    def __init__(self, width: int, height: int) -> None:
        self._geometry = FakeGeometry(width, height)

    def availableGeometry(self) -> FakeGeometry:
        return self._geometry


