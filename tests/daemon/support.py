from __future__ import annotations

from pathlib import Path

from data_engine.platform.workspace_policy import RuntimeLayoutPolicy


resolve_workspace_paths = RuntimeLayoutPolicy().resolve_paths


def _write_demo_flow(workspace_root: Path) -> None:
    flow_dir = workspace_root / "flow_modules"
    flow_dir.mkdir(parents=True, exist_ok=True)
    (flow_dir / "demo.py").write_text(
        """
from data_engine import Flow

DESCRIPTION = "Simple daemon test flow."

def emit_value(context):
    return 1

def build():
    return Flow(name="demo", label="demo", group="Demo").step(emit_value, label="Emit Value")
""".strip()
        + "\n",
        encoding="utf-8",
    )


def _write_blocking_group_flows(workspace_root: Path) -> None:
    flow_dir = workspace_root / "flow_modules"
    flow_dir.mkdir(parents=True, exist_ok=True)
    (flow_dir / "alpha.py").write_text(
        """
from data_engine import Flow

DESCRIPTION = "Blocking group flow alpha."

def build():
    return Flow(name="alpha", label="alpha", group="Shared").step(lambda context: 1, label="Emit Alpha")
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (flow_dir / "beta.py").write_text(
        """
from data_engine import Flow

DESCRIPTION = "Blocking group flow beta."

def build():
    return Flow(name="beta", label="beta", group="Shared").step(lambda context: 2, label="Emit Beta")
""".strip()
        + "\n",
        encoding="utf-8",
    )
