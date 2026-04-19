"""Shared debug-artifact models for author/runtime and operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class DebugArtifactRecord:
    """Describe one saved debug artifact and its linked metadata."""

    stem: str
    kind: str
    created_at_utc: str
    flow_name: str
    step_name: str | None
    artifact_path: Path
    metadata_path: Path
    source_path: str | None = None
    display_name: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


__all__ = ["DebugArtifactRecord"]
