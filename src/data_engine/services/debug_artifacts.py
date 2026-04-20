"""Workspace-local debug artifact storage and listing helpers."""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
import re
import shutil
from typing import Any

from data_engine.domain import DebugArtifactRecord
from data_engine.views.artifacts import classify_artifact_preview

DEBUG_ARTIFACTS_DIR_NAME = "debug_artifacts"
_SAFE_NAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


def debug_artifacts_dir(runtime_state_dir: Path) -> Path:
    """Return the workspace-local debug artifact directory."""
    return Path(runtime_state_dir).resolve() / DEBUG_ARTIFACTS_DIR_NAME


def sanitize_debug_name(value: str | None, *, fallback: str) -> str:
    """Return a filesystem-safe token for debug artifact names."""
    candidate = (value or "").strip()
    if not candidate:
        return fallback
    normalized = _SAFE_NAME_PATTERN.sub("-", candidate).strip("-._")
    return normalized or fallback


def write_debug_metadata(metadata_path: Path, payload: dict[str, object]) -> None:
    """Persist one debug metadata JSON file."""
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def list_debug_artifacts(runtime_state_dir: Path) -> tuple[DebugArtifactRecord, ...]:
    """Return saved debug artifacts for one workspace-local runtime root."""
    root = debug_artifacts_dir(runtime_state_dir)
    if not root.is_dir():
        return ()
    records: list[DebugArtifactRecord] = []
    seen_stems: set[str] = set()
    for path in sorted(root.iterdir()):
        if not path.is_file():
            continue
        if path.suffix.lower() == ".json":
            continue
        metadata: dict[str, Any] = {}
        artifact_path = path
        metadata_path = path.with_suffix(".json")
        if metadata_path.is_file():
            metadata = _read_json(metadata_path)
        kind = classify_artifact_preview(path).kind
        stem = path.stem
        if stem in seen_stems:
            continue
        seen_stems.add(stem)
        debug_info = metadata.get("debug") if isinstance(metadata.get("debug"), dict) else metadata
        created_at_utc = str(debug_info.get("saved_at_utc", ""))
        flow_name = str(debug_info.get("flow_name", "") or "")
        step_name_raw = debug_info.get("step_name")
        step_name = str(step_name_raw) if isinstance(step_name_raw, str) and step_name_raw.strip() else None
        source_raw = debug_info.get("source_path")
        source_path = str(source_raw) if isinstance(source_raw, str) and source_raw.strip() else None
        display_raw = debug_info.get("display_name")
        display_name = str(display_raw) if isinstance(display_raw, str) and display_raw.strip() else None
        records.append(
            DebugArtifactRecord(
                stem=stem,
                kind=kind,
                created_at_utc=created_at_utc,
                flow_name=flow_name,
                step_name=step_name,
                artifact_path=artifact_path,
                metadata_path=metadata_path,
                source_path=source_path,
                display_name=display_name,
                metadata=metadata,
            )
        )
    records.sort(key=lambda item: (item.created_at_utc, item.stem), reverse=True)
    return tuple(records)


def clear_debug_artifacts(runtime_state_dir: Path) -> int:
    """Delete saved debug artifacts for one workspace-local runtime root."""
    root = debug_artifacts_dir(runtime_state_dir)
    if not root.exists():
        return 0
    count = 0
    for path in tuple(root.iterdir()):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            count += 1
            continue
        try:
            path.unlink()
            count += 1
        except FileNotFoundError:
            continue
    return count


def build_debug_metadata(
    *,
    workspace_id: str | None,
    flow_name: str,
    step_name: str | None,
    run_id: str | None,
    source_path: str | None,
    artifact_kind: str,
    artifact_path: Path,
    saved_at_utc: str,
    display_name: str,
    info: dict[str, object] | None,
) -> dict[str, object]:
    """Build the default metadata payload saved beside a debug artifact."""
    payload: dict[str, object] = {
        "debug": {
            "workspace_id": workspace_id,
            "flow_name": flow_name,
            "step_name": step_name,
            "run_id": run_id,
            "source_path": source_path,
            "artifact_kind": artifact_kind,
            "artifact_path": str(artifact_path),
            "saved_at_utc": saved_at_utc,
            "display_name": display_name,
        }
    }
    if info:
        payload["info"] = dict(info)
    return payload


def serializable_json_value(value: object) -> object:
    """Return one JSON-serializable representation for arbitrary debug info values."""
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): serializable_json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set):
        return [serializable_json_value(item) for item in value]
    if hasattr(value, "__dataclass_fields__"):
        return serializable_json_value(asdict(value))
    return repr(value)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {"value": payload}


__all__ = [
    "DEBUG_ARTIFACTS_DIR_NAME",
    "build_debug_metadata",
    "clear_debug_artifacts",
    "debug_artifacts_dir",
    "list_debug_artifacts",
    "sanitize_debug_name",
    "serializable_json_value",
    "write_debug_metadata",
]
