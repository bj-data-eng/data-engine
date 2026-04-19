"""Shared artifact-preview presentation decisions across operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass
import mimetypes
from pathlib import Path


@dataclass(frozen=True)
class ArtifactPreviewSpec:
    """Describe how one artifact should be previewed."""

    kind: str
    label: str
    previewable: bool
    placeholder_message: str | None = None


def classify_artifact_preview(path: Path) -> ArtifactPreviewSpec:
    """Return the preview strategy for one output artifact."""
    suffix = path.suffix.lower()
    if suffix in {".parquet"}:
        return ArtifactPreviewSpec(kind="parquet", label="Parquet table preview", previewable=True)
    if suffix in {".xlsx", ".xls", ".xlsm", ".xlsb"}:
        return ArtifactPreviewSpec(kind="excel", label="Excel table preview", previewable=True)
    if suffix in {".json"}:
        return ArtifactPreviewSpec(kind="json", label="JSON table preview", previewable=True)
    if suffix in {".pdf"}:
        return ArtifactPreviewSpec(
            kind="pdf",
            label="PDF inspection",
            previewable=False,
            placeholder_message="PDF artifacts are recognized, but in-app PDF text inspection is not available yet.",
        )
    if is_text_artifact(path):
        return ArtifactPreviewSpec(kind="text", label="Text preview", previewable=True)
    return ArtifactPreviewSpec(
        kind="unsupported",
        label="Artifact inspection",
        previewable=False,
        placeholder_message="This artifact type is not previewable in the UI yet.",
    )


def is_text_artifact(path: Path) -> bool:
    """Return whether one artifact should use text-preview treatment."""
    suffix = path.suffix.lower()
    if suffix in {
        ".txt", ".log", ".md", ".json", ".csv", ".tsv", ".yaml", ".yml",
        ".xml", ".html", ".htm", ".sql", ".py", ".toml", ".ini", ".cfg",
    }:
        return True
    guessed_type, _encoding = mimetypes.guess_type(path.name)
    if guessed_type is None:
        return False
    return guessed_type.startswith("text/") or guessed_type in {"application/json", "application/xml", "application/x-yaml"}


__all__ = ["ArtifactPreviewSpec", "classify_artifact_preview", "is_text_artifact"]
