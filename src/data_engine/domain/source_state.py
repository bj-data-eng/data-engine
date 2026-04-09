"""Domain models for source-file freshness and change detection."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceSignature:
    """One concrete source file signature used for freshness checks."""

    source_path: str
    mtime_ns: int
    size_bytes: int


__all__ = ["SourceSignature"]
