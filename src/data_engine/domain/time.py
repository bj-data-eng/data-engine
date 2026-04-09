"""Shared UTC timestamp helpers."""

from __future__ import annotations

from datetime import UTC, datetime


def utcnow_text() -> str:
    """Return the current UTC timestamp in ISO 8601 text form."""
    return datetime.now(UTC).isoformat()


def parse_utc_text(value: str | None) -> datetime | None:
    """Return a parsed UTC datetime for persisted timestamp text."""
    if value in {None, ""}:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


__all__ = ["parse_utc_text", "utcnow_text"]
