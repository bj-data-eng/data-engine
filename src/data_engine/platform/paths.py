"""Cross-platform path normalization helpers."""

from __future__ import annotations

import os
from pathlib import Path
import unicodedata


def normalized_path_text(value: Path | str) -> str:
    """Return a stable forward-slash path string for display and comparisons."""
    return unicodedata.normalize("NFC", str(value).replace("\\", "/"))


def stable_absolute_path(value: Path | str) -> Path:
    """Return an absolute path without dereferencing Windows reparse points."""
    path = Path(value).expanduser()
    if os.name == "nt":
        return Path(os.path.abspath(os.fspath(path)))
    return path.resolve()


def stable_path_identity_text(value: Path | str, *, case_insensitive: bool | None = None) -> str:
    """Return normalized path text suitable for identity hashing and comparisons."""
    text = normalized_path_text(stable_absolute_path(value))
    if case_insensitive is None:
        case_insensitive = os.name == "nt"
    return text.casefold() if case_insensitive else text


def path_display(value: Path | str | None, *, empty: str = "(not set)") -> str:
    """Render a path value consistently for UI/display use."""
    if value is None:
        return empty
    return normalized_path_text(value)


def toml_path_text(value: Path | str) -> str:
    """Render a path as TOML-safe text without Windows backslash escapes."""
    return normalized_path_text(value)


def path_sort_key(value: Path | str) -> str:
    """Return a stable platform-aware sort key for filesystem paths."""
    if os.name == "nt":
        return stable_path_identity_text(value, case_insensitive=True)
    return normalized_path_text(value)


__all__ = [
    "normalized_path_text",
    "path_display",
    "path_sort_key",
    "stable_absolute_path",
    "stable_path_identity_text",
    "toml_path_text",
]
