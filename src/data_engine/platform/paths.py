"""Cross-platform path normalization helpers."""

from __future__ import annotations

import ctypes
import os
from pathlib import Path, PosixPath
import sys
import unicodedata


_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


class _WindowsFindData(ctypes.Structure):
    _fields_ = [
        ("attributes", ctypes.c_uint32),
        ("creation_time", ctypes.c_uint32 * 2),
        ("access_time", ctypes.c_uint32 * 2),
        ("write_time", ctypes.c_uint32 * 2),
        ("size_high", ctypes.c_uint32),
        ("size_low", ctypes.c_uint32),
        ("reserved_0", ctypes.c_uint32),
        ("reserved_1", ctypes.c_uint32),
        ("filename", ctypes.c_wchar * 260),
        ("alternate_filename", ctypes.c_wchar * 14),
    ]


def normalized_path_text(value: Path | str) -> str:
    """Return forward-slash path text preserving filesystem-significant Unicode."""
    return str(value).replace("\\", "/")


def stable_absolute_path(value: Path | str) -> Path:
    """Return an absolute path without dereferencing Windows reparse points."""
    expanded = os.path.expanduser(os.fspath(value))
    absolute = os.path.abspath(expanded)
    path_type = Path if sys.platform == "win32" else PosixPath
    return path_type(absolute)


def stable_path_identity_text(value: Path | str, *, case_insensitive: bool | None = None) -> str:
    """Return lexical absolute path identity without resolving reparse points.

    Distinct Unicode names stay distinct. macOS composition aliases use stored
    spelling only when the filesystem proves equivalence. Windows identities use
    filesystem spelling for existing Unicode components and keep missing
    components literal. ASCII stays lowercase for stable existing keys. Other
    hosts preserve case by default; explicit insensitive comparisons on those
    hosts fold ASCII only.
    """
    path = stable_absolute_path(value)
    text = normalized_path_text(path)
    if sys.platform == "darwin" and not text.isascii():
        text = _filesystem_unicode_path_text(path)
    if case_insensitive is None:
        case_insensitive = os.name == "nt"
    if not case_insensitive:
        return text
    if os.name == "nt" and not text.isascii():
        text = _windows_filesystem_path_text(path)
    return text.translate(_ASCII_LOWER)


def _filesystem_unicode_path_text(path: Path) -> str:
    """Canonicalize composition aliases only when the filesystem proves equivalence."""
    actual = Path(path.anchor)
    for component in path.parts[1:]:
        requested = actual / component
        if not component.isascii():
            try:
                candidates = tuple(actual.iterdir())
                for candidate in candidates:
                    if (
                        unicodedata.normalize("NFC", candidate.name) == unicodedata.normalize("NFC", component)
                        and candidate.samefile(requested)
                    ):
                        requested = candidate
                        break
            except (FileNotFoundError, NotADirectoryError):
                pass
        actual = requested
    return normalized_path_text(actual)


def _windows_filesystem_path_text(path: Path) -> str:
    # The volume's casing table can differ from the current OS Unicode table.
    # Query each Unicode component's actual name, retaining lexical reparse paths.
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    find_first = kernel32.FindFirstFileW
    find_first.argtypes = [ctypes.c_wchar_p, ctypes.POINTER(_WindowsFindData)]
    find_first.restype = ctypes.c_void_p
    find_close = kernel32.FindClose
    find_close.argtypes = [ctypes.c_void_p]
    find_close.restype = ctypes.c_int
    actual = Path(path.anchor)
    data = _WindowsFindData()
    for component in path.parts[1:]:
        actual = actual / component
        if component.isascii():
            continue
        if "*" in component or "?" in component:
            raise ValueError("Filesystem path identity requires literal path components.")
        lookup = str(actual)
        if not lookup.startswith("\\\\?\\"):
            lookup = (
                "\\\\?\\UNC\\" + lookup[2:]
                if lookup.startswith("\\\\")
                else "\\\\?\\" + lookup
            )
        handle = find_first(lookup, ctypes.byref(data))
        if handle == ctypes.c_void_p(-1).value:
            error = ctypes.get_last_error()
            if error not in {2, 3}:
                raise ctypes.WinError(error)
            continue
        try:
            actual = actual.with_name(data.filename)
        finally:
            find_close(handle)
    return normalized_path_text(actual)


def path_display(value: Path | str | None, *, empty: str = "(not set)") -> str:
    """Render a path value consistently for UI/display use."""
    if value is None:
        return empty
    return unicodedata.normalize("NFC", normalized_path_text(value))


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
