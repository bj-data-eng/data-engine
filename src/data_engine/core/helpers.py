"""Core helper functions for flow definitions."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import inspect
from pathlib import Path
import re
from typing import Callable

from data_engine.core.model import FlowValidationError

_FLOW_PATH_BASE_DIR: ContextVar[Path | None] = ContextVar("flow_path_base_dir", default=None)


@contextmanager
def _flow_path_base_dir(base_dir: Path | None):
    token = _FLOW_PATH_BASE_DIR.set(base_dir.resolve() if base_dir is not None else None)
    try:
        yield
    finally:
        _FLOW_PATH_BASE_DIR.reset(token)


def _parse_duration(value: str) -> float:
    raw = value.strip().lower()
    units = (
        ("ms", 0.001),
        ("s", 1.0),
        ("m", 60.0),
        ("h", 3600.0),
        ("d", 86400.0),
        ("w", 604800.0),
    )
    for suffix, multiplier in units:
        if raw.endswith(suffix):
            number = raw[: -len(suffix)].strip()
            try:
                parsed = float(number)
            except ValueError as exc:
                raise FlowValidationError(f"Invalid duration: {value!r}") from exc
            if parsed <= 0:
                raise FlowValidationError(f"Duration must be positive: {value!r}")
            return parsed * multiplier
    raise FlowValidationError(f"Unsupported duration format: {value!r}")


def _parse_schedule_at(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"(?P<hour>\d{2}):(?P<minute>\d{2})", value.strip())
    if match is None:
        raise FlowValidationError(f"Invalid schedule time: {value!r}")
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    if hour > 23 or minute > 59:
        raise FlowValidationError(f"Invalid schedule time: {value!r}")
    return hour, minute


def _normalize_watch_times(value: str | tuple[str, ...] | list[str] | set[str]) -> tuple[str, ...]:
    if isinstance(value, str):
        raw_values = [value]
    elif isinstance(value, (tuple, list, set)):
        raw_values = [str(item) for item in value]
    else:
        raise FlowValidationError("watch() time must be a time string or a collection of time strings.")

    if not raw_values:
        raise FlowValidationError("watch() time must include at least one time.")

    normalized_by_slot: dict[tuple[int, int], str] = {}
    for raw in raw_values:
        hour, minute = _parse_schedule_at(raw)
        normalized_by_slot[(hour, minute)] = f"{hour:02d}:{minute:02d}"

    return tuple(normalized_by_slot[slot] for slot in sorted(normalized_by_slot))


def _normalize_extensions(extensions: tuple[str, ...] | list[str] | set[str] | None) -> tuple[str, ...] | None:
    if extensions is None:
        return None
    normalized: list[str] = []
    for ext in extensions:
        value = str(ext).strip().lower()
        if not value:
            raise FlowValidationError("Empty extension is not allowed.")
        if not value.startswith("."):
            value = f".{value}"
        normalized.append(value)
    if not normalized:
        raise FlowValidationError("At least one extension is required.")
    return tuple(normalized)


def _title_case_words(value: str, *, empty: str = "Step") -> str:
    if not value:
        return empty
    snake = re.sub(r"[_\s]+", " ", value.strip())
    spaced = re.sub(r"(?<!^)(?=[A-Z])", " ", snake)
    words = [part for part in spaced.split() if part]
    return " ".join(word.capitalize() for word in words) or empty


def _callable_name(fn: Callable[..., object]) -> str:
    name = getattr(fn, "__name__", None)
    if isinstance(name, str) and name and name != "<lambda>":
        return _title_case_words(name)
    if inspect.isclass(fn):
        return _title_case_words(fn.__name__)
    fn_cls = getattr(fn, "__class__", None)
    if fn_cls is not None and getattr(fn_cls, "__name__", "") not in {"function", "method"}:
        return _title_case_words(fn_cls.__name__)
    return "Lambda"


def _callable_identifier(fn: Callable[..., object]) -> str:
    """Return a developer-facing callable identifier when available."""
    name = getattr(fn, "__name__", None)
    if isinstance(name, str) and name:
        return name
    if inspect.isclass(fn):
        return fn.__name__
    fn_cls = getattr(fn, "__class__", None)
    if fn_cls is not None and getattr(fn_cls, "__name__", "") not in {"function", "method"}:
        return fn_cls.__name__
    return "lambda"


def _resolve_flow_path(value: str | Path) -> Path:
    raw = Path(value).expanduser()
    if raw.is_absolute():
        return raw.resolve()
    base_dir = _FLOW_PATH_BASE_DIR.get()
    if base_dir is not None:
        return (base_dir / raw).resolve()
    return raw.resolve()


def _validate_slot_name(*, method_name: str, slot_name: str, value: str | None) -> str | None:
    """Validate and normalize one named runtime object slot reference."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise FlowValidationError(f"{method_name}() {slot_name} must be a non-empty string.")
    normalized = value.strip()
    if slot_name == "save_as" and normalized == "current":
        raise FlowValidationError(f"{method_name}() save_as cannot overwrite the runtime-owned 'current' slot.")
    return normalized


def _validate_label(*, method_name: str, label: str | None) -> str | None:
    """Validate one optional user-facing step label."""
    if label is None:
        return None
    if not isinstance(label, str) or not label.strip():
        raise FlowValidationError(f"{method_name}() label must be a non-empty string.")
    return label.strip()


__all__ = [
    "_callable_identifier",
    "_callable_name",
    "_normalize_extensions",
    "_normalize_watch_times",
    "_parse_duration",
    "_parse_schedule_at",
    "_resolve_flow_path",
    "_title_case_words",
    "_validate_label",
    "_validate_slot_name",
]
