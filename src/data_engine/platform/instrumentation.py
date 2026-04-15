"""Dev-only timing and trace helpers for local instrumentation."""

from __future__ import annotations

import atexit
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path
import threading
import time
from typing import Any
from uuid import uuid4

from data_engine.domain.time import utcnow_text
from data_engine.platform.identity import env_var


DATA_ENGINE_DEV_INSTRUMENT_ENV_VAR = env_var("dev_instrument")
DATA_ENGINE_DEV_VIZTRACE_ENV_VAR = env_var("dev_viztrace")
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_WRITE_LOCK = threading.RLock()
_VIZTRACER_LOCK = threading.RLock()
_ACTIVE_VIZTRACERS: dict[str, Any] = {}
_REGISTERED_VIZTRACERS: set[str] = set()


def _env_enabled(name: str) -> bool:
    value = __import__("os").environ.get(name, "")
    return value.strip().lower() in _TRUE_VALUES


def dev_instrumentation_enabled() -> bool:
    """Return whether dev timing instrumentation is enabled."""
    return _env_enabled(DATA_ENGINE_DEV_INSTRUMENT_ENV_VAR)


def dev_viztrace_enabled() -> bool:
    """Return whether VizTracer capture is enabled."""
    return _env_enabled(DATA_ENGINE_DEV_VIZTRACE_ENV_VAR)


def new_request_id(prefix: str = "req") -> str:
    """Return one short request id for correlating client and daemon timings."""
    normalized = prefix.strip().lower() or "req"
    return f"{normalized}-{uuid4().hex[:12]}"


def append_timing_line(
    log_path: Path | None,
    *,
    scope: str,
    event: str,
    phase: str = "mark",
    elapsed_ms: float | None = None,
    fields: Mapping[str, object] | None = None,
) -> None:
    """Append one structured timing line when dev instrumentation is enabled."""
    if log_path is None or not dev_instrumentation_enabled():
        return
    parts = [
        utcnow_text(),
        f"scope={scope}",
        f"event={event}",
        f"phase={phase}",
    ]
    if elapsed_ms is not None:
        parts.append(f"elapsed_ms={elapsed_ms:.3f}")
    if fields:
        for key in sorted(fields):
            value = fields[key]
            if value is None:
                continue
            text = str(value).replace("\n", "\\n").strip()
            if not text:
                continue
            parts.append(f"{key}={text}")
    line = " ".join(parts) + "\n"
    try:
        with _WRITE_LOCK:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(line)
    except Exception:
        pass


@contextmanager
def timed_operation(
    log_path: Path | None,
    *,
    scope: str,
    event: str,
    fields: Mapping[str, object] | None = None,
    threshold_ms: float = 200.0,
):
    """Record one sampled timing line for a slow dev-instrumented operation."""
    if log_path is None or not dev_instrumentation_enabled():
        yield
        return
    started = time.perf_counter()
    try:
        yield
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        append_timing_line(
            log_path,
            scope=scope,
            event=event,
            phase="error",
            elapsed_ms=elapsed_ms,
            fields={**dict(fields or {}), "error": type(exc).__name__},
        )
        raise
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if elapsed_ms < threshold_ms:
        return
    append_timing_line(
        log_path,
        scope=scope,
        event=event,
        phase="end",
        elapsed_ms=elapsed_ms,
        fields=fields,
    )


def maybe_start_viztracer(
    output_path: Path | None,
    *,
    process_name: str,
) -> object | None:
    """Start one long-lived VizTracer capture when explicitly requested."""
    if output_path is None or not dev_instrumentation_enabled() or not dev_viztrace_enabled():
        return None
    key = str(output_path)
    with _VIZTRACER_LOCK:
        existing = _ACTIVE_VIZTRACERS.get(key)
        if existing is not None:
            return existing
        try:
            from viztracer import VizTracer
        except Exception:
            return None
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            tracer = VizTracer(
                output_file=str(output_path),
                process_name=process_name,
                log_sparse=True,
                log_async=True,
                pid_suffix=True,
            )
            tracer.start()
        except Exception:
            return None
        _ACTIVE_VIZTRACERS[key] = tracer
        if key not in _REGISTERED_VIZTRACERS:
            atexit.register(_stop_viztracer, key)
            _REGISTERED_VIZTRACERS.add(key)
        return tracer


def _stop_viztracer(key: str) -> None:
    with _VIZTRACER_LOCK:
        tracer = _ACTIVE_VIZTRACERS.pop(key, None)
    if tracer is None:
        return
    try:
        tracer.stop()
        tracer.save()
    except Exception:
        pass


__all__ = [
    "DATA_ENGINE_DEV_INSTRUMENT_ENV_VAR",
    "DATA_ENGINE_DEV_VIZTRACE_ENV_VAR",
    "append_timing_line",
    "dev_instrumentation_enabled",
    "dev_viztrace_enabled",
    "maybe_start_viztracer",
    "new_request_id",
    "timed_operation",
]
