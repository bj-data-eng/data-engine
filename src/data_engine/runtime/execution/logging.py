"""Runtime log and ledger emission helpers for authored flows."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol

from data_engine.domain.time import utcnow_text

LOGGER = logging.getLogger(__name__)


class RuntimeLogSink(Protocol):
    """Interface for persisted runtime log writes."""

    def append_log(
        self,
        *,
        level: str,
        message: str,
        created_at_utc: str,
        run_id: str | None = None,
        flow_name: str | None = None,
        step_label: str | None = None,
    ) -> None:
        """Persist one runtime log line."""


class RuntimeLogEmitter:
    """Own runtime log persistence and logger emission."""

    def __init__(self, log_sink: RuntimeLogSink) -> None:
        self.log_sink = log_sink

    def log_runtime_message(
        self,
        message: str,
        *,
        level: str,
        run_id: str | None,
        flow_name: str | None,
        step_label: str | None = None,
        exc_info: bool = False,
    ) -> None:
        created_at_utc = utcnow_text()
        self.log_sink.append_log(
            level=level.upper(),
            message=message,
            created_at_utc=created_at_utc,
            run_id=run_id,
            flow_name=flow_name,
            step_label=step_label,
        )
        logger_method = LOGGER.error if level == "error" else LOGGER.info
        logger_method(message, exc_info=exc_info)

    def log_flow_event(
        self,
        run_id: str,
        flow_name: str,
        source_path: Path | None,
        *,
        status: str,
        elapsed: float | None = None,
        level: str = "info",
        exc_info: bool = False,
    ) -> None:
        message = f"run={run_id} flow={flow_name} source={source_path} status={status}"
        if elapsed is not None:
            message = f"{message} elapsed={elapsed:.6f}"
        self.log_runtime_message(message, level=level, run_id=run_id, flow_name=flow_name, exc_info=exc_info)

    def log_step_event(
        self,
        run_id: str,
        flow_name: str,
        step_label: str,
        source_path: Path | None,
        *,
        status: str,
        elapsed: float | None = None,
        level: str = "info",
        exc_info: bool = False,
    ) -> None:
        message = f"run={run_id} flow={flow_name} step={step_label} source={source_path} status={status}"
        if elapsed is not None:
            message = f"{message} elapsed={elapsed:.6f}"
        self.log_runtime_message(
            message,
            level=level,
            run_id=run_id,
            flow_name=flow_name,
            step_label=step_label,
            exc_info=exc_info,
        )


__all__ = ["RuntimeLogEmitter", "RuntimeLogSink"]
