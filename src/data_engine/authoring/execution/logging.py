"""Runtime log and ledger emission helpers for authored flows."""

from __future__ import annotations

import logging
from pathlib import Path

from data_engine.domain.time import utcnow_text
from data_engine.runtime.runtime_db import RuntimeLedger

LOGGER = logging.getLogger(__name__)


class RuntimeLogEmitter:
    """Own runtime log persistence and logger emission."""

    def __init__(self, runtime_ledger: RuntimeLedger) -> None:
        self.runtime_ledger = runtime_ledger

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
        self.runtime_ledger.append_log(
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


__all__ = ["RuntimeLogEmitter"]
