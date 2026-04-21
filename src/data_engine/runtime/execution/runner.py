"""Single flow-run execution lifecycle for authored flows."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING, Protocol

from data_engine.core.model import FlowExecutionError, FlowStoppedError, FlowValidationError
from data_engine.core.primitives import FlowContext, WatchSpec
from data_engine.domain.source_state import SourceSignature
from data_engine.domain.time import utcnow_text
from data_engine.platform.instrumentation import append_timing_line

if TYPE_CHECKING:
    from data_engine.core.flow import Flow
    from data_engine.core.primitives import StepSpec


class FlowContextBuilderPort(Protocol):
    """Interface for building runtime contexts for concrete flow runs."""

    def new_run_id(self) -> str:
        """Return a new runtime run id."""

    def build(self, flow: "Flow", source_path: Path | None, *, run_id: str) -> FlowContext:
        """Build a runtime context for one flow/source pair."""


class RuntimeSourceStatePort(Protocol):
    """Interface for source signatures and normalized source identity."""

    def poll_source_signature(self, flow: "Flow", source_path: Path | None) -> SourceSignature | None:
        """Return the current source signature for a polled source."""

    def normalized_source_path(self, source_path: Path | None) -> str | None:
        """Return the normalized persisted source path."""


class RuntimeStateWriterPort(Protocol):
    """Interface for writing runtime run, step, and source state."""

    def record_run_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        group_name: str,
        source_path: str | None,
        started_at_utc: str | None = None,
    ) -> None:
        """Record that one flow run started."""

    def record_run_finished(
        self,
        *,
        run_id: str,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> None:
        """Record that one flow run finished."""

    def record_step_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        step_label: str,
        started_at_utc: str | None = None,
    ) -> int:
        """Record that one step started and return the persisted step id."""

    def record_step_finished(
        self,
        *,
        step_run_id: int,
        status: str,
        finished_at_utc: str,
        elapsed_ms: int | None,
        error_text: str | None = None,
        output_path: str | None = None,
    ) -> None:
        """Record that one step finished."""

    def upsert_file_state(
        self,
        *,
        flow_name: str,
        signature: SourceSignature,
        status: str,
        run_id: str | None = None,
        finished_at_utc: str | None = None,
        error_text: str | None = None,
    ) -> None:
        """Write source freshness state for one polled file."""


class RuntimeEventWriterPort(Protocol):
    """Interface for runtime event emission."""

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
        """Emit one runtime message."""

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
        """Emit one flow event."""

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
        """Emit one step event."""


class RuntimeStopPort(Protocol):
    """Interface for registering and checking run-id stop requests."""

    def register_run(self, run_id: str) -> None:
        """Mark one run id as active."""

    def unregister_run(self, run_id: str) -> None:
        """Clear active and requested state for one completed run id."""

    def check_run(self, run_id: str | None) -> None:
        """Raise when stop was requested for one run id."""


@dataclass(frozen=True)
class FlowRunExecutionPorts:
    """Explicit collaborators needed to execute one flow run."""

    context_builder: FlowContextBuilderPort
    polling: RuntimeSourceStatePort
    state_writer: RuntimeStateWriterPort
    log_emitter: RuntimeEventWriterPort
    stop_controller: RuntimeStopPort
    timing_log_path: Path | None = None
    execution_mode: str = "oneshot"


class FlowRunExecutor:
    """Own one-run lifecycle, step execution, and ledger/log updates."""

    def __init__(self, ports: FlowRunExecutionPorts) -> None:
        self.ports = ports

    def _mark_timing(
        self,
        event: str,
        *,
        run_id: str | None,
        flow_name: str | None,
        step_label: str | None = None,
        source_path: Path | str | None = None,
        elapsed_ms: float | None = None,
        extra_fields: dict[str, object] | None = None,
    ) -> None:
        fields: dict[str, object] = {
            "execution_mode": self.ports.execution_mode,
            "run_id": run_id,
            "flow_name": flow_name,
            "step_label": step_label,
            "source_path": str(source_path) if source_path is not None else None,
        }
        if extra_fields:
            fields.update(extra_fields)
        append_timing_line(
            self.ports.timing_log_path,
            scope="runtime.step",
            event=event,
            phase="mark",
            elapsed_ms=elapsed_ms,
            fields=fields,
        )

    def run_one(self, flow: "Flow", source_path: "Path | None", *, batch_signatures=()) -> FlowContext:
        self.ports.stop_controller.check_run(None)
        run_id = self.ports.context_builder.new_run_id()
        self.ports.stop_controller.register_run(run_id)
        try:
            return self._run_one_registered(flow, source_path, run_id=run_id, batch_signatures=batch_signatures)
        finally:
            self.ports.stop_controller.unregister_run(run_id)

    def _run_one_registered(
        self,
        flow: "Flow",
        source_path: "Path | None",
        *,
        run_id: str,
        batch_signatures=(),
    ) -> FlowContext:
        context = self.ports.context_builder.build(flow, source_path, run_id=run_id)
        self._mark_timing(
            "run_context_built",
            run_id=run_id,
            flow_name=context.flow_name,
            source_path=source_path,
            extra_fields={"group_name": context.group},
        )
        signature = self.ports.polling.poll_source_signature(flow, source_path)
        effective_signatures = batch_signatures or ((signature,) if signature is not None else ())
        normalized_source_path = signature.source_path if signature is not None else self.ports.polling.normalized_source_path(source_path)
        self._mark_timing(
            "run_record_started_begin",
            run_id=run_id,
            flow_name=context.flow_name,
            source_path=normalized_source_path,
            extra_fields={"effective_signature_count": len(effective_signatures)},
        )
        self.ports.state_writer.record_run_started(
            run_id=run_id,
            flow_name=context.flow_name,
            group_name=context.group,
            source_path=normalized_source_path,
            started_at_utc=None,
        )
        self._mark_timing(
            "run_record_started_end",
            run_id=run_id,
            flow_name=context.flow_name,
            source_path=normalized_source_path,
        )
        run_started = monotonic()
        for effective_signature in effective_signatures:
            self._mark_timing(
                "file_state_started_begin",
                run_id=run_id,
                flow_name=context.flow_name,
                source_path=effective_signature.source_path,
            )
            self.ports.state_writer.upsert_file_state(flow_name=context.flow_name, signature=effective_signature, status="started")
            self._mark_timing(
                "file_state_started_end",
                run_id=run_id,
                flow_name=context.flow_name,
                source_path=effective_signature.source_path,
            )
        try:
            self._ensure_runtime_sources_available(flow, context, source_path)
            for step in flow.steps:
                self._mark_timing(
                    "step_loop_enter",
                    run_id=run_id,
                    flow_name=context.flow_name,
                    step_label=step.label,
                    source_path=source_path,
                )
                try:
                    self.ports.stop_controller.check_run(run_id)
                except FlowStoppedError:
                    self._mark_timing(
                        "step_stop_detected",
                        run_id=run_id,
                        flow_name=context.flow_name,
                        step_label=step.label,
                        source_path=source_path,
                    )
                    raise
                if context.debug is not None:
                    context.debug.set_step(step.label)
                self._mark_timing(
                    "step_load_current_begin",
                    run_id=run_id,
                    flow_name=context.flow_name,
                    step_label=step.label,
                    source_path=source_path,
                )
                self._load_current_for_step(context, step)
                self._mark_timing(
                    "step_load_current_end",
                    run_id=run_id,
                    flow_name=context.flow_name,
                    step_label=step.label,
                    source_path=source_path,
                )
                persist_started = monotonic()
                step_run_id = self.ports.state_writer.record_step_started(
                    run_id=run_id,
                    flow_name=context.flow_name,
                    step_label=step.label,
                    started_at_utc=None,
                )
                self._mark_timing(
                    "step_record_started_end",
                    run_id=run_id,
                    flow_name=context.flow_name,
                    step_label=step.label,
                    source_path=source_path,
                    elapsed_ms=(monotonic() - persist_started) * 1000.0,
                    extra_fields={"step_run_id": step_run_id},
                )
                step_started = monotonic()
                self._mark_timing(
                    "step_fn_begin",
                    run_id=run_id,
                    flow_name=context.flow_name,
                    step_label=step.label,
                    source_path=source_path,
                    extra_fields={"step_run_id": step_run_id},
                )
                try:
                    result = step.fn(context)
                except FlowStoppedError:
                    self._mark_timing(
                        "step_fn_stopped",
                        run_id=run_id,
                        flow_name=context.flow_name,
                        step_label=step.label,
                        source_path=source_path,
                        elapsed_ms=(monotonic() - step_started) * 1000.0,
                        extra_fields={"step_run_id": step_run_id},
                    )
                    raise
                except Exception as exc:
                    step_elapsed_ms = max(int((monotonic() - step_started) * 1000), 0)
                    self._mark_timing(
                        "step_fn_error",
                        run_id=run_id,
                        flow_name=context.flow_name,
                        step_label=step.label,
                        source_path=source_path,
                        elapsed_ms=step_elapsed_ms,
                        extra_fields={"step_run_id": step_run_id, "error": type(exc).__name__},
                    )
                    failure = FlowExecutionError(
                        flow_name=context.flow_name,
                        phase="step",
                        step_label=step.label,
                        function_name=step.function_name,
                        source_path=source_path,
                        detail=f"{type(exc).__name__}: {exc}",
                    )
                    elapsed_ms = step_elapsed_ms
                    self.ports.state_writer.record_step_finished(
                        step_run_id=step_run_id,
                        status="failed",
                        finished_at_utc=utcnow_text(),
                        elapsed_ms=elapsed_ms,
                        error_text=str(failure),
                    )
                    self.ports.log_emitter.log_runtime_message(
                        str(failure),
                        level="error",
                        run_id=run_id,
                        flow_name=context.flow_name,
                        step_label=step.label,
                    )
                    raise failure from exc
                self._mark_timing(
                    "step_fn_end",
                    run_id=run_id,
                    flow_name=context.flow_name,
                    step_label=step.label,
                    source_path=source_path,
                    elapsed_ms=(monotonic() - step_started) * 1000.0,
                    extra_fields={"step_run_id": step_run_id},
                )
                context.current = result
                if step.save_as is not None:
                    context.objects[step.save_as] = result
                if isinstance(result, Path) and result.exists():
                    step_outputs = context.metadata.setdefault("step_outputs", {})
                    if isinstance(step_outputs, dict):
                        step_outputs[step.label] = result
                elapsed = monotonic() - step_started
                elapsed_ms = max(int(elapsed * 1000), 0)
                persist_finished = monotonic()
                self.ports.state_writer.record_step_finished(
                    step_run_id=step_run_id,
                    status="success",
                    finished_at_utc=utcnow_text(),
                    elapsed_ms=elapsed_ms,
                    output_path=str(result.resolve()) if isinstance(result, Path) and result.exists() else None,
                )
                self._mark_timing(
                    "step_record_finished_end",
                    run_id=run_id,
                    flow_name=context.flow_name,
                    step_label=step.label,
                    source_path=source_path,
                    elapsed_ms=(monotonic() - persist_finished) * 1000.0,
                    extra_fields={
                        "step_run_id": step_run_id,
                        "step_elapsed_ms": elapsed_ms,
                    },
                )
        except FlowStoppedError as exc:
            finished_at_utc = utcnow_text()
            self.ports.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="stopped", elapsed=monotonic() - run_started)
            self.ports.state_writer.record_run_finished(run_id=run_id, status="stopped", finished_at_utc=finished_at_utc, error_text=str(exc))
            self._mark_timing(
                "run_stopped",
                run_id=run_id,
                flow_name=context.flow_name,
                source_path=source_path,
                elapsed_ms=(monotonic() - run_started) * 1000.0,
                extra_fields={"error": str(exc)},
            )
            for effective_signature in effective_signatures:
                self.ports.state_writer.upsert_file_state(
                    flow_name=context.flow_name,
                    signature=effective_signature,
                    status="stopped",
                    error_text=str(exc),
                )
            raise
        except Exception as exc:
            elapsed = monotonic() - run_started
            finished_at_utc = utcnow_text()
            failed_step = step.label if "step" in locals() else None
            failure_text = str(exc)
            self._mark_timing(
                "run_failed",
                run_id=run_id,
                flow_name=context.flow_name,
                step_label=failed_step,
                source_path=source_path,
                elapsed_ms=elapsed * 1000.0,
                extra_fields={"error": type(exc).__name__},
            )
            if failed_step is None:
                self.ports.log_emitter.log_runtime_message(failure_text, level="error", run_id=run_id, flow_name=context.flow_name)
                self.ports.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="failed", elapsed=elapsed, level="error", exc_info=True)
            else:
                self.ports.log_emitter.log_step_event(
                    run_id,
                    context.flow_name,
                    failed_step,
                    source_path,
                    status="failed",
                    elapsed=elapsed,
                    level="error",
                    exc_info=True,
                )
                self.ports.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="failed", elapsed=elapsed)
            self.ports.state_writer.record_run_finished(run_id=run_id, status="failed", finished_at_utc=finished_at_utc, error_text=failure_text)
            for effective_signature in effective_signatures:
                self.ports.state_writer.upsert_file_state(
                    flow_name=context.flow_name,
                    signature=effective_signature,
                    status="failed",
                    error_text=failure_text,
                )
            raise
        finished_at_utc = utcnow_text()
        self.ports.state_writer.record_run_finished(run_id=run_id, status="success", finished_at_utc=finished_at_utc)
        self._mark_timing(
            "run_finished",
            run_id=run_id,
            flow_name=context.flow_name,
            source_path=source_path,
            elapsed_ms=(monotonic() - run_started) * 1000.0,
        )
        for effective_signature in effective_signatures:
            self.ports.state_writer.upsert_file_state(
                flow_name=context.flow_name,
                signature=effective_signature,
                status="success",
                run_id=run_id,
                finished_at_utc=finished_at_utc,
            )
        return context

    def preview_one(self, flow: "Flow", source_path: "Path | None", *, use: str | None) -> FlowContext:
        self.ports.stop_controller.check_run(None)
        context = self.ports.context_builder.build(flow, source_path, run_id="preview")
        self._ensure_runtime_sources_available(flow, context, source_path)
        for step in flow.steps:
            self.ports.stop_controller.check_run("preview")
            if context.debug is not None:
                context.debug.set_step(step.label)
            self._load_current_for_step(context, step)
            try:
                result = step.fn(context)
            except FlowStoppedError:
                raise
            except Exception as exc:
                raise FlowExecutionError(
                    flow_name=context.flow_name,
                    phase="step",
                    step_label=step.label,
                    function_name=step.function_name,
                    source_path=source_path,
                    detail=f"{type(exc).__name__}: {exc}",
                ) from exc
            context.current = result
            if step.save_as is not None:
                context.objects[step.save_as] = result
                if use is not None and step.save_as == use:
                    context.current = result
                    return context
        return context

    def _ensure_runtime_sources_available(self, flow: "Flow", context: FlowContext, source_path: "Path | None") -> None:
        trigger = flow.trigger
        if not isinstance(trigger, WatchSpec) or trigger.source is None:
            return
        if not trigger.source.exists():
            raise FlowValidationError(f"Source path not found: {trigger.source}")
        if trigger.source.is_file():
            source_path = context.source.path if context.source is not None else source_path
            if source_path is None or not source_path.exists():
                raise FlowValidationError(f"Source file not found: {trigger.source}")
            if not source_path.is_file():
                raise FlowValidationError(f"Source file is not a file: {trigger.source}")
        elif not trigger.source.is_dir():
            raise FlowValidationError(f"Source path is neither a file nor a directory: {trigger.source}")

    def _load_current_for_step(self, context: FlowContext, step: "StepSpec") -> None:
        if step.use is None or step.use == "current":
            return
        if step.use not in context.objects:
            raise FlowValidationError(f"Step {step.label!r} requested missing object {step.use!r}.")
        context.current = context.objects[step.use]


__all__ = [
    "FlowContextBuilderPort",
    "FlowRunExecutionPorts",
    "FlowRunExecutor",
    "RuntimeEventWriterPort",
    "RuntimeSourceStatePort",
    "RuntimeStateWriterPort",
    "RuntimeStopPort",
]
