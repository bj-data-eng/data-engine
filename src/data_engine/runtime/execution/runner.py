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
        started_at_utc: str,
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
        started_at_utc: str,
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
    runtime_ledger: RuntimeStateWriterPort
    log_emitter: RuntimeEventWriterPort
    stop_controller: RuntimeStopPort


class FlowRunExecutor:
    """Own one-run lifecycle, step execution, and ledger/log updates."""

    def __init__(self, ports: FlowRunExecutionPorts) -> None:
        self.ports = ports

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
        run_started = monotonic()
        signature = self.ports.polling.poll_source_signature(flow, source_path)
        effective_signatures = batch_signatures or ((signature,) if signature is not None else ())
        started_at_utc = str(context.metadata["started_at_utc"])
        normalized_source_path = signature.source_path if signature is not None else self.ports.polling.normalized_source_path(source_path)
        self.ports.runtime_ledger.record_run_started(
            run_id=run_id,
            flow_name=context.flow_name,
            group_name=context.group,
            source_path=normalized_source_path,
            started_at_utc=started_at_utc,
        )
        for effective_signature in effective_signatures:
            self.ports.runtime_ledger.upsert_file_state(flow_name=context.flow_name, signature=effective_signature, status="started")
        self.ports.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="started")
        try:
            self._ensure_runtime_sources_available(flow, context, source_path)
            for step in flow.steps:
                self.ports.stop_controller.check_run(run_id)
                self._load_current_for_step(context, step)
                step_started = monotonic()
                step_started_at_utc = utcnow_text()
                step_run_id = self.ports.runtime_ledger.record_step_started(
                    run_id=run_id,
                    flow_name=context.flow_name,
                    step_label=step.label,
                    started_at_utc=step_started_at_utc,
                )
                self.ports.log_emitter.log_step_event(run_id, context.flow_name, step.label, source_path, status="started")
                try:
                    result = step.fn(context)
                except FlowStoppedError:
                    raise
                except Exception as exc:
                    failure = FlowExecutionError(
                        flow_name=context.flow_name,
                        phase="step",
                        step_label=step.label,
                        function_name=step.function_name,
                        source_path=source_path,
                        detail=f"{type(exc).__name__}: {exc}",
                    )
                    elapsed_ms = max(int((monotonic() - step_started) * 1000), 0)
                    self.ports.runtime_ledger.record_step_finished(
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
                context.current = result
                if step.save_as is not None:
                    context.objects[step.save_as] = result
                if isinstance(result, Path) and result.exists():
                    step_outputs = context.metadata.setdefault("step_outputs", {})
                    if isinstance(step_outputs, dict):
                        step_outputs[step.label] = result
                elapsed = monotonic() - step_started
                elapsed_ms = max(int(elapsed * 1000), 0)
                self.ports.runtime_ledger.record_step_finished(
                    step_run_id=step_run_id,
                    status="success",
                    finished_at_utc=utcnow_text(),
                    elapsed_ms=elapsed_ms,
                    output_path=str(result.resolve()) if isinstance(result, Path) and result.exists() else None,
                )
                self.ports.log_emitter.log_step_event(run_id, context.flow_name, step.label, source_path, status="success", elapsed=elapsed)
        except FlowStoppedError as exc:
            finished_at_utc = utcnow_text()
            self.ports.runtime_ledger.record_run_finished(run_id=run_id, status="stopped", finished_at_utc=finished_at_utc, error_text=str(exc))
            for effective_signature in effective_signatures:
                self.ports.runtime_ledger.upsert_file_state(
                    flow_name=context.flow_name,
                    signature=effective_signature,
                    status="stopped",
                    error_text=str(exc),
                )
            self.ports.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="stopped", elapsed=monotonic() - run_started)
            raise
        except Exception as exc:
            elapsed = monotonic() - run_started
            finished_at_utc = utcnow_text()
            failed_step = step.label if "step" in locals() else None
            failure_text = str(exc)
            self.ports.runtime_ledger.record_run_finished(run_id=run_id, status="failed", finished_at_utc=finished_at_utc, error_text=failure_text)
            for effective_signature in effective_signatures:
                self.ports.runtime_ledger.upsert_file_state(
                    flow_name=context.flow_name,
                    signature=effective_signature,
                    status="failed",
                    error_text=failure_text,
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
            raise
        total = monotonic() - run_started
        finished_at_utc = utcnow_text()
        self.ports.runtime_ledger.record_run_finished(run_id=run_id, status="success", finished_at_utc=finished_at_utc)
        for effective_signature in effective_signatures:
            self.ports.runtime_ledger.upsert_file_state(
                flow_name=context.flow_name,
                signature=effective_signature,
                status="success",
                run_id=run_id,
                finished_at_utc=finished_at_utc,
            )
        self.ports.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="success", elapsed=total)
        return context

    def preview_one(self, flow: "Flow", source_path: "Path | None", *, use: str | None) -> FlowContext:
        self.ports.stop_controller.check_run(None)
        context = self.ports.context_builder.build(flow, source_path, run_id="preview")
        self._ensure_runtime_sources_available(flow, context, source_path)
        for step in flow.steps:
            self.ports.stop_controller.check_run("preview")
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
