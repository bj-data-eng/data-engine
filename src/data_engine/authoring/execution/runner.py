"""Single flow-run execution lifecycle for authored flows."""

from __future__ import annotations

from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING

from data_engine.authoring.model import FlowExecutionError, FlowStoppedError, FlowValidationError
from data_engine.authoring.primitives import FlowContext, WatchSpec
from data_engine.domain.time import utcnow_text

if TYPE_CHECKING:
    from data_engine.authoring.execution.single import _FlowRuntime
    from data_engine.authoring.flow import Flow
    from data_engine.authoring.primitives import StepSpec


class FlowRunExecutor:
    """Own one-run lifecycle, step execution, and ledger/log updates."""

    def __init__(self, runtime: "_FlowRuntime") -> None:
        self.runtime = runtime

    def run_one(self, flow: "Flow", source_path: "Path | None", *, batch_signatures=()) -> FlowContext:
        self.runtime._check_flow_stop()
        run_id = self.runtime.context_builder.new_run_id()
        context = self.runtime.context_builder.build(flow, source_path, run_id=run_id)
        run_started = monotonic()
        signature = self.runtime.polling.poll_source_signature(flow, source_path)
        effective_signatures = batch_signatures or ((signature,) if signature is not None else ())
        started_at_utc = str(context.metadata["started_at_utc"])
        normalized_source_path = signature.source_path if signature is not None else self.runtime.polling.normalized_source_path(source_path)
        self.runtime.runtime_ledger.record_run_started(
            run_id=run_id,
            flow_name=context.flow_name,
            group_name=context.group,
            source_path=normalized_source_path,
            started_at_utc=started_at_utc,
        )
        for effective_signature in effective_signatures:
            self.runtime.runtime_ledger.upsert_file_state(flow_name=context.flow_name, signature=effective_signature, status="started")
        self.runtime.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="started")
        try:
            self._ensure_runtime_sources_available(flow, context, source_path)
            for step in flow.steps:
                self.runtime._check_flow_stop()
                self._load_current_for_step(context, step)
                step_started = monotonic()
                step_started_at_utc = utcnow_text()
                step_run_id = self.runtime.runtime_ledger.record_step_started(
                    run_id=run_id,
                    flow_name=context.flow_name,
                    step_label=step.label,
                    started_at_utc=step_started_at_utc,
                )
                self.runtime.log_emitter.log_step_event(run_id, context.flow_name, step.label, source_path, status="started")
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
                    self.runtime.runtime_ledger.record_step_finished(
                        step_run_id=step_run_id,
                        status="failed",
                        finished_at_utc=utcnow_text(),
                        elapsed_ms=elapsed_ms,
                        error_text=str(failure),
                    )
                    self.runtime.log_emitter.log_runtime_message(
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
                self.runtime.runtime_ledger.record_step_finished(
                    step_run_id=step_run_id,
                    status="success",
                    finished_at_utc=utcnow_text(),
                    elapsed_ms=elapsed_ms,
                    output_path=str(result.resolve()) if isinstance(result, Path) and result.exists() else None,
                )
                self.runtime.log_emitter.log_step_event(run_id, context.flow_name, step.label, source_path, status="success", elapsed=elapsed)
        except FlowStoppedError as exc:
            finished_at_utc = utcnow_text()
            self.runtime.runtime_ledger.record_run_finished(run_id=run_id, status="stopped", finished_at_utc=finished_at_utc, error_text=str(exc))
            for effective_signature in effective_signatures:
                self.runtime.runtime_ledger.upsert_file_state(
                    flow_name=context.flow_name,
                    signature=effective_signature,
                    status="stopped",
                    error_text=str(exc),
                )
            self.runtime.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="stopped", elapsed=monotonic() - run_started)
            raise
        except Exception as exc:
            elapsed = monotonic() - run_started
            finished_at_utc = utcnow_text()
            failed_step = step.label if "step" in locals() else None
            failure_text = str(exc)
            self.runtime.runtime_ledger.record_run_finished(run_id=run_id, status="failed", finished_at_utc=finished_at_utc, error_text=failure_text)
            for effective_signature in effective_signatures:
                self.runtime.runtime_ledger.upsert_file_state(
                    flow_name=context.flow_name,
                    signature=effective_signature,
                    status="failed",
                    error_text=failure_text,
                )
            if failed_step is None:
                self.runtime.log_emitter.log_runtime_message(failure_text, level="error", run_id=run_id, flow_name=context.flow_name)
                self.runtime.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="failed", elapsed=elapsed, level="error", exc_info=True)
            else:
                self.runtime.log_emitter.log_step_event(
                    run_id,
                    context.flow_name,
                    failed_step,
                    source_path,
                    status="failed",
                    elapsed=elapsed,
                    level="error",
                    exc_info=True,
                )
                self.runtime.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="failed", elapsed=elapsed)
            raise
        total = monotonic() - run_started
        finished_at_utc = utcnow_text()
        self.runtime.runtime_ledger.record_run_finished(run_id=run_id, status="success", finished_at_utc=finished_at_utc)
        for effective_signature in effective_signatures:
            self.runtime.runtime_ledger.upsert_file_state(
                flow_name=context.flow_name,
                signature=effective_signature,
                status="success",
                run_id=run_id,
                finished_at_utc=finished_at_utc,
            )
        self.runtime.log_emitter.log_flow_event(run_id, context.flow_name, source_path, status="success", elapsed=total)
        return context

    def preview_one(self, flow: "Flow", source_path: "Path | None", *, use: str | None) -> FlowContext:
        self.runtime._check_flow_stop()
        context = self.runtime.context_builder.build(flow, source_path, run_id="preview")
        self._ensure_runtime_sources_available(flow, context, source_path)
        for step in flow.steps:
            self.runtime._check_flow_stop()
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


__all__ = ["FlowRunExecutor"]
