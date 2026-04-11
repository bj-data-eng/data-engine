"""Flow DSL and public authoring entrypoints."""

from __future__ import annotations

from dataclasses import dataclass, replace
import inspect
from pathlib import Path
from typing import Callable

from data_engine.authoring.helpers import (
    _callable_identifier,
    _callable_name,
    _normalize_extensions,
    _normalize_watch_times,
    _parse_duration,
    _parse_schedule_at,
    _resolve_flow_path,
    _validate_label,
    _validate_slot_name,
)
from data_engine.authoring.model import FlowValidationError
from data_engine.authoring.primitives import Batch, FlowContext, MirrorSpec, StepSpec, WatchSpec, collect_files
from data_engine.flow_modules.flow_module_loader import (
    in_compiled_flow_module_context,
)
from data_engine.authoring.services import AuthoringServices, build_authoring_services, default_authoring_services
from data_engine.services.flow_execution import FlowExecutionService
from data_engine.services.runtime_execution import RuntimeExecutionService


def _resolve_authoring_services(
    *,
    authoring_services: AuthoringServices | None = None,
    runtime_execution_service: RuntimeExecutionService | None = None,
    flow_execution_service: FlowExecutionService | None = None,
) -> AuthoringServices:
    """Return one authoring collaborator bundle with explicit overrides applied."""
    services = authoring_services or default_authoring_services()
    if runtime_execution_service is None and flow_execution_service is None:
        return services
    return build_authoring_services(
        runtime_execution_service=runtime_execution_service or services.runtime_execution_service,
        flow_execution_service=flow_execution_service or services.flow_execution_service,
    )


@dataclass(frozen=True)
class Flow:
    """Immutable fluent builder for generic runtime flows.

    Attributes
    ----------
    group : str
        Display group used by the GUI, TUI, and runtime summaries.
    name : str | None
        Stable flow identifier. When omitted in a flow module, the module loader
        can derive it from the module name.
    label : str | None
        Optional human-readable display label.
    trigger : WatchSpec | None
        Runtime trigger configuration.
    mirror_spec : MirrorSpec | None
        Mirrored output path configuration.
    steps : tuple[StepSpec, ...]
        Ordered callable steps to run.

    Notes
    -----
    Builder methods return a new ``Flow`` instance, so calls can be chained.
    """

    group: str
    name: str | None = None
    label: str | None = None
    trigger: WatchSpec | None = None
    mirror_spec: MirrorSpec | None = None
    steps: tuple[StepSpec, ...] = ()
    _workspace_root: Path | None = None

    def __post_init__(self) -> None:
        if self.name is not None and (not isinstance(self.name, str) or not self.name.strip()):
            raise FlowValidationError("Flow name must be a non-empty string when provided.")
        if self.label is not None and (not isinstance(self.label, str) or not self.label.strip()):
            raise FlowValidationError("Flow label must be a non-empty string when provided.")
        if not isinstance(self.group, str) or not self.group.strip():
            raise FlowValidationError("Flow group must be a non-empty string.")

    def _clone(self, **kwargs) -> "Flow":
        return replace(self, **kwargs)

    def _append(self, step: StepSpec) -> "Flow":
        return self._clone(steps=(*self.steps, step))

    def watch(
        self,
        *,
        mode: str,
        run_as: str = "individual",
        source: str | Path | None = None,
        interval: str | None = None,
        time: str | tuple[str, ...] | list[str] | set[str] | None = None,
        extensions: tuple[str, ...] | list[str] | set[str] | None = None,
        settle: int = 1,
    ) -> "Flow":
        """Configure how this flow is triggered.

        Parameters
        ----------
        mode : str
            Trigger mode: ``"manual"``, ``"poll"``, or ``"schedule"``.
        run_as : str
            ``"individual"`` to run once per source file, or ``"batch"`` to run
            once for the full source set.
        source : str | Path | None
            File or directory watched by poll/schedule triggers.
        interval : str | None
            Duration string such as ``"10s"`` or ``"5m"`` for poll intervals or
            recurring schedules.
        time : str | tuple[str, ...] | list[str] | set[str] | None
            One or more ``HH:MM`` daily schedule times.
        extensions : tuple[str, ...] | list[str] | set[str] | None
            Optional file extensions used when discovering source files.
        settle : int
            Polling settle window in seconds before a changed file is queued.

        Returns
        -------
        Flow
            A new flow with the trigger configuration attached.

        Raises
        ------
        FlowValidationError
            If the trigger mode, source, schedule, or polling options are
            inconsistent.
        """
        normalized_mode = str(mode).strip().lower()
        if normalized_mode not in {"manual", "poll", "schedule"}:
            raise FlowValidationError("watch() mode must be one of 'manual', 'poll', or 'schedule'.")

        normalized_run_as = str(run_as).strip().lower()
        if normalized_run_as not in {"individual", "batch"}:
            raise FlowValidationError("watch() run_as must be either 'individual' or 'batch'.")

        if not isinstance(settle, int) or settle < 0:
            raise FlowValidationError("watch() settle must be an integer greater than or equal to zero.")

        resolved_source = _resolve_flow_path(source) if source is not None else None
        normalized_extensions = _normalize_extensions(extensions)

        if normalized_mode == "manual":
            if interval is not None or time is not None:
                raise FlowValidationError("watch(mode='manual') does not accept interval= or time=.")
            if settle != 1:
                raise FlowValidationError("watch(mode='manual') does not accept settle=.")
            return self._clone(
                trigger=WatchSpec(
                    mode="manual",
                    run_as=normalized_run_as,
                    source=resolved_source,
                    extensions=normalized_extensions,
                )
            )

        if normalized_mode == "poll":
            if resolved_source is None:
                raise FlowValidationError("watch(mode='poll') requires source=.")
            if interval is None:
                raise FlowValidationError("watch(mode='poll') requires interval=.")
            if time is not None:
                raise FlowValidationError("watch(mode='poll') does not accept time=.")
            return self._clone(
                trigger=WatchSpec(
                    mode="poll",
                    run_as=normalized_run_as,
                    source=resolved_source,
                    interval=interval,
                    interval_seconds=_parse_duration(interval),
                    extensions=normalized_extensions,
                    settle=settle,
                )
            )

        if (interval is None) == (time is None):
            raise FlowValidationError("watch(mode='schedule') accepts exactly one of interval= or time=.")
        if settle != 1:
            raise FlowValidationError("watch(mode='schedule') does not accept settle=.")
        if interval is not None:
            return self._clone(
                trigger=WatchSpec(
                    mode="schedule",
                    run_as=normalized_run_as,
                    source=resolved_source,
                    interval=interval,
                    interval_seconds=_parse_duration(interval),
                    extensions=normalized_extensions,
                )
            )
        assert time is not None
        time_values = _normalize_watch_times(time)
        return self._clone(
            trigger=WatchSpec(
                mode="schedule",
                run_as=normalized_run_as,
                source=resolved_source,
                time=time_values[0] if len(time_values) == 1 else time_values,
                times=time_values,
                time_slots=tuple(_parse_schedule_at(value) for value in time_values),
                extensions=normalized_extensions,
            )
        )

    def mirror(self, *, root: str | Path) -> "Flow":
        """Bind a mirrored output namespace rooted at one directory.

        Parameters
        ----------
        root : str | Path
            Directory used by ``context.mirror`` helpers when writing outputs.

        Returns
        -------
        Flow
            A new flow with mirror output helpers enabled.
        """
        return self._clone(mirror_spec=MirrorSpec(root=_resolve_flow_path(root)))

    def step(
        self,
        fn: Callable[[FlowContext], object],
        *,
        use: str | None = None,
        save_as: str | None = None,
        label: str | None = None,
    ) -> "Flow":
        """Append one callable step to the flow.

        Parameters
        ----------
        fn : Callable[[FlowContext], object]
            Callable that accepts a single ``FlowContext`` and returns the next
            value for ``context.current``.
        use : str | None
            Optional named object slot to load into ``context.current`` before
            the step runs.
        save_as : str | None
            Optional named object slot to store the step result in.
        label : str | None
            Optional display label for logs and UI step summaries.

        Returns
        -------
        Flow
            A new flow with the step appended.

        Raises
        ------
        FlowValidationError
            If ``fn`` is not callable or does not accept exactly one argument.
        """
        if not callable(fn):
            raise FlowValidationError("step() fn must be callable")
        normalized_use = _validate_slot_name(method_name="step", slot_name="use", value=use)
        normalized_save_as = _validate_slot_name(method_name="step", slot_name="save_as", value=save_as)
        normalized_label = _validate_label(method_name="step", label=label)
        signature = inspect.signature(fn)
        if len(signature.parameters) != 1:
            raise FlowValidationError("step() callables must accept exactly one context parameter.")
        return self._append(
            StepSpec(
                fn=fn,
                use=normalized_use,
                save_as=normalized_save_as,
                label=normalized_label or _callable_name(fn),
                function_name=_callable_identifier(fn),
            )
        )

    def map(
        self,
        fn: Callable[..., object],
        *,
        use: str | None = None,
        save_as: str | None = None,
        label: str | None = None,
    ) -> "Flow":
        """Append a step that maps a callable over the current iterable value.

        Parameters
        ----------
        fn : Callable[..., object]
            Callable accepting either ``item`` or ``context, item``.
        use : str | None
            Optional named object slot to map instead of the current value.
        save_as : str | None
            Optional named object slot to store the mapped ``Batch`` result in.
        label : str | None
            Optional display label for logs and UI step summaries.

        Returns
        -------
        Flow
            A new flow with the mapping step appended.

        Raises
        ------
        FlowValidationError
            If ``fn`` is not callable, has an unsupported signature, or the
            mapped current value is not iterable.
        """
        if not callable(fn):
            raise FlowValidationError("map() fn must be callable")
        normalized_use = _validate_slot_name(method_name="map", slot_name="use", value=use)
        normalized_save_as = _validate_slot_name(method_name="map", slot_name="save_as", value=save_as)
        normalized_label = _validate_label(method_name="map", label=label)
        signature = inspect.signature(fn)
        parameter_count = len(signature.parameters)
        if parameter_count not in {1, 2}:
            raise FlowValidationError("map() callables must accept either (item) or (context, item).")

        def _run_each(context: FlowContext):
            current = context.current
            if isinstance(current, Batch):
                items = current.items
            elif current is None or isinstance(current, (str, bytes, dict)):
                raise FlowValidationError("map() requires an iterable current value.")
            else:
                try:
                    items = tuple(current)
                except TypeError as exc:
                    raise FlowValidationError("map() requires an iterable current value.") from exc
            if not items:
                raise FlowValidationError("map() requires at least one item.")
            if parameter_count == 1:
                return Batch(tuple(fn(item) for item in items))
            return Batch(tuple(fn(context, item) for item in items))

        return self._append(
            StepSpec(
                fn=_run_each,
                use=normalized_use,
                save_as=normalized_save_as,
                label=normalized_label or _callable_name(fn),
                function_name=_callable_identifier(fn),
            )
        )

    def collect(
        self,
        extensions: tuple[str, ...] | list[str] | set[str],
        *,
        root: str | Path | None = None,
        recursive: bool = False,
        use: str | None = None,
        save_as: str | None = None,
        label: str | None = None,
    ) -> "Flow":
        """Append a step that collects source files into a ``Batch``.

        Parameters
        ----------
        extensions : tuple[str, ...] | list[str] | set[str]
            File extensions to include, such as ``(".xlsx", ".csv")``.
        root : str | Path | None
            Optional search root. Defaults to the active source root.
        recursive : bool
            Whether to search child directories.
        use : str | None
            Optional named object slot to load before collecting.
        save_as : str | None
            Optional named object slot to store the collected batch in.
        label : str | None
            Optional display label for logs and UI step summaries.

        Returns
        -------
        Flow
            A new flow with the collection step appended.
        """
        normalized_use = _validate_slot_name(method_name="collect", slot_name="use", value=use)
        normalized_save_as = _validate_slot_name(method_name="collect", slot_name="save_as", value=save_as)
        normalized_label = _validate_label(method_name="collect", label=label)
        return self.step(
            collect_files(extensions, root=root, recursive=recursive),
            use=normalized_use,
            save_as=normalized_save_as,
            label=normalized_label or "Collect Files",
        )

    def step_each(
        self,
        fn: Callable[..., object],
        *,
        use: str | None = None,
        save_as: str | None = None,
        label: str | None = None,
    ) -> "Flow":
        """Alias for ``map`` that reads naturally in step chains.

        Parameters
        ----------
        fn : Callable[..., object]
            Callable accepting either ``item`` or ``context, item``.
        use : str | None
            Optional named object slot to map instead of the current value.
        save_as : str | None
            Optional named object slot to store the mapped ``Batch`` result in.
        label : str | None
            Optional display label for logs and UI step summaries.

        Returns
        -------
        Flow
            A new flow with the per-item step appended.
        """
        return self.map(fn, use=use, save_as=save_as, label=label)

    @property
    def mode(self) -> str:
        if isinstance(self.trigger, WatchSpec):
            return self.trigger.mode
        return "manual"

    def run_once(
        self,
        *,
        authoring_services: AuthoringServices | None = None,
        runtime_execution_service: RuntimeExecutionService | None = None,
    ) -> list[FlowContext]:
        """Run this flow once and return completed runtime contexts.

        Parameters
        ----------
        authoring_services : AuthoringServices | None
            Optional service bundle used by tests or embedded hosts.
        runtime_execution_service : RuntimeExecutionService | None
            Optional runtime execution service override.

        Returns
        -------
        list[FlowContext]
            One context per executed source.
        """
        service = _resolve_authoring_services(
            authoring_services=authoring_services,
            runtime_execution_service=runtime_execution_service,
        ).runtime_execution_service
        return service.run_once(self)

    def preview(
        self,
        *,
        use: str | None = None,
        authoring_services: AuthoringServices | None = None,
        runtime_execution_service: RuntimeExecutionService | None = None,
    ) -> object:
        """Run this flow in preview mode and return one preview value.

        Parameters
        ----------
        use : str | None
            Optional named object slot to preview instead of the final current
            value.
        authoring_services : AuthoringServices | None
            Optional service bundle used by tests or embedded hosts.
        runtime_execution_service : RuntimeExecutionService | None
            Optional runtime execution service override.

        Returns
        -------
        object
            Preview value returned by the runtime execution service.

        Raises
        ------
        FlowValidationError
            If preview is requested from inside a compiled flow module.
        """
        if in_compiled_flow_module_context():
            raise FlowValidationError("preview() is not available inside compiled flow modules.")
        normalized_use = _validate_slot_name(method_name="preview", slot_name="use", value=use)
        service = _resolve_authoring_services(
            authoring_services=authoring_services,
            runtime_execution_service=runtime_execution_service,
        ).runtime_execution_service
        return service.preview(self, use=normalized_use)

    def show(self) -> object:
        """Run this flow once and return the single final current value.

        Returns
        -------
        object
            Final ``context.current`` value.

        Raises
        ------
        FlowValidationError
            If called from a compiled flow module or the flow produces anything
            other than one result.
        """
        if in_compiled_flow_module_context():
            raise FlowValidationError("show() is not available inside compiled flow modules.")
        results = self.run_once()
        if len(results) != 1:
            raise FlowValidationError(f"show() requires exactly one result, found {len(results)}.")
        return results[0].current

    def run(
        self,
        *,
        authoring_services: AuthoringServices | None = None,
        runtime_execution_service: RuntimeExecutionService | None = None,
    ) -> list[FlowContext]:
        """Run this flow continuously according to its trigger.

        Parameters
        ----------
        authoring_services : AuthoringServices | None
            Optional service bundle used by tests or embedded hosts.
        runtime_execution_service : RuntimeExecutionService | None
            Optional runtime execution service override.

        Returns
        -------
        list[FlowContext]
            Completed contexts collected before the runtime exits.
        """
        service = _resolve_authoring_services(
            authoring_services=authoring_services,
            runtime_execution_service=runtime_execution_service,
        ).runtime_execution_service
        return service.run_continuous(self)


def load_flow(
    name: str,
    *,
    data_root: Path | None = None,
    authoring_services: AuthoringServices | None = None,
    flow_execution_service: FlowExecutionService | None = None,
) -> Flow:
    """Load one code-defined flow by flow-module name."""
    service = _resolve_authoring_services(
        authoring_services=authoring_services,
        flow_execution_service=flow_execution_service,
    ).flow_execution_service
    return service.load_flow(name, workspace_root=data_root)


def discover_flows(
    *,
    data_root: Path | None = None,
    authoring_services: AuthoringServices | None = None,
    flow_execution_service: FlowExecutionService | None = None,
) -> tuple[Flow, ...]:
    """Discover and build all code-defined flows from compiled flow modules."""
    service = _resolve_authoring_services(
        authoring_services=authoring_services,
        flow_execution_service=flow_execution_service,
    ).flow_execution_service
    return service.discover_flows(workspace_root=data_root)


def run(
    *flows: Flow,
    authoring_services: AuthoringServices | None = None,
    runtime_execution_service: RuntimeExecutionService | None = None,
) -> list[FlowContext]:
    """Run multiple flows with sequential execution per group and parallel groups."""
    service = _resolve_authoring_services(
        authoring_services=authoring_services,
        runtime_execution_service=runtime_execution_service,
    ).runtime_execution_service
    return service.run_grouped_continuous(tuple(flows))


__all__ = ["Flow", "discover_flows", "load_flow", "run"]
