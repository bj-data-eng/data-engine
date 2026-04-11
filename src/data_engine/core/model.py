"""Core errors for Data Engine flow definitions and execution."""

from __future__ import annotations

from pathlib import Path


class FlowValidationError(ValueError):
    """Raised when a flow configuration or runtime input cannot be validated."""


class FlowStoppedError(RuntimeError):
    """Raised when a running flow is stopped by an external control."""


class FlowExecutionError(FlowValidationError):
    """Raised when a flow module fails during import, build, or runtime execution."""

    def __init__(
        self,
        *,
        flow_name: str,
        phase: str,
        detail: str,
        step_label: str | None = None,
        function_name: str | None = None,
        source_path: Path | str | None = None,
    ) -> None:
        self.flow_name = flow_name
        self.phase = phase
        self.detail = detail
        self.step_label = step_label
        self.function_name = function_name
        self.source_path = str(source_path) if source_path is not None else None
        super().__init__(self._render())

    def _render(self) -> str:
        if self.phase == "step":
            message = f'Flow "{self.flow_name}" failed in step "{self.step_label or "Unknown Step"}"'
            if self.function_name:
                message = f"{message} (function {self.function_name})"
            if self.source_path:
                message = f'{message} for source "{self.source_path}"'
            return f"{message}: {self.detail}"
        if self.phase == "build":
            if self.function_name:
                return f'Flow module "{self.flow_name}" failed during build() in {self.function_name}: {self.detail}'
            return f'Flow module "{self.flow_name}" failed during build(): {self.detail}'
        if self.phase == "import":
            return f'Flow module "{self.flow_name}" failed during import: {self.detail}'
        if self.phase == "compile":
            return f'Flow module "{self.flow_name}" failed during compilation: {self.detail}'
        return f'Flow module "{self.flow_name}" failed during {self.phase}: {self.detail}'


__all__ = [
    "FlowExecutionError",
    "FlowStoppedError",
    "FlowValidationError",
]
