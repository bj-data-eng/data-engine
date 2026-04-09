"""Explicit parsed error models shared across operator surfaces."""

from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class StructuredErrorField:
    """One labeled field in a parsed operator error."""

    label: str
    value: str


@dataclass(frozen=True)
class StructuredErrorState:
    """Structured presentation state for one operator-facing error."""

    title: str
    fields: tuple[StructuredErrorField, ...]
    detail: str
    raw_text: str

    @classmethod
    def parse(cls, text: str) -> "StructuredErrorState | None":
        """Parse one known verbose error string into structured fields when possible."""
        step_match = re.fullmatch(
            r'Flow "(?P<flow>[^"]+)" failed in step "(?P<step>[^"]+)"'
            r'(?: \(function (?P<function>[^)]+)\))?'
            r'(?: for source "(?P<source>[^"]+)")?: (?P<detail>.+)',
            text,
        )
        if step_match is not None:
            fields = [
                StructuredErrorField("Flow", step_match.group("flow")),
                StructuredErrorField("Phase", "step"),
                StructuredErrorField("Step", step_match.group("step")),
            ]
            function_name = step_match.group("function")
            source_name = step_match.group("source")
            if function_name:
                fields.append(StructuredErrorField("Function", function_name))
            if source_name:
                fields.append(StructuredErrorField("Source", source_name))
            return cls(
                title="Flow Failed",
                fields=tuple(fields),
                detail=step_match.group("detail"),
                raw_text=text,
            )

        build_match = re.fullmatch(
            r'Flow module "(?P<flow_module>[^"]+)" failed during build\(\)'
            r'(?: in (?P<function>[^:]+))?: (?P<detail>.+)',
            text,
        )
        if build_match is not None:
            fields = [
                StructuredErrorField("Flow Module", build_match.group("flow_module")),
                StructuredErrorField("Phase", "build"),
            ]
            function_name = build_match.group("function")
            if function_name:
                fields.append(StructuredErrorField("Function", function_name))
            return cls(
                title="Flow Module Failed",
                fields=tuple(fields),
                detail=build_match.group("detail"),
                raw_text=text,
            )

        import_match = re.fullmatch(r'Flow module "(?P<flow_module>[^"]+)" failed during import: (?P<detail>.+)', text)
        if import_match is not None:
            return cls(
                title="Flow Module Failed",
                fields=(
                    StructuredErrorField("Flow Module", import_match.group("flow_module")),
                    StructuredErrorField("Phase", "import"),
                ),
                detail=import_match.group("detail"),
                raw_text=text,
            )

        missing_match = re.fullmatch(
            r"Flow module '(?P<flow_module>[^']+)' is not available in (?P<path>.+?)\. Available flow modules: (?P<available>.+)\.",
            text,
        )
        if missing_match is not None:
            return cls(
                title="Flow Module Not Found",
                fields=(
                    StructuredErrorField("Flow Module", missing_match.group("flow_module")),
                    StructuredErrorField("Workspace", missing_match.group("path")),
                    StructuredErrorField("Available", missing_match.group("available")),
                ),
                detail=text,
                raw_text=text,
            )
        return None


__all__ = ["StructuredErrorField", "StructuredErrorState"]
