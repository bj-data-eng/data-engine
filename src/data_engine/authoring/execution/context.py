"""Runtime context building helpers for authored flows."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from data_engine.authoring.primitives import FlowContext, MirrorContext, SourceContext, WatchSpec, WorkspaceConfigContext
from data_engine.domain.source_state import SourceSignature
from data_engine.domain.time import utcnow_text

if TYPE_CHECKING:
    from data_engine.authoring.flow import Flow


@dataclass(frozen=True)
class _QueuedJob:
    """One queued runtime job for a flow and an optional concrete source file."""

    flow: "Flow"
    source_path: Path | None
    batch_signatures: tuple[SourceSignature, ...] = ()


class RuntimeContextBuilder:
    """Build runtime flow contexts for concrete or root-level executions."""

    @staticmethod
    def _source_key_text(*, source_path: Path | None, relative_path: Path | None) -> str | None:
        if relative_path is not None:
            return relative_path.as_posix()
        if source_path is not None:
            return source_path.as_posix()
        return None

    @classmethod
    def _source_file_hash(cls, *, source_path: Path | None, relative_path: Path | None) -> str | None:
        source_key = cls._source_key_text(source_path=source_path, relative_path=relative_path)
        if source_key is None:
            return None
        return hashlib.sha1(source_key.encode("utf-8")).hexdigest()

    def new_run_id(self) -> str:
        return uuid4().hex

    def build(self, flow: "Flow", source_path: Path | None, *, run_id: str) -> FlowContext:
        metadata: dict[str, object] = {"started_at_utc": utcnow_text(), "run_id": run_id, "step_outputs": {}}
        context = FlowContext(
            flow_name=flow.name,
            group=flow.group,
            metadata=metadata,
            config=WorkspaceConfigContext(workspace_root=flow._workspace_root),
        )
        source_root: Path | None = None
        resolved_source_path: Path | None = None
        relative_path: Path | None = None
        trigger = flow.trigger
        if isinstance(trigger, WatchSpec) and trigger.source is not None:
            if trigger.source.exists() and trigger.source.is_dir():
                source_root = trigger.source
                if source_path is not None:
                    resolved_source_path = source_path
                    relative_path = source_path.relative_to(trigger.source)
            elif trigger.source.exists() and trigger.source.is_file():
                resolved_source = source_path if source_path is not None else trigger.source
                resolved_source_path = resolved_source
                source_root = trigger.source.parent
                relative_path = Path(resolved_source.name)
        if source_root is not None:
            context.source = SourceContext(root=source_root, path=resolved_source_path, relative_path=relative_path)
        file_hash = self._source_file_hash(source_path=resolved_source_path, relative_path=relative_path)
        if file_hash is not None:
            context.metadata["file_hash"] = file_hash
        if flow.mirror_spec is not None:
            context.mirror = MirrorContext(
                root=flow.mirror_spec.root,
                source_path=context.source.path if context.source is not None else None,
                relative_path=context.source.relative_path if context.source is not None else None,
            )
        return context


__all__ = ["RuntimeContextBuilder", "_QueuedJob"]
