"""Polling, scheduling, and source-queue helpers for authored flows."""

from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from data_engine.core.primitives import WatchSpec
from data_engine.runtime.execution.context import QueuedRunJob
from data_engine.domain.source_state import SourceSignature
from data_engine.runtime.file_watch import PollingWatcher, iter_candidate_paths

if TYPE_CHECKING:
    from data_engine.core.flow import Flow


class RuntimeSourceStateStore(Protocol):
    """Interface for source freshness state reads and writes."""

    def normalize_path(self, source_path: Path | str) -> str:
        """Normalize a source path for stable persistence and comparisons."""

    def signature_for_path(self, source_path: Path) -> SourceSignature | None:
        """Return the current source signature when available."""

    def is_stale(self, flow_name: str, signature: SourceSignature | None) -> bool:
        """Return whether a source signature should be rerun."""

    def prune_missing(self, *, flow_name: str, current_source_paths: set[str]) -> None:
        """Delete source state for files that are no longer present."""


class RuntimePollingSupport:
    """Own watcher creation, queueing, and stale-source detection."""

    def __init__(self, source_state_store: RuntimeSourceStateStore) -> None:
        self.source_state_store = source_state_store

    def make_watcher(self, trigger: WatchSpec) -> PollingWatcher:
        return PollingWatcher(trigger.source, recursive=True, extensions=trigger.extensions, settle=trigger.settle)

    def startup_sources(self, flow: "Flow", *, allow_missing: bool = False) -> list[Path | None]:
        trigger = flow.trigger
        if not isinstance(trigger, WatchSpec) or trigger.source is None:
            return [None]
        if not trigger.source.exists():
            return [None]
        if trigger.source.is_file():
            return [trigger.source]
        if trigger.run_as == "batch":
            return [None]
        return list(
            iter_candidate_paths(
                trigger.source,
                extensions=trigger.extensions,
                recursive=True,
                allow_missing=allow_missing,
            )
        )

    def enqueue_job(
        self,
        queue: deque[QueuedRunJob],
        queued_keys: set[tuple[str, str | None]],
        flow: "Flow",
        source_path: Path | None,
        *,
        batch_signatures: tuple[SourceSignature, ...] = (),
    ) -> None:
        key = self.job_key(flow, source_path)
        if key in queued_keys:
            if batch_signatures:
                for index, job in enumerate(queue):
                    if self.job_key(job.flow, job.source_path) != key:
                        continue
                    merged = {signature.source_path: signature for signature in job.batch_signatures}
                    for signature in batch_signatures:
                        merged[signature.source_path] = signature
                    queue[index] = QueuedRunJob(
                        flow=job.flow,
                        source_path=job.source_path,
                        batch_signatures=tuple(merged[path] for path in sorted(merged)),
                    )
                    break
            return
        queue.append(QueuedRunJob(flow, source_path, batch_signatures))
        queued_keys.add(key)

    def job_key(self, flow: "Flow", source_path: Path | None) -> tuple[str, str | None]:
        return (flow.name, str(source_path) if source_path is not None else None)

    def stale_poll_sources(self, flow: "Flow") -> list[Path | None]:
        current_source_paths: set[str] = set()
        stale: list[Path | None] = []
        trigger = flow.trigger
        if not isinstance(trigger, WatchSpec) or trigger.mode != "poll" or trigger.source is None:
            return stale
        if not trigger.source.exists():
            return [None]
        if trigger.run_as == "batch" and trigger.source.is_dir():
            for source_path in iter_candidate_paths(trigger.source, extensions=trigger.extensions, recursive=True, allow_missing=True):
                current_source_paths.add(self.source_state_store.normalize_path(source_path))
                if self.is_poll_source_stale(flow, source_path):
                    stale.append(None)
                    break
            self.source_state_store.prune_missing(flow_name=flow.name, current_source_paths=current_source_paths)
            return stale
        for source_path in self.startup_sources(flow, allow_missing=True):
            if source_path is None:
                stale.append(None)
                continue
            current_source_paths.add(self.source_state_store.normalize_path(source_path))
            if self.is_poll_source_stale(flow, source_path):
                stale.append(source_path)
        self.source_state_store.prune_missing(flow_name=flow.name, current_source_paths=current_source_paths)
        return stale

    def stale_batch_poll_signatures(self, flow: "Flow") -> tuple[SourceSignature, ...]:
        trigger = flow.trigger
        if not isinstance(trigger, WatchSpec) or trigger.mode != "poll" or trigger.source is None or not trigger.source.is_dir():
            return ()
        signatures: dict[str, SourceSignature] = {}
        for source_path in iter_candidate_paths(trigger.source, extensions=trigger.extensions, recursive=True, allow_missing=True):
            if not self.is_poll_source_stale(flow, source_path):
                continue
            signature = self.poll_source_signature(flow, source_path)
            if signature is not None:
                signatures[signature.source_path] = signature
        return tuple(signatures[path] for path in sorted(signatures))

    def is_poll_source_stale(self, flow: "Flow", source_path: Path | None) -> bool:
        trigger = flow.trigger
        if not isinstance(trigger, WatchSpec) or trigger.mode != "poll":
            return False
        if source_path is None or not source_path.exists():
            return True
        signature = self.poll_source_signature(flow, source_path)
        if signature is None and trigger.source is not None and trigger.source.exists() and trigger.source.is_file():
            return True
        return self.source_state_store.is_stale(flow.name, signature)

    def poll_source_signature(self, flow: "Flow", source_path: Path | None) -> SourceSignature | None:
        if source_path is None or not isinstance(flow.trigger, WatchSpec) or flow.trigger.mode != "poll":
            return None
        return self.source_state_store.signature_for_path(source_path)

    def normalized_source_path(self, source_path: Path | None) -> str | None:
        if source_path is None:
            return None
        return self.source_state_store.normalize_path(source_path)


__all__ = ["RuntimePollingSupport", "RuntimeSourceStateStore"]
