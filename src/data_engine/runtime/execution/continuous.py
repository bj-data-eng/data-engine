"""Continuous polling loop for one sequential flow runtime."""

from __future__ import annotations

from collections import deque
from time import monotonic, sleep
from typing import TYPE_CHECKING

from data_engine.runtime.execution.context import _QueuedJob
from data_engine.core.model import FlowStoppedError
from data_engine.core.primitives import WatchSpec
from data_engine.runtime.file_watch import PollingWatcher

if TYPE_CHECKING:
    from data_engine.runtime.execution.single import _FlowRuntime
    from data_engine.core.primitives import FlowContext


class ContinuousRuntimeLoop:
    """Own the polling loop for one sequential runtime."""

    def __init__(self, runtime: "_FlowRuntime") -> None:
        self.runtime = runtime

    def run(self) -> list["FlowContext"]:
        results: list["FlowContext"] = []
        queue: deque[_QueuedJob] = deque()
        queued_keys: set[tuple[str, str | None]] = set()
        watch_entries: list[dict[str, object]] = []
        now = monotonic()

        for flow in self.runtime.flows:
            if flow.trigger is None:
                for source_path in self.runtime.polling.startup_sources(flow):
                    self.runtime.polling.enqueue_job(queue, queued_keys, flow, source_path)
                continue
            if isinstance(flow.trigger, WatchSpec) and flow.trigger.mode == "poll":
                watcher = self.runtime.polling.make_watcher(flow.trigger)
                watcher.start()
                for source_path in self.runtime.polling.stale_poll_sources(flow):
                    batch_signatures = self.runtime.polling.stale_batch_poll_signatures(flow) if source_path is None else ()
                    self.runtime.polling.enqueue_job(queue, queued_keys, flow, source_path, batch_signatures=batch_signatures)
                watch_entries.append(
                    {
                        "flow": flow,
                        "interval": flow.trigger.interval_seconds,
                        "next_poll": now + float(flow.trigger.interval_seconds),
                        "watcher": watcher,
                    }
                )

        self.runtime._emit_status("Polling watcher running.")
        try:
            while True:
                if self.runtime.runtime_stop_event is not None and self.runtime.runtime_stop_event.is_set():
                    self.runtime._emit_status("Polling watcher stopped.")
                    break
                now = monotonic()
                self._poll_watch_entries(watch_entries, queue, queued_keys, now)

                job = queue.popleft() if queue else None
                if job is not None:
                    queued_keys.discard(self.runtime.polling.job_key(job.flow, job.source_path))
                    try:
                        results.append(
                            self.runtime.run_executor.run_one(
                                job.flow,
                                job.source_path,
                                batch_signatures=job.batch_signatures,
                            )
                        )
                    except FlowStoppedError:
                        if self.runtime.flow_stop_event is not None:
                            self.runtime.flow_stop_event.clear()
                    except Exception:
                        continue
                    continue

                sleep(0.05)
        finally:
            for entry in watch_entries:
                watcher = entry["watcher"]
                if isinstance(watcher, PollingWatcher):
                    watcher.stop()
        return results

    def _poll_watch_entries(
        self,
        watch_entries: list[dict[str, object]],
        queue: deque[_QueuedJob],
        queued_keys: set[tuple[str, str | None]],
        now: float,
    ) -> None:
        for entry in watch_entries:
            if now < entry["next_poll"]:
                continue
            watched_flow = entry["flow"]
            watcher = entry["watcher"]
            assert isinstance(watcher, PollingWatcher)
            for path in watcher.drain_events():
                watched_trigger = watched_flow.trigger
                assert isinstance(watched_trigger, WatchSpec)
                if watched_trigger.run_as == "batch" and watched_trigger.source is not None and watched_trigger.source.is_dir():
                    signature = self.runtime.polling.poll_source_signature(watched_flow, path)
                    self.runtime.polling.enqueue_job(
                        queue,
                        queued_keys,
                        watched_flow,
                        None,
                        batch_signatures=(signature,) if signature is not None else (),
                    )
                    break
                if self.runtime.polling.is_poll_source_stale(watched_flow, path):
                    self.runtime.polling.enqueue_job(queue, queued_keys, watched_flow, path)
            entry["next_poll"] = now + float(entry["interval"])

__all__ = ["ContinuousRuntimeLoop"]
