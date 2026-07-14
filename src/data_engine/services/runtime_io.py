"""Process-wide machine-local runtime IO layer for cached reads and serialized writes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from queue import Queue
import threading
from time import monotonic
from typing import Callable, Generic, TypeVar

from data_engine.domain.source_state import SourceSignature
from data_engine.runtime.ledger_models import PersistedFileState, PersistedLogEntry, PersistedRun, PersistedStepRun
from data_engine.runtime.runtime_cache_store import RuntimeCacheLedger
from data_engine.services.runtime_ports import RuntimeCacheStore

_T = TypeVar("_T")


class _RuntimeHandleClosedError(RuntimeError):
    """Signal that lease acquisition lost a race with final handle closure."""


@dataclass
class _QueuedWrite(Generic[_T]):
    action: str
    payload: dict[str, object]
    completed: threading.Event
    result: list[_T | Exception]


@dataclass(frozen=True)
class _ReadCacheEntry(Generic[_T]):
    value: _T
    cached_at_monotonic: float
    generation: int
    sqlite_signature: tuple[tuple[bool, int | None, int | None], ...]


class _RuntimeCacheHandle:
    """Own one shared runtime cache ledger, read cache, and serialized write queue."""

    def __init__(
        self,
        db_path: Path,
        *,
        cache_ttl_seconds: float = 1.0,
        max_read_cache_entries: int = 512,
    ) -> None:
        self.db_path = Path(db_path).expanduser().resolve()
        self.cache_ttl_seconds = cache_ttl_seconds
        self.max_read_cache_entries = max(int(max_read_cache_entries), 1)
        self._ledger = RuntimeCacheLedger(self.db_path)
        self._lock = threading.RLock()
        self._refcount = 0
        self._closed = False
        self._write_generation = 0
        self._read_cache: dict[tuple[object, ...], _ReadCacheEntry[object]] = {}
        self._write_queue: Queue[_QueuedWrite[object] | None] = Queue()
        self._writer = threading.Thread(target=self._run_writer, name="data-engine-runtime-io", daemon=True)
        self._writer.start()

    def acquire(self) -> None:
        with self._lock:
            if self._closed:
                raise _RuntimeHandleClosedError("Runtime IO handle is already closed.")
            self._refcount += 1

    def release(self) -> None:
        should_close = False
        with self._lock:
            if self._refcount > 0:
                self._refcount -= 1
            should_close = self._refcount == 0 and not self._closed
            if should_close:
                self._closed = True
        if should_close:
            self._write_queue.put(None)
            self._writer.join()
            self._ledger.close()

    @property
    def ledger(self) -> RuntimeCacheLedger:
        return self._ledger

    def _run_writer(self) -> None:
        while True:
            item = self._write_queue.get()
            if item is None:
                break
            try:
                result = self._perform_write(item.action, item.payload)
            except Exception as exc:  # pragma: no cover - failure propagation
                item.result.append(exc)
            else:
                item.result.append(result)
            finally:
                item.completed.set()

    def _perform_write(self, action: str, payload: dict[str, object]) -> object:
        if action == "run_started":
            self._ledger.runs.record_started(**payload)
            self._invalidate_caches()
            return None
        if action == "run_finished":
            self._ledger.runs.record_finished(**payload)
            self._invalidate_caches()
            return None
        if action == "step_started":
            step_run_id = self._ledger.step_outputs.record_started(**payload)
            self._invalidate_caches()
            return step_run_id
        if action == "step_finished":
            self._ledger.step_outputs.record_finished(**payload)
            self._invalidate_caches()
            return None
        if action == "file_state_upsert":
            self._ledger.source_signatures.upsert_file_state(**payload)
            self._invalidate_caches()
            return None
        if action == "logs_append":
            self._ledger.logs.append(**payload)
            self._invalidate_caches()
            return None
        if action == "logs_append_many":
            self._ledger.logs.append_many(payload["rows"])
            self._invalidate_caches()
            return None
        if action == "snapshot_replace":
            replaced = self._ledger.snapshots.replace(
                generation_id=payload["generation_id"],
                runs=payload["runs"],
                step_runs=payload["step_runs"],
                logs=payload["logs"],
                file_states=payload["file_states"],
            )
            if replaced:
                self._invalidate_caches()
            return replaced
        raise ValueError(f"Unknown runtime IO write action: {action}")

    def _invalidate_caches(self) -> None:
        with self._lock:
            self._write_generation += 1
            self._read_cache.clear()

    def submit_write(self, action: str, /, **payload: object) -> object:
        completed = threading.Event()
        result_box: list[object | Exception] = []
        with self._lock:
            if self._closed:
                raise RuntimeError("Runtime IO handle is already closed.")
            self._write_queue.put(
                _QueuedWrite(
                    action=action,
                    payload=dict(payload),
                    completed=completed,
                    result=result_box,
                )
            )
        completed.wait()
        result = result_box[0] if result_box else None
        if isinstance(result, Exception):
            raise result
        return result

    def _sqlite_signature(self) -> tuple[tuple[bool, int | None, int | None], ...]:
        signatures: list[tuple[bool, int | None, int | None]] = []
        for candidate in (self.db_path, self.db_path.with_suffix(f"{self.db_path.suffix}-wal")):
            try:
                stat = candidate.stat()
            except FileNotFoundError:
                signatures.append((False, None, None))
            else:
                signatures.append((True, stat.st_mtime_ns, stat.st_size))
        return tuple(signatures)

    def read_cached(self, key: tuple[object, ...], loader: Callable[[], object]) -> object:
        now = monotonic()
        current_signature = None
        with self._lock:
            self._prune_read_cache_locked(now)
            generation = self._write_generation
            entry = self._read_cache.get(key)
            if entry is not None and entry.generation == generation:
                if now - entry.cached_at_monotonic < self.cache_ttl_seconds:
                    return entry.value
                current_signature = self._sqlite_signature()
                if current_signature == entry.sqlite_signature:
                    self._read_cache[key] = _ReadCacheEntry(
                        value=entry.value,
                        cached_at_monotonic=now,
                        generation=entry.generation,
                        sqlite_signature=entry.sqlite_signature,
                    )
                    return entry.value
        value = loader()
        signature = current_signature or self._sqlite_signature()
        with self._lock:
            if self._write_generation != generation:
                return value
            self._read_cache[key] = _ReadCacheEntry(
                value=value,
                cached_at_monotonic=now,
                generation=generation,
                sqlite_signature=signature,
            )
            self._prune_read_cache_locked(now)
        return value

    def _prune_read_cache_locked(self, now: float) -> None:
        expired_keys = tuple(
            cache_key
            for cache_key, entry in self._read_cache.items()
            if now - entry.cached_at_monotonic >= self.cache_ttl_seconds
        )
        for cache_key in expired_keys:
            self._read_cache.pop(cache_key, None)
        overflow = len(self._read_cache) - self.max_read_cache_entries
        if overflow <= 0:
            return
        oldest_keys = sorted(
            self._read_cache,
            key=lambda cache_key: self._read_cache[cache_key].cached_at_monotonic,
        )[:overflow]
        for cache_key in oldest_keys:
            self._read_cache.pop(cache_key, None)


class _RuntimeCacheLease:
    """Own one idempotently releasable reference to a shared runtime cache handle."""

    def __init__(self, handle: _RuntimeCacheHandle) -> None:
        self._handle = handle
        self._condition = threading.Condition()
        self._active_operations = 0
        self._closed = False
        self._released = False
        self._handle.acquire()

    @property
    def ledger(self) -> RuntimeCacheLedger:
        with self._condition:
            if self._closed:
                raise RuntimeError("Runtime IO cache store is closed.")
            return self._handle.ledger

    def call(self, operation: Callable[[_RuntimeCacheHandle], _T]) -> _T:
        with self._condition:
            if self._closed:
                raise RuntimeError("Runtime IO cache store is closed.")
            self._active_operations += 1
        try:
            return operation(self._handle)
        finally:
            with self._condition:
                self._active_operations -= 1
                if self._active_operations == 0:
                    self._condition.notify_all()

    def read_cached(self, key: tuple[object, ...], loader: Callable[[], object]) -> object:
        return self.call(lambda handle: handle.read_cached(key, loader))

    def submit_write(self, action: str, /, **payload: object) -> object:
        return self.call(lambda handle: handle.submit_write(action, **payload))

    def invalidate_caches(self) -> None:
        self.call(lambda handle: handle._invalidate_caches())

    def close(self) -> None:
        with self._condition:
            if self._closed:
                while not self._released:
                    self._condition.wait()
                return
            self._closed = True
            while self._active_operations:
                self._condition.wait()
        try:
            self._handle.release()
        finally:
            with self._condition:
                self._released = True
                self._condition.notify_all()


def _guarded_delegate_attribute(lease: _RuntimeCacheLease, delegate: object, name: str) -> object:
    attribute = lease.call(lambda _handle: getattr(delegate, name))
    if not callable(attribute):
        return attribute

    def guarded(*args: object, **kwargs: object) -> object:
        return lease.call(lambda _handle: attribute(*args, **kwargs))

    return guarded


class _RuntimeRunsProxy:
    def __init__(self, handle: _RuntimeCacheLease) -> None:
        self._handle = handle
        self._delegate = handle.ledger.runs

    def get(self, run_id: str) -> PersistedRun | None:
        return self._handle.read_cached(("runs.get", run_id), lambda: self._delegate.get(run_id))

    def list(self, *, flow_name: str | None = None) -> tuple[PersistedRun, ...]:
        return self._handle.read_cached(("runs.list", flow_name), lambda: self._delegate.list(flow_name=flow_name))

    def list_active(self, *, flow_name: str | None = None) -> tuple[PersistedRun, ...]:
        return self._handle.read_cached(
            ("runs.list_active", flow_name),
            lambda: self._delegate.list_active(flow_name=flow_name),
        )

    def record_started(self, **kwargs: object) -> None:
        self._handle.submit_write("run_started", **kwargs)

    def record_finished(self, **kwargs: object) -> None:
        self._handle.submit_write("run_finished", **kwargs)

    def __getattr__(self, name: str):
        return _guarded_delegate_attribute(self._handle, self._delegate, name)


class _RuntimeStepOutputsProxy:
    def __init__(self, handle: _RuntimeCacheLease) -> None:
        self._handle = handle
        self._delegate = handle.ledger.step_outputs

    def get(self, step_run_id: int) -> PersistedStepRun | None:
        return self._handle.read_cached(
            ("step_outputs.get", step_run_id),
            lambda: self._delegate.get(step_run_id),
        )

    def list_for_run(self, run_id: str) -> tuple[PersistedStepRun, ...]:
        return self._handle.read_cached(
            ("step_outputs.list_for_run", run_id),
            lambda: self._delegate.list_for_run(run_id),
        )

    def list(
        self,
        *,
        flow_name: str | None = None,
        after_id: int | None = None,
    ) -> tuple[PersistedStepRun, ...]:
        return self._handle.read_cached(
            ("step_outputs.list", flow_name, after_id),
            lambda: self._delegate.list(flow_name=flow_name, after_id=after_id),
        )

    def list_active(self, *, run_id: str | None = None) -> tuple[PersistedStepRun, ...]:
        return self._handle.read_cached(
            ("step_outputs.list_active", run_id),
            lambda: self._delegate.list_active(run_id=run_id),
        )

    def record_started(self, **kwargs: object) -> int:
        return int(self._handle.submit_write("step_started", **kwargs))

    def record_finished(self, **kwargs: object) -> None:
        self._handle.submit_write("step_finished", **kwargs)

    def __getattr__(self, name: str):
        return _guarded_delegate_attribute(self._handle, self._delegate, name)


class _RuntimeLogsProxy:
    def __init__(self, handle: _RuntimeCacheLease) -> None:
        self._handle = handle
        self._delegate = handle.ledger.logs

    def list(
        self,
        *,
        flow_name: str | None = None,
        run_id: str | None = None,
        after_id: int | None = None,
        limit: int | None = None,
    ) -> tuple[PersistedLogEntry, ...]:
        return self._handle.read_cached(
            ("logs.list", flow_name, run_id, after_id, limit),
            lambda: self._delegate.list(flow_name=flow_name, run_id=run_id, after_id=after_id, limit=limit),
        )

    def append(self, **kwargs: object) -> None:
        self._handle.submit_write("logs_append", **kwargs)

    def append_many(self, rows: tuple[PersistedLogEntry, ...]) -> None:
        self._handle.submit_write("logs_append_many", rows=rows)

    def __getattr__(self, name: str):
        return _guarded_delegate_attribute(self._handle, self._delegate, name)


class _RuntimeSourceSignaturesProxy:
    def __init__(self, handle: _RuntimeCacheLease) -> None:
        self._handle = handle
        self._delegate = handle.ledger.source_signatures

    def normalize_path(self, source_path: Path | str) -> str:
        return self._handle.call(lambda _handle: self._delegate.normalize_path(source_path))

    def signature_for_path(self, source_path: Path) -> SourceSignature | None:
        return self._handle.call(lambda _handle: self._delegate.signature_for_path(source_path))

    def is_stale(self, flow_name: str, signature: SourceSignature | None) -> bool:
        return self._handle.call(lambda _handle: self._delegate.is_stale(flow_name, signature))

    def prune_missing(self, *, flow_name: str, current_source_paths: set[str]) -> None:
        def prune(handle: _RuntimeCacheHandle) -> None:
            self._delegate.prune_missing(flow_name=flow_name, current_source_paths=current_source_paths)
            handle._invalidate_caches()

        self._handle.call(prune)

    def list_file_states(self, *, flow_name: str | None = None) -> tuple[PersistedFileState, ...]:
        return self._handle.read_cached(
            ("source_signatures.list_file_states", flow_name),
            lambda: self._delegate.list_file_states(flow_name=flow_name),
        )

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
        self._handle.submit_write(
            "file_state_upsert",
            flow_name=flow_name,
            signature=signature,
            status=status,
            run_id=run_id,
            finished_at_utc=finished_at_utc,
            error_text=error_text,
        )

    def __getattr__(self, name: str):
        return _guarded_delegate_attribute(self._handle, self._delegate, name)


class _RuntimeSnapshotsProxy:
    def __init__(self, handle: _RuntimeCacheLease) -> None:
        self._handle = handle

    def export(
        self,
    ) -> tuple[
        tuple[PersistedRun, ...],
        tuple[PersistedStepRun, ...],
        tuple[PersistedLogEntry, ...],
        tuple[PersistedFileState, ...],
    ]:
        """Read one transactionally consistent runtime snapshot."""
        return self._handle.call(lambda handle: handle.ledger.snapshots.export())

    def applied_generation_id(self) -> str | None:
        """Return the shared generation last applied to this cache."""
        return self._handle.call(lambda handle: handle.ledger.snapshots.applied_generation_id())

    def replace(
        self,
        *,
        generation_id: str,
        runs: tuple[PersistedRun, ...],
        step_runs: tuple[PersistedStepRun, ...],
        logs: tuple[PersistedLogEntry, ...],
        file_states: tuple[PersistedFileState, ...],
    ) -> bool:
        """Apply one shared snapshot generation through the serialized writer."""
        return bool(
            self._handle.submit_write(
                "snapshot_replace",
                generation_id=generation_id,
                runs=runs,
                step_runs=step_runs,
                logs=logs,
                file_states=file_states,
            )
        )


class _RuntimeExecutionStateProxy:
    def __init__(self, handle: _RuntimeCacheLease) -> None:
        self._handle = handle

    def record_run_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        group_name: str,
        source_path: str | None,
        started_at_utc: str | None = None,
    ) -> None:
        self._handle.submit_write(
            "run_started",
            run_id=run_id,
            flow_name=flow_name,
            group_name=group_name,
            source_path=source_path,
            started_at_utc=started_at_utc,
        )

    def record_run_finished(
        self,
        *,
        run_id: str,
        status: str,
        finished_at_utc: str,
        error_text: str | None = None,
    ) -> None:
        self._handle.submit_write(
            "run_finished",
            run_id=run_id,
            status=status,
            finished_at_utc=finished_at_utc,
            error_text=error_text,
        )

    def record_step_started(
        self,
        *,
        run_id: str,
        flow_name: str,
        step_label: str,
        started_at_utc: str | None = None,
    ) -> int:
        return int(
            self._handle.submit_write(
                "step_started",
                run_id=run_id,
                flow_name=flow_name,
                step_label=step_label,
                started_at_utc=started_at_utc,
            )
        )

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
        self._handle.submit_write(
            "step_finished",
            step_run_id=step_run_id,
            status=status,
            finished_at_utc=finished_at_utc,
            elapsed_ms=elapsed_ms,
            error_text=error_text,
            output_path=output_path,
        )

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
        self._handle.submit_write(
            "file_state_upsert",
            flow_name=flow_name,
            signature=signature,
            status=status,
            run_id=run_id,
            finished_at_utc=finished_at_utc,
            error_text=error_text,
        )


class RuntimeIoCacheStore(RuntimeCacheStore):
    """Proxy one runtime cache store through the shared runtime IO layer."""

    def __init__(self, handle: _RuntimeCacheHandle) -> None:
        self._handle = handle
        self._lease = _RuntimeCacheLease(handle)
        self.runs = _RuntimeRunsProxy(self._lease)
        self.step_outputs = _RuntimeStepOutputsProxy(self._lease)
        self.logs = _RuntimeLogsProxy(self._lease)
        self.source_signatures = _RuntimeSourceSignaturesProxy(self._lease)
        self.snapshots = _RuntimeSnapshotsProxy(self._lease)
        self.execution_state = _RuntimeExecutionStateProxy(self._lease)

    def close(self) -> None:
        self._lease.close()

    def snapshot_change_token(self) -> object:
        """Return a constant-time token for runtime snapshot publication."""
        return self._lease.call(lambda handle: handle.ledger.snapshot_change_token())

    def snapshot_incarnation(self) -> str:
        """Return the stable identity of the underlying open ledger."""
        return self._lease.call(lambda handle: handle.ledger.snapshot_incarnation())

    def refresh_external_state(self) -> None:
        """Drop cached reads so the next query reflects external daemon writes immediately."""
        self._lease.invalidate_caches()

    def reset_flow(self, flow_name: str) -> None:
        def reset(handle: _RuntimeCacheHandle) -> None:
            handle.ledger.reset_flow(flow_name)
            handle._invalidate_caches()

        self._lease.call(reset)

    def reset_all(self) -> None:
        def reset(handle: _RuntimeCacheHandle) -> None:
            handle.ledger.reset_all()
            handle._invalidate_caches()

        self._lease.call(reset)

    def reconcile_orphaned_activity(
        self,
        *,
        finished_at_utc: str,
        status: str = "stopped",
        error_text: str | None = None,
    ) -> tuple[int, int]:
        def reconcile(handle: _RuntimeCacheHandle) -> tuple[int, int]:
            result = handle.ledger.reconcile_orphaned_activity(
                finished_at_utc=finished_at_utc,
                status=status,
                error_text=error_text,
            )
            handle._invalidate_caches()
            return result

        return self._lease.call(reconcile)

    def __getattr__(self, name: str):
        return _guarded_delegate_attribute(self._lease, self._handle.ledger, name)


class RuntimeIoLayer:
    """Own shared runtime cache-store proxies, write serialization, and read caching."""

    def __init__(self, *, cache_ttl_seconds: float = 1.0, max_read_cache_entries: int = 512) -> None:
        self.cache_ttl_seconds = cache_ttl_seconds
        self.max_read_cache_entries = max(int(max_read_cache_entries), 1)
        self._lock = threading.RLock()
        self._handles: dict[Path, _RuntimeCacheHandle] = {}

    def open_cache_store(self, db_path: Path) -> RuntimeIoCacheStore:
        normalized = Path(db_path).expanduser().resolve()
        while True:
            with self._lock:
                handle = self._handles.get(normalized)
                if handle is None or handle._closed:
                    handle = _RuntimeCacheHandle(
                        normalized,
                        cache_ttl_seconds=self.cache_ttl_seconds,
                        max_read_cache_entries=self.max_read_cache_entries,
                    )
                    self._handles[normalized] = handle
                try:
                    return RuntimeIoCacheStore(handle)
                except _RuntimeHandleClosedError:
                    if self._handles.get(normalized) is handle:
                        self._handles.pop(normalized)


_DEFAULT_RUNTIME_IO_LAYER: RuntimeIoLayer | None = None
_DEFAULT_RUNTIME_IO_LAYER_LOCK = threading.RLock()


def default_runtime_io_layer() -> RuntimeIoLayer:
    """Return the process-wide runtime IO layer."""
    global _DEFAULT_RUNTIME_IO_LAYER
    with _DEFAULT_RUNTIME_IO_LAYER_LOCK:
        if _DEFAULT_RUNTIME_IO_LAYER is None:
            _DEFAULT_RUNTIME_IO_LAYER = RuntimeIoLayer()
        return _DEFAULT_RUNTIME_IO_LAYER


__all__ = ["RuntimeIoCacheStore", "RuntimeIoLayer", "default_runtime_io_layer"]
