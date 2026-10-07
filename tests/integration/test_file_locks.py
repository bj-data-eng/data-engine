from __future__ import annotations

from contextlib import contextmanager
import multiprocessing
import os

import pytest

import data_engine.hosts.daemon.client as client_module
import data_engine.runtime.shared_state as shared_state_module
from tests.services.support import resolve_workspace_paths


@contextmanager
def _guard(kind, root):
    if kind == "topology":
        paths = resolve_workspace_paths(workspace_root=root, workspace_id="workspace")
        with shared_state_module._topology_lock(paths):
            yield paths.workspace_state_dir / ".lease-topology.lock"
    else:
        root.mkdir(parents=True, exist_ok=True)
        with client_module._authkey_mutation_lock(root / "daemon.auth"):
            yield root / client_module._DAEMON_AUTHKEY_LOCK_FILE_NAME


def _hold_lock(kind, root, ready, release):
    with _guard(kind, root):
        ready.set()
        if not release.wait(15.0):
            raise TimeoutError("Test lock holder was not released.")


def _wait_for_lock(kind, root, attempting, acquired):
    attempting.set()
    with _guard(kind, root):
        acquired.set()


@pytest.mark.parametrize("kind", ["topology", "authkey"])
def test_file_lock_serializes_processes_without_initializing_contents(tmp_path, kind):
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    attempting = context.Event()
    acquired = context.Event()
    root = tmp_path / "workspace"
    holder = context.Process(target=_hold_lock, args=(kind, root, ready, release))
    waiter = context.Process(target=_wait_for_lock, args=(kind, root, attempting, acquired))
    processes = []
    try:
        holder.start()
        processes.append(holder)
        assert ready.wait(10.0)
        waiter.start()
        processes.append(waiter)
        assert attempting.wait(10.0)
        assert not acquired.wait(0.25)
        release.set()
        assert acquired.wait(10.0)
        for process in processes:
            process.join(timeout=10.0)
        assert all(process.exitcode == 0 for process in processes)
        with _guard(kind, root) as lock_path:
            assert lock_path.stat().st_size == 0
    finally:
        release.set()
        for process in processes:
            process.join(timeout=5.0)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5.0)
            process.close()


@pytest.mark.skipif(os.name != "nt", reason="Windows mandatory byte-range locks are required")
@pytest.mark.parametrize("kind", ["topology", "authkey"])
def test_empty_file_lock_does_not_write_into_another_owners_locked_byte(tmp_path, monkeypatch, kind):
    import msvcrt

    root = tmp_path / "workspace"
    if kind == "topology":
        paths = resolve_workspace_paths(workspace_root=root, workspace_id="workspace")
        lock_path = paths.workspace_state_dir / ".lease-topology.lock"
    else:
        lock_path = root / client_module._DAEMON_AUTHKEY_LOCK_FILE_NAME
    lock_path.parent.mkdir(parents=True)
    owner = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    real_locking = msvcrt.locking
    owner_held = False
    attempts = []
    try:
        real_locking(owner, msvcrt.LK_NBLCK, 1)
        owner_held = True

        def release_owner_when_contender_locks(fd, mode, size):
            nonlocal owner_held
            if fd != owner and mode in {msvcrt.LK_LOCK, msvcrt.LK_NBLCK}:
                attempts.append((mode, size))
                real_locking(owner, msvcrt.LK_UNLCK, 1)
                owner_held = False
            return real_locking(fd, mode, size)

        monkeypatch.setattr(msvcrt, "locking", release_owner_when_contender_locks)
        with _guard(kind, root):
            assert len(attempts) == 1
            assert not owner_held
        assert lock_path.stat().st_size == 0
    finally:
        if owner_held:
            real_locking(owner, msvcrt.LK_UNLCK, 1)
        os.close(owner)


@pytest.mark.parametrize("kind", ["topology", "authkey"])
def test_file_lock_releases_after_exception(tmp_path, kind):
    root = tmp_path / "workspace"
    with pytest.raises(RuntimeError, match="guard failed"):
        with _guard(kind, root):
            raise RuntimeError("guard failed")
    with _guard(kind, root) as lock_path:
        assert lock_path.stat().st_size == 0


def test_topology_lock_nested_guard_preserves_outer_ownership(tmp_path):
    root = tmp_path / "workspace"
    with _guard("topology", root) as lock_path:
        lock_key = str(lock_path.absolute())
        assert shared_state_module._TOPOLOGY_LOCK_DEPTH.by_path[lock_key] == 1
        with _guard("topology", root):
            assert shared_state_module._TOPOLOGY_LOCK_DEPTH.by_path[lock_key] == 2
        assert shared_state_module._TOPOLOGY_LOCK_DEPTH.by_path[lock_key] == 1
        with _guard("topology", root):
            assert shared_state_module._TOPOLOGY_LOCK_DEPTH.by_path[lock_key] == 2
    assert lock_key not in shared_state_module._TOPOLOGY_LOCK_DEPTH.by_path


@pytest.mark.parametrize("kind", ["topology", "authkey"])
@pytest.mark.parametrize("payload", [b"\0", b"existing lock"])
def test_file_lock_preserves_existing_contents(tmp_path, kind, payload):
    root = tmp_path / "workspace"
    with _guard(kind, root) as lock_path:
        pass
    lock_path.write_bytes(payload)
    with _guard(kind, root):
        pass
    assert lock_path.read_bytes() == payload
