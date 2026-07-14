# Repository correctness and quality review

Date: 2026-07-13

Revision notes:

- Updated after removal of the retired operator interface; findings and scope specific to it have been removed.
- P1-7 is resolved by idempotent runtime-I/O leases that reject stale operations and let in-flight work drain before releasing the shared writer.
- P2-9 is closed as intended product behavior: CLI workspace creation replaces the auto-provisioned VS Code settings file with the current Data Engine settings.
- All P2 and P3 findings are resolved. The accepted implementations avoid new background pollers, remove redundant layout and response-polling work, bound preview/materialization costs, and keep recovery-only checks off valid hot paths.

Reviewed commit: `b6a01b70d37c7d6bb9885f132e1218eb07d46f62` (`main`)

Resolution progress verified through commit `76b3f5d`.

Remote status at review baseline: the reviewed commit matched `origin/main` after `git fetch --prune`.

Scope: runtime, shared state, daemon lifecycle, platform behavior, CLI, GUI, authoring helpers, tests, packaging, and repository quality controls

## Executive summary

The repository has a strong clean baseline: all 1,002 current tests pass, Ruff and pydoclint are clean, the Windows runtime lock resolves with hashes enforced, and the PEP 517 build and Twine metadata checks succeed. The review initially found 26 reproducible or deterministic issues. Nineteen are now resolved or closed, leaving the seven P1 findings below open:

| Priority | Open | Meaning |
|---|---:|---|
| P1 | 7 | Safety, ownership, data-consistency, cross-workspace, or indefinite-hang risk; address before relying on the affected path in production. |
| P2 | 0 | All material functional and platform-correctness findings are resolved. |
| P3 | 0 | All lower-frequency edge cases are resolved. |

The highest-risk cluster is daemon/workspace ownership. Lease mutation is not fenced, forced shutdown can target a reused PID, stop/handoff can release ownership while worker threads are still alive, and POSIX force-stop does not terminate descendants. The next cluster is runtime projection consistency: a reader can accept a torn shared snapshot, and hydration can invalidate incremental cursors indefinitely.

## P1 findings

### P1-1 — Lease checkpoint and release are unfenced, so a stale daemon can overwrite or release a newer owner's lease

Evidence:

- `src/data_engine/hosts/daemon/ownership.py:98-119`
- `src/data_engine/hosts/daemon/state_sync.py:116-134`
- `src/data_engine/runtime/shared_state.py:177-185,232-263,316-321`

`release_workspace_claim()` deletes lease metadata and renames the leased marker using only shared paths. Checkpointing likewise writes shared snapshots and metadata without proving that the calling `daemon_id` still owns the lease. In a deterministic two-owner reproduction, daemon A was paused until its lease became stale, daemon B recovered and claimed it, and resumed daemon A then deleted B's metadata and changed B's leased marker back to available. A can also checkpoint over B's shared snapshot.

Impact: split-brain execution, ownership clobbering, and shared-state corruption after a pause, sleep, network-share stall, or stale recovery.

Recommendation: issue an immutable fencing token/generation at claim time. Require an atomic token match for every checkpoint, metadata update, and release. A stale daemon that observes a mismatch must stop without writing or releasing anything.

### P1-2 — Stop/handoff forgets worker threads after 1.5 seconds and releases ownership while work can still be running

Evidence:

- `src/data_engine/hosts/daemon/runtime_control.py:11-32`
- `src/data_engine/hosts/daemon/lifecycle.py:87-113,215-235`

`stop_active_work()` joins each worker for only 1.5 seconds and then unconditionally calls `end_runtime()`, which clears the engine-thread state. Handoff and shutdown proceed to release the workspace and close ledgers. A reproduction using a non-cooperative worker returned after 1.5 seconds with the worker still alive while `engine_thread` had been cleared.

Impact: old work can keep writing after ownership moves to another machine, or after its storage has been closed. This compounds P1-1 and can corrupt outputs or runtime state.

Recommendation: retain worker references and active state until every worker has actually stopped. On timeout, fail the handoff or escalate through a safe worker/process termination path; never release ownership or close storage while work remains alive.

### P1-3 — Forced shutdown can kill an unrelated process after PID reuse

Evidence:

- `src/data_engine/hosts/daemon/client.py:215-227,473-503`

When the daemon is unreachable, `force_shutdown_daemon_process()` accepts any positive PID from same-host lease metadata. It does not verify process start time, executable, command, endpoint identity, or daemon token. A reproduction placed the audit process's PID in stale metadata; the code selected that PID and reached the kill path.

Impact: destructive termination of an unrelated local process when stale metadata outlives the original daemon and the OS reuses its PID.

Recommendation: persist process start identity and a daemon token, inspect the live process, and require all identity fields to match before escalating. If identity cannot be verified, clean or quarantine stale metadata without sending a signal.

### P1-4 — POSIX force-stop kills only the daemon parent, not its process tree

Evidence:

- `src/data_engine/platform/processes.py:194-207`
- `src/data_engine/hosts/daemon/client.py:443-465`

Windows uses `taskkill /T`, but the POSIX branch sends `SIGKILL` only to the supplied PID despite the helper's process-tree contract. A live reproduction started a daemon-like process with a child: the parent died and the child remained alive. Daemons are deliberately started with `start_new_session=True`, so a known process group is available.

Impact: authored-flow subprocesses can survive force-stop, retain locks, keep writing outputs, or continue consuming resources after the daemon appears stopped.

Recommendation: terminate the verified daemon process group or enumerate descendants safely. Couple this with the process-identity protections in P1-3.

### P1-5 — The shared snapshot reader accepts torn multi-file generations and can crash SQLite hydration

Evidence:

- `src/data_engine/runtime/shared_state.py:353-383,444-493,532-538,569-601`
- `src/data_engine/runtime/runtime_cache_store.py:849-940`

The writer updates four Parquet files one at a time. Empty runs, steps, and file-state tables are represented by deleting their file; empty logs have no row from which to read a generation. The consistency reader discards missing generations and accepts the snapshot whenever the remaining set contains at most one generation.

A deterministic checkpoint-interleaving reproduction returned zero runs with one old-generation step. `hydrate_local_runtime_state()` then raised `sqlite3.IntegrityError: FOREIGN KEY constraint failed` while inserting the orphan step. Per-file atomic replacement does not make the four-file set atomic, and the retries do not delay or compare a committed manifest.

Impact: observers and startup hydration can fail during ordinary transitions to an empty table.

Recommendation: write generation-bearing artifacts for empty tables, publish a committed manifest only after all artifacts are ready, and require every table generation to equal the committed manifest before replacing SQLite.

### P1-6 — Snapshot hydration reassigns row IDs while incremental caches retain obsolete high-water marks

Evidence:

- `src/data_engine/runtime/runtime_cache_store.py:849-912`
- `src/data_engine/services/runtime_binding.py:271-300`
- `src/data_engine/services/logs.py:26-42`
- `src/data_engine/views/logs.py:20-21,112-115`

`RuntimeSnapshotRepository.replace()` deletes and reinserts step and log rows without their supplied IDs. Because these are `INTEGER PRIMARY KEY` tables without `AUTOINCREMENT`, IDs can restart at 1. The log store and step-output cache continue querying after their previous high-water IDs. A reproduction replaced old ID-1 rows with different new ID-1 rows: SQLite contained the new output and log, while both operator caches continued to show the old values indefinitely.

Impact: observers can remain permanently stale after hydration or ownership changes, even though the local database is current.

Recommendation: preserve incoming IDs during full replacement, or make hydration generation-aware and explicitly reset every log, step-output, and runtime-I/O cursor/cache.

### P1-7 — Resolved: runtime-I/O proxy closure is idempotent and cannot strand another client

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/services/runtime_io.py:65-76,132-147,454-467`

`RuntimeIoCacheStore.close()` has no per-proxy closed flag. Repeated close calls decrement the shared handle repeatedly. With two proxies, closing one twice reduced the reference count from 2 to 0 and stopped the writer. A write through the second live proxy then queued behind the shutdown sentinel and waited forever because `submit_write()` has no closed-state check or timeout. The root review reproduced this with a write thread that remained blocked.

Impact: an ordinary duplicate cleanup path can permanently hang another window, binding, or service using the same runtime database.

Resolution: each proxy now owns an idempotent lease. Closing a lease waits without polling for its own in-flight operations, releases the shared handle exactly once, and rejects later use. Handle shutdown and write admission share one lock, so no write can be queued behind the shutdown sentinel. Snapshot replacement is serialized through the same writer, and a read overlapping invalidation is never cached under the newer generation. Concurrent-close, final-close/open, stale-snapshot, overlapping-read, and surviving-client coverage verifies the lifecycle boundary.

### P1-8 — Hostname-only machine identity can collapse distinct workstations into one owner

Evidence:

- `src/data_engine/platform/workspace_models.py:151-153`
- `src/data_engine/hosts/daemon/ownership.py:21-29`
- `src/data_engine/hosts/daemon/client.py:184-227`

`machine_id_text()` is only `socket.gethostname()`. Hostnames can be duplicated by cloning, imaging, corporate naming, containers, or manual configuration, and can change over time. A duplicate name causes another machine's control request to be ignored as self-originated and allows a remote lease PID to be treated as local.

Impact: broken control transfer and, combined with P1-3, possible signaling of an unrelated local PID.

Recommendation: persist a random installation UUID as machine identity and store hostname separately as display metadata. Include a migration strategy for existing leases.

## P2 findings

### P2-1 — Resolved: explicit workspace IDs are propagated consistently into daemon paths

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/hosts/daemon/composition.py:52-69`
- `src/data_engine/hosts/daemon/client.py:443-453`
- `src/data_engine/hosts/daemon/entrypoints.py:47-85`

Daemon dependency construction ignores the already-resolved `paths.runtime_control_db_path` and re-resolves from `workspace_root`, deriving the root folder name instead of an explicit alias. Daemon spawning also omits the supported `--workspace-id` argument. With root `folder` and ID `alias`, the parent and child use different control databases and IPC endpoints; the parent times out while the child remains running elsewhere.

Resolution: daemon construction now opens the already-resolved control-ledger path, and process spawning always passes the resolved workspace ID. Alias coverage proves that a root folder and a differing explicit ID produce identical parent/child control databases and IPC endpoints. This also removes redundant settings and layout resolution during binding construction.

### P2-2 — Resolved: real run-step detail queries use the persisted duration contract

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/services/operator_queries.py:369-382`
- `src/data_engine/runtime/ledger_models.py:37-50`
- `tests/services/test_operator_queries.py:56-126`

`HistoryQueryService.get_run_steps()` reads `step_run.elapsed_seconds`, but `PersistedStepRun` exposes only `elapsed_ms`. A real-ledger reproduction raised `AttributeError: 'PersistedStepRun' object has no attribute 'elapsed_seconds'`. The unit test uses a fake row that invents the missing property and masks the contract mismatch.

Resolution: the query derives seconds from canonical `elapsed_ms`, and ledger-backed coverage replaces the shape-divergent fake that masked the error.

### P2-3 — Resolved: continuous batch polling retains every coalesced change signature

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/runtime/execution/continuous.py:122-150`
- `src/data_engine/runtime/file_watch.py:128-153`

For a batch watcher, the loop computes the first drained path's signature, enqueues it, and breaks. `PollingWatcher.drain_events()` has already marked every returned path as emitted, so the remaining paths do not reappear. A two-file reproduction queued only `a.txt`; the next drain was empty.

Impact: a batch may process all files but persist freshness for only one, leaving other processed inputs stale or untracked.

Resolution: batch polling now collects every drained signature before enqueueing one batch job. Two-file coverage verifies that no drained change is silently discarded.

### P2-4 — Resolved: stopping inside a step records a terminal step state

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/runtime/execution/runner.py:330-342,416-435`

The step-level `FlowStoppedError` handler re-raises without recording a terminal step state. The outer handler finishes only the run. A ledger-backed reproduction produced run status `stopped` and step status `started` with no finish timestamp.

Resolution: the stop path records the active step as stopped with its finish time and elapsed duration before propagating the flow stop. Persisted-state coverage verifies both run and step terminal states.

### P2-5 — Resolved: author-facing `settle` uses elapsed monotonic seconds

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/core/flow.py:140-148`
- `src/data_engine/runtime/file_watch.py:128-153`
- `src/data_engine/docs/sphinx_source/guides/flow-methods.md:73`

The API and guide define `settle` in seconds, but the watcher increments an integer on each unchanged `drain_events()` call. `settle=2` emitted after three immediate calls in 0.00019 seconds; at a 30-second polling interval it instead waits about 60 seconds.

Resolution: the watcher tracks when each unchanged signature was first observed and compares monotonic elapsed time to `settle`. Deterministic clock coverage verifies behavior independently of polling frequency.

### P2-6 — Resolved: “latest run” summaries select the newest retained run

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/services/runtime_state.py:659-670`
- `src/data_engine/runtime/runtime_cache_store.py:196-213`

The repository returns runs newest-first, but `_latest_run_times_for_flow()` selects `persisted_runs[-1]`. A two-run reproduction returned the older run's timestamps and error.

Resolution: the summary selects the first row from the repository's explicit newest-first ordering, with multi-run coverage for timestamps and errors.

### P2-7 — Resolved: JSON debug artifacts round-trip through the in-app viewer

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/core/primitives.py:574-597`
- `src/data_engine/services/debug_artifacts.py:39-85`

`FlowDebugContext.save_json()` writes a standalone JSON artifact with embedded debug metadata, explicitly for in-app viewing. The listing service skips every `.json` file under the assumption that JSON files are metadata sidecars. A round-trip reproduction created the artifact and returned an empty listing.

Resolution: listing pre-indexes linked sidecars and recognizes only generated standalone JSON artifacts whose embedded debug metadata declares `artifact_kind="json"`. Malformed and unrelated JSON remains ignored, and selection renders a bounded tabular preview. Save/list/view coverage exercises the complete round trip.

### P2-8 — Resolved: GUI workspace switching detaches the old client session

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/ui/gui/presenters/workspace_binding.py:75-109`
- `src/data_engine/services/runtime_binding.py:190-205`
- `src/data_engine/runtime/runtime_control_store.py:140-179`
- `src/data_engine/hosts/daemon/lifecycle.py:138-154`

The GUI registers the new binding and closes the old binding without removing the old client session. Session liveness is based on the UI PID, which remains alive after switching, so every old row remains live. Ephemeral-daemon shutdown depends on the live count reaching zero. An existing GUI test explicitly asserts that no removal occurs.

Impact: old workspace daemons remain alive and automated work can continue after the UI has detached.

Resolution: rebinding removes the old workspace's client session before closing its binding and registering the new session. The daemon's existing no-client lifecycle policy remains responsible for stopping work and exiting.

### P2-9 — Closed: CLI workspace creation intentionally replaces auto-provisioned VS Code settings

Status: closed as expected product behavior on 2026-07-13.

Evidence:

- `src/data_engine/ui/cli/commands_workspace.py:32-51,78-87`
- `src/data_engine/services/workspace_provisioning.py:137-161,181-205`

Creating an empty child workspace forces `overwrite=True` for the parent collection's `.vscode/settings.json`, replacing the entire document. This is the intended CLI provisioning contract: the file is generated and owned by Data Engine so its interpreter and environment settings stay synchronized with the active installation and workspace collection.

Verification: CLI coverage now starts with unrelated existing content, creates a workspace, and asserts that the replacement exactly matches the current generated collection settings.

### P2-10 — Resolved: Excel template composition handles merged cells

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/helpers/excel.py:183-215`

`_clear_worksheet_data()` assigns `None` to every iterated cell, including openpyxl `MergedCell` placeholders whose value is read-only. A minimal template containing merged `A1:B1` reproduced `AttributeError: 'MergedCell' object attribute 'value' is read-only`.

Resolution: composition unmerges only ranges that overlap the replacement output, preserves unrelated merges and formatting, and skips read-only `MergedCell` placeholders while clearing. Clearing now walks openpyxl's populated-cell store instead of materializing a sparse sheet's full bounding rectangle.

### P2-11 — Resolved: checkpoint recovery survives state-publication failures

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/hosts/daemon/lifecycle.py:45-84`

After the second checkpoint failure, the exception handler calls `_update_daemon_state(status='degraded')` outside a nested guard. If the control ledger is the failing dependency, that call raises the same exception out of the loop, so the third-failure relinquish path is never reached. A failure-injection reproduction stopped at count 2.

Resolution: degraded and failed event/state publication are isolated as best-effort operations. The third-failure relinquish path still runs when the control ledger fails, and an incomplete relinquish remains on the existing retry cadence rather than terminating the checkpoint thread. The normal checkpoint path adds no work.

### P2-12 — Resolved: Windows daemon request timeouts bound named-pipe connection time

Status: resolved on 2026-07-13 with deterministic Win32-boundary tests; live Windows verification remains pending.

Evidence:

- `src/data_engine/hosts/daemon/client.py:113-139`

The requested timeout starts only after `multiprocessing.connection.Client()` returns. On Python 3.14, the Windows `PipeClient` connection path has its own much longer wait for a busy named pipe, so `is_daemon_live()` with a nominal one-second timeout can block for roughly 20 seconds.

Resolution: positive AF_PIPE timeouts now use one absolute deadline across bounded `CreateFile`/`WaitNamedPipe` connection attempts, standard multiprocessing authentication reads, and response waiting. Windows imports remain lazy, no helper thread or background poller is created, and response waiting uses one blocking poll instead of repeated 50 ms polls. AF_UNIX retains its response-only timeout behavior. Platform-neutral tests cover unavailable and busy pipes, authentication cleanup, and remaining-deadline propagation.

### P2-13 — Resolved: workspace IDs and daemon endpoints are portable and bounded

Status: resolved on 2026-07-13. The endpoint scheme is an intentional clean cutover; no legacy-daemon compatibility bridge is required.

Evidence:

- `src/data_engine/platform/workspace_models.py:59-70`
- `src/data_engine/platform/workspace_policy.py:289-300,322-328`

Validation accepts Windows-reserved names (`CON`, `NUL`), reserved characters (`: * ? |`), and trailing dots/spaces. Those values become marker and Parquet filenames. It also permits long IDs in AF_UNIX endpoint names; a valid 90-character ID produced a 125-byte socket path and failed on macOS with `OSError: AF_UNIX path too long`.

Resolution: workspace IDs are limited to 64 UTF-8 bytes and reject boundary whitespace, trailing dots, control characters, Windows-reserved characters, invalid UTF-8 text, and case-insensitive device basenames through extensions. Existing local cache namespaces retain their readable shape and are bounded to 77 bytes. Daemon endpoints now use a fixed prefix plus a BLAKE2s-128 digest over the full root/runtime/alias identity, yielding a 54-byte Unix socket path and 53-character Windows pipe name while preserving distinct roots and explicit aliases.

### P2-14 — Resolved: the explicit collection-root environment variable is authoritative

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/platform/workspace_policy.py:63-89`

When `DATA_ENGINE_WORKSPACE_COLLECTION_ROOT` is set, `load_settings()` initially resolves it and then overwrites it with a stored collection root if one exists. A reproduction with differing paths returned the stored value.

Resolution: settings loading returns the explicit collection root without overwriting it from persisted state, with differing environment/stored-root coverage.

### P2-15 — Resolved: failed engine starts always clear their reservation

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/hosts/daemon/runtime_commands.py:173-206`

`start_engine()` reserves engine startup before calling `automated_flow_names(force=True)`. That call occurs outside the cleanup `try`. If catalog loading raises, the reservation is never cleared and later start requests coalesce as though startup were still in progress.

Resolution: every operation after reserving startup is inside a failure-only `finally`; only committed runtime state suppresses cleanup. Fault-injection coverage proves a catalog failure clears `engine_starting` and a subsequent start succeeds without duplicate successful-path work.

### P2-16 — Resolved: daemon-manager liveness agrees with failed status requests

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/hosts/daemon/manager.py:107-169`

If the initial ping succeeds but the following status request fails, `sync()` returns a snapshot with `live=False` without resetting the manager's `_daemon_live` field. A reproduction produced that exact disagreement.

Impact: callers can gate commands using contradictory liveness values.

Resolution: status-request failure clears the manager's retained liveness before returning the dead snapshot, with race-focused coverage for the ping/status disagreement.

## P3 findings

### P3-1 — Resolved: the public GUI launcher respects caller-owned Qt applications

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/ui/gui/launcher.py:24-36`
- `src/data_engine/ui/gui/__init__.py:6-24`

After choosing an existing `QApplication` or creating one, `QApplication.instance() is app` is necessarily true. A fake-existing-app reproduction recorded an unwanted `exec()` call. Embedded Qt callers can therefore attempt a nested event loop, and the locally scoped window is not returned or retained.

Resolution: launch ownership is captured before application construction. The launcher enters the event loop only for an application it creates and returns the window so embedded callers can retain it. Stubbed owned/existing-application tests cover both paths with no added steady-state work.

### P3-2 — Resolved: malformed daemon auth keys recover safely

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/hosts/daemon/client.py:66-89,113-130`

Non-hex auth-key text raises raw `ValueError`, which is not converted to `DaemonClientError`, and the bad file remains in place. Every later request repeats the same failure until manual deletion.

Resolution: auth keys now require exactly 32 decoded bytes. Malformed files are atomically quarantined and regenerated under a cross-process repair lock only after local daemon ownership checks make replacement safe; credible live or uncertain local ownership raises `DaemonClientError`. Remote leases and stale Unix socket files do not block machine-local recovery. Valid-key requests retain one read/decode operation and do not open SQLite, inspect leases, or take the repair lock.

## Quality assessment

### What is working well

- The repository was clean and synchronized with `origin/main` before this report was added.
- The full suite passes after the P2/P3 repair batch: `1002 passed in 35.17s` on Python 3.14.6.
- Ruff passes with no configured violations.
- pydoclint reports no violations under `src/data_engine`.
- `pip check` reports no broken requirements.
- The review-baseline full-suite statement coverage was 83% (`19,162` statements, `3,211` missed); coverage was not re-measured after the repair batch.
- A PEP 517 wheel build from the post-removal working tree succeeded: `py_data_engine-0.3.12-py3-none-any.whl`.
- The hash-locked Windows CPython 3.14 runtime requirements resolved and downloaded successfully with `--require-hashes`.
- Focused runtime, daemon/CLI/platform, and UI/authoring suites also passed during the audit.

### Systemic gaps exposed by the findings

1. **No validation CI for ordinary changes.** `.github/workflows/` contains only a manually dispatched PyPI build/publish workflow. There is no push/pull-request job for tests, Ruff, pydoclint, dependency checks, or packaging validation.

2. **No static type-checking gate.** The now-resolved real/fake `PersistedStepRun` mismatch in P2-2 is the kind of protocol drift a configured Pyright or mypy check should catch before runtime.

3. **Coverage was weakest in orchestration boundaries where several findings lived.** Baseline examples included GUI launcher 0%, platform process helpers 68%, continuous execution 73%, daemon ownership 73%, runtime commands 78%, and daemon manager 79%. Targeted tests now cover the repaired branches, but branch/fault/interleaving behavior matters more than aggregate line coverage for these modules.

4. **Fakes can diverge from production contracts.** P2-2 originally passed because its fake supplied a nonexistent property; that test now uses the real ledger contract. Continue preferring protocol-conforming fakes plus at least one real-store integration test for service boundaries.

5. **Lifecycle and coordination need adversarial tests.** Add deterministic tests for stale-owner resumption, lease fencing, non-cooperative workers, PID reuse, workspace switch during queued updates, checkpoint failure cascades, and multi-file snapshot interleavings.

6. **Platform checks need real hosts.** Add Windows coverage for named-pipe deadlines, reserved workspace names, launcher-process behavior, and daemon lifetime; add macOS/Linux coverage for AF_UNIX length and process-group termination.

The dependency-refresh package-tooling environment completed the PEP 517 build and `twine check` successfully.

## Recommended repair order

1. Introduce lease fencing and verified daemon/process identity (P1-1, P1-3, P1-8).
2. Make stop/handoff wait for actual worker termination and implement safe POSIX tree termination (P1-2, P1-4).
3. Add a committed shared-snapshot manifest and generation-aware cache invalidation (P1-5, P1-6).
4. Add the CI, type-checking, concurrency, and real-host platform test gates described above after the P1 safety work is complete.

## Review limitations

- No live multi-machine network share was available; fencing failures were reproduced deterministically with two logical owners over the same temporary shared state.
- Windows-specific named-pipe timing and filesystem failures were established from the Python 3.14 implementation and path rules but were not executed on a Windows host in this review.
- No long-running GUI visual exploration was performed; GUI behavior was covered by the existing Qt suite and targeted launcher reproduction.
- This was a correctness and quality review, not a dedicated security audit or performance benchmark.
