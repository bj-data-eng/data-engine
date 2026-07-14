# Repository correctness and quality review

Date: 2026-07-13

Revision notes:

- P1-2 is resolved by retaining every accepted runtime worker through finalization and refusing to release ownership or close storage until the workers and checkpoint thread have drained.
- P1-5 and P1-6 are resolved by manifest-committed immutable snapshot generations, transactional exports, preserved row IDs, and generation-aware cache invalidation.
- P1-8 is resolved by a durable installation UUID with hostname retained only as display metadata. This is an intentional clean protocol cutover; no compatibility bridge for old daemons was added.
- P1-7 is resolved by idempotent runtime-I/O leases that reject stale operations and let in-flight work drain before releasing the shared writer.
- P2-9 is closed as intended product behavior: CLI workspace creation replaces the auto-provisioned VS Code settings file with the current Data Engine settings.
- P1-1 is resolved by immutable token-owned workspace bundles. Every checkpoint, heartbeat, reset, recovery, and release is fenced to the exact current token; long exports renew their heartbeat without a standing poller.
- P1-3 and P1-4 are resolved by end-to-end process identity, gated containment-first launch, nonce-authenticated POSIX watchdogs, atomic Windows Job assignment, and exact verified shutdown.
- All P1, P2, and P3 findings are resolved or closed. The accepted implementations avoid new steady-state pollers, bound IPC and preview/materialization costs, remove redundant layout and response-polling work, and keep recovery-only checks off valid hot paths.

Reviewed commit: `b6a01b70d37c7d6bb9885f132e1218eb07d46f62` (`main`)

Resolution progress verified through implementation commit `ebea02f`.

Remote status at review baseline: the reviewed commit matched `origin/main` after `git fetch --prune`.

Scope: runtime, shared state, daemon lifecycle, platform behavior, CLI, GUI, authoring helpers, tests, packaging, and repository quality controls

## Executive summary

The repository has a strong clean baseline: all 1,253 current tests pass with 2 platform skips, Ruff and pydoclint are clean, the Windows runtime lock resolves with hashes enforced, and the PEP 517 build and Twine metadata checks succeed. The review found 26 reproducible or deterministic issues; all 26 are now resolved or closed:

| Priority | Open | Meaning |
|---|---:|---|
| P1 | 0 | All safety and ownership findings are resolved. |
| P2 | 0 | All material functional and platform-correctness findings are resolved. |
| P3 | 0 | All lower-frequency edge cases are resolved. |

Forced daemon shutdown now verifies one complete process incarnation and its nonce-bound containment object before termination. POSIX uses a direct-child watchdog that pins the isolated process group and performs authenticated group termination; Windows assigns the suspended daemon to a kill-on-close Job before it can execute application code. Startup, persistence, recovery, and shutdown are fenced to the same identity generation, with no compatibility bridge for legacy PID-only daemon state.

## P1 findings

### P1-1 — Resolved: immutable lease tokens fence every shared-state mutation

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/runtime/shared_state.py:547-643,733-924,1120-1475`
- `src/data_engine/hosts/daemon/state_sync.py:117-148`
- `src/data_engine/hosts/daemon/runtime_commands.py:370-414`
- `tests/services/test_shared_state.py:1314-1410,1450-1608`
- `tests/daemon/test_runtime_commands.py:1470-1708`

Before resolution, `release_workspace_claim()` deleted lease metadata and renamed the leased marker using only shared paths. Checkpointing likewise wrote shared snapshots and metadata without proving that the calling `daemon_id` still owned the lease. In a deterministic two-owner reproduction, daemon A was paused until its lease became stale, daemon B recovered and claimed it, and resumed daemon A then deleted B's metadata and changed B's leased marker back to available. A could also checkpoint over B's shared snapshot.

Impact: split-brain execution, ownership clobbering, and shared-state corruption after a pause, sleep, network-share stall, or stale recovery.

Recommendation: issue an immutable fencing token/generation at claim time. Require an atomic token match for every checkpoint, metadata update, and release. A stale daemon that observes a mismatch must stop without writing or releasing anything.

Resolution: an available workspace is one movable bundle. Claiming it creates a cryptographically random 128-bit token and atomically renames the bundle to `leased/<workspace_id>__<token>`. The token is retained by the owning daemon and is required by every heartbeat, metadata update, snapshot publication, reset, recovery, and release. Hot writes check the exact immutable token path in O(1); ownership transitions and destructive maintenance additionally use a cross-process topology lock and revalidate the token.

Checkpoint and flow-reset operations share one lock, with reset admission rechecked after waiting, so an older checkpoint cannot resurrect deleted state. A checkpoint that outlives the normal interval starts a scoped heartbeat thread only for the duration of its export; the normal path remains one blocking wait with no permanent poller, and the final timestamp records completion time. Snapshot generation cleanup is best effort after manifest commit while still propagating token loss or corrupt topology. Token-scoped temporary cleanup and fail-closed checks for redirected bundle, manifest, metadata, generation, and artifact paths prevent stale owners or filesystem redirects from mutating another owner's or external state. Deterministic stale-owner, recovery, reset/checkpoint, long-export, cleanup, redirect, and external-sentinel regressions verify the boundary.

### P1-2 — Resolved: stop/handoff retains ownership until every accepted worker has drained

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/hosts/daemon/runtime_control.py:11-32`
- `src/data_engine/hosts/daemon/lifecycle.py:87-113,215-235`

`stop_active_work()` joins each worker for only 1.5 seconds and then unconditionally calls `end_runtime()`, which clears the engine-thread state. Handoff and shutdown proceed to release the workspace and close ledgers. A reproduction using a non-cooperative worker returned after 1.5 seconds with the worker still alive while `engine_thread` had been cleared.

Impact: old work can keep writing after ownership moves to another machine, or after its storage has been closed. This compounds P1-1 and can corrupt outputs or runtime state.

Resolution: startup, runtime, and finalization threads remain tracked until completion. Stop and handoff enter a shared drain-admission state, reject late work and lease reclaims, perform a bounded initial wait, and retry completion through the existing checkpoint lifecycle without adding a poller. Fatal teardown signals shutdown, closes accepted IPC connections, joins command and checkpoint threads, and only then releases ownership and closes ledgers. Deterministic late-claim, non-cooperative-worker, fatal-listener, and silent-client regressions cover the boundary.

### P1-3 — Resolved: forced shutdown targets only the exact recorded process incarnation

Status: resolved on 2026-07-14.

Evidence:

- `src/data_engine/hosts/daemon/client.py`
- `src/data_engine/runtime/runtime_control_store.py`
- `src/data_engine/runtime/ledger_models.py`
- `src/data_engine/runtime/shared_state.py`
- `tests/daemon/test_client_process.py`
- `tests/services/test_runtime_db.py`
- `tests/test_process_identity.py`

When the daemon is unreachable, `force_shutdown_daemon_process()` accepts any positive PID from same-host lease metadata. It does not verify process start time, executable, command, endpoint identity, or daemon token. A reproduction placed the audit process's PID in stale metadata; the code selected that PID and reached the kill path.

Impact: destructive termination of an unrelated local process when stale metadata outlives the original daemon and the OS reuses its PID.

Resolution: daemon control state, shared lease metadata, status payloads, and launch handshakes now carry the PID, boot-scoped start key, normalized executable path, process group, session, and a 256-bit containment nonce. A provisional launch record is installed with compare-and-swap semantics before the daemon is released to initialize, and the daemon replaces it only after independently verifying its current identity and containment.

Force shutdown reloads and corroborates the exact local control and lease records, re-inspects the live process immediately before signaling, and refuses to terminate on any mismatch, missing identity field, unverifiable containment object, remote owner, or reused PID. Cleanup releases only the exact verified lease generation after the process tree has drained. Legacy PID-only control rows are deleted during migration as an intentional clean cutover because no old daemon compatibility bridge is required. Deterministic PID-reuse, mismatched-executable, changed-start-key, partial-record, remote-owner, lease-race, and exact-cleanup regressions cover the boundary.

### P1-4 — Resolved: daemon launch and force-stop use OS-backed process-tree containment

Status: resolved on 2026-07-14.

Evidence:

- `src/data_engine/daemon_bootstrap.py`
- `src/data_engine/platform/posix_watchdog.py`
- `src/data_engine/platform/processes.py`
- `src/data_engine/platform/windows_spawn.py`
- `src/data_engine/hosts/daemon/client.py`
- `src/data_engine/hosts/daemon/server.py`
- `tests/daemon/test_daemon_bootstrap.py`
- `tests/platform/test_posix_watchdog.py`
- `tests/platform/test_windows_spawn.py`
- `tests/test_process_containment.py`

Windows uses `taskkill /T`, but the POSIX branch sends `SIGKILL` only to the supplied PID despite the helper's process-tree contract. A live reproduction started a daemon-like process with a child: the parent died and the child remained alive. Daemons are deliberately started with `start_new_session=True`, so a known process group is available.

Impact: authored-flow subprocesses can survive force-stop, retain locks, keep writing outputs, or continue consuming resources after the daemon appears stopped.

Resolution: the absolute first-stage bootstrap enters an isolated POSIX session, arms a direct-child watchdog, publishes its stable identity through a gated pipe, waits for durable identity persistence, and then `exec`s the normal `-P -m data_engine.daemon_bootstrap` stage. The watchdog remains inside and pins the process group, monitors parent death with pidfd on Linux or kqueue on macOS, accepts only the nonce-authenticated local datagram command, and kills its complete group on forced termination or unexpected parent exit. The server re-verifies containment before resolving paths, opening SQLite, or claiming shared state.

Windows creation is atomic: the process is created suspended, assigned to a nonce-named kill-on-close Job, identity-checked, and only then resumed. The launcher retains the Job until the daemon has opened and verified its own handle. Forced shutdown opens the same verified Job and terminates all members. Startup locks serialize all post-lock lease and ownership decisions, and native macOS lifecycle coverage verifies launch, authentication, shutdown, descendant cleanup, watchdog cleanup, and repeated operation without residue. Deterministic Windows boundary tests cover suspended creation, assignment failure, resume failure, Job membership, and complete cleanup; live Windows execution remains a host-matrix follow-up rather than an open repository finding.

### P1-5 — Resolved: shared snapshots publish as one manifest-committed generation

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/runtime/shared_state.py:353-383,444-493,532-538,569-601`
- `src/data_engine/runtime/runtime_cache_store.py:849-940`

The writer updates four Parquet files one at a time. Empty runs, steps, and file-state tables are represented by deleting their file; empty logs have no row from which to read a generation. The consistency reader discards missing generations and accepts the snapshot whenever the remaining set contains at most one generation.

A deterministic checkpoint-interleaving reproduction returned zero runs with one old-generation step. `hydrate_local_runtime_state()` then raised `sqlite3.IntegrityError: FOREIGN KEY constraint failed` while inserting the orphan step. Per-file atomic replacement does not make the four-file set atomic, and the retries do not delay or compare a committed manifest.

Impact: observers and startup hydration can fail during ordinary transitions to an empty table.

Resolution: one SQLite export transaction captures all four runtime tables, including typed empty tables. The writer commits immutable generation artifacts first and atomically replaces a JSON manifest last; readers accept only the manifest-selected generation and validate every artifact before replacing SQLite. Publication and garbage collection share a per-workspace process lock, continuously pin the manifest generation, and retain bounded prior generations. Concurrent-publication, empty-table, interrupted-generation, and hydration-retry coverage verifies the contract.

### P1-6 — Resolved: hydration preserves IDs and independently invalidates incremental consumers

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/runtime/runtime_cache_store.py:849-912`
- `src/data_engine/services/runtime_binding.py:271-300`
- `src/data_engine/services/logs.py:26-42`
- `src/data_engine/views/logs.py:20-21,112-115`

`RuntimeSnapshotRepository.replace()` deletes and reinserts step and log rows without their supplied IDs. Because these are `INTEGER PRIMARY KEY` tables without `AUTOINCREMENT`, IDs can restart at 1. The log store and step-output cache continue querying after their previous high-water IDs. A reproduction replaced old ID-1 rows with different new ID-1 rows: SQLite contained the new output and log, while both operator caches continued to show the old values indefinitely.

Impact: observers can remain permanently stale after hydration or ownership changes, even though the local database is current.

Resolution: snapshot replacement preserves persisted step and log IDs and records the applied generation transactionally. Logs and step outputs track independent generation cursors, so either consumer can refresh first without consuming the other consumer's invalidation signal. Every open runtime ledger also carries a unique incarnation in checkpoint and hydration tokens, preventing equal SQLite counters on distinct handles from suppressing publication or hydration. Reused-ID and equal-counter regressions verify both cache paths.

### P1-7 — Resolved: runtime-I/O proxy closure is idempotent and cannot strand another client

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/services/runtime_io.py:65-76,132-147,454-467`

`RuntimeIoCacheStore.close()` has no per-proxy closed flag. Repeated close calls decrement the shared handle repeatedly. With two proxies, closing one twice reduced the reference count from 2 to 0 and stopped the writer. A write through the second live proxy then queued behind the shutdown sentinel and waited forever because `submit_write()` has no closed-state check or timeout. The root review reproduced this with a write thread that remained blocked.

Impact: an ordinary duplicate cleanup path can permanently hang another window, binding, or service using the same runtime database.

Resolution: each proxy now owns an idempotent lease. Closing a lease waits without polling for its own in-flight operations, releases the shared handle exactly once, and rejects later use. Handle shutdown and write admission share one lock, so no write can be queued behind the shutdown sentinel. Snapshot replacement is serialized through the same writer, and a read overlapping invalidation is never cached under the newer generation. Concurrent-close, final-close/open, stale-snapshot, overlapping-read, and surviving-client coverage verifies the lifecycle boundary.

### P1-8 — Resolved: installation UUID is the ownership identity and hostname is display-only

Status: resolved on 2026-07-13.

Evidence:

- `src/data_engine/platform/workspace_models.py:151-153`
- `src/data_engine/hosts/daemon/ownership.py:21-29`
- `src/data_engine/hosts/daemon/client.py:184-227`

`machine_id_text()` is only `socket.gethostname()`. Hostnames can be duplicated by cloning, imaging, corporate naming, containers, or manual configuration, and can change over time. A duplicate name causes another machine's control request to be ignored as self-originated and allows a remote lease PID to be treated as local.

Impact: broken control transfer and, combined with P1-3, possible signaling of an unrelated local PID.

Resolution: a canonical UUIDv4 is created or repaired under an immediate SQLite transaction in the machine-local settings store and cached by settings path. Lease and control decisions use that durable UUID; hostname is stored separately for display. Force-stop cleanup mutates metadata only when the lease UUID matches the current installation, including when two machines share a hostname. The daemon protocol uses a clean cutover because no old daemon processes require compatibility.

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

Resolution: positive AF_PIPE and AF_UNIX timeouts now use one absolute deadline across connection establishment, both directions of multiprocessing authentication, complete framed request writes, and complete framed response reads. Native Unix framing uses nonblocking descriptors with `poll`, bounded 64 KiB chunks, and exact framing compatibility. Windows uses overlapped reads and writes, cancels pending operations on deadline, and retires each overlapped operation before returning. Requests are capped at 1 MiB, responses at 64 MiB, and the server admits at most 32 concurrent connection workers so incomplete or hostile clients cannot create unbounded threads or memory use. Windows imports remain lazy and no helper poller is added. Deterministic tests cover unavailable and busy pipes, partial headers and bodies, blocked writes, pending-operation cancellation, oversized frames, authentication cleanup, high-numbered descriptors, worker saturation, and remaining-deadline propagation.

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
- The final full suite passes after all repairs: `1253 passed, 2 skipped in 36.04s` on Python 3.14.6.
- Ruff passes with no configured violations.
- pydoclint reports no violations under `src/data_engine`.
- `pip check` reports no broken requirements.
- The review-baseline full-suite statement coverage was 83% (`19,162` statements, `3,211` missed); coverage was not re-measured after the repair batch.
- A clean PEP 517 sdist and wheel build succeeded, including the containment bootstrap and POSIX watchdog modules: `py_data_engine-0.3.12.tar.gz` and `py_data_engine-0.3.12-py3-none-any.whl`; `twine check` passed for both.
- The hash-locked Windows CPython 3.14 runtime requirements resolved and downloaded successfully with `--require-hashes`.
- Focused runtime, daemon/CLI/platform, and UI/authoring suites also passed during the audit.

### Systemic gaps exposed by the findings

1. **No validation CI for ordinary changes.** `.github/workflows/` contains only a manually dispatched PyPI build/publish workflow. There is no push/pull-request job for tests, Ruff, pydoclint, dependency checks, or packaging validation.

2. **No static type-checking gate.** The now-resolved real/fake `PersistedStepRun` mismatch in P2-2 is the kind of protocol drift a configured Pyright or mypy check should catch before runtime.

3. **Coverage was weakest in orchestration boundaries where several findings lived.** Baseline examples included GUI launcher 0%, platform process helpers 68%, continuous execution 73%, daemon ownership 73%, runtime commands 78%, and daemon manager 79%. Targeted tests now cover the repaired branches, but branch/fault/interleaving behavior matters more than aggregate line coverage for these modules.

4. **Fakes can diverge from production contracts.** P2-2 originally passed because its fake supplied a nonexistent property; that test now uses the real ledger contract. Continue preferring protocol-conforming fakes plus at least one real-store integration test for service boundaries.

5. **Platform checks still need a real-host matrix.** Native macOS launch, authentication, graceful shutdown, forced containment, and cleanup passed during resolution. Add continuous Windows coverage for overlapped named-pipe deadlines, suspended Job assignment, launcher-process behavior, and daemon lifetime, plus Linux coverage for pidfd watchdog behavior.

The dependency-refresh package-tooling environment completed the PEP 517 build and `twine check` successfully.

## Recommended follow-up order

All review findings are closed. The remaining work is preventive quality infrastructure:

1. Add push and pull-request CI for the full test suite, Ruff, pydoclint, dependency validation, and PEP 517/Twine packaging checks.
2. Add a real Windows and Linux host matrix for named-pipe deadlines, Windows Job containment, pidfd watchdog behavior, daemon lifecycle, and packaging entry points.
3. Add a static type-checking gate and retain deterministic fault, race, and process-lifecycle regressions alongside the real-host tests.

## Review limitations

- No live multi-machine network share was available; fencing failures were reproduced deterministically with two logical owners over the same temporary shared state.
- Windows-specific named-pipe timing, overlapped I/O cancellation, Job assignment, and filesystem failures were verified at deterministic native-boundary tests but were not executed on a Windows host in this review.
- The complete contained lifecycle was executed repeatedly on macOS; Linux pidfd behavior was covered deterministically but not on a live Linux host.
- No long-running GUI visual exploration was performed; GUI behavior was covered by the existing Qt suite and targeted launcher reproduction.
- This was a correctness and quality review, not a dedicated security audit or performance benchmark.
