# Agent Guide

This repository is a Python 3.14 package for the Data Engine workflow runtime, GUI, and daemon. Use this guide when making automated changes in the repo.

## Project Shape

- Runtime code lives under `src/data_engine/`.
- Tests live under `tests/`.
- Installer scripts live under `INSTALL/`.
- Smoke-data generation lives under `scripts/` and `src/data_engine/devtools/`.
- Generated local data/workspaces are intentionally ignored: `data/`, `data2/`, and `workspaces/`.
- Build artifacts are ignored: `build/`, `dist/`, `*.egg-info/`, and generated docs under `src/data_engine/docs/html/`.

## Environment

- Use the repo virtualenv on Windows: `.\.venv\Scripts\python.exe`.
- The package requires Python `>=3.14`.
- VS Code provisioning should point to the interpreter that launched `data-engine`. On Windows, normalize `pythonw.exe` to the sibling `python.exe`.

## Testing

- Full suite:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

- Qt-focused suite:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_qt_ui.py -q
```

- Daemon-focused suite:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_daemon.py tests\test_live_runtime_suite.py -q
```

- Lint and docstring checks:

```powershell
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\pydoclint.exe src\data_engine
```

- Packaging check:

```powershell
.\.venv\Scripts\python.exe -m build
.\.venv\Scripts\python.exe -m twine check dist\*
```

## Parallel Worktrees

- Use semi-permanent worker lanes for high-throughput parallel work: `C:\DEV_PROJECT\data-engine-worktrees\mini-1` through `C:\DEV_PROJECT\data-engine-worktrees\mini-6`.
- The coordinator owns task slicing, architecture decisions, integration, final tests, and commits. Mini workers own bounded implementation or audit tasks inside their assigned lane.
- Prefer fresh worker agents per task while reusing the same worktree lane. Keep worker prompts small: objective, lane path, branch name, owned files/modules, tests to run, and expected report format.
- Give each worker a disjoint write scope. Do not ask multiple workers to edit the same files unless the coordinator explicitly serializes the work.
- Worker branches should be task-scoped from the lane branch, using names like `codex/mini-1-runtime-audit`. After integration, reset or recreate the lane branch for the next task.
- Do not add internal compatibility shims just to preserve old project-internal call shapes during refactors. Update affected internal callers to the new boundary instead. Preserve stability for the author-facing surface: flows, flow context, and `data_engine.helpers`.
- For speed, worker lanes may use the main repo venv at `C:\DEV_PROJECT\data-engine\.venv` for `python -m pytest`, `ruff`, and `pydoclint` when the command runs from the worker worktree root.
- Do not rely on the main repo console scripts, such as `data-engine.exe`, to test worker-lane code; console entry points may resolve to the installed checkout. Use `python -m ...` from the worker root, or create a lane-local `.venv` when testing packaging, console scripts, editable installs, or dependency changes.
- Workers should report only changed files, summary, tests run, and blockers. The coordinator handles synthesis and broader follow-up.

## Windows And Unix Compatibility

- This project is actively tested on Windows. Do not add Unix-only subprocess calls such as `ps`, shell-specific path assumptions, or POSIX-only daemon semantics without a Windows path.
- Keep Unix/macOS behavior intact when fixing Windows issues. Prefer platform-specific branches with tests for both paths.
- Use `data_engine.platform.processes` for local process listing, PID liveness, process-tree termination, Windows launcher-process collapsing, and Windows subprocess creation flags.
- Use `data_engine.platform.interpreters` for `python.exe` / `pythonw.exe` selection and host-concrete interpreter paths.
- Use `data_engine.platform.paths` for generic path display, stable absolute paths, path identity text, TOML-safe path text, and platform-aware sort keys. `workspace_models` re-exports some of these for compatibility, but new non-workspace code should import from `platform.paths`.
- Windows venv launchers can produce parent/child `pythonw.exe` process pairs. Daemon diagnostics should collapse launcher shims and report the real Data Engine process.
- Do not use `DETACHED_PROCESS` for Windows daemon launch unless retesting live daemon lifetime and terminal-window behavior. The stable path uses Windows creation flags that avoid console windows while keeping the daemon reachable.
- Windows daemon PID liveness must not rely on Unix `ps` output. Use the shared platform-aware process inspection helpers.

## Workspace And Runtime State

- Shared workspace state lives inside a workspace under `.workspace_state/`.
- Machine-local runtime state lives under the local app data runtime artifacts directory.
- The GUI must not create real runtime SQLite bindings for the synthetic unconfigured workspace placeholder.
- Prefer explicit `runtime_cache_ledger` and `runtime_control_ledger` names in new internal code. Keep the public `RuntimeLedger` and `runtime_ledger` aliases only where needed for API/test compatibility.
- Do not add internal compatibility shims just to preserve old project-internal call shapes during refactors. Update the affected internal callers to the new boundary instead. Preserve stability for the author-facing surface: flows, flow context, and `data_engine.helpers`.
- Client-session tracking is important for ephemeral daemon lifetime. If the UI flickers between "has control" and disconnected, inspect the selected workspace control DB and daemon log before changing UI code.
- Workspace provisioning is target-workspace specific. The Settings view has its own workspace selector and should make the provisioning target explicit.

## Generated Smoke Data

- Use the smoke-data generator when live-testing flows:

```powershell
.\.venv\Scripts\python.exe scripts\generate_smoke_data.py --root . --workspace-id example_workspace --workspace-id claims2
```

- Do not commit generated files from `data/`, `data2/`, or `workspaces/`.
- If large workbook generation is interrupted, clean up partial files before using the workspace for behavior tests.

## Packaging

- Current distribution name is `py-data-engine`.
- Before publishing, verify:

```powershell
git status --short
.\.venv\Scripts\python.exe -m build
.\.venv\Scripts\python.exe -m twine check dist\*
```

- Keep version values centralized in `src/data_engine/platform/identity.py`; package metadata, daemon constants, and Sphinx config should read from `APP_VERSION`.

## Git Hygiene

- Check `git status --short` before and after edits.
- Do not stage or commit ignored local workspaces, smoke data, build outputs, or machine-local settings.
- Do not revert user edits unless explicitly asked.
