# App Runtime and Workspaces

This guide explains how the desktop app, authored workspaces, shared workspace state, and machine-local runtime state fit together.

If you are writing flows, this is the missing "how the whole thing hangs together" page.

## The two roots to keep in mind

There are usually two important folders:

- the workspace collection root
- one authored workspace inside that collection

Example:

```text
workspaces/
  example_workspace/
    flow_modules/
    flow_modules/flow_helpers/
    config/
    databases/
    .workspace_state/
```

The collection root is the parent folder that contains one or more authored workspaces.

The authored workspace is the folder that contains the authoring surface for one logical workspace:

- `flow_modules/`
- `flow_modules/flow_helpers/`
- `config/`
- `databases/`

That authored workspace is what the app binds to when you select a workspace in the UI.

## How the app is structured

The desktop app is a single-window operator surface that binds to one authored workspace at a time.

When you change the selected workspace, the app rebinds:

- workspace paths
- flow discovery
- daemon client and daemon manager
- local runtime ledger
- visible run history and log views
- control state and lease state

This means the app is multi-workspace for discovery and selection, but single-workspace for active runtime context.

That distinction matters when you are reasoning about:

- what is cheap to inspect globally
- what is authoritative for the currently selected workspace
- why the UI can feel like one workspace "becomes" the app until you switch again

## Authored files vs generated runtime artifacts

The authored workspace is intentionally small and human-owned.

Author-owned folders:

- `flow_modules/`: runnable flow modules
- `flow_modules/flow_helpers/`: reusable helper code imported by flows
- `config/`: workspace-local TOML config files
- `databases/`: a conventional home for workspace-local database files

Generated or runtime-managed state lives elsewhere:

- shared workspace state inside `.workspace_state/`
- machine-local runtime artifacts under the app runtime root

That split is deliberate:

- the authored workspace is what you share, edit, and reason about
- runtime caches and ledgers are free to be machine-local and disposable

## Shared workspace state

Every authored workspace can also contain a shared control and checkpoint folder:

```text
.workspace_state/
  available/
  leased/
  stale/
  leases/
  control_requests/
  state/
    runs/
    step_runs/
    logs/
    file_state/
```

This is the workspace-coordination layer.

It is used for:

- control ownership
- lease heartbeat/checkpoint state
- stale-lease recovery
- handoff requests between workstations
- shared runtime snapshots

### Available, leased, and stale

The app and daemon use simple marker folders to represent workspace control:

- `available/<workspace_id>` means nobody currently owns the workspace
- `leased/<workspace_id>` means one machine currently owns it
- `stale/...` is where stale leased markers are quarantined during recovery

Only one workstation should actively own a workspace at a time.

### Lease metadata and heartbeat

When a daemon owns a workspace, it writes lease metadata in:

- `.workspace_state/leases/<workspace_id>.parquet`

That metadata includes:

- workspace id
- machine id / host name
- daemon id
- PID
- status
- started time
- last checkpoint time
- app version
- snapshot generation id

The checkpoint time is the heartbeat signal. The daemon refreshes it during normal operation so other clients can tell:

- the workspace is still controlled
- who controls it
- whether the controlling daemon looks healthy

The control model currently uses:

- a target checkpoint interval of 30 seconds
- a stale threshold of 90 seconds

Those numbers come from the runtime domain model and define the control behavior used across surfaces.

### Shared runtime snapshots

The shared runtime snapshot is written into parquet files beneath:

- `.workspace_state/state/runs/`
- `.workspace_state/state/step_runs/`
- `.workspace_state/state/logs/`
- `.workspace_state/state/file_state/`

These files let one workstation publish the current runtime picture so another workstation can hydrate a local read model while observing the shared workspace.

This lets the app show meaningful status while another machine owns the workspace daemon.

## Local state vs workspace state

Data Engine uses both shared workspace state and machine-local state.

### Shared workspace state

Shared workspace state lives inside the authored workspace under `.workspace_state/`.

It exists so multiple workstations can coordinate around:

- control ownership
- control requests
- shared run history snapshots
- shared logs
- file freshness state

### Machine-local state

Machine-local state lives under the app runtime root and local settings store.

This includes:

- the local SQLite runtime ledger for the currently selected workspace
- compiled flow-module artifacts
- runtime caches
- daemon log files
- app-local workspace selection and collection-root settings

The local runtime ledger path is resolved per workspace and stays machine-local.

When no workspace collection root is configured, the app stays in an explicit "no workspace" state. The empty-state UI uses that state directly and avoids per-workspace daemon or runtime artifacts.

Compiled flow-module artifacts are also workspace-local. Data Engine loads helper imports against the active workspace's compiled artifacts so similarly named helper modules in different workspaces stay isolated from each other.

That local ledger is important because the desktop app needs a fast local read model even when the authoritative daemon is elsewhere.

### Why both exist

The split gives the system two useful properties:

- one workstation can own and publish runtime state for a workspace
- another workstation can still open the workspace and observe it without taking control

It also keeps the authored workspace from becoming a dumping ground for every cache and local artifact.

## Control, handoff, and control requests

Workspace control is intentionally conservative.

The basic model is:

1. a workstation claims the workspace
2. that workstation's daemon becomes the active owner
3. it keeps the lease alive through checkpoints
4. other workstations observe that the workspace is leased

If another workstation wants control, it can request it. Those requests are written to:

- `.workspace_state/control_requests/<workspace_id>.parquet`

A control request records:

- requester machine id
- requester host name
- requester pid
- requester client kind
- request time

The app surfaces this as "control requested" and makes the handoff visible to operators.

### Handoff and takeover

The control UI distinguishes between:

- local ownership
- another machine owning the workspace
- a pending local request for takeover
- takeover becoming available after the remote lease appears stale

That behavior comes from `WorkspaceControlState`, which derives operator-facing status from:

- the last daemon snapshot
- whether the daemon is live
- the current lease metadata checkpoint age
- any pending control request

### When a takeover is available

If a workspace is leased but the last checkpoint is older than the stale threshold, the UI can surface takeover availability.

The system can also quarantine stale lease state and recover it into the `stale/` area before reclaiming the workspace.

## The daemon and the selected workspace

The desktop app talks to a per-workspace local daemon.

For GUI use, the daemon lifecycle is intentionally ephemeral:

- it is created for the selected workspace as needed
- it can survive workspace switches when active work is still running
- it follows the selected workspace lifecycle and can stay alive while active work continues

The important behavior is this:

- switching away from a workspace leaves active work running
- switching back should rehydrate the selected workspace's daemon state immediately

That immediate rehydration is what keeps engine state, manual runs, and control state accurate after a workspace switch.

## Workspace selection

The workspace selector in the app chooses which authored workspace the window is currently bound to.

When you switch workspaces, the app:

- closes workspace-scoped preview dialogs
- invalidates stale deferred message-box callbacks
- hides the selector popup
- queues the actual rebind one Qt tick later

That last step is important because it lets the native combo-box popup finish closing before the rest of the workspace state is rebuilt.

Practically, the selected workspace governs:

- which flows are loaded
- which runtime ledger is open
- which daemon is being queried or controlled
- which logs and runs are visible in the main view
- which workspace-relative `context.config(...)` and `context.database(...)` calls make sense during authoring

## Workspace provisioning

Provisioning is deliberately safe and additive.

Provisioning a workspace creates missing conventional folders without overwriting existing files:

- `flow_modules/`
- `flow_modules/helpers/`
- `config/`
- `databases/`
- `.vscode/settings.json`

Provisioning also writes a `.vscode/settings.json` at the collection root.

If those files already exist, the provisioning service preserves the existing authored files by default.

This is meant to make a new workspace usable immediately without turning provisioning into a heavy bootstrap system.

## VS Code provisioning

Data Engine now writes VS Code settings in two places:

- at the workspace collection root
- at the individual authored workspace root

Both settings files use a workspace-relative interpreter:

```json
"python.defaultInterpreterPath": "${workspaceFolder}/.venv"
```

That makes the settings portable across workstations as long as each workstation keeps its venv in the same relative place.

The generated settings also:

- hide `.workspace_state` from VS Code Explorer and search
- set terminal environment variables for Data Engine paths on Linux, macOS, and Windows
- add `src/` to `python.analysis.extraPaths` when running from a checkout
- enable pytest configuration when a checkout-local `tests/` folder exists

The collection-root settings are for the "open the whole workspace collection in VS Code" workflow.

The authored-workspace settings are for the "open just one workspace" workflow.

## Flow-module compilation

Flow modules authored as notebooks or Python files are compiled into machine-local runtime artifacts before discovery and execution.

That compilation path intentionally favors structural correctness over filesystem timing quirks:

- recompilation is based on rendered content changes
- helper imports resolve from the current workspace
- mirrored helper packages swap into place as complete directory trees

Those guarantees matter most on network filesystems, cross-platform checkouts, and fast edit/save cycles with coarse timestamp granularity.

## Logging and run history

There are a few different log and history concepts that are easy to blur together.

### Shared runtime logs

The daemon publishes shared log snapshots into `.workspace_state/state/logs/`.

Those snapshots are part of the shared runtime picture used by observing clients.

### Local runtime ledger

The selected workspace also has a machine-local SQLite runtime ledger. That is the app's fast local runtime store and is what powers most local querying, hydrated snapshots, and UI views.

### GUI run history limits

The GUI intentionally limits how much visible run history it renders at once. The current run-history sidebar/view is capped to 50 visible run groups in the UI.

That cap is a presentation choice for the current UI view.

### "Runs last 7 days"

The small footer tag on the home view shows:

- modules
- groups
- flows
- runs in the last 7 days

That 7-day value is a summary count for the currently selected workspace.

## The kill switch

The Settings pane exposes an emergency kill switch for the selected workspace daemon.

This is intentionally coarse.

It works at the daemon-process level:

1. asks the daemon to shut down normally
2. waits briefly for a graceful exit
3. force-kills the daemon process if it is still alive
4. performs best-effort cleanup of local daemon/lease state

That is the right emergency tool when a flow is stuck inside a blocking native call or an uninterruptible external library path.

It is intentionally user-driven and appears as an explicit operator action.

## How this affects flow authors

The important authoring consequence is that a flow module is only one part of the overall system.

Your flow code runs inside:

- one authored workspace
- one selected app binding
- one daemon or manual run context
- one shared-control model

That is why the `FlowContext` surface is so valuable:

- `context.source` and `context.mirror` understand source-relative and output-relative paths
- `context.config` gives you structured workspace-local TOML config
- `context.database(...)` gives you a conventional workspace-local database path
- `context.metadata` lets you publish runtime details back into the UI/runtime model

For the authoring-level details, continue with [FlowContext](flow-context.md).
