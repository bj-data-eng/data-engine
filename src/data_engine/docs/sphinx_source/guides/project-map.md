# Project Map

This page is a small structural map of the current `data_engine` package, based on the AST mapper in `src/data_engine/devtools/project_ast_map.py`.

It is meant to answer:

- where the codebase is heaviest
- how the main packages are divided
- which modules are acting like stitching points

This is not a hand-wavy architecture diagram. It is a lightweight snapshot derived from the current Python source tree.

## Regenerating the map

The source for this page comes from:

```bash
python -m data_engine.devtools.project_ast_map \
  src/data_engine
```

If the package structure changes substantially, this page should be refreshed.

## Package Rollup

These counts are package-level rollups from the current AST snapshot.

| Package | Modules | Functions | Classes | Flows | Lines |
| --- | ---: | ---: | ---: | ---: | ---: |
| `data_engine` | 1 | 1 | 0 | 0 | 37 |
| `data_engine.application` | 7 | 5 | 19 | 0 | 1029 |
| `data_engine.authoring` | 16 | 20 | 24 | 0 | 2072 |
| `data_engine.devtools` | 2 | 12 | 5 | 0 | 360 |
| `data_engine.docs` | 2 | 0 | 0 | 0 | 38 |
| `data_engine.domain` | 16 | 9 | 40 | 0 | 1811 |
| `data_engine.flow_modules` | 3 | 17 | 2 | 0 | 381 |
| `data_engine.helpers` | 2 | 20 | 0 | 0 | 662 |
| `data_engine.hosts` | 17 | 55 | 14 | 0 | 2383 |
| `data_engine.platform` | 6 | 20 | 9 | 0 | 933 |
| `data_engine.runtime` | 5 | 37 | 9 | 0 | 1763 |
| `data_engine.services` | 15 | 13 | 16 | 0 | 1189 |
| `data_engine.ui` | 65 | 174 | 35 | 0 | 8607 |
| `data_engine.views` | 11 | 29 | 9 | 0 | 822 |

## How To Read It

The package split currently looks like this:

- `data_engine.ui` is by far the largest surface. That is expected because it includes both the Qt GUI and the TUI, plus their presenters, controllers, widgets, dialogs, rendering helpers, and bootstrapping.
- `data_engine.hosts`, `data_engine.runtime`, and `data_engine.application` are the runtime control spine. That is where daemon orchestration, runtime state, and host-agnostic application use cases live.
- `data_engine.authoring`, `data_engine.helpers`, and `data_engine.flow_modules` are the flow-authoring side of the package.
- `data_engine.domain`, `data_engine.platform`, `data_engine.services`, and `data_engine.views` are the supporting layers that hold shared models, path policy, services, and rendering/state helpers.

That means the current codebase is not “all runtime” or “all UI.” It is a UI-heavy operator product built on a fairly distinct runtime and authoring core.

## Largest Modules

The largest modules in the current tree are:

| Module | Lines | Functions | Classes |
| --- | ---: | ---: | ---: |
| `data_engine.runtime.runtime_db` | 938 | 0 | 1 |
| `data_engine.ui.gui.theme` | 720 | 1 | 0 |
| `data_engine.helpers.duckdb` | 639 | 20 | 0 |
| `data_engine.runtime.shared_state` | 523 | 30 | 0 |
| `data_engine.ui.gui.widgets.panels` | 507 | 12 | 0 |
| `data_engine.ui.gui.bootstrap` | 487 | 6 | 2 |
| `data_engine.ui.tui.bootstrap` | 475 | 6 | 2 |
| `data_engine.hosts.daemon.client` | 465 | 26 | 2 |
| `data_engine.application.runtime` | 449 | 4 | 8 |
| `data_engine.ui.gui.controllers.flows` | 439 | 0 | 3 |

### What jumps out

- `runtime_db` is the densest persistence hotspot.
- `helpers.duckdb` has already become a meaningful public convenience layer.
- `ui.gui.theme` is large in a very different way: it is styling density, not orchestration density.
- GUI and TUI bootstraps are both sizable, which means the app has two real presentation surfaces, not one thin shell around the other.

## Internal Stitching Points

The AST map also highlights modules with the most internal import fan-out. These tend to be the places where many parts of the system are assembled together.

| Module | Internal Imports | Lines |
| --- | ---: | ---: |
| `data_engine.domain` | 65 | 92 |
| `data_engine.views` | 61 | 109 |
| `data_engine.hosts.daemon.app` | 45 | 199 |
| `data_engine.ui.gui.render_support` | 43 | 241 |
| `data_engine.ui.gui.bootstrap` | 40 | 487 |
| `data_engine.ui.tui.bootstrap` | 39 | 475 |
| `data_engine.ui.gui.presenters` | 39 | 72 |
| `data_engine.platform.workspace_policy` | 35 | 302 |
| `data_engine.ui.gui.helpers` | 30 | 62 |
| `data_engine.authoring.flow` | 29 | 361 |

### What that means

- `data_engine.domain` and `data_engine.views` are acting as aggregation packages.
- `data_engine.hosts.daemon.app` is a strong assembly point for the daemon host.
- `data_engine.ui.gui.bootstrap` and `data_engine.ui.tui.bootstrap` are real composition roots.
- `data_engine.platform.workspace_policy` is central enough that path/layout drift shows up there quickly.
- `data_engine.authoring.flow` remains one of the most important authoring core modules.

## Practical Mental Model

If you are navigating the repo, this is a good compact way to think about it:

1. Start in `data_engine.authoring` when you are changing how flows are expressed or executed.
2. Start in `data_engine.helpers` when you are improving operator-friendly flow utilities like the DuckDB helpers.
3. Start in `data_engine.runtime` and `data_engine.hosts` when the problem is about daemon behavior, state publication, logging, leasing, or checkpoints.
4. Start in `data_engine.application` when the issue is host-agnostic use-case behavior rather than UI details.
5. Start in `data_engine.ui` when the issue is interaction, rendering, presentation, or operator workflow.
6. Start in `data_engine.platform.workspace_policy` when the issue is workspace discovery, path resolution, or local-vs-shared state layout.

## Current Shape In One Sentence

The package is currently a UI-heavy operator application wrapped around a fairly well-separated runtime, authoring, and workspace-control core.
