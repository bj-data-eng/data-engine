# Project Map

This page is generated from the current AST map and summarizes package size and stitching points.

## Regenerating the map

This page is regenerated automatically during packaged docs builds. To refresh it manually, run:

```bash
python -m data_engine.devtools.project_ast_map src/data_engine --write-docs src/data_engine/docs/sphinx_source/guides
```

## Package Rollup

These counts are package-level rollups from the current AST snapshot.

| Package | Modules | Functions | Classes | Flows | Lines |
| --- | ---: | ---: | ---: | ---: | ---: |
| `data_engine` | 1 | 1 | 0 | 0 | 37 |
| `data_engine.application` | 7 | 5 | 19 | 0 | 1029 |
| `data_engine.authoring` | 3 | 7 | 2 | 0 | 257 |
| `data_engine.core` | 5 | 12 | 14 | 0 | 1107 |
| `data_engine.devtools` | 3 | 39 | 5 | 0 | 1189 |
| `data_engine.docs` | 2 | 0 | 0 | 0 | 41 |
| `data_engine.domain` | 16 | 10 | 40 | 0 | 1827 |
| `data_engine.flow_modules` | 3 | 17 | 2 | 0 | 384 |
| `data_engine.helpers` | 3 | 30 | 5 | 0 | 1308 |
| `data_engine.hosts` | 18 | 58 | 17 | 0 | 2577 |
| `data_engine.platform` | 9 | 35 | 10 | 0 | 1308 |
| `data_engine.runtime` | 16 | 38 | 32 | 0 | 3192 |
| `data_engine.services` | 15 | 14 | 18 | 0 | 1296 |
| `data_engine.ui` | 65 | 172 | 35 | 0 | 8558 |
| `data_engine.views` | 11 | 29 | 9 | 0 | 835 |

## Largest Modules

| Module | Lines | Functions | Classes |
| --- | ---: | ---: | ---: |
| `data_engine.helpers.duckdb` | 980 | 23 | 0 |
| `data_engine.runtime.runtime_db` | 980 | 0 | 5 |
| `data_engine.ui.gui.theme` | 736 | 1 | 0 |
| `data_engine.devtools.project_ast_map` | 597 | 20 | 5 |
| `data_engine.devtools.smoke_data` | 577 | 19 | 0 |
| `data_engine.hosts.daemon.client` | 520 | 29 | 2 |
| `data_engine.runtime.shared_state` | 520 | 30 | 0 |
| `data_engine.ui.gui.widgets.panels` | 498 | 12 | 0 |
| `data_engine.ui.gui.bootstrap` | 489 | 6 | 2 |
| `data_engine.ui.tui.bootstrap` | 475 | 6 | 2 |

## Internal Stitching Points

Modules with higher internal import fan-out tend to be composition or aggregation points.

| Module | Internal Imports | Lines |
| --- | ---: | ---: |
| `data_engine.domain` | 65 | 92 |
| `data_engine.views` | 61 | 109 |
| `data_engine.hosts.daemon.app` | 44 | 225 |
| `data_engine.ui.gui.render_support` | 43 | 241 |
| `data_engine.ui.gui.bootstrap` | 40 | 489 |
| `data_engine.ui.tui.bootstrap` | 39 | 475 |
| `data_engine.platform.workspace_policy` | 38 | 343 |
| `data_engine.ui.gui.presenters` | 35 | 64 |
| `data_engine.ui.gui.helpers` | 30 | 62 |
| `data_engine.ui.cli.app` | 28 | 160 |

## Practical Mental Model

- Start in `data_engine.authoring` when changing how flows are expressed.
- Start in `data_engine.runtime` when changing how flows are executed.
- Start in `data_engine.helpers` when improving operator-friendly flow utilities.
- Start in `data_engine.runtime` and `data_engine.hosts` for daemon behavior, state publication, logging, leasing, or checkpoints.
- Start in `data_engine.application` for host-agnostic use-case behavior.
- Start in `data_engine.ui` for interaction, rendering, presentation, or operator workflow.
- Start in `data_engine.platform` for workspace discovery, path resolution, or platform compatibility.

