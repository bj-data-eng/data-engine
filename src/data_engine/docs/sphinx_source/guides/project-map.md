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
| `data_engine.application` | 7 | 6 | 19 | 0 | 1109 |
| `data_engine.authoring` | 3 | 9 | 2 | 0 | 288 |
| `data_engine.core` | 5 | 12 | 15 | 0 | 1412 |
| `data_engine.devtools` | 3 | 44 | 5 | 0 | 1371 |
| `data_engine.docs` | 2 | 0 | 0 | 0 | 41 |
| `data_engine.domain` | 17 | 10 | 44 | 0 | 2232 |
| `data_engine.flow_modules` | 3 | 22 | 3 | 0 | 518 |
| `data_engine.helpers` | 9 | 62 | 7 | 0 | 3078 |
| `data_engine.hosts` | 20 | 63 | 25 | 0 | 3980 |
| `data_engine.platform` | 10 | 43 | 10 | 0 | 1495 |
| `data_engine.runtime` | 20 | 44 | 48 | 0 | 4405 |
| `data_engine.services` | 23 | 38 | 81 | 0 | 5173 |
| `data_engine.ui` | 73 | 228 | 47 | 0 | 13725 |
| `data_engine.views` | 12 | 44 | 11 | 0 | 1414 |

## Largest Modules

| Module | Lines | Functions | Classes |
| --- | ---: | ---: | ---: |
| `data_engine.helpers.polars` | 1594 | 28 | 2 |
| `data_engine.ui.gui.rendering.artifacts` | 1420 | 14 | 7 |
| `data_engine.runtime.runtime_cache_store` | 1095 | 0 | 8 |
| `data_engine.ui.gui.theme` | 996 | 3 | 0 |
| `data_engine.services.runtime_state` | 930 | 3 | 12 |
| `data_engine.ui.gui.controllers.flows` | 836 | 0 | 3 |
| `data_engine.ui.gui.controllers.runtime` | 802 | 0 | 1 |
| `data_engine.devtools.smoke_data` | 759 | 24 | 0 |
| `data_engine.core.primitives` | 631 | 1 | 11 |
| `data_engine.runtime.shared_state` | 621 | 33 | 6 |

## Internal Stitching Points

Modules with higher internal import fan-out tend to be composition or aggregation points.

| Module | Internal Imports | Lines |
| --- | ---: | ---: |
| `data_engine.domain` | 70 | 99 |
| `data_engine.views` | 65 | 113 |
| `data_engine.hosts.daemon.app` | 55 | 390 |
| `data_engine.helpers` | 44 | 49 |
| `data_engine.ui.gui.render_support` | 43 | 240 |
| `data_engine.ui.gui.bootstrap` | 42 | 472 |
| `data_engine.ui.tui.bootstrap` | 41 | 471 |
| `data_engine.ui.gui.presenters` | 39 | 68 |
| `data_engine.platform.workspace_policy` | 38 | 335 |
| `data_engine.ui.cli.app` | 33 | 172 |

## Practical Mental Model

- Start in `data_engine.authoring` when changing how flows are expressed.
- Start in `data_engine.runtime` when changing how flows are executed.
- Start in `data_engine.helpers` when improving operator-friendly flow utilities.
- Start in `data_engine.runtime` and `data_engine.hosts` for daemon behavior, state publication, logging, leasing, or checkpoints.
- Start in `data_engine.application` for host-agnostic use-case behavior.
- Start in `data_engine.ui` for interaction, rendering, presentation, or operator workflow.
- Start in `data_engine.platform` for workspace discovery, path resolution, or platform compatibility.

