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
| `data_engine.application` | 7 | 6 | 19 | 0 | 1040 |
| `data_engine.authoring` | 3 | 7 | 2 | 0 | 262 |
| `data_engine.core` | 5 | 12 | 14 | 0 | 1309 |
| `data_engine.devtools` | 3 | 44 | 5 | 0 | 1371 |
| `data_engine.docs` | 2 | 0 | 0 | 0 | 41 |
| `data_engine.domain` | 16 | 10 | 40 | 0 | 1847 |
| `data_engine.flow_modules` | 3 | 21 | 3 | 0 | 500 |
| `data_engine.helpers` | 4 | 35 | 7 | 0 | 2204 |
| `data_engine.hosts` | 18 | 55 | 17 | 0 | 2575 |
| `data_engine.platform` | 9 | 35 | 10 | 0 | 1300 |
| `data_engine.runtime` | 19 | 42 | 48 | 0 | 3795 |
| `data_engine.services` | 17 | 15 | 34 | 0 | 1746 |
| `data_engine.ui` | 65 | 175 | 35 | 0 | 8756 |
| `data_engine.views` | 11 | 29 | 9 | 0 | 885 |

## Largest Modules

| Module | Lines | Functions | Classes |
| --- | ---: | ---: | ---: |
| `data_engine.helpers.duckdb` | 980 | 23 | 0 |
| `data_engine.runtime.runtime_cache_store` | 893 | 0 | 8 |
| `data_engine.helpers.polars` | 773 | 5 | 2 |
| `data_engine.devtools.smoke_data` | 759 | 24 | 0 |
| `data_engine.ui.gui.theme` | 736 | 1 | 0 |
| `data_engine.runtime.shared_state` | 621 | 33 | 6 |
| `data_engine.devtools.project_ast_map` | 597 | 20 | 5 |
| `data_engine.core.primitives` | 528 | 1 | 10 |
| `data_engine.hosts.daemon.client` | 520 | 29 | 2 |
| `data_engine.core.flow` | 515 | 0 | 1 |

## Internal Stitching Points

Modules with higher internal import fan-out tend to be composition or aggregation points.

| Module | Internal Imports | Lines |
| --- | ---: | ---: |
| `data_engine.domain` | 65 | 92 |
| `data_engine.views` | 61 | 109 |
| `data_engine.hosts.daemon.app` | 44 | 220 |
| `data_engine.ui.gui.render_support` | 43 | 241 |
| `data_engine.ui.gui.bootstrap` | 41 | 503 |
| `data_engine.ui.tui.bootstrap` | 40 | 489 |
| `data_engine.platform.workspace_policy` | 38 | 335 |
| `data_engine.helpers` | 38 | 43 |
| `data_engine.ui.gui.presenters` | 36 | 66 |
| `data_engine.ui.gui.helpers` | 30 | 62 |

## Practical Mental Model

- Start in `data_engine.authoring` when changing how flows are expressed.
- Start in `data_engine.runtime` when changing how flows are executed.
- Start in `data_engine.helpers` when improving operator-friendly flow utilities.
- Start in `data_engine.runtime` and `data_engine.hosts` for daemon behavior, state publication, logging, leasing, or checkpoints.
- Start in `data_engine.application` for host-agnostic use-case behavior.
- Start in `data_engine.ui` for interaction, rendering, presentation, or operator workflow.
- Start in `data_engine.platform` for workspace discovery, path resolution, or platform compatibility.

