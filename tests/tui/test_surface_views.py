from __future__ import annotations

import polars as pl
import pytest
from textual.widgets import DataTable, Static

from tests.tui.support import make_tui


@pytest.mark.anyio
async def test_tui_switches_between_operator_surfaces():
    app = make_tui()
    async with app.run_test():
        app.action_show_dataframes()

        assert app.query_one("#screen-title", Static).render().plain == "Dataframes"
        assert app.query_one("#dataframes-view").display is True
        assert app.query_one("#body").display is False

        app.action_show_home()

        assert app.query_one("#screen-title", Static).render().plain == "Flow Control"
        assert app.query_one("#body").display is True


@pytest.mark.anyio
async def test_tui_dataframe_view_previews_parquet_file(tmp_path):
    app = make_tui()
    source = tmp_path / "preview.parquet"
    pl.DataFrame({"claim_id": [1, 2], "status": ["open", "closed"]}).write_parquet(source)

    async with app.run_test():
        app.query_one("#dataframe-path-input").value = str(source)
        app._connect_dataframe_source()

        table = app.query_one("#dataframe-table", DataTable)
        status = app.query_one("#dataframe-status", Static).render().plain

        assert table.row_count == 2
        assert "preview.parquet" in str(status)
        assert "2 rows" in str(status)


@pytest.mark.anyio
async def test_tui_docs_view_lists_packaged_guides():
    app = make_tui()
    async with app.run_test():
        app._refresh_docs_view()

        assert len(app.query_one("#docs-page-list").children) > 0
        assert "packaged document" in app.query_one("#docs-status", Static).render().plain
