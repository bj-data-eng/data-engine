from __future__ import annotations

from pathlib import Path
import warnings

from openpyxl import Workbook
from openpyxl import load_workbook
from openpyxl.worksheet.table import Table
from openpyxl.worksheet.table import TableStyleInfo
import polars as pl
from polars.testing import assert_frame_equal
import pytest
import xlsxwriter
from xlsxwriter.worksheet import Worksheet

from data_engine.helpers import ExcelSheet
from data_engine.helpers import compose_excel


def test_compose_excel_writes_multiple_named_sheets_and_tables(tmp_path: Path):
    target = tmp_path / "nested" / "report.xlsx"
    claims = pl.DataFrame({"claim_id": [1, 2], "workflow": ["Appeals", "Enrollment"]})
    summary = pl.DataFrame({"workflow": ["Appeals", "Enrollment"], "count": [1, 1]})

    returned_path = compose_excel(
        target,
        sheets=[
            ExcelSheet(name="Claims", df=claims, table_name="claims", freeze_panes="A2"),
            ExcelSheet(name="Summary", df=summary, table_name="workflow_summary"),
        ],
    )

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_excel(target, sheet_name="Claims"), claims)
    assert_frame_equal(pl.read_excel(target, sheet_name="Summary"), summary)

    workbook = load_workbook(target)
    assert workbook.sheetnames == ["Claims", "Summary"]
    assert set(workbook["Claims"].tables) == {"claims"}
    assert set(workbook["Summary"].tables) == {"workflow_summary"}
    assert workbook["Claims"].freeze_panes == "A2"
    assert list(target.parent.glob(f".{target.name}.*.tmp.xlsx")) == []


def test_compose_excel_collects_lazy_frames(tmp_path: Path):
    target = tmp_path / "lazy.xlsx"
    frame = pl.DataFrame({"claim_id": [2, 1], "amount": [10, 20]})

    compose_excel(
        target,
        sheets=[
            ExcelSheet(
                name="Claims",
                df=frame.lazy().sort("claim_id"),
                table_name="claims",
            )
        ],
    )

    expected = pl.DataFrame({"claim_id": [1, 2], "amount": [20, 10]})
    assert_frame_equal(pl.read_excel(target, sheet_name="Claims"), expected)


def test_compose_excel_replaces_existing_workbook_atomically(tmp_path: Path):
    target = tmp_path / "report.xlsx"
    old_frame = pl.DataFrame({"claim_id": [0]})
    new_frame = pl.DataFrame({"claim_id": [1, 2]})

    compose_excel(target, sheets=[ExcelSheet(name="Claims", df=old_frame, table_name="old_claims")])
    returned_path = compose_excel(target, sheets=[ExcelSheet(name="Claims", df=new_frame, table_name="new_claims")])

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_excel(target, sheet_name="Claims"), new_frame)
    workbook = load_workbook(target)
    assert set(workbook["Claims"].tables) == {"new_claims"}
    assert list(target.parent.glob(f".{target.name}.*.tmp.xlsx")) == []


def test_compose_excel_applies_named_table_style_in_fresh_workbook(tmp_path: Path):
    target = tmp_path / "styled.xlsx"

    compose_excel(
        target,
        sheets=[
            ExcelSheet(
                name="Claims",
                df=pl.DataFrame({"claim_id": [1], "amount": [10.5]}),
                table_name="claims",
                table_style="TableStyleMedium9",
            )
        ],
    )

    table = load_workbook(target)["Claims"].tables["claims"]
    assert table.tableStyleInfo is not None
    assert table.tableStyleInfo.name == "TableStyleMedium9"


def test_compose_excel_forwards_fresh_workbook_write_options(tmp_path: Path):
    target = tmp_path / "formats.xlsx"

    compose_excel(
        target,
        sheets=[
            ExcelSheet(
                name="Claims",
                df=pl.DataFrame({"claim_id": [1], "amount": [10.5]}),
                table_name="claims",
                write_options={"hide_gridlines": True, "sheet_zoom": 125},
            )
        ],
    )

    worksheet = load_workbook(target)["Claims"]
    assert worksheet.sheet_view.showGridLines is False
    assert worksheet.sheet_view.zoomScale == 125


def test_compose_excel_template_path_can_update_same_target_and_preserve_other_sheets(tmp_path: Path):
    target = tmp_path / "template_report.xlsx"
    workbook = Workbook()
    data = workbook.active
    data.title = "Claims"
    data["A1"] = "old"
    pivot = workbook.create_sheet("Pivot")
    pivot["A1"] = "Keep this pivot-like sheet"
    workbook.save(target)

    frame = pl.DataFrame({"claim_id": [1, 2], "workflow": ["Appeals", "Enrollment"]})

    returned_path = compose_excel(
        target,
        sheets=[ExcelSheet(name="Claims", df=frame, table_name="claims", freeze_panes="A2")],
        template=target,
    )

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_excel(target, sheet_name="Claims"), frame)
    updated = load_workbook(target)
    assert updated.sheetnames == ["Claims", "Pivot"]
    assert updated["Pivot"]["A1"].value == "Keep this pivot-like sheet"
    assert set(updated["Claims"].tables) == {"claims"}
    assert updated["Claims"].freeze_panes == "A2"
    assert list(target.parent.glob(f".{target.name}.*.tmp.xlsx")) == []


def test_compose_excel_template_applies_named_table_style(tmp_path: Path):
    template = tmp_path / "template.xlsx"
    workbook = Workbook()
    workbook.active.title = "Claims"
    workbook.save(template)

    compose_excel(
        template,
        sheets=[
            ExcelSheet(
                name="Claims",
                df=pl.DataFrame({"claim_id": [1]}),
                table_name="claims",
                table_style="TableStyleMedium9",
            )
        ],
        template=template,
    )

    table = load_workbook(template)["Claims"].tables["claims"]
    assert table.tableStyleInfo is not None
    assert table.tableStyleInfo.name == "TableStyleMedium9"


def test_compose_excel_template_resizes_existing_table_to_new_frame_shape(tmp_path: Path):
    template = tmp_path / "template.xlsx"
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Claims"
    worksheet.append(["claim_id", "workflow"])
    worksheet.append([1, "Old"])
    table = Table(displayName="claims", ref="A1:B2")
    table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
    worksheet.add_table(table)
    workbook.save(template)

    frame = pl.DataFrame(
        {
            "claim_id": [1, 2, 3, 4, 5, 6],
            "workflow": ["Appeals", "Enrollment", "Intake", "Review", "Archive", "Done"],
        }
    )

    compose_excel(
        template,
        sheets=[ExcelSheet(name="Claims", df=frame, table_name="claims")],
        template=template,
    )

    updated = load_workbook(template)
    assert updated["Claims"].tables["claims"].ref == "A1:B7"
    assert_frame_equal(pl.read_excel(template, sheet_name="Claims"), frame)


def test_compose_excel_template_unmerges_output_ranges_and_preserves_unrelated_merges(tmp_path: Path):
    template = tmp_path / "merged_template.xlsx"
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Claims"
    worksheet.merge_cells("A1:B1")
    worksheet["A1"] = "old heading"
    worksheet.merge_cells("D4:E4")
    worksheet["D4"] = "old note"
    worksheet["D4"].number_format = "0.00"
    workbook.save(template)

    frame = pl.DataFrame({"claim_id": [1], "workflow": ["Appeals"]})

    compose_excel(
        template,
        sheets=[ExcelSheet(name="Claims", df=frame, table_name="claims")],
        template=template,
    )

    updated = load_workbook(template)["Claims"]
    assert "A1:B1" not in updated.merged_cells
    assert "D4:E4" in updated.merged_cells
    assert updated["D4"].value is None
    assert updated["D4"].number_format == "0.00"
    assert updated.tables["claims"].ref == "A1:B2"
    assert_frame_equal(pl.read_excel(template, sheet_name="Claims"), frame)


def test_compose_excel_template_updates_multiple_sheets_in_one_call(tmp_path: Path):
    template = tmp_path / "multi_template.xlsx"
    workbook = Workbook()
    workbook.active.title = "Claims"
    workbook.create_sheet("Summary")
    workbook.create_sheet("Pivot")
    workbook["Pivot"]["A1"] = "preserved"
    workbook.save(template)
    claims = pl.DataFrame({"claim_id": [1, 2], "workflow": ["Appeals", "Review"]})
    summary = pl.DataFrame({"workflow": ["Appeals", "Review"], "count": [1, 1]})

    compose_excel(
        template,
        sheets=[
            ExcelSheet(name="Claims", df=claims, table_name="claims"),
            ExcelSheet(name="Summary", df=summary, table_name="workflow_summary"),
        ],
        template=template,
    )

    updated = load_workbook(template)
    assert updated.sheetnames == ["Claims", "Summary", "Pivot"]
    assert updated["Pivot"]["A1"].value == "preserved"
    assert set(updated["Claims"].tables) == {"claims"}
    assert set(updated["Summary"].tables) == {"workflow_summary"}
    assert_frame_equal(pl.read_excel(template, sheet_name="Claims"), claims)
    assert_frame_equal(pl.read_excel(template, sheet_name="Summary"), summary)


def test_compose_excel_template_supports_cell_and_tuple_positions(tmp_path: Path):
    template = tmp_path / "template.xlsx"
    workbook = Workbook()
    workbook.active.title = "Positioned"
    workbook.create_sheet("TuplePosition")
    workbook.save(template)

    compose_excel(
        template,
        sheets=[
            ExcelSheet(
                name="Positioned",
                df=pl.DataFrame({"claim_id": [1], "workflow": ["Appeals"]}),
                table_name="positioned",
                position="C3",
            ),
            ExcelSheet(
                name="TuplePosition",
                df=pl.DataFrame({"claim_id": [2], "workflow": ["Review"]}),
                table_name="tuple_positioned",
                position=(1, 2),
            ),
        ],
        template=template,
    )

    workbook = load_workbook(template)
    positioned = workbook["Positioned"]
    assert positioned["C3"].value == "claim_id"
    assert positioned["D3"].value == "workflow"
    assert positioned["C4"].value == 1
    assert positioned.tables["positioned"].ref == "C3:D4"
    tuple_positioned = workbook["TuplePosition"]
    assert tuple_positioned["C2"].value == "claim_id"
    assert tuple_positioned["D2"].value == "workflow"
    assert tuple_positioned["C3"].value == 2
    assert tuple_positioned.tables["tuple_positioned"].ref == "C2:D3"


def test_compose_excel_template_path_writes_output_without_changing_template(tmp_path: Path):
    template = tmp_path / "source_template.xlsx"
    target = tmp_path / "output" / "report.xlsx"
    workbook = Workbook()
    workbook.active.title = "Claims"
    workbook.active["A1"] = "old"
    notes = workbook.create_sheet("Notes")
    notes["A1"] = "preserved"
    workbook.save(template)

    frame = pl.DataFrame({"claim_id": [3]})

    compose_excel(
        target,
        sheets=[ExcelSheet(name="Claims", df=frame, table_name="claims")],
        template=template,
    )

    assert_frame_equal(pl.read_excel(target, sheet_name="Claims"), frame)
    output = load_workbook(target)
    assert output["Notes"]["A1"].value == "preserved"
    assert set(output["Claims"].tables) == {"claims"}
    unchanged_template = load_workbook(template)
    assert unchanged_template["Claims"]["A1"].value == "old"


def test_compose_excel_template_requires_existing_workbook(tmp_path: Path):
    with pytest.raises(ValueError, match="template workbook does not exist"):
        compose_excel(
            tmp_path / "report.xlsx",
            sheets=[ExcelSheet(name="Claims", df=pl.DataFrame({"claim_id": [1]}))],
            template=tmp_path / "missing_template.xlsx",
        )


def test_dataframe_namespace_composes_single_sheet_workbook(tmp_path: Path):
    target = tmp_path / "namespace.xlsx"
    frame = pl.DataFrame({"claim_id": [1], "workflow": ["Appeals"]})

    returned_path = frame.de.compose_excel(
        target,
        sheet_name="Claims",
        table_name="claims",
        table_style="TableStyleMedium9",
        freeze_panes="A2",
    )

    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_excel(target, sheet_name="Claims"), frame)
    worksheet = load_workbook(target)["Claims"]
    assert worksheet.tables["claims"].tableStyleInfo.name == "TableStyleMedium9"
    assert worksheet.freeze_panes == "A2"


def test_lazyframe_namespace_composes_single_sheet_workbook(tmp_path: Path):
    target = tmp_path / "lazy_namespace.xlsx"
    frame = pl.DataFrame({"claim_id": [2, 1], "workflow": ["Review", "Appeals"]})

    returned_path = frame.lazy().sort("claim_id").de.compose_excel(
        target,
        sheet_name="Claims",
        table_name="claims",
    )

    expected = pl.DataFrame({"claim_id": [1, 2], "workflow": ["Appeals", "Review"]})
    assert returned_path == target.resolve()
    assert_frame_equal(pl.read_excel(target, sheet_name="Claims"), expected)
    assert set(load_workbook(target)["Claims"].tables) == {"claims"}


def test_dataframe_namespace_compose_excel_supports_template_mode(tmp_path: Path):
    template = tmp_path / "template.xlsx"
    workbook = Workbook()
    workbook.active.title = "Claims"
    workbook.create_sheet("Pivot")
    workbook["Pivot"]["A1"] = "preserved"
    workbook.save(template)
    frame = pl.DataFrame({"claim_id": [1], "workflow": ["Appeals"]})

    frame.de.compose_excel(
        template,
        sheet_name="Claims",
        table_name="claims",
        template=template,
    )

    workbook = load_workbook(template)
    assert workbook["Pivot"]["A1"].value == "preserved"
    assert_frame_equal(pl.read_excel(template, sheet_name="Claims"), frame)


@pytest.mark.parametrize(
    ("sheets", "message"),
    [
        ([], "at least one"),
        ([ExcelSheet(name="", df=pl.DataFrame({"a": [1]}))], "sheet names must not be blank"),
        ([ExcelSheet(name="Bad/Name", df=pl.DataFrame({"a": [1]}))], "invalid Excel sheet-name"),
        (
            [
                ExcelSheet(name="Claims", df=pl.DataFrame({"a": [1]})),
                ExcelSheet(name="claims", df=pl.DataFrame({"a": [2]})),
            ],
            "sheet names must be unique",
        ),
        ([ExcelSheet(name="Claims", df=pl.DataFrame({"a": [1]}), table_name="bad table")], "table name"),
        ([ExcelSheet(name="Claims", df=pl.DataFrame({"a": [1]}), table_name="A1")], "cell reference"),
        (
            [
                ExcelSheet(name="Claims", df=pl.DataFrame({"a": [1]}), table_name="claims"),
                ExcelSheet(name="Summary", df=pl.DataFrame({"a": [2]}), table_name="CLAIMS"),
            ],
            "table names must be unique",
        ),
    ],
)
def test_compose_excel_validates_workbook_specs(tmp_path: Path, sheets, message: str):
    with pytest.raises(ValueError, match=message):
        compose_excel(tmp_path / "report.xlsx", sheets=sheets)


def test_compose_excel_rejects_non_polars_frames(tmp_path: Path):
    with pytest.raises(ValueError, match="DataFrame or LazyFrame"):
        compose_excel(
            tmp_path / "report.xlsx",
            sheets=[ExcelSheet(name="Claims", df={"claim_id": [1]})],  # type: ignore[arg-type]
        )

    assert list(tmp_path.glob(".report.xlsx.*.tmp.xlsx")) == []


@pytest.mark.parametrize("template_mode", [False, True])
@pytest.mark.parametrize("name", ["R", "r", "C", "c", "R1C1", "r123c456", "C1R1", "A1", "XFD1048576", " claims "])
def test_invalid_table_names_preserve_existing_output(tmp_path, template_mode, name):
    target = tmp_path / "report.xlsx"
    compose_excel(target, [ExcelSheet("Claims", pl.DataFrame({"id": [7]}), table_name="claims")])
    before = target.read_bytes()
    with pytest.raises(ValueError, match="table name"):
        compose_excel(
            target,
            [ExcelSheet("Claims", pl.DataFrame({"id": [1]}), table_name=name)],
            template=target if template_mode else None,
        )
    assert target.read_bytes() == before
    assert list(tmp_path.glob(".report.xlsx.*.tmp.xlsx")) == []


@pytest.mark.parametrize("failure", ["warning", "return_code", "silent_skip"])
@pytest.mark.parametrize("named_table", [False, True])
def test_failed_table_writes_preserve_existing_output(tmp_path, monkeypatch, failure, named_table):
    target = tmp_path / "report.xlsx"
    compose_excel(target, [ExcelSheet("Claims", pl.DataFrame({"id": [7]}), table_name="claims")])
    before = target.read_bytes()
    real_add_table = Worksheet.add_table

    def failing_add_table(self, *args, **kwargs):
        if failure == "warning":
            real_add_table(self, *args, **kwargs)
            warnings.warn("table write warning", UserWarning, stacklevel=2)
            return 0
        return -2 if failure == "return_code" else 0

    monkeypatch.setattr(Worksheet, "add_table", failing_add_table)
    with pytest.raises((ValueError, UserWarning), match="table"):
        compose_excel(
            target,
            [ExcelSheet("Claims", pl.DataFrame({"id": [1]}), table_name="new_claims" if named_table else None)],
        )
    assert target.read_bytes() == before
    assert list(tmp_path.glob(".report.xlsx.*.tmp.xlsx")) == []


@pytest.mark.parametrize("template_mode", [False, True])
def test_duplicate_case_insensitive_headers_fail_without_publication(tmp_path, template_mode):
    target = tmp_path / "report.xlsx"
    compose_excel(target, [ExcelSheet("Claims", pl.DataFrame({"id": [7]}), table_name="claims")])
    before = target.read_bytes()
    with pytest.raises((ValueError, UserWarning), match="header"):
        compose_excel(
            target,
            [ExcelSheet("Claims", pl.DataFrame({"id": [1], "ID": [2]}), table_name="new_claims")],
            template=target if template_mode else None,
        )
    assert target.read_bytes() == before
    assert list(tmp_path.glob(".report.xlsx.*.tmp.xlsx")) == []


@pytest.mark.parametrize("failure", ["warning", "silent_skip"])
def test_failed_template_table_writes_preserve_output(tmp_path, monkeypatch, failure):
    from openpyxl.worksheet.worksheet import Worksheet as TemplateWorksheet

    target = tmp_path / "report.xlsx"
    compose_excel(target, [ExcelSheet("Claims", pl.DataFrame({"id": [7]}), table_name="claims")])
    before = target.read_bytes()
    real_add_table = TemplateWorksheet.add_table

    def failing_add_table(self, table):
        if table.displayName != "new_claims":
            return real_add_table(self, table)
        if failure == "warning":
            real_add_table(self, table)
            warnings.warn("table write warning", UserWarning, stacklevel=2)

    monkeypatch.setattr(TemplateWorksheet, "add_table", failing_add_table)
    with pytest.raises((ValueError, UserWarning), match="table"):
        compose_excel(
            target,
            [ExcelSheet("Claims", pl.DataFrame({"id": [1]}), table_name="new_claims")],
            template=target,
        )
    assert target.read_bytes() == before
    assert list(tmp_path.glob(".report.xlsx.*.tmp.xlsx")) == []


def test_missing_serialized_table_preserves_output(tmp_path, monkeypatch):
    target = tmp_path / "report.xlsx"
    compose_excel(target, [ExcelSheet("Claims", pl.DataFrame({"id": [7]}), table_name="claims")])
    before = target.read_bytes()
    real_close = xlsxwriter.Workbook.close

    def close_without_tables(self):
        for worksheet in self.worksheets():
            worksheet.tables.clear()
        return real_close(self)

    monkeypatch.setattr(xlsxwriter.Workbook, "close", close_without_tables)
    with pytest.raises(ValueError, match="table output is incomplete"):
        compose_excel(target, [ExcelSheet("Claims", pl.DataFrame({"id": [1]}), table_name="new_claims")])
    assert target.read_bytes() == before
    assert list(tmp_path.glob(".report.xlsx.*.tmp.xlsx")) == []


def test_fresh_workbook_preserves_explicit_formula_options(tmp_path):
    target = tmp_path / "formulas.xlsx"
    compose_excel(
        target,
        [
            ExcelSheet(
                "Claims",
                pl.DataFrame({"amount": [2], "text": ["=1+2"]}),
                table_name="claims",
                write_options={"formulas": {"double": "=[@amount]*2"}},
            )
        ],
    )
    workbook = load_workbook(target, data_only=False)
    try:
        assert workbook["Claims"]["B2"].data_type == "s"
        assert workbook["Claims"]["C2"].data_type == "f"
    finally:
        workbook.close()


@pytest.mark.parametrize("template_mode", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_fresh_and_template_empty_or_nullable_data_round_trip(tmp_path, template_mode, empty):
    template = tmp_path / "template.xlsx"
    workbook = Workbook()
    workbook.active.title = "Claims"
    workbook.save(template)
    workbook.close()
    frame = pl.DataFrame({"text": [None, "=1+2"], "amount": [3, None]})
    if empty:
        frame = frame.head(0)
    target = tmp_path / "report.xlsx"
    compose_excel(target, [ExcelSheet("Claims", frame, table_name="claims")], template=template if template_mode else None)
    result = load_workbook(target)
    try:
        worksheet = result["Claims"]
        assert worksheet.tables["claims"].ref == ("A1:B2" if empty else "A1:B3")
        assert [worksheet.cell(1, column).value for column in (1, 2)] == frame.columns
        for row_offset, row in enumerate(frame.rows(), start=2):
            assert tuple(worksheet.cell(row_offset, column).value for column in (1, 2)) == row
        if not empty:
            assert worksheet["A3"].data_type == "s"
    finally:
        result.close()


@pytest.mark.parametrize("template_mode", [False, True])
def test_invalid_table_name_does_not_create_output(tmp_path, template_mode):
    template = tmp_path / "template.xlsx"
    workbook = Workbook()
    workbook.save(template)
    workbook.close()
    target = tmp_path / "report.xlsx"
    with pytest.raises(ValueError, match="reserved"):
        compose_excel(
            target,
            [ExcelSheet("Claims", pl.DataFrame({"id": [1]}), table_name="r")],
            template=template if template_mode else None,
        )
    assert not target.exists()
    assert list(tmp_path.glob(".report.xlsx.*.tmp.xlsx")) == []


@pytest.mark.parametrize("lazy", [False, True])
@pytest.mark.parametrize("table_name", [None, "claims"])
@pytest.mark.parametrize("position", ["A1", (2, 1)])
def test_fresh_and_template_strings_are_literal_text(tmp_path, lazy, table_name, position):
    template = tmp_path / "template.xlsx"
    workbook = Workbook()
    workbook.active.title = "Claims"
    workbook["Claims"]["A1"] = "=99+1"
    workbook["Claims"]["H20"] = "=SUM(A1:A2)"
    workbook.create_sheet("Summary")["A1"] = "=Claims!H20"
    workbook.save(template)
    workbook.close()
    before = template.read_bytes()
    values = ["=1+2", "=SUM(A1:A2)", "{=1+2}", "#DIV/0!", "+1+2", "-1+2", "@SUM(A1:A2)", "001", "plain"]
    frame = pl.DataFrame({"=header": values, "#N/A": values[::-1]})
    spec = ExcelSheet("Claims", frame.lazy() if lazy else frame, table_name=table_name, position=position)
    outputs = [tmp_path / "fresh.xlsx", tmp_path / "from_template.xlsx"]
    compose_excel(outputs[0], [spec])
    compose_excel(outputs[1], [spec], template=template)
    start_row, start_column = (1, 1) if position == "A1" else (3, 2)
    for path in outputs:
        result = load_workbook(path, data_only=False)
        try:
            worksheet = result["Claims"]
            for row_offset, row in enumerate([tuple(frame.columns), *frame.rows()]):
                for column_offset, value in enumerate(row):
                    cell = worksheet.cell(start_row + row_offset, start_column + column_offset)
                    assert (cell.value, cell.data_type) == (value, "s")
            if path == outputs[1]:
                assert (worksheet["H20"].value, worksheet["H20"].data_type) == ("=SUM(A1:A2)", "f")
                assert (result["Summary"]["A1"].value, result["Summary"]["A1"].data_type) == ("=Claims!H20", "f")
                if position != "A1":
                    assert worksheet["A1"].data_type == "f"
        finally:
            result.close()
    assert template.read_bytes() == before
