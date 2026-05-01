# Excel Helpers

`data_engine.helpers.compose_excel` writes one Excel workbook from one or more
Polars-backed sheet specifications. Use it when a flow needs a workbook output
with stable sheet names, optional Excel table names, and atomic replacement of
the target file.

The public helpers are:

```python
from data_engine.helpers import ExcelSheet
from data_engine.helpers import compose_excel
```

## `compose_excel(...)`

Signature:

```python
compose_excel(
    path,
    sheets,
    *,
    template=None,
)
```

Behavior:

- requires at least one `ExcelSheet`
- accepts `pl.DataFrame` and `pl.LazyFrame` sheet data
- collects lazy frames while composing the workbook
- creates the target parent directory
- writes to a same-directory temporary `.tmp.xlsx` file
- replaces the target with `os.replace`
- retries Windows access-denied replacement failures briefly
- removes the temporary file after failures when possible
- returns the resolved target `Path`

Example:

```python
import polars as pl

from data_engine.helpers import ExcelSheet, compose_excel

compose_excel(
    context.mirror.root_file("reports/claims.xlsx"),
    sheets=[
        ExcelSheet(
            name="Claims",
            df=pl.DataFrame({"claim_id": [1001, 1002], "amount": [10.25, 42.00]}),
            table_name="claims",
            freeze_panes="A2",
        ),
        ExcelSheet(
            name="Summary",
            df=pl.DataFrame({"metric": ["rows"], "value": [2]}),
            table_name="summary",
        ),
    ],
)
```

## `ExcelSheet(...)`

`ExcelSheet` is a dataclass that describes one dataframe-backed worksheet.

```python
ExcelSheet(
    name,
    df,
    table_name=None,
    position="A1",
    table_style=None,
    autofit=True,
    autofilter=True,
    freeze_panes=None,
    write_options={},
)
```

Fields:

- `name` is the worksheet name.
- `df` is a Polars `DataFrame` or `LazyFrame`.
- `table_name` is the optional Excel table name.
- `position` is the top-left cell for the dataframe table.
- `table_style` is forwarded as the table style.
- `autofit` controls Polars column autofit in fresh-workbook mode.
- `autofilter` controls Polars table filter controls in fresh-workbook mode.
- `freeze_panes` is forwarded to the writer.
- `write_options` holds additional `pl.DataFrame.write_excel` keyword options.

Validation:

- sheet names must not be blank
- sheet names must be 31 characters or fewer
- sheet names must not contain `[]:*?/\`
- sheet names must be unique, case-insensitively
- table names must be unique, case-insensitively
- table names must start with a letter or underscore
- table names may contain only letters, numbers, and underscores
- table names must not look like an Excel cell reference
- table names must be 255 characters or fewer

Excel table names live in a workbook-wide namespace, so `table_name="claims"`
cannot be reused on a different sheet in the same workbook.

## Template Mode

Pass `template=...` when an existing workbook should provide the shell:

```python
compose_excel(
    context.mirror.root_file("reports/claims.xlsx"),
    template=template_path,
    sheets=[
        ExcelSheet(
            name="Claims",
            df=claims_df,
            table_name="claims",
            position="A4",
            table_style="TableStyleMedium2",
            freeze_panes="A5",
        ),
    ],
)
```

Template mode requires `openpyxl`. The helper copies the template to a
temporary workbook, updates each requested sheet, saves the copy, and then
atomically replaces the target path.

Template caveats:

- an existing requested sheet is reused; a missing requested sheet is created
- existing table definitions on the requested sheet are removed
- existing cell values on the requested sheet are cleared
- workbook content outside requested sheets is preserved
- dataframe values are written through `openpyxl`, not Polars
- `write_options`, `autofit`, and `autofilter` are not applied in template mode
- `table_style` is used when adding an `openpyxl` table; dict styles are not expanded
- `position` may be an Excel cell reference or a zero-based `(row, column)` tuple
- `freeze_panes` is assigned to the worksheet when provided

Use template mode for formatted shells, static cover sheets, formulas, or charts
that should remain in the workbook. Use fresh-workbook mode when you want Polars
to own the Excel writing behavior for every sheet.

## Namespace Convenience

Data Engine also exposes single-sheet convenience wrappers on the Polars
namespace helpers:

```python
claims_df.de.compose_excel(
    context.mirror.root_file("reports/claims.xlsx"),
    sheet_name="Claims",
    table_name="claims",
)

claims_lazy.de.compose_excel(
    context.mirror.root_file("reports/claims.xlsx"),
    sheet_name="Claims",
    table_name="claims",
)
```

Those namespace methods create one `ExcelSheet` and call the same top-level
`compose_excel(...)` helper. Use the top-level helper directly when composing a
multi-sheet workbook.
