import os
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter


INTERNAL_COLUMNS = {
    "ref_norm",
    "match_key",
    "amount_pence",
    "currency",
    "amount_gbp",
}

COLUMN_ORDER = [
    "Date",
    "amount",
    "reference",
    "source",
    "Status",
    "Match Type",
    "Resolution Status",
    "Variance (£)",
    "Match Reason",
]


STATUS_FILLS = {
    "Matched": PatternFill("solid", start_color="C6EFCE"),
    "Partially Matched": PatternFill("solid", start_color="FFF2CC"),
    "Unmatched": PatternFill("solid", start_color="F4CCCC"),
}

THIN = Border(*(Side(style="thin") for _ in range(4)))


def write_table(ws, df, start_row):
    headers = [c for c in COLUMN_ORDER if c in df.columns]
    df = df[headers]

    for i, h in enumerate(headers, start=2):
        cell = ws.cell(start_row, i, h)
        cell.font = Font(bold=True)
        cell.border = THIN

    for r, row in enumerate(df.itertuples(index=False), start=start_row + 1):
        for c, val in enumerate(row, start=2):
            cell = ws.cell(r, c, val)
            cell.border = THIN

            if headers[c - 2] == "Status":
                cell.fill = STATUS_FILLS.get(val, None)
                cell.alignment = Alignment(horizontal="center")

    return start_row + len(df) + 2


def write_styled_workbook(master, matched, partial, unmatched, client, out_path):
    wb = Workbook()
    ws = wb.active
    ws.title = "Dashboard"
    ws.sheet_view.showGridLines = False

    ws["B1"] = client
    ws["B1"].font = Font(size=20, bold=True)

    ws["I1"] = "Automated Reconciliation Core"
    ws["I1"].alignment = Alignment(horizontal="right")

    row = 3
    row = write_table(ws, matched, row)
    row = write_table(ws, partial, row)
    write_table(ws, unmatched, row)

    ws["B18"] = "Prepared automatically by ARC Solutions"
    ws["B19"] = f"Generated on: {datetime.today().date()}"

    for col in range(2, ws.max_column + 1):
        ws.column_dimensions[get_column_letter(col)].width = 22

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    wb.save(out_path)
