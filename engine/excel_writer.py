from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from datetime import datetime

HIDE_COLUMNS = {
    "ref_norm", "match_key", "amount_pence", "amount_gbp", "currency", "fuzzy_status"
}

STATUS_FILL = {
    "Matched": PatternFill("solid", "C6EFCE"),
    "Partially Matched": PatternFill("solid", "FFF2CC"),
    "Unmatched": PatternFill("solid", "F4CCCC"),
}

THIN = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)


def clean(df):
    df = df.copy()
    df.rename(columns={
        "reference": "Reference",
        "amount": "Amount (£)",
        "source": "Source"
    }, inplace=True)

    return df[[c for c in df.columns if c not in HIDE_COLUMNS]]


def write_styled_workbook(summary, matched, partial, unmatched, client, path):
    wb = Workbook()
    ws = wb.active
    ws.title = "Reconciliation"

    ws["B1"] = client
    ws["B1"].font = Font(size=20, bold=True)
    ws["B3"] = f"Generated: {datetime.today().date()}"

    row = 5

    def write_table(df, title):
        nonlocal row
        ws[f"B{row}"] = title
        ws[f"B{row}"].font = Font(bold=True)
        row += 1

        df = clean(df)

        for i, col in enumerate(df.columns, start=2):
            cell = ws.cell(row=row, column=i, value=col)
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", "000000")
            cell.font = Font(color="FFFFFF")
            cell.border = THIN

        row += 1

        for _, r in df.iterrows():
            for i, v in enumerate(r, start=2):
                cell = ws.cell(row=row, column=i, value=v)
                cell.border = THIN
                if col := df.columns[i-2] == "Status":
                    cell.fill = STATUS_FILL.get(v)
            row += 1

        row += 2

    write_table(matched, "Matched")
    write_table(partial, "Partially Matched")
    write_table(unmatched, "Unmatched")

    wb.save(path)
