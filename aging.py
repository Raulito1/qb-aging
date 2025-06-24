#!/usr/bin/env python3
from pathlib import Path
from datetime import date
import os, sys, glob, pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv
import re
from gspread.utils import rowcol_to_a1

# ------------------------------------------------------------------------
# Optional formatting helpers (checkboxes, dropdowns).  If the extra
# dependency is missing we just skip UI‑enhancement but the upload still
# works.
# ------------------------------------------------------------------------
try:
    from gspread_formatting import (
        set_data_validation_for_cell_range,
        DataValidationRule,
        set_number_format,
        NumberFormat,
    )
except ImportError:
    set_data_validation_for_cell_range = None
    DataValidationRule = None
    set_number_format = None
    NumberFormat = None

BASE = Path(__file__).parent
load_dotenv(BASE / ".env")               # user copies .env from template

SHEET_ID     = os.getenv("GOOGLE_SHEET_ID")
SERVICE_JSON = "service_account.json"    # stays in project root
TARGET_TAB   = "Overdue aging"
CSV_PATTERN  = str(BASE / "incoming_csv" / "*.csv")

try:
    csv_path = max(glob.glob(CSV_PATTERN), key=os.path.getmtime)
except ValueError:
    sys.exit("❌  No CSV found in incoming_csv/.  Aborting.")

# QuickBooks exports include a title row; real headers start on row 2
df = pd.read_csv(csv_path, dtype=str, skiprows=1)
# --- Clean header whitespace & unify case --------------------------------
df.columns = df.columns.str.strip()
# Keep an original copy for debugging but work with lower‑case keys
df.columns = df.columns.str.lower()
# Normalize weird whitespace (non‑breaking spaces, double spaces, tabs)
df.columns = df.columns.str.replace("\u00A0", " ", regex=False)
df.columns = df.columns.str.replace(r"\s+", " ", regex=True)

# Case-insensitive mapping for alternate column names
ALT_NAMES = {
    # balance synonyms
    "open balance": "balance",
    "amount": "balance",
    "balance": "balance",
    # due date synonyms
    "due date": "due date",
    "duedate": "due date",
    "invoice due date": "due date",
    "invoice date": "due date",
}
df.rename(columns=ALT_NAMES, inplace=True)
# If multiple synonyms mapped to the same name (e.g., "balance"), keep the last occurrence
if df.columns.duplicated().any():
    df = df.loc[:, ~df.columns.duplicated(keep="last")]

# Drop subtotal / rubric rows such as "OUT OF RANGE"
if "date" in df.columns:
    df = df[~df["date"].str.contains("OUT OF RANGE", na=False)]

# Finally, standardise to capitalised names used later
df.rename(columns={"due date": "Due Date", "balance": "Balance"}, inplace=True)
for col in ("Due Date", "Balance"):
    if col not in df.columns:
        sys.exit(f"❌  CSV missing '{col}' column.")

df["Due Date"]     = pd.to_datetime(df["Due Date"], errors="coerce")
df["Balance"]      = pd.to_numeric(df["Balance"], errors="coerce")
df["Days Overdue"] = (pd.Timestamp(date.today()) - df["Due Date"]).dt.days

# Only actionable items: unpaid invoices that are at least 21 days late
overdue = df.query("Balance > 0 and `Days Overdue` >= 21", engine="python").copy()
# Collections workflow starts after 20+ days overdue:
#   21‑30, 31‑45, 46‑60, 61‑90, 91+
# Invoices ≤20 days late are left un‑bucketed (NaN) so analysts can
# focus only on actionable rows.
bins   = [20, 30, 45, 60, 90, float("inf")]
labels = ["21-30", "31-45", "46-60", "61-90", "91+"]
overdue["Bucket"] = pd.cut(overdue["Days Overdue"], bins, labels=labels)

# Derive “Collection Item” from the bucket label
bucket_to_collection = {
    "21-30": "Accounting Outreach",
    "31-45": "CSM/AE Outreach",
    "46-60": "Manager Escalation",
    "61-90": "Add to No Work List",
    "91+":   "Demand Letter",
}
# Convert category to string to ensure mapping works; missing buckets stay NaN
overdue["Collection Item"] = overdue["Bucket"].astype(str).map(bucket_to_collection)

# ----------------------- constants ---------------------------------
# Target column layout for the “Overdue aging” sheet
HEADERS = [
    "Customer",
    "Amount",
    "Date",
    "Days Outstanding",
    "Bucket",
    "Collection Item",
    "Action Taken",
    "Slack Updated",
    "No Work List",
    "Removed from No Work List Approver",
    "Demand Letter",
]

START_COL  = 2   # column B
HEADER_ROW = 3   # header appears in row 3
# -------------------------------------------------------------------
# ---- Conform DataFrame to predefined HEADERS --------------------------
rename_map = {
    "Balance": "Amount",
    "Due Date": "Date",
    "Days Overdue": "Days Outstanding",
    "customer": "Customer",
    "customer name": "Customer",
    "customer full name": "Customer",
}
overdue.rename(columns=rename_map, inplace=True)

# Ensure all expected columns exist (blank if not populated yet)
for col in HEADERS:
    if col not in overdue.columns:
        overdue[col] = None

# Re‑order columns to match the Google Sheet
overdue = overdue[HEADERS]

# ---- 4. push to Google Sheets (skip column A, headers on row 3, blank row 4) ----

gc = gspread.service_account(filename=BASE / SERVICE_JSON)
sh = gc.open_by_key(SHEET_ID)

# Ensure worksheet exists with enough space
try:
    ws = sh.worksheet(TARGET_TAB)
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(
        title=TARGET_TAB,
        rows=2000,
        cols=len(HEADERS) + START_COL + 5
    )

    # --- Prime the worksheet with header row and UI helpers -------------
    header_cell = rowcol_to_a1(HEADER_ROW, START_COL)
    ws.update(header_cell, [HEADERS])

    # Add check‑boxes and dropdown if gspread‑formatting is available
    if DataValidationRule and set_data_validation_for_cell_range:
        max_rows = 2000
        checkbox_rule = DataValidationRule(
            condition_type="BOOLEAN",
            showCustomUi=True
        )
        # Column offsets (0‑based) from START_COL
        checkbox_offsets = [7, 8, 10]        # Slack Updated, No Work List, Demand Letter
        for offset in checkbox_offsets:
            col = START_COL + offset
            rng = f"{rowcol_to_a1(HEADER_ROW + 1, col)}:{rowcol_to_a1(max_rows, col)}"
            set_data_validation_for_cell_range(ws, rng, checkbox_rule)

        # Dropdown for Action Taken
        actions = ["Add to No Work List", "Payment Plan Proposed", "Payment Plan Established", "Manager Escalation", "CSM/AE Notified", "Accounting Email Sent"]
        dropdown_rule = DataValidationRule(
            condition_type="ONE_OF_LIST",
            condition_values=actions,
            showCustomUi=True
        )
        action_col = START_COL + 6  # “Action Taken” (after inserting Bucket)
        rng = f"{rowcol_to_a1(HEADER_ROW + 1, action_col)}:{rowcol_to_a1(max_rows, action_col)}"
        set_data_validation_for_cell_range(ws, rng, dropdown_rule)
    else:
        print("ℹ️  gspread‑formatting not installed; skipping checkbox/dropdown setup.")

    # ── Ensure header row is present and refresh UI helpers / formats ─────────
    if not ws.cell(HEADER_ROW, START_COL).value:
        ws.update(rowcol_to_a1(HEADER_ROW, START_COL), [HEADERS])

    if (
        DataValidationRule
        and set_data_validation_for_cell_range
        and set_number_format
        and NumberFormat
    ):
        max_rows = 2000

        # --- 1. Check‑boxes ---------------------------------------------------
        checkbox_rule = DataValidationRule(condition_type="BOOLEAN", showCustomUi=True)
        for offset in (7, 8, 10):  # Slack Updated, No Work List, Demand Letter
            col = START_COL + offset
            rng = (
                f"{rowcol_to_a1(HEADER_ROW + 1, col)}:"
                f"{rowcol_to_a1(max_rows, col)}"
            )
            set_data_validation_for_cell_range(ws, rng, checkbox_rule)

        # --- 2. Action Taken drop‑down ---------------------------------------
        actions = [
            "Add to No Work List",
            "Payment Plan Proposed",
            "Payment Plan Established",
            "Manager Escalation",
            "CSM/AE Notified",
            "Accounting Email Sent",
        ]
        dropdown_rule = DataValidationRule(
            condition_type="ONE_OF_LIST",
            condition_values=actions,
            showCustomUi=True,
        )
        action_col = START_COL + 6  # “Action Taken”
        rng = (
            f"{rowcol_to_a1(HEADER_ROW + 1, action_col)}:"
            f"{rowcol_to_a1(max_rows, action_col)}"
        )
        set_data_validation_for_cell_range(ws, rng, dropdown_rule)

        # --- 3. Column number/date formats -----------------------------------
        # Amount column  (index 1 relative to START_COL)
        amt_col = START_COL + 1
        amt_rng = (
            f"{rowcol_to_a1(HEADER_ROW + 1, amt_col)}:"
            f"{rowcol_to_a1(max_rows, amt_col)}"
        )
        set_number_format(
            ws,
            amt_rng,
            NumberFormat(type="NUMBER", pattern="$#,##0.00"),
        )

        # Date column  (index 2 relative to START_COL)
        date_col = START_COL + 2
        date_rng = (
            f"{rowcol_to_a1(HEADER_ROW + 1, date_col)}:"
            f"{rowcol_to_a1(max_rows, date_col)}"
        )
        set_number_format(
            ws,
            date_rng,
            NumberFormat(type="DATE", pattern="yyyy-mm-dd"),
        )
    else:
        print("ℹ️  gspread‑formatting partially missing; UI helpers not refreshed.")

# -------------------------------------------------------------------
# Determine where the next upload should begin.
#   • On the very first run (no headers yet) we write headers at
#     HEADER_ROW and data starts on the row below.
#   • On subsequent runs we keep the existing data and simply append
#     new rows underneath it without rewriting headers.
# -------------------------------------------------------------------
try:
    header_present = bool(ws.cell(HEADER_ROW, START_COL).value)
except gspread.exceptions.APIError:
    header_present = False  # worksheet is empty

if header_present:
    # Count the non‑empty cells in the destination column to find the
    # first free row (col_values includes the header row).
    existing_values = ws.col_values(START_COL)
    write_row = len(existing_values) + 1
    include_header = False
else:
    # First upload: create header row and leave data to start beneath it.
    write_row = HEADER_ROW
    include_header = True

set_with_dataframe(
    ws,
    overdue,
    include_index=False,
    include_column_header=include_header,
    resize=False,
    row=write_row,
    col=START_COL
)

print(
    f"✅ Appended {len(overdue)} overdue invoices to "
    f"'{TARGET_TAB}' starting at {rowcol_to_a1(write_row, START_COL)}"
)