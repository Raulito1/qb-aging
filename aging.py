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
# Optional formatting helpers (checkboxes, dropdowns).
# ------------------------------------------------------------------------
try:
    from gspread_formatting import (
        set_data_validation_for_cell_range,
        DataValidationRule,
        set_number_format,
        NumberFormat,
    )
    FORMATTING_AVAILABLE = True
except ImportError:
    FORMATTING_AVAILABLE = False
    print("⚠️  gspread-formatting not installed. Install with: pip install gspread-formatting")

BASE = Path(__file__).parent
load_dotenv(BASE / ".env")

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SERVICE_JSON = "service_account.json"
TARGET_TAB = "Overdue aging"
CSV_PATTERN = str(BASE / "incoming_csv" / "*.csv")

try:
    csv_path = max(glob.glob(CSV_PATTERN), key=os.path.getmtime)
except ValueError:
    sys.exit("❌  No CSV found in incoming_csv/.  Aborting.")

# Process CSV
df = pd.read_csv(csv_path, dtype=str, skiprows=1)
df.columns = df.columns.str.strip().str.lower()
df.columns = df.columns.str.replace("\u00A0", " ", regex=False)
df.columns = df.columns.str.replace(r"\s+", " ", regex=True)

# Column mapping
ALT_NAMES = {
    "open balance": "balance",
    "amount": "balance",
    "balance": "balance",
    "due date": "due date",
    "duedate": "due date",
    "invoice due date": "due date",
    "invoice date": "due date",
}
df.rename(columns=ALT_NAMES, inplace=True)

if df.columns.duplicated().any():
    df = df.loc[:, ~df.columns.duplicated(keep="last")]

if "date" in df.columns:
    df = df[~df["date"].str.contains("OUT OF RANGE", na=False)]

df.rename(columns={"due date": "Due Date", "balance": "Balance"}, inplace=True)

for col in ("Due Date", "Balance"):
    if col not in df.columns:
        sys.exit(f"❌  CSV missing '{col}' column.")

df["Due Date"] = pd.to_datetime(df["Due Date"], errors="coerce")
df["Balance"] = pd.to_numeric(df["Balance"], errors="coerce")
df["Days Overdue"] = (pd.Timestamp(date.today()) - df["Due Date"]).dt.days

overdue = df.query("Balance > 0 and `Days Overdue` >= 21", engine="python").copy()

bins = [20, 30, 45, 60, 90, float("inf")]
labels = ["21-30", "31-45", "46-60", "61-90", "91+"]
overdue["Bucket"] = pd.cut(overdue["Days Overdue"], bins, labels=labels)

bucket_to_collection = {
    "21-30": "Accounting Outreach",
    "31-45": "CSM/AE Outreach",
    "46-60": "Manager Escalation",
    "61-90": "Add to No Work List",
    "91+": "Demand Letter",
}
overdue["Collection Item"] = overdue["Bucket"].astype(str).map(bucket_to_collection)

# Constants
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

START_COL = 2   # column B
HEADER_ROW = 3  # header appears in row 3
MAX_ROWS = 2000

# Conform DataFrame
rename_map = {
    "Balance": "Amount",
    "Due Date": "Date",
    "Days Overdue": "Days Outstanding",
    "customer": "Customer",
    "customer name": "Customer",
    "customer full name": "Customer",
}
overdue.rename(columns=rename_map, inplace=True)

for col in HEADERS:
    if col not in overdue.columns:
        overdue[col] = None

overdue = overdue[HEADERS]

# Connect to Google Sheets
gc = gspread.service_account(filename=BASE / SERVICE_JSON)
sh = gc.open_by_key(SHEET_ID)

# Get or create worksheet
try:
    ws = sh.worksheet(TARGET_TAB)
    worksheet_created = False
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(
        title=TARGET_TAB,
        rows=MAX_ROWS,
        cols=len(HEADERS) + START_COL + 5
    )
    worksheet_created = True

# Setup formatting function
def setup_formatting(worksheet):
    """Setup dropdowns and checkboxes for the worksheet"""
    if not FORMATTING_AVAILABLE:
        print("⚠️  Skipping formatting setup - gspread-formatting not available")
        return
    
    print("🔧 Setting up dropdowns and checkboxes...")
    
    # Checkboxes for columns: Slack Updated (7), No Work List (8), Demand Letter (10)
    checkbox_rule = DataValidationRule(
        condition_type="BOOLEAN",
        showCustomUi=True
    )
    
    for offset in [7, 8, 10]:
        col = START_COL + offset
        rng = f"{rowcol_to_a1(HEADER_ROW + 1, col)}:{rowcol_to_a1(MAX_ROWS, col)}"
        set_data_validation_for_cell_range(worksheet, rng, checkbox_rule)
    
    # Dropdown for Action Taken (column 6)
    actions = [
        "Add to No Work List",
        "Payment Plan Proposed",
        "Payment Plan Established",
        "Manager Escalation",
        "CSM/AE Notified",
        "Accounting Email Sent"
    ]
    dropdown_rule = DataValidationRule(
        condition_type="ONE_OF_LIST",
        condition_values=actions,
        showCustomUi=True
    )
    action_col = START_COL + 6
    rng = f"{rowcol_to_a1(HEADER_ROW + 1, action_col)}:{rowcol_to_a1(MAX_ROWS, action_col)}"
    set_data_validation_for_cell_range(worksheet, rng, dropdown_rule)
    
    # Number formatting for Amount column
    amt_col = START_COL + 1
    amt_rng = f"{rowcol_to_a1(HEADER_ROW + 1, amt_col)}:{rowcol_to_a1(MAX_ROWS, amt_col)}"
    set_number_format(
        worksheet,
        amt_rng,
        NumberFormat(type="NUMBER", pattern="$#,##0.00")
    )
    
    # Date formatting for Date column
    date_col = START_COL + 2
    date_rng = f"{rowcol_to_a1(HEADER_ROW + 1, date_col)}:{rowcol_to_a1(MAX_ROWS, date_col)}"
    set_number_format(
        worksheet,
        date_rng,
        NumberFormat(type="DATE", pattern="yyyy-mm-dd")
    )
    
    print("✅ Formatting setup complete")

# Check if headers exist
try:
    header_present = bool(ws.cell(HEADER_ROW, START_COL).value)
except gspread.exceptions.APIError:
    header_present = False

# Write headers if needed
if not header_present:
    ws.update(rowcol_to_a1(HEADER_ROW, START_COL), [HEADERS])
    write_row = HEADER_ROW + 1
    include_header = False
    # Setup formatting after headers are written
    setup_formatting(ws)
else:
    # Find next empty row
    existing_values = ws.col_values(START_COL)
    write_row = len(existing_values) + 1
    include_header = False

# If worksheet was just created or this is first run, setup formatting
if worksheet_created or not header_present:
    setup_formatting(ws)

# Write data
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