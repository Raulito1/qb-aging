#!/usr/bin/env python3
from pathlib import Path
from datetime import date
import os, sys, glob, pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv
import re
from gspread.utils import rowcol_to_a1

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

overdue = df.query("Balance > 0 and `Days Overdue` > 0", engine="python").copy()
bins   = [0, 30, 60, 90, 120, float("inf")]
labels = ["1-30", "31-60", "61-90", "91-120", "120+"]
overdue["Bucket"] = pd.cut(overdue["Days Overdue"], bins, labels=labels)

# ---- 4. push to Google Sheets (skip column A, headers on row 3, blank row 4) ----
START_COL = 2        # column B
HEADER_ROW = 3       # headers in row 3

gc = gspread.service_account(filename=BASE / SERVICE_JSON)
sh = gc.open_by_key(SHEET_ID)

# Ensure worksheet exists with enough space
try:
    ws = sh.worksheet(TARGET_TAB)
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(
        title=TARGET_TAB,
        rows=2000,
        cols=len(overdue.columns) + START_COL + 5
    )

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