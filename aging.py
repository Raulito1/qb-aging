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

df = pd.read_csv(csv_path, dtype=str)
# --- Clean header whitespace & unify case --------------------------------
df.columns = df.columns.str.strip()
# Keep an original copy for debugging but work with lower‑case keys
df.columns = df.columns.str.lower()

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

# Clear previous data block only (keep column A and rows 1‑2 intact)
last_col_index = START_COL + len(overdue.columns) - 1
last_col_letter = re.sub(r"\d", "", rowcol_to_a1(1, last_col_index))
clear_range = f"{rowcol_to_a1(HEADER_ROW, START_COL)}:{last_col_letter}"
ws.batch_clear([clear_range])

# Write headers at HEADER_ROW, data follows automatically (row 4 stays blank)
set_with_dataframe(
    ws,
    overdue,
    include_index=False,
    include_column_header=True,
    resize=False,
    row=HEADER_ROW,
    col=START_COL
)

print(
    f"✅ Uploaded {len(overdue)} overdue invoices to "
    f"'{TARGET_TAB}' starting at {rowcol_to_a1(HEADER_ROW, START_COL)}"
)