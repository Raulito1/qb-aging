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
load_dotenv(BASE / ".env")

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SERVICE_JSON = "service_account.json"
TARGET_TAB = "Overdue aging"
CSV_PATTERN = str(BASE / "incoming_csv" / "*.csv")

# Add option to process all invoices
PROCESS_ALL = os.getenv("PROCESS_ALL_INVOICES", "false").lower() == "true"
MINIMUM_DAYS_OVERDUE = 21 if not PROCESS_ALL else 0

try:
    csv_path = max(glob.glob(CSV_PATTERN), key=os.path.getmtime)
except ValueError:
    sys.exit("❌  No CSV found in incoming_csv/.  Aborting.")

# Process CSV
df = pd.read_csv(csv_path, dtype=str, skiprows=1)
print(f"📊 Total rows in CSV: {len(df)}")

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

df["Due Date"] = pd.to_datetime(df["Due Date"], format='%m/%d/%Y', errors="coerce")
df["Balance"] = pd.to_numeric(df["Balance"].str.replace(',', ''), errors="coerce")
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


def setup_formatting_with_api(spreadsheet, worksheet):
    """Setup dropdowns and checkboxes using Sheets API directly"""
    
    sheet_id = worksheet.id
    
    # Prepare batch update requests
    requests = []
    
    # 1. Add checkboxes for columns: Slack Updated (7), No Work List (8), Demand Letter (10)
    checkbox_columns = [7, 8, 10]  # 0-based offsets from START_COL
    
    for offset in checkbox_columns:
        col_index = START_COL + offset - 1  # Convert to 0-based index
        requests.append({
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": HEADER_ROW,  # Row 4 (0-based)
                    "endRowIndex": MAX_ROWS,
                    "startColumnIndex": col_index,
                    "endColumnIndex": col_index + 1
                },
                "rule": {
                    "condition": {
                        "type": "BOOLEAN"
                    },
                    "showCustomUi": True
                }
            }
        })
    
    # 2. Add dropdown for Action Taken (column 6)
    action_col_index = START_COL + 6 - 1  # Convert to 0-based
    actions = [
        "Add to No Work List",
        "Payment Plan Proposed",
        "Payment Plan Established",
        "Manager Escalation",
        "CSM/AE Notified",
        "Accounting Email Sent"
    ]
    
    requests.append({
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": HEADER_ROW,
                "endRowIndex": MAX_ROWS,
                "startColumnIndex": action_col_index,
                "endColumnIndex": action_col_index + 1
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": action} for action in actions]
                },
                "showCustomUi": True
            }
        }
    })
    
    # 3. Format Amount column as currency
    amt_col_index = START_COL + 1 - 1  # Convert to 0-based
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": HEADER_ROW,
                "endRowIndex": MAX_ROWS,
                "startColumnIndex": amt_col_index,
                "endColumnIndex": amt_col_index + 1
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "$#,##0.00"
                    }
                }
            },
            "fields": "userEnteredFormat.numberFormat"
        }
    })
    
    # 4. Format Date column
    date_col_index = START_COL + 2 - 1  # Convert to 0-based
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": HEADER_ROW,
                "endRowIndex": MAX_ROWS,
                "startColumnIndex": date_col_index,
                "endColumnIndex": date_col_index + 1
            },
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                        "type": "DATE",
                        "pattern": "yyyy-mm-dd"
                    }
                }
            },
            "fields": "userEnteredFormat.numberFormat"
        }
    })
    
    # Execute batch update
    if requests:
        try:
            spreadsheet.batch_update({"requests": requests})
            print("✅ Formatting applied successfully (dropdowns, checkboxes, number formats)")
        except Exception as e:
            print(f"⚠️  Error applying formatting: {e}")


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
    setup_formatting_with_api(sh, ws)
else:
    # Find next empty row
    existing_values = ws.col_values(START_COL)
    write_row = len(existing_values) + 1
    include_header = False

# If worksheet was just created, setup formatting
if worksheet_created:
    setup_formatting_with_api(sh, ws)

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