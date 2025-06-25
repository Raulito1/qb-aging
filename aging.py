#!/usr/bin/env python3
from pathlib import Path
from datetime import date
import os, sys, glob, pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv
import re
from csv import Sniffer
from gspread.utils import rowcol_to_a1

BASE = Path(__file__).parent
load_dotenv(BASE / ".env")

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SERVICE_JSON = "service_account.json"
TARGET_TAB = "Overdue aging"
CSV_PATTERN = str(BASE / "incoming_csv" / "*.csv")
MINIMUM_DAYS_OVERDUE = 21

try:
    csv_path = max(glob.glob(CSV_PATTERN), key=os.path.getmtime)
except ValueError:
    sys.exit("❌  No CSV found in incoming_csv/.  Aborting.")

#
# Read CSV ── Handle the specific structure with title row
#
def read_ar_aging_csv(csv_path):
    """Read AR Aging CSV with title row structure"""
    with open(csv_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Find the header row (contains "Date", "Due date", etc.)
    header_row_idx = None
    for i, line in enumerate(lines):
        if 'Due date' in line and 'Customer full name' in line:
            header_row_idx = i
            break
    
    if header_row_idx is None:
        sys.exit("❌  Could not find header row with 'Due date' and 'Customer full name'")
    
    print(f"🔎 Found headers at line {header_row_idx + 1}")
    
    # Join lines from header onwards
    csv_content = ''.join(lines[header_row_idx:])
    
    # Detect delimiter
    sniffer = Sniffer()
    dialect = sniffer.sniff(csv_content[:1024], delimiters=",;\t")
    delimiter = dialect.delimiter
    
    print(f"🔎 Detected delimiter: '{delimiter}'")
    
    # Read with pandas from the header line onwards
    from io import StringIO
    df = pd.read_csv(
        StringIO(csv_content),
        dtype=str,
        delimiter=delimiter,
        skip_blank_lines=True,
        keep_default_na=False
    )
    
    return df

df = read_ar_aging_csv(csv_path)
print(f"📊 Total rows in CSV: {len(df)}")
print(f"📊 Original columns: {list(df.columns)}")

# Clean column names
df.columns = df.columns.str.strip().str.lower()
df.columns = df.columns.str.replace("\u00A0", " ", regex=False)
df.columns = df.columns.str.replace(r"\s+", " ", regex=True)

print(f"📊 Cleaned columns: {list(df.columns)}")

# Updated column mapping for your specific CSV structure
ALT_NAMES = {
    # Balance synonyms - use "open balance" as it's the current amount owed
    "open balance": "balance",
    "amount": "balance",  # fallback
    "balance": "balance",
    
    # Due date synonyms
    "due date": "due date",
    "duedate": "due date",
    "invoice due date": "due date",
    
    # Invoice date synonyms
    "invoice date": "invoice date", 
    "date": "invoice date",  # This is transaction/invoice date, not due date
    
    # Customer synonyms
    "customer full name": "customer",
    "customer name": "customer",
    "customer": "customer",
}

df.rename(columns=ALT_NAMES, inplace=True)
print(f"📊 Columns after mapping: {list(df.columns)}")

# Remove duplicate columns
if df.columns.duplicated().any():
    df = df.loc[:, ~df.columns.duplicated(keep="last")]

# Drop subtotal/rubric rows such as "OUT OF RANGE"
before_drop = len(df)
# Check multiple possible columns for OUT OF RANGE
for col in df.columns:
    if df[col].astype(str).str.contains("OUT OF RANGE", na=False).any():
        df = df[~df[col].astype(str).str.contains("OUT OF RANGE", na=False)]
        break

print(f"📊 Dropped {before_drop - len(df)} 'OUT OF RANGE' rows, {len(df)} remaining")

# Standardize final column names for processing
df.rename(columns={"due date": "Due Date", "balance": "Balance"}, inplace=True)
print(f"📊 Final columns: {list(df.columns)}")

# Verify required columns exist
for col in ("Due Date", "Balance"):
    if col not in df.columns:
        print(f"❌  Available columns: {list(df.columns)}")
        sys.exit(f"❌  CSV missing '{col}' column after mapping.")

# Process dates and amounts
df["Due Date"] = pd.to_datetime(df["Due Date"], format='%m/%d/%Y', errors="coerce")
df["Balance_raw"] = df["Balance"]
df["Balance"] = pd.to_numeric(df["Balance"].str.replace(',', ''), errors="coerce")
df["Days Overdue"] = (pd.Timestamp(date.today()) - df["Due Date"]).dt.days

# Debug: Check for data conversion issues
print(f"📊 Rows with valid Due Date: {df['Due Date'].notna().sum()}")
print(f"📊 Rows with valid Balance: {df['Balance'].notna().sum()}")
print(f"📊 Rows with Balance > 0: {(df['Balance'] > 0).sum()}")

# Check for balance conversion issues
balance_conversion_failed = df[df["Balance"].isna() & df["Balance_raw"].notna()]
if len(balance_conversion_failed) > 0:
    print(f"⚠️  {len(balance_conversion_failed)} rows failed balance conversion")
    print("   Sample raw balance values:", balance_conversion_failed["Balance_raw"].head().tolist())

# Filter based on minimum days overdue threshold
print(f"\n🔧 Processing only invoices {MINIMUM_DAYS_OVERDUE}+ days overdue")
overdue = df.query(f"Balance > 0 and `Days Overdue` >= {MINIMUM_DAYS_OVERDUE}", engine="python").copy()

print(f"📊 Final rows to process: {len(overdue)}")

# Show distribution for debugging
print("\n📊 Days Overdue distribution (for Balance > 0):")
positive_balance = df[df["Balance"] > 0]
if len(positive_balance) > 0:
    print(f"  - Less than 21 days: {(positive_balance['Days Overdue'] < 21).sum()}")
    print(f"  - 21+ days: {(positive_balance['Days Overdue'] >= 21).sum()}")
    print(f"  - Invalid/NaT dates: {positive_balance['Days Overdue'].isna().sum()}")

if len(overdue) == 0:
    print("⚠️  No overdue records found. Exiting.")
    sys.exit(0)

# ─────────────────────────────────────────────────────────────────────────────
# 🔧  Normalize column names BEFORE aggregation
# ─────────────────────────────────────────────────────────────────────────────
rename_map_pre = {
    "Balance": "Amount",
    "Due Date": "Date", 
    "Days Overdue": "Days Outstanding",
    "customer": "Customer",
    "customer name": "Customer",
    "customer full name": "Customer",
}
overdue.rename(columns=rename_map_pre, inplace=True)

# Clean up customer names
if "Customer" in overdue.columns:
    overdue["Customer"] = overdue["Customer"].str.split(":").str[0]
    overdue["Customer"] = overdue["Customer"].str.replace(r"([a-z])([A-Z])", r"\1 \2", regex=True)
    overdue["Customer"] = overdue["Customer"].str.strip()
else:
    # Fallback: find any column containing 'customer'
    fallback_cols = [c for c in overdue.columns if "customer" in c.lower()]
    if fallback_cols:
        overdue.rename(columns={fallback_cols[0]: "Customer"}, inplace=True)
        print(f"⚠️  Using fallback column '{fallback_cols[0]}' as 'Customer'")
        overdue["Customer"] = overdue["Customer"].str.split(":").str[0]
        overdue["Customer"] = overdue["Customer"].str.replace(r"([a-z])([A-Z])", r"\1 \2", regex=True)
        overdue["Customer"] = overdue["Customer"].str.strip()
    else:
        raise KeyError(f"❌  Could not locate a customer column. Available columns: {list(overdue.columns)}")

print(f"📊 Columns before aggregation: {list(overdue.columns)}")

# ─────────────────────────────────────────────────────────────────────────────
# 🔄  Aggregate multiple invoices per Customer
# ─────────────────────────────────────────────────────────────────────────────
agg_cols = {
    "Amount": "sum",
    "Date": "min",  # oldest outstanding invoice
}
overdue = (
    overdue
    .groupby("Customer", as_index=False)
    .agg(agg_cols)
    .copy()
)

# Recompute Days Outstanding using the oldest Date
overdue["Date"] = pd.to_datetime(overdue["Date"], errors="coerce")
overdue["Days Outstanding"] = (pd.Timestamp(date.today()) - overdue["Date"]).dt.days

print(f"📊 Aggregated to unique customers: {len(overdue)} rows")

# Collections workflow buckets (21+ days only)
bins = [20, 30, 45, 60, 90, float("inf")]
labels = ["21-30", "31-45", "46-60", "61-90", "91+"]

overdue["Bucket"] = pd.cut(overdue["Days Outstanding"], bins, labels=labels)

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

# Ensure all expected columns exist
for col in HEADERS:
    if col not in overdue.columns:
        overdue[col] = None

# Format the Date column
if "Date" in overdue.columns and not overdue.empty:
    overdue["Date"] = pd.to_datetime(overdue["Date"], errors='coerce').dt.strftime('%Y-%m-%d')
    print(f"📊 Formatted dates - sample: {overdue['Date'].head(5).tolist()}")

# Re-order columns to match the Google Sheet
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

    # 3. Add dropdown for Removed from No Work List Approver (column 10)
    approver_col_index = START_COL + 9 - 1
    approvers = ["Julie Harris", "Ben Terrill", "Esau Quiroz"]

    requests.append({
        "setDataValidation": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": HEADER_ROW,
                "endRowIndex": MAX_ROWS,
                "startColumnIndex": approver_col_index,
                "endColumnIndex": approver_col_index + 1
            },
            "rule": {
                "condition": {
                    "type": "ONE_OF_LIST",
                    "values": [{"userEnteredValue": approver} for approver in approvers]
                },
                "showCustomUi": True
            }
        }
    })

    # 4. Format Amount column as currency
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
    
    # 5. Format Date column
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
    setup_formatting_with_api(sh, ws)

# ─────────────────────────────────────────────────────────────────────────────
# 🔄  Incremental update: update existing customers; append new ones
# ─────────────────────────────────────────────────────────────────────────────
existing_customers = ws.col_values(START_COL)  # Column B
existing_customers = existing_customers[HEADER_ROW:]
customer_to_row = {
    name.strip(): idx
    for idx, name in enumerate(existing_customers, start=HEADER_ROW + 1)
    if name.strip()
}

new_rows = []

for _, r in overdue.iterrows():
    cust = r["Customer"]
    # Sanitize NaNs for Sheets API
    row_values = [
        ("" if pd.isna(r[h]) else r[h]) for h in HEADERS
    ]

    if cust in customer_to_row:
        # Update columns B‑G (Customer .. Collection Item)
        sheet_row = customer_to_row[cust]
        start_cell = rowcol_to_a1(sheet_row, START_COL)
        end_cell = rowcol_to_a1(sheet_row, START_COL + 5)
        ws.update(f"{start_cell}:{end_cell}", [row_values[:6]])
        print(f"🔄 Updated existing customer '{cust}' at row {sheet_row}")
    else:
        new_rows.append(row_values)

if new_rows:
    ws.append_rows(
        new_rows,
        table_range=rowcol_to_a1(HEADER_ROW + 1, START_COL),
        value_input_option="USER_ENTERED"
    )
    print(f"➕ Added {len(new_rows)} new customers")

print("✅ Sheet synchronised with latest CSV data")

# Ensure formatting is applied
setup_formatting_with_api(sh, ws)