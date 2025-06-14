#!/usr/bin/env python3
from pathlib import Path
from datetime import date
import os, sys, glob, pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv

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
for col in ("Due Date", "Balance"):
    if col not in df.columns:
        sys.exit(f"❌  CSV missing '{col}' column.")

df["Due Date"]     = pd.to_datetime(df["Due Date"], errors="coerce")
df["Balance"]      = pd.to_numeric(df["Balance"], errors="coerce")
df["Days Overdue"] = (pd.Timestamp(date.today()) - df["Due Date"]).dt.days

overdue = df.query("Balance > 0 and DaysOverdue > 0", engine="python").copy()
bins   = [0, 30, 60, 90, 120, float("inf")]
labels = ["1-30", "31-60", "61-90", "91-120", "120+"]
overdue["Bucket"] = pd.cut(overdue["Days Overdue"], bins, labels=labels)

gc = gspread.service_account(filename=BASE / SERVICE_JSON)
sh = gc.open_by_key(SHEET_ID)
try:
    ws = sh.worksheet(TARGET_TAB)
    ws.clear()
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=TARGET_TAB, rows=1, cols=len(overdue.columns)+2)

set_with_dataframe(ws, overdue, include_index=False, resize=True)
print(f"✅ Uploaded {len(overdue)} overdue invoices from {Path(csv_path).name}.")