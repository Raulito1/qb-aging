## QuickBooks → Google Sheets Aging Updater

1. Copy **service_account.json** and rename **.env.example → .env** (fill the sheet ID).
2. Drop your QuickBooks CSV into `incoming_csv\`.
3. Run **setup.bat** once, then **aging.bat** any time.
4. Add a Windows Task Scheduler entry to call  
   `C:\qb-aging\.venv\Scripts\python.exe aging.py`  
   every Monday at 08:00. Done.