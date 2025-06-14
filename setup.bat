@echo off
python -m venv .venv
call .venv\Scripts\activate
pip install -r requirements.txt
echo.
echo Setup complete.  Copy service_account.json and .env, then drop a CSV into incoming_csv\
pause