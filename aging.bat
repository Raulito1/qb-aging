@echo off
call %~dp0.venv\Scripts\activate
python %~dp0aging.py
pause