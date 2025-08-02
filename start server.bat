@echo off
REM Changes the directory to the folder where the batch file is located
cd /d "%~dp0"

echo Starting CT Tracker Server...
echo.

REM Runs your Python script
python ct_tracker_server_Version7.py

echo.
echo The server has stopped or crashed.
pause