@echo off
echo Installing Batch Data Fetcher & Exporter...
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Python is not installed or not in PATH!
    echo Please install Python 3.8+ from https://python.org
    pause
    exit /b 1
)

echo Installing required packages...
echo.

REM Install all dependencies
pip install pyodbc pandas openpyxl matplotlib tkcalendar

if errorlevel 1 (
    echo.
    echo Failed to install packages!
    echo Trying with pip3...
    echo.
    pip3 install pyodbc pandas openpyxl matplotlib tkcalendar
)

if errorlevel 1 (
    echo.
    echo Installation failed!
    echo Please install manually:
    echo pip install pyodbc pandas openpyxl matplotlib tkcalendar
    pause
    exit /b 1
)

echo.
echo Installation completed successfully!
echo.
echo To run the application:
echo   python app.py
echo.
pause
@echo off
echo Installing Data Fetcher Application...
echo.
pip install pyodbc pandas openpyxl tkcalendar
echo.
echo Installation complete!
echo Run: python app.py
pause