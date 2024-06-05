@echo off

:: Check if the virtual environment already exists
if not exist "venv\Scripts\activate.bat" (
  :: Create a virtual environment
  python -m venv venv
)

:: Install the required packages
.\venv\Scripts\pip.exe install -r requirements.txt

:: Run the Python script
.\venv\Scripts\python.exe .\main.py

pause