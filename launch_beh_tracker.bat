@echo off

rem Activate Miniconda
call "C:\Users\%USERNAME%\miniconda3\Scripts\activate.bat" "C:\Users\%USERNAME%\miniconda3"

rem Activate Conda environment
call conda activate neuropy

REM Get the directory of this .bat file
set SCRIPT_DIR=%~dp0

REM Run the Python script from the same directory
python "%SCRIPT_DIR%behavior_tracker.py"