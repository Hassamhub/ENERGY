@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d %~dp0
echo Starting Backend API...
set VENV_CFG=venv311\pyvenv.cfg
set PYTHON_EXEC=venv311\Scripts\python.exe
if exist %VENV_CFG% (
  %PYTHON_EXEC% -m uvicorn backend.main:app --host localhost --port 8000 --log-level info
) else (
  python -m uvicorn backend.main:app --host localhost --port 8000 --log-level info
)
