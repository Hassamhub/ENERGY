@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d %~dp0
echo Starting DO Worker...
set VENV_CFG=venv311\pyvenv.cfg
set PYTHON_EXEC=venv311\Scripts\python.exe
if exist %VENV_CFG% (
  %PYTHON_EXEC% -m backend.do_worker
) else (
  python -m backend.do_worker
)
