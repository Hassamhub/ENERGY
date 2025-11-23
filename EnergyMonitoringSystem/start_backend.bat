@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d %~dp0
echo Starting Backend API...
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --log-level info
