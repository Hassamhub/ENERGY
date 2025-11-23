@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d %~dp0
echo Starting DO Worker...
python backend\do_worker.py
