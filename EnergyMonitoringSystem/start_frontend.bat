@echo off
setlocal ENABLEDELAYEDEXPANSION
cd /d %~dp0\frontend
echo Installing frontend dependencies...
npm install
echo Starting frontend dev server...
npm run dev
