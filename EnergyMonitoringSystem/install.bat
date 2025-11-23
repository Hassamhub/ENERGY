@echo off
echo ===========================================
echo PAC3220 Energy Monitoring System Setup
echo ===========================================
echo.

echo This script will set up the database and prepare the system for deployment.
echo Make sure you have SQL Server running and configured.
echo.

set /p DB_SERVER="Enter SQL Server hostname/instance (default: localhost): "
if "%DB_SERVER%"=="" set DB_SERVER=localhost

set /p DB_NAME="Enter database name (default: EnergyMonitoringDB): "
if "%DB_NAME%"=="" set DB_NAME=EnergyMonitoringDB

set /p SA_PASSWORD="Enter SA password: "
if "%SA_PASSWORD%"=="" (
    echo Error: SA password is required
    pause
    exit /b 1
)

echo.
echo Creating database and schema...
echo.

REM Create database if it doesn't exist
sqlcmd -S %DB_SERVER% -U sa -P %SA_PASSWORD% -Q "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '%DB_NAME%') CREATE DATABASE [%DB_NAME%];"

REM Run the schema creation script
sqlcmd -S %DB_SERVER% -U sa -P %SA_PASSWORD% -d %DB_NAME% -i scripts\database_init.sql

if %errorlevel% neq 0 (
    echo.
    echo ERROR: Database setup failed!
    echo Please check your SQL Server configuration and try again.
    pause
    exit /b 1
)

echo.
echo ===========================================
echo Database setup completed successfully!
echo ===========================================
echo.
echo Next steps:
echo 1. Configure your .env file with database credentials
echo 2. Start the backend API: python -m uvicorn backend.main:app --reload
echo 3. Start the worker: python backend/do_worker.py
echo 4. Access the web interface at http://localhost:3000
echo.
echo Default admin credentials:
echo Username: admin
echo Password: Admin123!
echo.
echo IMPORTANT: Change the default password after first login!
echo.

pause
