#!/usr/bin/env python3
"""
PAC3220 Prepaid Energy Monitoring System - Startup Script
Starts all system components for testing and production.
"""

import os
import sys
import subprocess
import time
from pathlib import Path

def check_database_connection():
    """Check if database is accessible"""
    try:
        from backend.dal.database import db_helper
        result = db_helper.test_connection()
        if result:
            print("[OK] Database connection successful")
            return True
        else:
            print("[ERROR] Database connection failed")
            return False
    except Exception as e:
        print(f"[ERROR] Database connection error: {e}")
        return False

def start_backend_api():
    """Start the FastAPI backend server"""
    print("Starting Backend API...")
    try:
        # Change to backend directory
        backend_dir = Path(__file__).parent / "backend"
        os.chdir(backend_dir)

        # Start uvicorn server
        cmd = [
            sys.executable, "-m", "uvicorn",
            "main:app",
            "--host", "0.0.0.0",
            "--port", "8000",
            "--reload",
            "--log-level", "info"
        ]

        return subprocess.Popen(cmd, cwd=backend_dir)
    except Exception as e:
        print(f"[ERROR] Failed to start backend API: {e}")
        return None

def start_worker():
    print("Starting Command Worker...")
    try:
        worker_path = Path(__file__).parent / "backend" / "do_worker.py"
        cmd = [sys.executable, str(worker_path)]
        return subprocess.Popen(cmd)
    except Exception as e:
        print(f"[ERROR] Failed to start worker: {e}")
        return None

def main():
    """Main startup function"""
    print("=" * 60)
    print("PAC3220 Prepaid Energy Monitoring System")
    print("=" * 60)

    # Check database connection
    if not check_database_connection():
        print("[ERROR] Cannot start system - database connection failed")
        print("Please check your database configuration in .env file")
        sys.exit(1)

    processes = []

    try:
        # Start backend API
        api_process = start_backend_api()
        if api_process:
            processes.append(("Backend API", api_process))

        # Wait a moment for API to start
        time.sleep(3)

        # Start worker
        worker_process = start_worker()
        if worker_process:
            processes.append(("Command Worker", worker_process))

        print("\n" + "=" * 60)
        print("SYSTEM STARTUP COMPLETE")
        print("=" * 60)
        print("Services running:")
        for name, process in processes:
            print(f"   - {name}: PID {process.pid}")

        print("\nAccess points:")
        print("   - API Documentation: http://localhost:8000/docs")
        print("   - Alternative Docs: http://localhost:8000/redoc")
        print("   - Frontend: http://localhost:3000 (if started separately)")
        pass

        print("\n" + "=" * 60)
        print("Press Ctrl+C to stop all services")
        print("=" * 60)

        # Keep running until interrupted
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\nShutting down services...")

    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
    finally:
        # Cleanup processes
        for name, process in processes:
            try:
                print(f"Stopping {name}...")
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print(f"Force killing {name}...")
            except Exception as e:
                print(f"Error stopping {name}: {e}")

        print("All services stopped. Goodbye!")

if __name__ == "__main__":
    main()
