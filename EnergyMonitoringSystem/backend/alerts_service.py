import os
import asyncio
from datetime import datetime, timedelta
from typing import Optional, List

from backend.dal.database import db_helper
from backend.utils.email_client import send_email

CHECK_INTERVAL_SECONDS = int(os.getenv("ALERTS_CHECK_INTERVAL", "60"))

async def _check_low_balance_and_notify() -> None:
    try:
        # Threshold can be absolute kWh or %; here treat as absolute kWh
        thr_env = os.getenv("LOW_BALANCE_THRESHOLD_KWH", "5")
        try:
            threshold = float(thr_env)
        except Exception:
            threshold = 5.0

        # Find users below threshold (RemainingKWh assumed computed or stored)
        rows = db_helper.execute_query(
            """
            SELECT UserID, Username, Email, RemainingKWh
            FROM app.Users
            WHERE ISNULL(RemainingKWh, 0) < ? AND ISNULL(IsActive,1) = 1
            """,
            (threshold,)
        ) or []

        for r in rows:
            email = r.get("Email")
            if not email:
                continue
            subject = "Low Balance Alert"
            body = f"Dear {r.get('Username')}, your RemainingKWh is {r.get('RemainingKWh')} kWh, which is below {threshold} kWh."
            send_email(subject, body, [email], html=False)
    except Exception:
        # Avoid crashing scheduler
        pass

async def _check_offline_devices_and_notify() -> None:
    try:
        # Determine poll interval to gauge offline threshold
        poll = 60
        try:
            cfg = db_helper.execute_query("SELECT ConfigValue FROM ops.Configuration WHERE ConfigKey = 'system.poller_interval'")
            if cfg and cfg[0].get("ConfigValue"):
                poll = int(cfg[0]["ConfigValue"])
        except Exception:
            pass
        # Offline if LastSeen older than 3x poll interval
        minutes = max(1, (poll * 3) // 60 if poll >= 60 else 1)
        rows = db_helper.execute_query(
            f"""
            SELECT a.AnalyzerID, a.SerialNumber, a.LastSeen, a.ConnectionStatus,
                   u.Email, u.Username
            FROM app.Analyzers a
            LEFT JOIN app.Users u ON a.UserID = u.UserID
            WHERE a.IsActive = 1
              AND (a.LastSeen IS NULL OR a.LastSeen < DATEADD(MINUTE, -{minutes}, GETUTCDATE()))
            """
        ) or []
        for r in rows:
            email = r.get("Email")
            if not email:
                continue
            subject = "Device Offline Alert"
            body = (
                f"Analyzer {r.get('SerialNumber') or r.get('AnalyzerID')} appears OFFLINE. "
                f"LastSeen: {r.get('LastSeen')}"
            )
            send_email(subject, body, [email], html=False)
    except Exception:
        pass

async def start_alerts_scheduler():
    # Run forever on app startup if enabled
    async def loop():
        while True:
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            await _check_low_balance_and_notify()
            await _check_offline_devices_and_notify()
    # Start background loop
    asyncio.create_task(loop())
