"""
Energy readings API routes
Handles retrieval of device readings and historical data.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel

from backend.dal.database import db_helper
from backend.api.routes_auth import get_current_user

router = APIRouter()
security = HTTPBearer()

class ReadingFilterRequest(BaseModel):
    device_id: Optional[int] = None
    parameter_id: Optional[int] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: Optional[int] = 1000

@router.get("/latest/{device_id}")
async def get_latest_readings(device_id: int, current_user: Dict = Depends(get_current_user)):
    """Get latest readings for a specific device"""
    try:
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        # Check analyzer ownership
        device_query = "SELECT UserID FROM app.Analyzers WHERE AnalyzerID = ? AND IsActive = 1"
        devices = db_helper.execute_query(device_query, (device_id,))

        if not devices or len(devices) == 0:
            raise HTTPException(status_code=404, detail="Device not found")

        device = devices[0]

        if user_role != "Admin" and str(device.get("UserID")) != str(user_id):
            raise HTTPException(status_code=403, detail="Access denied")

        # Get latest row from app.Readings and expand into parameter list
        latest_query = """
        SELECT TOP 1 *
        FROM app.Readings
        WHERE AnalyzerID = ?
        ORDER BY Timestamp DESC
        """

        row_list = db_helper.execute_query(latest_query, (device_id,)) or []

        formatted = []
        if row_list:
            r = row_list[0]
            def add_param(name, unit, value):
                if value is None:
                    return
                formatted.append({
                    "parameter_id": name,
                    "parameter_name": name,
                    "unit": unit,
                    "value": value,
                    "timestamp": r["Timestamp"],
                    "quality": r.get("Quality", "GOOD")
                })

            add_param("KW_Total", "kW", r.get("KW_Total"))
            add_param("KW_L1", "kW", r.get("KW_L1"))
            add_param("KW_L2", "kW", r.get("KW_L2"))
            add_param("KW_L3", "kW", r.get("KW_L3"))
            add_param("VL1", "V", r.get("VL1"))
            add_param("VL2", "V", r.get("VL2"))
            add_param("VL3", "V", r.get("VL3"))
            add_param("IL1", "A", r.get("IL1"))
            add_param("IL2", "A", r.get("IL2"))
            add_param("IL3", "A", r.get("IL3"))
            add_param("ITotal", "A", r.get("ITotal"))
            add_param("Hz", "Hz", r.get("Hz"))
            add_param("PF_Avg", "", r.get("PF_Avg"))
            add_param("PF_L1", "", r.get("PF_L1"))
            add_param("PF_L2", "", r.get("PF_L2"))
            add_param("PF_L3", "", r.get("PF_L3"))
            add_param("KWh_Total", "kWh", r.get("KWh_Total"))
            add_param("KWh_Grid", "kWh", r.get("KWh_Grid"))
            add_param("KWh_Generator", "kWh", r.get("KWh_Generator"))

        return {
            "success": True,
            "device_id": device_id,
            "readings_count": len(formatted),
            "readings": formatted,
            "timestamp": datetime.utcnow()
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get latest readings error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve latest readings")

@router.get("/history/{device_id}")
async def get_reading_history(
    device_id: int,
    hours: int = Query(24, description="Hours of history to retrieve", ge=1, le=8760),
    parameter_id: Optional[int] = Query(None, description="Specific parameter ID to filter"),
    current_user: Dict = Depends(get_current_user)
):
    """Get historical readings for a device (KW_Total hourly buckets using ReadingDate + ReadingHour)"""
    try:
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        # Check analyzer ownership
        device_query = "SELECT UserID FROM app.Analyzers WHERE AnalyzerID = ? AND IsActive = 1"
        devices = db_helper.execute_query(device_query, (device_id,))

        if not devices or len(devices) == 0:
            raise HTTPException(status_code=404, detail="Device not found")

        device = devices[0]

        if user_role != "Admin" and str(device.get("UserID")) != str(user_id):
            raise HTTPException(status_code=403, detail="Access denied")

        # Build hourly buckets using ReadingDate + ReadingHour
        base_query = """
        SELECT TOP 720
               MIN(r.ReadingID) as DataID,
               'KW_Total' as ParameterName,
               'kW' as Unit,
               AVG(CAST(r.KW_Total AS FLOAT)) as Value,
               MIN(r.Timestamp) as FirstTs,
               MAX(r.Timestamp) as LastTs,
               r.ReadingDate,
               r.ReadingHour
        FROM app.Readings r
        WHERE r.AnalyzerID = ?
          AND r.Timestamp >= DATEADD(HOUR, -?, GETUTCDATE())
        GROUP BY r.ReadingDate, r.ReadingHour
        ORDER BY r.ReadingDate DESC, r.ReadingHour DESC
        """

        readings = db_helper.execute_query(base_query, (device_id, hours))

        return {
            "success": True,
            "device_id": device_id,
            "hours_requested": hours,
            "parameter_filter": parameter_id,
            "readings_count": len(readings) if readings else 0,
            "readings": readings or []
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get reading history error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve reading history")

@router.get("/parameters")
async def get_parameters(current_user: Dict = Depends(get_current_user)):
    """Get all available parameters"""
    try:
        param_defs = [
            {"parameter_id": "KW_Total", "parameter_code": "KW_Total", "parameter_name": "KW Total", "unit": "kW"},
            {"parameter_id": "KW_L1", "parameter_code": "KW_L1", "parameter_name": "KW L1", "unit": "kW"},
            {"parameter_id": "KW_L2", "parameter_code": "KW_L2", "parameter_name": "KW L2", "unit": "kW"},
            {"parameter_id": "KW_L3", "parameter_code": "KW_L3", "parameter_name": "KW L3", "unit": "kW"},
            {"parameter_id": "VL1", "parameter_code": "VL1", "parameter_name": "Voltage L1", "unit": "V"},
            {"parameter_id": "VL2", "parameter_code": "VL2", "parameter_name": "Voltage L2", "unit": "V"},
            {"parameter_id": "VL3", "parameter_code": "VL3", "parameter_name": "Voltage L3", "unit": "V"},
            {"parameter_id": "IL1", "parameter_code": "IL1", "parameter_name": "Current L1", "unit": "A"},
            {"parameter_id": "IL2", "parameter_code": "IL2", "parameter_name": "Current L2", "unit": "A"},
            {"parameter_id": "IL3", "parameter_code": "IL3", "parameter_name": "Current L3", "unit": "A"},
            {"parameter_id": "ITotal", "parameter_code": "ITotal", "parameter_name": "Current Total", "unit": "A"},
            {"parameter_id": "Hz", "parameter_code": "Hz", "parameter_name": "Frequency", "unit": "Hz"},
            {"parameter_id": "PF_Avg", "parameter_code": "PF_Avg", "parameter_name": "Power Factor Avg", "unit": ""},
            {"parameter_id": "PF_L1", "parameter_code": "PF_L1", "parameter_name": "Power Factor L1", "unit": ""},
            {"parameter_id": "PF_L2", "parameter_code": "PF_L2", "parameter_name": "Power Factor L2", "unit": ""},
            {"parameter_id": "PF_L3", "parameter_code": "PF_L3", "parameter_name": "Power Factor L3", "unit": ""},
            {"parameter_id": "KWh_Total", "parameter_code": "KWh_Total", "parameter_name": "Energy Total", "unit": "kWh"},
            {"parameter_id": "KWh_Grid", "parameter_code": "KWh_Grid", "parameter_name": "Energy Grid", "unit": "kWh"},
            {"parameter_id": "KWh_Generator", "parameter_code": "KWh_Generator", "parameter_name": "Energy Generator", "unit": "kWh"}
        ]

        return {
            "success": True,
            "count": len(param_defs),
            "parameters": param_defs
        }

    except Exception as e:
        print(f"Get parameters error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve parameters")

@router.get("/summary/{device_id}")
async def get_device_summary(
    device_id: int,
    days: int = Query(7, description="Days to summarize", ge=1, le=30),
    current_user: Dict = Depends(get_current_user)
):
    """Get summary statistics for a device over a period (KW_Total aggregates)"""
    try:
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        # Check device ownership
        device_query = "SELECT UserID, SerialNumber FROM app.Analyzers WHERE AnalyzerID = ? AND IsActive = 1"
        devices = db_helper.execute_query(device_query, (device_id,))

        if not devices or len(devices) == 0:
            raise HTTPException(status_code=404, detail="Device not found")

        device = devices[0]

        if user_role != "Admin" and str(device.get("UserID")) != str(user_id):
            raise HTTPException(status_code=403, detail="Access denied")

        # Get summary statistics (KW_Total)
        summary_query = """
        SELECT
            COUNT(*) as ReadingCount,
            AVG(r.KW_Total) as AvgKW,
            MIN(r.KW_Total) as MinKW,
            MAX(r.KW_Total) as MaxKW,
            MIN(r.Timestamp) as FirstReading,
            MAX(r.Timestamp) as LastReading
        FROM app.Readings r
        WHERE r.AnalyzerID = ?
          AND r.Timestamp >= DATEADD(DAY, -?, GETUTCDATE())
        """

        summary_rows = db_helper.execute_query(summary_query, (device_id, days)) or []
        summary = summary_rows[0] if summary_rows else {}

        return {
            "success": True,
            "device_id": device_id,
            "device_name": device.get("SerialNumber"),
            "days_analyzed": days,
            "summary_count": summary.get("ReadingCount", 0) if summary else 0,
            "summary": summary or {}
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get device summary error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve device summary")

@router.get("/realtime")
async def get_realtime_readings(current_user: Dict = Depends(get_current_user)):
    """Get real-time readings for user's devices"""
    try:
        if hasattr(db_helper, "test_connection") and not db_helper.test_connection():
            return {"success": True, "count": 0, "devices": [], "timestamp": datetime.utcnow()}
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        if user_role == "Admin":
            # Admin gets all devices
            device_query = """
            SELECT a.AnalyzerID as DeviceID, a.SerialNumber as DeviceName, a.IPAddress, a.LastSeen,
                   u.Username as OwnerUsername
            FROM app.Analyzers a
            LEFT JOIN app.Users u ON a.UserID = u.UserID
            WHERE a.IsActive = 1
            """
            devices = db_helper.execute_query(device_query)
        else:
            # User gets only their devices
            device_query = """
            SELECT a.AnalyzerID as DeviceID, a.SerialNumber as DeviceName, a.IPAddress, a.LastSeen
            FROM app.Analyzers a
            WHERE a.UserID = ? AND a.IsActive = 1
            """
            devices = db_helper.execute_query(device_query, (user_id,))

        result = []

        for device in devices or []:
            device_id = device["DeviceID"]

            # Fetch latest app.Readings row
            latest_row = db_helper.execute_query(
                """
                SELECT TOP 1 *
                FROM app.Readings
                WHERE AnalyzerID = ?
                ORDER BY Timestamp DESC
                """,
                (device_id,)
            )

            latest_timestamp = None
            device_readings: Dict[str, Any] = {}
            if latest_row:
                r = latest_row[0]
                latest_timestamp = r["Timestamp"]
                def setv(key, val):
                    if val is not None:
                        device_readings[key] = {"parameter_name": key, "unit": "", "value": val}
                setv("KW_Total", r.get("KW_Total"))
                setv("KW_L1", r.get("KW_L1"))
                setv("KW_L2", r.get("KW_L2"))
                setv("KW_L3", r.get("KW_L3"))
                setv("VL1", r.get("VL1"))
                setv("VL2", r.get("VL2"))
                setv("VL3", r.get("VL3"))
                setv("IL1", r.get("IL1"))
                setv("IL2", r.get("IL2"))
                setv("IL3", r.get("IL3"))
                setv("ITotal", r.get("ITotal"))
                setv("Hz", r.get("Hz"))
                setv("PF_Avg", r.get("PF_Avg"))
                setv("KWh_Total", r.get("KWh_Total"))
                setv("KWh_Grid", r.get("KWh_Grid"))
                setv("KWh_Generator", r.get("KWh_Generator"))

            result.append({
                "device_id": device_id,
                "device_name": device["DeviceName"],
                "owner": device.get("OwnerUsername", "N/A") if user_role == "Admin" else None,
                "ip_address": device["IPAddress"],
                "last_seen": device["LastSeen"],
                "latest_timestamp": latest_timestamp,
                "readings": device_readings
            })

        return {
            "success": True,
            "count": len(result),
            "devices": result,
            "timestamp": datetime.utcnow()
        }

    except Exception as e:
        print(f"Get realtime readings error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve realtime readings")

@router.get("/realtime/v2")
async def get_realtime_readings_v2(current_user: Dict = Depends(get_current_user)):
    """Get real-time readings with a consistent schema keyed by ParameterName and including ParameterCode.
    Returns per-device entries with:
    - device_id, device_name, latest_timestamp
    - readings: list of {name, code, unit, value}
    """
    try:
        if hasattr(db_helper, "test_connection") and not db_helper.test_connection():
            return {"success": True, "count": 0, "devices": [], "timestamp": datetime.utcnow()}
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        if user_role == "Admin":
            device_query = (
                """
                SELECT a.AnalyzerID as DeviceID, a.SerialNumber as DeviceName, a.IPAddress, a.LastSeen
                FROM app.Analyzers a
                WHERE a.IsActive = 1
                """
            )
            devices = db_helper.execute_query(device_query)
        else:
            device_query = (
                """
                SELECT a.AnalyzerID as DeviceID, a.SerialNumber as DeviceName, a.IPAddress, a.LastSeen
                FROM app.Analyzers a
                WHERE a.UserID = ? AND a.IsActive = 1
                """
            )
            devices = db_helper.execute_query(device_query, (user_id,))

        result = []

        for device in devices or []:
            device_id = device["DeviceID"]

            # Fetch the latest timestamp for readings per device
            ts_query = (
                """
                SELECT MAX(Timestamp) as LatestTs
                FROM app.Readings
                WHERE AnalyzerID = ?
                """
            )
            ts_row = db_helper.execute_query(ts_query, (device_id,))
            latest_ts = ts_row[0]["LatestTs"] if ts_row else None

            readings = []
            if latest_ts is not None:
                latest_row = db_helper.execute_query(
                    """
                    SELECT TOP 1 *
                    FROM app.Readings
                    WHERE AnalyzerID = ? AND Timestamp = ?
                    ORDER BY Timestamp DESC
                    """,
                    (device_id, latest_ts)
                )
                if latest_row:
                    rr = latest_row[0]
                    def push(code, unit, val):
                        if val is None:
                            return
                        try:
                            v = float(val)
                            if v != v or v == float("inf") or v == float("-inf"):
                                return
                        except Exception:
                            return
                        readings.append({
                            "name": code,
                            "code": code,
                            "unit": unit,
                            "value": v
                        })
                    pf_val = rr.get("PF_Avg")
                    try:
                        kw = float(rr.get("KW_Total") or 0)
                        it = float(rr.get("ITotal") or 0)
                        v1 = float(rr.get("VL1") or 0)
                        if (pf_val is None or float(pf_val) == 0.0) and kw < 0.001 and it < 0.01 and v1 > 100.0:
                            pf_val = 1.0
                    except Exception:
                        pass

                    # Map DB columns to frontend parameter codes
                    push("power_kw_total", "kW", rr.get("KW_Total"))
                    push("power_kw_l1", "kW", rr.get("KW_L1"))
                    push("power_kw_l2", "kW", rr.get("KW_L2"))
                    push("power_kw_l3", "kW", rr.get("KW_L3"))
                    push("voltage_l1", "V", rr.get("VL1"))
                    push("voltage_l2", "V", rr.get("VL2"))
                    push("voltage_l3", "V", rr.get("VL3"))
                    push("current_l1", "A", rr.get("IL1"))
                    push("current_l2", "A", rr.get("IL2"))
                    push("current_l3", "A", rr.get("IL3"))
                    push("current_total", "A", rr.get("ITotal"))
                    push("frequency", "Hz", rr.get("Hz"))
                    push("pf_total", "", pf_val)
                    push("energy_kwh_total", "kWh", rr.get("KWh_Total"))
                    push("energy_kwh_grid", "kWh", rr.get("KWh_Grid"))
                    push("energy_kwh_generator", "kWh", rr.get("KWh_Generator"))

            result.append({
                "device_id": device_id,
                "device_name": device["DeviceName"],
                "latest_timestamp": latest_ts,
                "readings": readings
            })

        return {
            "success": True,
            "count": len(result),
            "devices": result,
            "timestamp": datetime.utcnow()
        }

    except Exception as e:
        print(f"Get realtime readings v2 error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve realtime readings v2")
