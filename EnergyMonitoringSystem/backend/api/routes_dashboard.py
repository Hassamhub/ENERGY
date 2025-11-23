"""
Dashboard API routes
Provides dashboard data for both user and admin interfaces.
"""

from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Dict, Any
from datetime import datetime, timedelta

from backend.dal.database import db_helper
from backend.api.routes_auth import get_current_user

router = APIRouter()
security = HTTPBearer()

@router.get("/user")
async def get_user_dashboard(current_user: Dict = Depends(get_current_user)):
    """Get user dashboard data"""
    try:
        if hasattr(db_helper, "test_connection") and not db_helper.test_connection():
            return {"success": True, "data": {}, "timestamp": datetime.utcnow()}
        user_id = current_user.get("sub")
        user_role = current_user.get("role", "User")

        # Get user dashboard data
        result = None
        try:
            result = db_helper.execute_stored_procedure("app.sp_GetUserDashboard", {"@UserID": user_id})
        except Exception:
            try:
                result = db_helper.execute_query("SELECT * FROM app.vw_UserDashboard WHERE UserID = ?", (user_id,))
            except Exception:
                result = []

        return {
            "success": True,
            "data": result[0] if result else {},
            "timestamp": datetime.utcnow()
        }

    except Exception as e:
        print(f"Get user dashboard error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve dashboard data")

@router.get("/admin")
async def get_admin_dashboard(current_user: Dict = Depends(get_current_user)):
    """Get admin dashboard data"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        # Get admin dashboard data
        dashboard_query = """
        SELECT
            (SELECT COUNT(*) FROM app.Users WHERE IsActive = 1) as total_users,
            (SELECT COUNT(*) FROM app.Analyzers WHERE IsActive = 1) as total_analyzers,
            (SELECT COUNT(*) FROM app.Analyzers WHERE ConnectionStatus = 'ONLINE' AND IsActive = 1) as online_analyzers,
            (SELECT COUNT(*) FROM app.Alerts WHERE IsActive = 1 AND IsRead = 0) as unread_alerts,
            (SELECT SUM(AllocatedKWh) FROM app.Users WHERE IsActive = 1) as total_allocated_kwh,
            (SELECT SUM(UsedKWh) FROM app.Users WHERE IsActive = 1) as total_used_kwh,
            (SELECT COUNT(*) FROM app.Readings WHERE Timestamp >= DATEADD(HOUR, -1, GETUTCDATE())) as readings_last_hour,
            (SELECT COUNT(*) FROM ops.Events WHERE Timestamp >= DATEADD(HOUR, -24, GETUTCDATE())) as events_last_24h,
            (SELECT COUNT(*) FROM app.Allocations WHERE RequestedAt >= DATEADD(DAY, -7, GETUTCDATE())) as allocations_last_week
        """

        result = db_helper.execute_query(dashboard_query)

        return {
            "success": True,
            "data": result[0] if result else {},
            "timestamp": datetime.utcnow()
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get admin dashboard error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve dashboard data")

@router.get("/user/{user_id}")
async def get_user_dashboard_for_admin(user_id: int, current_user: Dict = Depends(get_current_user)):
    """Admin view of specific user's dashboard"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        # Get user info
        user_query = """
        SELECT UserID, Username, FullName, AllocatedKWh, UsedKWh, RemainingKWh, IsLocked
        FROM app.Users
        WHERE UserID = ?
        """

        users = db_helper.execute_query(user_query, (user_id,))

        if not users or len(users) == 0:
            raise HTTPException(status_code=404, detail="User not found")

        user = users[0]

        devices_query = """
        SELECT a.AnalyzerID as DeviceID, a.SerialNumber as DeviceName, a.IPAddress,
               a.ConnectionStatus as Status, a.LastSeen,
               COUNT(r.ReadingID) as ReadingCount
        FROM app.Analyzers a
        LEFT JOIN app.Readings r ON a.AnalyzerID = r.AnalyzerID
            AND r.Timestamp >= DATEADD(HOUR, -24, GETUTCDATE())
        WHERE a.UserID = ? AND a.IsActive = 1
        GROUP BY a.AnalyzerID, a.SerialNumber, a.IPAddress, a.ConnectionStatus, a.LastSeen
        """

        devices = db_helper.execute_query(devices_query, (user_id,))

        # Get recent events
        events_query = """
        SELECT TOP 10 EventID, Level, EventType, Message, Timestamp
        FROM ops.Events
        WHERE UserID = ?
        ORDER BY Timestamp DESC
        """

        events = db_helper.execute_query(events_query, (user_id,))

        device_readings = {}
        for device in devices or []:
            latest_row = db_helper.execute_query(
                """
                SELECT TOP 1 *
                FROM app.Readings
                WHERE AnalyzerID = ?
                ORDER BY Timestamp DESC
                """,
                (device["DeviceID"],)
            )
            readings_list = []
            if latest_row:
                r = latest_row[0]
                def add_param(name, unit, value):
                    if value is None:
                        return
                    readings_list.append({
                        "ParameterName": name,
                        "Value": value,
                        "Timestamp": r["Timestamp"],
                        "Unit": unit
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
                add_param("KWh_Total", "kWh", r.get("KWh_Total"))
                add_param("KWh_Grid", "kWh", r.get("KWh_Grid"))
                add_param("KWh_Generator", "kWh", r.get("KWh_Generator"))
            device_readings[device["DeviceID"]] = readings_list

        return {
            "success": True,
            "user": user,
            "devices": devices or [],
            "device_readings": device_readings,
            "recent_events": events or [],
            "timestamp": datetime.utcnow()
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get user dashboard for admin error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve user dashboard")

@router.get("/analytics/overview")
async def get_system_analytics(hours: int = 24, current_user: Dict = Depends(get_current_user)):
    """Get system-wide analytics"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        # System performance metrics
        analytics_query = f"""
        SELECT
            COUNT(DISTINCT a.AnalyzerID) as ActiveDevices,
            COUNT(r.ReadingID) as TotalReadings,
            AVG(r.KW_Total) as AvgKWTotal,
            COUNT(DISTINCT CASE WHEN r.Timestamp >= DATEADD(HOUR, -{hours}, GETUTCDATE()) THEN r.AnalyzerID END) as DevicesReportingRecently,
            COUNT(CASE WHEN e.Level = 'CRITICAL' AND e.CreatedAt >= DATEADD(HOUR, -{hours}, GETUTCDATE()) THEN 1 END) as CriticalEvents,
            COUNT(CASE WHEN e.Level = 'WARN' AND e.CreatedAt >= DATEADD(HOUR, -{hours}, GETUTCDATE()) THEN 1 END) as WarningEvents
        FROM app.Analyzers a
        LEFT JOIN app.Readings r ON a.AnalyzerID = r.AnalyzerID
            AND r.Timestamp >= DATEADD(HOUR, -{hours}, GETUTCDATE())
        LEFT JOIN ops.Events e ON a.AnalyzerID = e.AnalyzerID
            AND e.Timestamp >= DATEADD(HOUR, -{hours}, GETUTCDATE())
        WHERE a.IsActive = 1
        """

        analytics = db_helper.execute_query(analytics_query)

        # Hourly activity for the last 24 hours
        hourly_activity_query = f"""
        SELECT
            DATEPART(HOUR, r.Timestamp) as Hour,
            COUNT(*) as ReadingCount,
            COUNT(DISTINCT r.AnalyzerID) as ActiveDevices
        FROM app.Readings r
        WHERE r.Timestamp >= DATEADD(HOUR, -{hours}, GETUTCDATE())
        GROUP BY DATEPART(HOUR, r.Timestamp)
        ORDER BY Hour
        """

        hourly_activity = db_helper.execute_query(hourly_activity_query)

        # Top parameters by reading frequency
        agg_query = f"""
        SELECT
            COUNT(CASE WHEN KW_Total IS NOT NULL THEN 1 END) as KW_Total_Count,
            AVG(KW_Total) as KW_Total_Avg,
            MIN(KW_Total) as KW_Total_Min,
            MAX(KW_Total) as KW_Total_Max,
            COUNT(CASE WHEN KWh_Total IS NOT NULL THEN 1 END) as KWh_Total_Count,
            AVG(KWh_Total) as KWh_Total_Avg,
            MIN(KWh_Total) as KWh_Total_Min,
            MAX(KWh_Total) as KWh_Total_Max,
            COUNT(CASE WHEN VL1 IS NOT NULL THEN 1 END) as VL1_Count,
            AVG(VL1) as VL1_Avg,
            MIN(VL1) as VL1_Min,
            MAX(VL1) as VL1_Max,
            COUNT(CASE WHEN IL1 IS NOT NULL THEN 1 END) as IL1_Count,
            AVG(IL1) as IL1_Avg,
            MIN(IL1) as IL1_Min,
            MAX(IL1) as IL1_Max
        FROM app.Readings
        WHERE Timestamp >= DATEADD(HOUR, -{hours}, GETUTCDATE())
        """

        agg_rows = db_helper.execute_query(agg_query) or []
        top_parameters = []
        if agg_rows:
            a = agg_rows[0]
            def add_top(name, count, avg, minv, maxv, unit):
                if count and count > 0:
                    top_parameters.append({
                        "ParameterName": name,
                        "ReadingCount": count,
                        "AvgValue": avg,
                        "MinValue": minv,
                        "MaxValue": maxv,
                        "Unit": unit
                    })
            add_top("KW_Total", a.get("KW_Total_Count"), a.get("KW_Total_Avg"), a.get("KW_Total_Min"), a.get("KW_Total_Max"), "kW")
            add_top("KWh_Total", a.get("KWh_Total_Count"), a.get("KWh_Total_Avg"), a.get("KWh_Total_Min"), a.get("KWh_Total_Max"), "kWh")
            add_top("VL1", a.get("VL1_Count"), a.get("VL1_Avg"), a.get("VL1_Min"), a.get("VL1_Max"), "V")
            add_top("IL1", a.get("IL1_Count"), a.get("IL1_Avg"), a.get("IL1_Min"), a.get("IL1_Max"), "A")

        return {
            "success": True,
            "analytics_period_hours": hours,
            "system_metrics": analytics[0] if analytics else {},
            "hourly_activity": hourly_activity or [],
            "top_parameters": top_parameters or [],
            "timestamp": datetime.utcnow()
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get system analytics error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve analytics")

@router.get("/health")
async def get_system_health(current_user: Dict = Depends(get_current_user)):
    """Get system health status"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        # Database connectivity check
        db_healthy = db_helper.test_connection()

        # Recent activity check
        recent_readings_query = """
        SELECT COUNT(*) as RecentReadings
        FROM app.Readings
        WHERE Timestamp >= DATEADD(MINUTE, -5, GETUTCDATE())
        """

        recent_data = db_helper.execute_query(recent_readings_query)
        recent_readings = recent_data[0]["RecentReadings"] if recent_data else 0

        # System status determination
        overall_status = "healthy"
        issues = []

        if not db_healthy:
            overall_status = "critical"
            issues.append("Database connection failed")

        if recent_readings == 0:
            overall_status = "warning" if overall_status == "healthy" else overall_status
            issues.append("No recent device readings")

        # Get component statuses
        components = {
            "database": "healthy" if db_healthy else "critical",
            "data_ingestion": "healthy" if recent_readings > 0 else "warning",
            "api_server": "healthy",  # This endpoint is responding
            "kepware_ingestion": "external"
        }

        # System uptime (simplified)
        uptime_query = "SELECT DATEDIFF(HOUR, sqlserver_start_time, GETUTCDATE()) as UptimeHours FROM sys.dm_os_sys_info"
        uptime_data = db_helper.execute_query(uptime_query)
        uptime_hours = uptime_data[0]["UptimeHours"] if uptime_data else 0

        return {
            "success": True,
            "overall_status": overall_status,
            "issues": issues,
            "components": components,
            "metrics": {
                "database_uptime_hours": uptime_hours,
                "recent_readings_last_5min": recent_readings,
                "active_devices": len(db_helper.execute_query("SELECT AnalyzerID FROM app.Analyzers WHERE IsActive = 1") or [])
            },
            "timestamp": datetime.utcnow()
        }

    except Exception as e:
        print(f"Get system health error: {e}")
        return {
            "success": False,
            "overall_status": "critical",
            "issues": ["Health check failed"],
            "error": str(e),
            "timestamp": datetime.utcnow()
        }
