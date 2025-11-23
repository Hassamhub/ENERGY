"""
Admin API routes
Handles administrative functions like user management, recharging, and system control.
"""

from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from datetime import datetime

from backend.dal.database import db_helper
from backend.api.routes_auth import get_current_user

router = APIRouter()
security = HTTPBearer()

class RechargeRequest(BaseModel):
    amount: float
    reason: Optional[str] = None

class AdminDOEnqueueRequest(BaseModel):
    analyzer_id: int
    coil_address: int
    command: str
    notes: Optional[str] = None

class UserUpdateRequest(BaseModel):
    username: Optional[str] = None
    full_name: Optional[str] = None
    email: Optional[str] = None
    allocated_kwh: Optional[float] = None
    is_locked: Optional[bool] = None

class ConfigUpdateRequest(BaseModel):
    value: str

class CreateUserRequest(BaseModel):
    username: str
    password: str
    full_name: Optional[str] = None
    email: Optional[str] = None
    allocated_kwh: Optional[float] = 0.0
    assign_analyzer_ip: Optional[str] = None

@router.post("/do/enqueue")
async def admin_do_enqueue(request: AdminDOEnqueueRequest, current_user: Dict = Depends(get_current_user)):
    try:
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        if request.command not in ["ON", "OFF", "TOGGLE"]:
            raise HTTPException(status_code=400, detail="Invalid command. Must be ON, OFF, or TOGGLE")

        if not (0 <= int(request.coil_address) <= 9999):
            raise HTTPException(status_code=400, detail="Invalid coil address")

        cmd_params = {
            "@AnalyzerID": int(request.analyzer_id),
            "@CoilAddress": int(request.coil_address),
            "@Command": request.command,
            "@RequestedBy": current_user.get("user_id"),
            "@MaxRetries": 3,
            "@Notes": request.notes,
        }

        result = db_helper.execute_stored_procedure("app.sp_ControlDigitalOutput", cmd_params)
        if not result:
            result = [{
                "CommandID": None,
                "AnalyzerID": cmd_params["@AnalyzerID"],
                "CoilAddress": cmd_params["@CoilAddress"],
                "Command": cmd_params["@Command"],
                "RequestedBy": cmd_params["@RequestedBy"],
                "MaxRetries": cmd_params["@MaxRetries"],
            }]

        audit_params = {
            "@ActorUserID": current_user.get("user_id"),
            "@Action": "AdminDOEnqueue",
            "@Details": f"Admin {current_user.get('username')} enqueued {request.command} for analyzer {request.analyzer_id} coil {request.coil_address}",
            "@AffectedAnalyzerID": int(request.analyzer_id),
        }
        try:
            db_helper.execute_stored_procedure("ops.sp_LogAuditEvent", audit_params)
        except Exception:
            pass

        return {"success": True, "command": result[0]}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Admin DO enqueue error: {e}")
        raise HTTPException(status_code=500, detail="Failed to enqueue control command")

@router.get("/users")
async def get_all_users(current_user: Dict = Depends(get_current_user)):
    """Get all users (admin only)"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        if hasattr(db_helper, "test_connection") and not db_helper.test_connection():
            return {"success": True, "count": 0, "users": []}

        # Prefer stored procedure if available; otherwise fallback to direct query
        result = None
        try:
            result = db_helper.execute_stored_procedure("app.sp_GetAdminUsersOverview")
        except Exception:
            result = None

        if not result:
            query = (
                """
                SELECT UserID, Username, FullName, Email, Role,
                       AllocatedKWh, UsedKWh, RemainingKWh, IsLocked,
                       CreatedAt, LastLoginAt
                FROM app.Users
                WHERE ISNULL(IsActive, 1) = 1
                ORDER BY CreatedAt DESC
                """
            )
            result = db_helper.execute_query(query) or []

        return {
            "success": True,
            "count": len(result) if result else 0,
            "users": result or []
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get all users error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve users")

@router.post("/users")
async def create_user(request: CreateUserRequest, current_user: Dict = Depends(get_current_user)):
    try:
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        if not request.username or not request.password:
            raise HTTPException(status_code=400, detail="Username and password are required")

        existing = db_helper.execute_query(
            "SELECT UserID FROM app.Users WHERE Username = ?",
            (request.username,)
        )
        if existing:
            raise HTTPException(status_code=400, detail="Username already exists")

        if request.email:
            email_exists = db_helper.execute_query(
                "SELECT UserID FROM app.Users WHERE Email = ?",
                (request.email,)
            )
            if email_exists:
                raise HTTPException(status_code=400, detail="Email already in use")

        alloc = float(request.allocated_kwh or 0.0)
        insert_q = (
            """
            INSERT INTO app.Users (Username, FullName, Email, Password, Role, AllocatedKWh, UsedKWh, IsLocked, IsActive)
            OUTPUT INSERTED.UserID AS UserID
            VALUES (?, ?, ?, ?, 'USER', ?, 0, 0, 1)
            """
        )
        rows = db_helper.execute_query(
            insert_q,
            (
                request.username,
                request.full_name,
                request.email,
                request.password,
                alloc,
            )
        ) or []
        if not rows:
            raise HTTPException(status_code=500, detail="Failed to create user")
        new_id = rows[0]["UserID"] if isinstance(rows[0], dict) else rows[0]

        if request.assign_analyzer_ip:
            try:
                db_helper.execute_query(
                    "UPDATE app.Analyzers SET UserID = ?, UpdatedAt = GETUTCDATE() WHERE IPAddress = ?",
                    (new_id, request.assign_analyzer_ip)
                )
            except Exception:
                pass

        return {
            "success": True,
            "user": {
                "UserID": new_id,
                "Username": request.username,
                "FullName": request.full_name,
                "Email": request.email,
                "Role": "User",
                "AllocatedKWh": alloc,
                "RemainingKWh": alloc,
                "IsLocked": False,
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Create user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to create user")

@router.get("/users/{user_id}")
async def get_user_details(user_id: int, current_user: Dict = Depends(get_current_user)):
    """Get detailed user information"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        # Get user details
        user_query = """
        SELECT UserID, Username, FullName, Email, Role, AllocatedKWh,
               UsedKWh, RemainingKWh, IsLocked, CreatedAt, LastLoginAt
        FROM app.Users
        WHERE UserID = ?
        """

        users = db_helper.execute_query(user_query, (user_id,))

        if not users or len(users) == 0:
            raise HTTPException(status_code=404, detail="User not found")

        user = users[0]

        # Get user's analyzers
        analyzers_query = """
        SELECT AnalyzerID, SerialNumber, IPAddress, IsActive, LastSeen, ConnectionStatus
        FROM app.Analyzers
        WHERE UserID = ? AND IsActive = 1
        ORDER BY CreatedAt DESC
        """

        analyzers = db_helper.execute_query(analyzers_query, (user_id,))

        # Get recent allocations
        allocations_query = """
        SELECT TOP 5 AllocationID, AmountKWh, Status, RequestedAt, ProcessedAt
        FROM app.Allocations
        WHERE UserID = ?
        ORDER BY RequestedAt DESC
        """

        allocations = db_helper.execute_query(allocations_query, (user_id,))

        user["analyzers"] = analyzers or []
        user["recent_allocations"] = allocations or []
        user["device_count"] = len(analyzers) if analyzers else 0

        return {
            "success": True,
            "user": user
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get user details error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve user details")

@router.post("/users/{user_id}/recharge")
async def recharge_user(user_id: int, request: RechargeRequest, current_user: Dict = Depends(get_current_user)):
    """Recharge user's energy allocation"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        # Validate request
        if request.amount <= 0:
            raise HTTPException(status_code=400, detail="Recharge amount must be positive")

        # Check if user exists
        user_query = "SELECT UserID, Username FROM app.Users WHERE UserID = ?"
        users = db_helper.execute_query(user_query, (user_id,))

        if not users or len(users) == 0:
            raise HTTPException(status_code=404, detail="User not found")

        # Execute recharge stored procedure
        params = {
            "@UserID": user_id,
            "@AddKWh": request.amount,
            "@AdminUserID": current_user.get("user_id"),
            "@Reference": f"ADMIN_RECHARGE_{user_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "@Notes": request.reason or f"Admin recharge by {current_user.get('username')}"
        }

        result = db_helper.execute_stored_procedure("app.sp_RechargeUser", params)

        if result:
            return {
                "success": True,
                "message": f"Successfully recharged {request.amount} kWh for user {users[0]['Username']}",
                "new_allocation": result[0] if result else None
            }
        else:
            raise HTTPException(status_code=500, detail="Recharge failed")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Recharge user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to recharge user")

@router.get("/config")
async def get_config(current_user: Dict = Depends(get_current_user)):
    try:
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")
        rows = db_helper.execute_query("SELECT ConfigKey, ConfigValue, UpdatedAt FROM ops.Configuration ORDER BY ConfigKey")
        return {"success": True, "count": len(rows) if rows else 0, "config": rows or []}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Get config error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve configuration")

@router.put("/config/{key}")
async def update_config(key: str, req: ConfigUpdateRequest, current_user: Dict = Depends(get_current_user)):
    try:
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")
        exists = db_helper.execute_query("SELECT ConfigID FROM ops.Configuration WHERE ConfigKey = ?", (key,))
        if exists:
            db_helper.execute_query(
                "UPDATE ops.Configuration SET ConfigValue = ?, UpdatedAt = GETUTCDATE(), UpdatedBy = ? WHERE ConfigKey = ?",
                (req.value, current_user.get("sub"), key)
            )
        else:
            db_helper.execute_query(
                "INSERT INTO ops.Configuration (ConfigKey, ConfigValue, UpdatedBy) VALUES (?, ?, ?)",
                (key, req.value, current_user.get("sub"))
            )
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Update config error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update configuration")

# Legacy endpoints removed: use /api/admin/do/enqueue only


@router.put("/users/{user_id}")
async def update_user(user_id: int, request: UserUpdateRequest, current_user: Dict = Depends(get_current_user)):
    """Update user information"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        # Check if user exists
        user_query = "SELECT UserID FROM app.Users WHERE UserID = ?"
        users = db_helper.execute_query(user_query, (user_id,))

        if not users or len(users) == 0:
            raise HTTPException(status_code=404, detail="User not found")

        # Build update query
        update_fields = []
        params = []

        if request.username is not None:
            # Check if username is already taken
            username_check = db_helper.execute_query(
                "SELECT UserID FROM app.Users WHERE Username = ? AND UserID != ?",
                (request.username, user_id)
            )
            if username_check and len(username_check) > 0:
                raise HTTPException(status_code=400, detail="Username already taken")

            update_fields.append("Username = ?")
            params.append(request.username)

        if request.full_name is not None:
            update_fields.append("FullName = ?")
            params.append(request.full_name)

        if request.email is not None:
            # Check if email is already taken
            email_check = db_helper.execute_query(
                "SELECT UserID FROM app.Users WHERE Email = ? AND UserID != ?",
                (request.email, user_id)
            )
            if email_check and len(email_check) > 0:
                raise HTTPException(status_code=400, detail="Email already in use")

            update_fields.append("Email = ?")
            params.append(request.email)

        if request.allocated_kwh is not None:
            update_fields.append("AllocatedKWh = ?")
            params.append(request.allocated_kwh)

        if request.is_locked is not None:
            update_fields.append("IsLocked = ?")
            params.append(1 if request.is_locked else 0)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        # Execute update
        update_query = f"UPDATE app.Users SET {', '.join(update_fields)}, UpdatedAt = GETUTCDATE() WHERE UserID = ?"
        params.append(user_id)

        db_helper.execute_query(update_query, tuple(params))

        # Audit log
        audit_params = {
            "@Action": "UserUpdated",
            "@Details": f"User {user_id} updated by admin {current_user.get('username')}"
        }
        db_helper.execute_stored_procedure("ops.sp_LogAuditEvent", audit_params)

        return {
            "success": True,
            "message": "User updated successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Update user error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update user")

@router.get("/dashboard")
async def get_admin_dashboard(current_user: Dict = Depends(get_current_user)):
    """Get admin dashboard overview"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        if hasattr(db_helper, "test_connection") and not db_helper.test_connection():
            return {
                "success": True,
                "dashboard": {
                    "total_users": 0,
                    "total_analyzers": 0,
                    "online_analyzers": 0,
                    "unread_alerts": 0,
                    "total_allocated_kwh": 0,
                    "total_used_kwh": 0,
                    "readings_last_24h": 0
                },
                "timestamp": datetime.utcnow()
            }

        # Get dashboard statistics
        dashboard_query = """
        SELECT
            (SELECT COUNT(*) FROM app.Users WHERE IsActive = 1) as total_users,
            (SELECT COUNT(*) FROM app.Analyzers WHERE IsActive = 1) as total_analyzers,
            (SELECT COUNT(*) FROM app.Analyzers WHERE ConnectionStatus = 'ONLINE' AND IsActive = 1) as online_analyzers,
            (SELECT COUNT(*) FROM app.Alerts WHERE IsActive = 1 AND IsRead = 0) as unread_alerts,
            (SELECT SUM(AllocatedKWh) FROM app.Users WHERE IsActive = 1) as total_allocated_kwh,
            (SELECT SUM(UsedKWh) FROM app.Users WHERE IsActive = 1) as total_used_kwh,
            (SELECT COUNT(*) FROM app.Readings WHERE Timestamp >= DATEADD(HOUR, -24, GETUTCDATE())) as readings_last_24h
        """

        dashboard = db_helper.execute_query(dashboard_query)

        return {
            "success": True,
            "dashboard": dashboard[0] if dashboard else {},
            "timestamp": datetime.utcnow()
        }

    except Exception as e:
        print(f"Get admin dashboard error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve dashboard data")

@router.get("/events")
async def get_system_events(
    limit: int = 50,
    hours: int = 24,
    current_user: Dict = Depends(get_current_user)
):
    """Get system events and alerts"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        if hasattr(db_helper, "test_connection") and not db_helper.test_connection():
            return {"success": True, "count": 0, "events": []}

        query = """
        SELECT TOP (?) e.EventID, e.UserID, e.AnalyzerID, e.Level, e.EventType,
               e.Message, e.MetaData, e.Timestamp, 0 as IsRead,
               u.Username, a.SerialNumber as AnalyzerName
        FROM ops.Events e
        LEFT JOIN app.Users u ON e.UserID = u.UserID
        LEFT JOIN app.Analyzers a ON e.AnalyzerID = a.AnalyzerID
        WHERE e.Timestamp >= DATEADD(HOUR, -?, GETUTCDATE())
        ORDER BY e.Timestamp DESC
        """

        events = db_helper.execute_query(query, (limit, hours))

        return {
            "success": True,
            "count": len(events) if events else 0,
            "events": events or []
        }

    except Exception as e:
        print(f"Get system events error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve system events")

@router.post("/events/{event_id}/mark-read")
async def mark_event_read(event_id: int, current_user: Dict = Depends(get_current_user)):
    """Mark an event as read"""
    try:
        # Check admin permission
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        try:
            update_query = "UPDATE ops.Events SET IsRead = 1 WHERE EventID = ?"
            db_helper.execute_query(update_query, (event_id,))
        except Exception:
            # Schema may not support IsRead; treat as no-op
            pass

        return {
            "success": True,
            "message": "Event marked as read"
        }

    except Exception as e:
        print(f"Mark event read error: {e}")
        raise HTTPException(status_code=500, detail="Failed to mark event as read")

# Legacy coil enqueue removed
