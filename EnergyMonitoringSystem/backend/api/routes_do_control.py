"""
Digital Output Control API routes
Handles breaker control and digital output management.
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import asyncio

from backend.dal.database import db_helper
from backend.api.routes_auth import get_current_user
 

router = APIRouter()
security = HTTPBearer()

class DOControlRequest(BaseModel):
    coil_address: int
    command: str  # ON, OFF, TOGGLE
    max_retries: Optional[int] = 3
    notes: Optional[str] = None

class BreakerConfigRequest(BaseModel):
    breaker_coil_address: Optional[int] = None
    breaker_enabled: Optional[bool] = None
    auto_disconnect_enabled: Optional[bool] = None

@router.post("/{analyzer_id}/control")
async def control_digital_output(
    analyzer_id: int,
    request: DOControlRequest,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user)
):
    """Control digital output (breaker) for an analyzer"""
    try:
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        # Validate permissions (admin or device owner)
        if user_role != "Admin":
            analyzer_query = "SELECT UserID FROM app.Analyzers WHERE AnalyzerID = ? AND IsActive = 1"
            analyzers = db_helper.execute_query(analyzer_query, (analyzer_id,))
            if not analyzers or str(analyzers[0]["UserID"]) != str(user_id):
                raise HTTPException(status_code=403, detail="Access denied")

        # Validate command
        if request.command not in ["ON", "OFF", "TOGGLE"]:
            raise HTTPException(status_code=400, detail="Invalid command. Must be ON, OFF, or TOGGLE")

        # Validate coil address
        if not (0 <= request.coil_address <= 9999):
            raise HTTPException(status_code=400, detail="Invalid coil address")

        # Create control command in database
        command_params = {
            "@AnalyzerID": analyzer_id,
            "@CoilAddress": request.coil_address,
            "@Command": request.command,
            "@RequestedBy": user_id,
            "@MaxRetries": request.max_retries,
            "@Notes": request.notes
        }

        command_result = db_helper.execute_stored_procedure("app.sp_ControlDigitalOutput", command_params)

        if not command_result:
            raise HTTPException(status_code=500, detail="Failed to create control command")

        command_id = command_result[0].get("CommandID")

        # Get analyzer details for Modbus connection
        analyzer_details = command_result[0]

        # Worker is the only executor; do not execute in API background

        return {
            "success": True,
            "message": "Digital output control initiated",
            "command_id": command_id,
            "analyzer_id": analyzer_id,
            "coil_address": request.coil_address,
            "command": request.command
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"DO control error: {e}")
        raise HTTPException(status_code=500, detail="Failed to control digital output")

@router.get("/{analyzer_id}/status")
async def get_do_status(analyzer_id: int, current_user: Dict = Depends(get_current_user)):
    """Get digital output status for an analyzer"""
    try:
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        # Check permissions
        if user_role != "Admin":
            analyzer_query = "SELECT UserID FROM app.Analyzers WHERE AnalyzerID = ? AND IsActive = 1"
            analyzers = db_helper.execute_query(analyzer_query, (analyzer_id,))
            if not analyzers or str(analyzers[0]["UserID"]) != str(user_id):
                raise HTTPException(status_code=403, detail="Access denied")

        # Get current DO status
        status_query = "SELECT CoilAddress, State, LastUpdated, UpdateSource FROM app.DigitalOutputStatus WHERE AnalyzerID = ?"
        status_result = db_helper.execute_query(status_query, (analyzer_id,))

        # Get breaker configuration
        breaker_query = "SELECT BreakerCoilAddress, BreakerEnabled, AutoDisconnectEnabled, LastBreakerState, BreakerLastChanged FROM app.Analyzers WHERE AnalyzerID = ?"
        breaker_result = db_helper.execute_query(breaker_query, (analyzer_id,))

        return {
            "success": True,
            "analyzer_id": analyzer_id,
            "digital_outputs": status_result or [],
            "breaker_config": breaker_result[0] if breaker_result else None
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"DO status error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get digital output status")

@router.put("/{analyzer_id}/breaker-config")
async def configure_breaker(
    analyzer_id: int,
    request: BreakerConfigRequest,
    current_user: Dict = Depends(get_current_user)
):
    """Configure breaker settings for an analyzer"""
    try:
        user_role = current_user.get("role", "User")

        # Only admins can configure breakers
        if user_role != "Admin":
            raise HTTPException(status_code=403, detail="Only administrators can configure breaker settings")

        # Validate coil address if provided
        if request.breaker_coil_address is not None and not (0 <= request.breaker_coil_address <= 9999):
            raise HTTPException(status_code=400, detail="Invalid breaker coil address")

        # Build update query
        update_fields = []
        params = []

        if request.breaker_coil_address is not None:
            update_fields.append("BreakerCoilAddress = ?")
            params.append(request.breaker_coil_address)

        if request.breaker_enabled is not None:
            update_fields.append("BreakerEnabled = ?")
            params.append(1 if request.breaker_enabled else 0)

        if request.auto_disconnect_enabled is not None:
            update_fields.append("AutoDisconnectEnabled = ?")
            params.append(1 if request.auto_disconnect_enabled else 0)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No configuration fields provided")

        # Execute update
        update_query = f"UPDATE app.Analyzers SET {', '.join(update_fields)}, UpdatedAt = GETUTCDATE() WHERE AnalyzerID = ?"
        params.append(analyzer_id)

        db_helper.execute_query(update_query, tuple(params))

        return {
            "success": True,
            "message": "Breaker configuration updated successfully",
            "analyzer_id": analyzer_id
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Breaker config error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update breaker configuration")

@router.get("/commands")
async def get_do_commands(
    status: Optional[str] = None,
    limit: int = 50,
    current_user: Dict = Depends(get_current_user)
):
    """Get digital output commands history"""
    try:
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        # Build query based on role
        if user_role == "Admin":
            query = """
            SELECT TOP (?) c.CommandID, c.AnalyzerID, c.CoilAddress, c.Command,
                   c.RequestedAt, c.ExecutedAt, c.ExecutionResult, c.ErrorMessage, c.Notes,
                   u.Username as RequestedByUsername, a.SerialNumber
            FROM app.DigitalOutputCommands c
            JOIN app.Users u ON c.RequestedBy = u.UserID
            LEFT JOIN app.Analyzers a ON c.AnalyzerID = a.AnalyzerID
            """
            params = [limit]
            if status:
                query += " WHERE c.ExecutionResult = ?"
                params.append(status)
            query += " ORDER BY c.RequestedAt DESC"
            params = tuple(params)
        else:
            query = """
            SELECT TOP (?) c.CommandID, c.AnalyzerID, c.CoilAddress, c.Command,
                   c.RequestedAt, c.ExecutedAt, c.ExecutionResult, c.ErrorMessage, c.Notes,
                   u.Username as RequestedByUsername, a.SerialNumber
            FROM app.DigitalOutputCommands c
            JOIN app.Users u ON c.RequestedBy = u.UserID
            LEFT JOIN app.Analyzers a ON c.AnalyzerID = a.AnalyzerID
            WHERE c.RequestedBy = ?
            """
            params = [limit, user_id]
            if status:
                query += " AND c.ExecutionResult = ?"
                params.append(status)
            query += " ORDER BY c.RequestedAt DESC"
            params = tuple(params)

        commands = db_helper.execute_query(query, params)

        return {
            "success": True,
            "count": len(commands) if commands else 0,
            "commands": commands or []
        }

    except Exception as e:
        print(f"Get DO commands error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve digital output commands")

# Background execution removed; worker updates results via app.sp_UpdateDigitalOutputResult
