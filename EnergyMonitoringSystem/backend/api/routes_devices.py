"""
Analyzer management API routes
Handles analyzer (PAC3220) CRUD operations and status monitoring.
"""

from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field

from backend.dal.database import db_helper
from backend.api.routes_auth import get_current_user

router = APIRouter()
security = HTTPBearer()

class DeviceCreateRequest(BaseModel):
    device_name: Optional[str] = None
    serial_number: Optional[str] = None
    ip_address: str
    modbus_unit_id: int = Field(1, ge=1, le=247)
    location: Optional[str] = None
    description: Optional[str] = None

class DeviceUpdateRequest(BaseModel):
    serial_number: Optional[str] = None
    ip_address: Optional[str] = None
    modbus_unit_id: Optional[int] = None
    location: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None

@router.get("/")
async def get_devices(current_user: Dict = Depends(get_current_user)):
    """Get all analyzers (filtered by user role)"""

    try:
        user_role = current_user.get("role", "User")

        if user_role == "Admin":
            # Admin can see all analyzers
            query = """
            SELECT a.AnalyzerID, a.UserID, a.SerialNumber, a.IPAddress,
                   a.ModbusID, a.Location, a.Description, a.IsActive, a.CreatedAt, a.UpdatedAt,
                   a.ConnectionStatus, a.LastSeen,
                   u.Username as OwnerUsername, u.FullName as OwnerFullName
            FROM app.Analyzers a
            LEFT JOIN app.Users u ON a.UserID = u.UserID
            ORDER BY a.CreatedAt DESC
            """
            devices = db_helper.execute_query(query)
        else:
            # Regular users can only see their own devices
            user_id = current_user.get("sub")
            query = """
            SELECT a.AnalyzerID, a.UserID, a.SerialNumber, a.IPAddress,
                   a.ModbusID, a.Location, a.Description, a.IsActive, a.CreatedAt, a.UpdatedAt,
                   a.ConnectionStatus, a.LastSeen,
                   u.Username as OwnerUsername, u.FullName as OwnerFullName
            FROM app.Analyzers a
            LEFT JOIN app.Users u ON a.UserID = u.UserID
            WHERE a.UserID = ?
            ORDER BY a.CreatedAt DESC
            """
            devices = db_helper.execute_query(query, (user_id,))

        return {
            "success": True,
            "count": len(devices) if devices else 0,
            "devices": devices or []
        }

    except Exception as e:
        print(f"Get devices error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve devices")

@router.get("/{device_id}")
async def get_device(device_id: int, current_user: Dict = Depends(get_current_user)):
    """Get specific analyzer details"""

    try:
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        query = """
        SELECT a.AnalyzerID, a.UserID, a.SerialNumber, a.IPAddress,
               a.ModbusID, a.Location, a.Description, a.IsActive, a.CreatedAt, a.UpdatedAt,
               a.ConnectionStatus, a.LastSeen,
               u.Username as OwnerUsername, u.FullName as OwnerFullName
        FROM app.Analyzers a
        LEFT JOIN app.Users u ON a.UserID = u.UserID
        WHERE a.AnalyzerID = ?
        """

        devices = db_helper.execute_query(query, (device_id,))

        if not devices or len(devices) == 0:
            raise HTTPException(status_code=404, detail="Device not found")

        device = devices[0]

        # Check permissions
        if user_role != "Admin" and str(device.get("UserID")) != str(user_id):
            raise HTTPException(status_code=403, detail="Access denied")

        # Compose status info from analyzer fields
        device["status_info"] = {
            "ConnectionStatus": device.get("ConnectionStatus"),
            "LastSeen": device.get("LastSeen")
        }

        return {
            "success": True,
            "device": device
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get device error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve device")

@router.post("/")
async def create_device(request: DeviceCreateRequest, current_user: Dict = Depends(get_current_user)):
    """Create a new analyzer"""

    try:
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        # Check if user can create devices
        if user_role != "Admin":
            raise HTTPException(status_code=403, detail="Only administrators can create devices")

        # Validate IP address format (basic check)
        import ipaddress
        try:
            ipaddress.ip_address(request.ip_address)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid IP address format")

        # Check if IP address is already in use
        existing_query = "SELECT AnalyzerID FROM app.Analyzers WHERE IPAddress = ? AND IsActive = 1"
        existing = db_helper.execute_query(existing_query, (request.ip_address,))

        if existing and len(existing) > 0:
            raise HTTPException(status_code=400, detail="IP address already in use")

        # Insert new analyzer
        insert_query = """
        INSERT INTO app.Analyzers (UserID, SerialNumber, IPAddress, ModbusID, Location, Description, IsActive)
        OUTPUT INSERTED.AnalyzerID AS AnalyzerID
        VALUES (?, ?, ?, ?, ?, ?, 1)
        """

        try:
            result = db_helper.execute_query(
                insert_query,
                (
                    user_id,
                    request.serial_number,
                    request.ip_address,
                    request.modbus_unit_id,
                    request.location,
                    request.description
                )
            )
        except Exception as e:
            msg = str(e).encode('ascii', 'replace').decode('ascii')
            # Handle duplicates or constraint violations more gracefully
            if "duplicate" in msg.lower() or "unique" in msg.lower() or "violation" in msg.lower():
                raise HTTPException(status_code=400, detail="Duplicate analyzer details (IP or Serial)")
            raise

        if result and len(result) > 0:
            device_id = int(result[0].get("AnalyzerID") or result[0].get("Expr1000") or result[0].get("SCOPE_IDENTITY"))
            if not device_id:
                # Fallback lookup by IP
                lookup = db_helper.execute_query("SELECT TOP 1 AnalyzerID FROM app.Analyzers WHERE IPAddress = ? ORDER BY CreatedAt DESC", (request.ip_address,))
                if lookup and len(lookup) > 0:
                    device_id = int(lookup[0]["AnalyzerID"])

            audit_query = (
                """
                INSERT INTO ops.AuditLogs (ActorUserID, Action, Details, AffectedAnalyzerID)
                VALUES (?, 'AnalyzerCreated', ?, ?)
                """
            )
            db_helper.execute_query(
                audit_query,
                (
                    int(current_user.get("sub")),
                    f"Analyzer {request.serial_number or device_id} created",
                    device_id,
                ),
            )

            device_row = db_helper.execute_query(
                """
                SELECT AnalyzerID, UserID, SerialNumber, IPAddress,
                       ModbusID, Location, Description, IsActive, CreatedAt, UpdatedAt,
                       ConnectionStatus, LastSeen
                FROM app.Analyzers WHERE AnalyzerID = ?
                """,
                (device_id,)
            )
            return {
                "success": True,
                "message": "Analyzer created successfully",
                "device_id": device_id,
                "device": (device_row[0] if device_row else None)
            }

        else:
            raise HTTPException(status_code=500, detail="Failed to create device")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Create device error: {e}")
        raise HTTPException(status_code=500, detail="Failed to create device")

@router.put("/{device_id}")
async def update_device(device_id: int, request: DeviceUpdateRequest, current_user: Dict = Depends(get_current_user)):
    """Update analyzer information"""

    try:
        user_role = current_user.get("role", "User")

        # Check permissions
        if user_role != "Admin":
            raise HTTPException(status_code=403, detail="Only administrators can update devices")

        # Check if device exists
        existing_query = "SELECT AnalyzerID, UserID FROM app.Analyzers WHERE AnalyzerID = ?"
        existing = db_helper.execute_query(existing_query, (device_id,))

        if not existing or len(existing) == 0:
            raise HTTPException(status_code=404, detail="Device not found")

        # Build update query dynamically
        update_fields = []
        params = []

        if request.serial_number is not None:
            update_fields.append("SerialNumber = ?")
            params.append(request.serial_number)

        if request.ip_address is not None:
            # Validate IP address
            import ipaddress
            try:
                ipaddress.ip_address(request.ip_address)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid IP address format")

            # Check if IP is already used by another analyzer
            ip_check_query = "SELECT AnalyzerID FROM app.Analyzers WHERE IPAddress = ? AND AnalyzerID != ? AND IsActive = 1"
            ip_check = db_helper.execute_query(ip_check_query, (request.ip_address, device_id))

            if ip_check and len(ip_check) > 0:
                raise HTTPException(status_code=400, detail="IP address already in use by another device")

            update_fields.append("IPAddress = ?")
            params.append(request.ip_address)

        if request.modbus_unit_id is not None:
            if not 1 <= request.modbus_unit_id <= 247:
                raise HTTPException(status_code=400, detail="Modbus unit ID must be between 1 and 247")
            update_fields.append("ModbusID = ?")
            params.append(request.modbus_unit_id)

        if request.location is not None:
            update_fields.append("Location = ?")
            params.append(request.location)

        if request.description is not None:
            update_fields.append("Description = ?")
            params.append(request.description)

        if request.is_active is not None:
            update_fields.append("IsActive = ?")
            params.append(1 if request.is_active else 0)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        # Add UpdatedAt timestamp
        update_fields.append("UpdatedAt = GETUTCDATE()")

        # Execute update
        update_query = f"UPDATE app.Analyzers SET {', '.join(update_fields)} WHERE AnalyzerID = ?"
        params.append(device_id)

        db_helper.execute_query(update_query, tuple(params))

        audit_query = (
            """
            INSERT INTO ops.AuditLogs (ActorUserID, Action, Details, AffectedAnalyzerID)
            VALUES (?, 'AnalyzerUpdated', ?, ?)
            """
        )
        db_helper.execute_query(
            audit_query,
            (
                int(current_user.get("sub")),
                f"Analyzer {device_id} updated",
                device_id,
            ),
        )

        return {
            "success": True,
            "message": "Device updated successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Update device error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update device")

@router.delete("/{device_id}")
async def delete_device(device_id: int, current_user: Dict = Depends(get_current_user)):
    """Delete an analyzer (soft delete by setting inactive)"""

    try:
        user_role = current_user.get("role", "User")

        # Check permissions
        if user_role != "Admin":
            raise HTTPException(status_code=403, detail="Only administrators can delete devices")

        # Check if device exists
        existing_query = "SELECT AnalyzerID FROM app.Analyzers WHERE AnalyzerID = ?"
        existing = db_helper.execute_query(existing_query, (device_id,))

        if not existing or len(existing) == 0:
            raise HTTPException(status_code=404, detail="Device not found")

        # Soft delete by setting inactive
        update_query = "UPDATE app.Analyzers SET IsActive = 0, UpdatedAt = GETUTCDATE() WHERE AnalyzerID = ?"
        db_helper.execute_query(update_query, (device_id,))

        audit_query = (
            """
            INSERT INTO ops.AuditLogs (ActorUserID, Action, Details, AffectedAnalyzerID)
            VALUES (?, 'AnalyzerDeleted', ?, ?)
            """
        )
        db_helper.execute_query(
            audit_query,
            (
                int(current_user.get("sub")),
                f"Analyzer {device_id} soft-deleted",
                device_id,
            ),
        )

        return {
            "success": True,
            "message": "Device deleted successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Delete device error: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete device")

@router.get("/{device_id}/status")
async def get_device_status(device_id: int, current_user: Dict = Depends(get_current_user)):
    """Get analyzer connectivity and operational status"""

    try:
        user_role = current_user.get("role", "User")
        user_id = current_user.get("sub")

        # Check permissions
        device_query = "SELECT UserID, SerialNumber, IPAddress FROM app.Analyzers WHERE AnalyzerID = ? AND IsActive = 1"
        devices = db_helper.execute_query(device_query, (device_id,))

        if not devices or len(devices) == 0:
            raise HTTPException(status_code=404, detail="Device not found")

        device = devices[0]

        if user_role != "Admin" and str(device.get("UserID")) != str(user_id):
            raise HTTPException(status_code=403, detail="Access denied")

        # Get recent readings count
        readings_query = """
        SELECT COUNT(*) as ReadingCount
        FROM app.Readings
        WHERE AnalyzerID = ? AND Timestamp >= DATEADD(HOUR, -24, GETUTCDATE())
        """

        readings = db_helper.execute_query(readings_query, (device_id,))
        reading_count = readings[0]["ReadingCount"] if readings else 0

        # Determine last_seen based on latest reading from Kepware ingestion
        hist_rows = db_helper.execute_query(
            """
            SELECT TOP 1 Timestamp FROM app.Readings WHERE AnalyzerID = ? ORDER BY Timestamp DESC
            """,
            (device_id,)
        )
        last_seen = (hist_rows[0]["Timestamp"] if hist_rows else None)
        current_time = datetime.utcnow()
        poll_interval = 60
        try:
            cfg = db_helper.execute_query("SELECT ConfigValue FROM ops.Configuration WHERE ConfigKey = 'system.poller_interval'")
            if cfg and cfg[0].get("ConfigValue"):
                poll_interval = int(cfg[0]["ConfigValue"]) or 60
        except Exception:
            pass
        # Fixed thresholds per requirements: ONLINE (0–30s), WARNING (30–120s), OFFLINE (>120s)
        if last_seen:
            seconds = (current_time - last_seen).total_seconds()
            if seconds <= 30:
                status = "Online"
            elif seconds <= 120:
                status = "Warning"
            else:
                status = "Offline"
        else:
            status = "Unknown"

        # Get latest reading timestamp
        latest_query = """
        SELECT TOP 1 Timestamp
        FROM app.Readings
        WHERE AnalyzerID = ?
        ORDER BY Timestamp DESC
        """

        latest = db_helper.execute_query(latest_query, (device_id,))
        latest_reading = latest[0]["Timestamp"] if latest else None

        return {
            "success": True,
            "device_id": device_id,
            "device_name": device.get("SerialNumber"),
            "status": status,
            "last_seen": last_seen,
            "latest_reading": latest_reading,
            "readings_last_24h": reading_count,
            "ip_address": device["IPAddress"]
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Get device status error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get device status")
