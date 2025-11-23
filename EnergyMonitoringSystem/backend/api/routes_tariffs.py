"""
Tariff management API routes
CRUD for tariffs with validity windows and activation toggles.
"""

from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer
from typing import Optional, Dict
from pydantic import BaseModel, Field
from datetime import datetime

from backend.dal.database import db_helper
from backend.api.routes_auth import get_current_user

router = APIRouter()
security = HTTPBearer()

class TariffCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    grid_rate: float = Field(..., ge=0)
    generator_rate: float = Field(..., ge=0)
    effective_from: Optional[datetime] = None
    effective_to: Optional[datetime] = None
    is_active: bool = True

class TariffUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    grid_rate: Optional[float] = Field(None, ge=0)
    generator_rate: Optional[float] = Field(None, ge=0)
    effective_from: Optional[datetime] = None
    effective_to: Optional[datetime] = None
    is_active: Optional[bool] = None

@router.get("/")
async def list_tariffs(current_user: Dict = Depends(get_current_user)):
    try:
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        rows = db_helper.execute_query("""
            SELECT TariffID, Name, Description, GridRate, GeneratorRate,
                   IsActive, EffectiveFrom, EffectiveTo, CreatedAt, UpdatedAt
            FROM app.Tariffs
            ORDER BY ISNULL(EffectiveTo, '9999-12-31') DESC, EffectiveFrom DESC
        """)
        return {"success": True, "count": len(rows) if rows else 0, "tariffs": rows or []}
    except HTTPException:
        raise
    except Exception as e:
        print(f"List tariffs error: {e}")
        raise HTTPException(status_code=500, detail="Failed to list tariffs")

@router.post("/")
async def create_tariff(req: TariffCreateRequest, current_user: Dict = Depends(get_current_user)):
    try:
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        # Validate effective dates
        if req.effective_from and req.effective_to and req.effective_to <= req.effective_from:
            raise HTTPException(status_code=400, detail="effective_to must be after effective_from")

        sql = """
        INSERT INTO app.Tariffs (Name, Description, GridRate, GeneratorRate, IsActive, EffectiveFrom, EffectiveTo)
        VALUES (?, ?, ?, ?, ?, ISNULL(?, GETUTCDATE()), ?);
        SELECT SCOPE_IDENTITY() AS TariffID;
        """
        rows = db_helper.execute_query(sql, (
            req.name, req.description, req.grid_rate, req.generator_rate,
            1 if req.is_active else 0, req.effective_from, req.effective_to
        ))
        tariff_id = int(rows[0]["TariffID"]) if rows else None

        # If activating this tariff, ensure others are deactivated
        if req.is_active and tariff_id is not None:
            db_helper.execute_query(
                "UPDATE app.Tariffs SET IsActive = 0, UpdatedAt = GETUTCDATE() WHERE TariffID <> ? AND IsActive = 1",
                (tariff_id,)
            )

        # Audit
        db_helper.execute_stored_procedure("ops.sp_LogAuditEvent", {
            "@Action": "TariffCreated",
            "@Details": f"Tariff {req.name} created by {current_user['username']}"
        })

        return {"success": True, "tariff_id": tariff_id}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Create tariff error: {e}")
        raise HTTPException(status_code=500, detail="Failed to create tariff")

@router.put("/{tariff_id}")
async def update_tariff(tariff_id: int, req: TariffUpdateRequest, current_user: Dict = Depends(get_current_user)):
    try:
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")

        exists = db_helper.execute_query("SELECT TariffID FROM app.Tariffs WHERE TariffID = ?", (tariff_id,))
        if not exists:
            raise HTTPException(status_code=404, detail="Tariff not found")

        # Validate effective dates if both present
        if req.effective_from and req.effective_to and req.effective_to <= req.effective_from:
            raise HTTPException(status_code=400, detail="effective_to must be after effective_from")

        fields = []
        params = []
        if req.name is not None:
            fields.append("Name = ?"); params.append(req.name)
        if req.description is not None:
            fields.append("Description = ?"); params.append(req.description)
        if req.grid_rate is not None:
            fields.append("GridRate = ?"); params.append(req.grid_rate)
        if req.generator_rate is not None:
            fields.append("GeneratorRate = ?"); params.append(req.generator_rate)
        if req.effective_from is not None:
            fields.append("EffectiveFrom = ?"); params.append(req.effective_from)
        if req.effective_to is not None:
            fields.append("EffectiveTo = ?"); params.append(req.effective_to)
        if req.is_active is not None:
            fields.append("IsActive = ?"); params.append(1 if req.is_active else 0)
        if not fields:
            raise HTTPException(status_code=400, detail="No fields to update")
        fields.append("UpdatedAt = GETUTCDATE()")
        sql = f"UPDATE app.Tariffs SET {', '.join(fields)} WHERE TariffID = ?"
        params.append(tariff_id)
        db_helper.execute_query(sql, tuple(params))

        # Enforce single active tariff if turning this one active
        if req.is_active:
            db_helper.execute_query(
                "UPDATE app.Tariffs SET IsActive = 0, UpdatedAt = GETUTCDATE() WHERE TariffID <> ? AND IsActive = 1",
                (tariff_id,)
            )

        db_helper.execute_stored_procedure("ops.sp_LogAuditEvent", {
            "@Action": "TariffUpdated",
            "@Details": f"Tariff {tariff_id} updated by {current_user['username']}"
        })
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Update tariff error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update tariff")

@router.delete("/{tariff_id}")
async def delete_tariff(tariff_id: int, current_user: Dict = Depends(get_current_user)):
    try:
        if current_user.get("role") != "Admin":
            raise HTTPException(status_code=403, detail="Admin access required")
        exists = db_helper.execute_query("SELECT TariffID FROM app.Tariffs WHERE TariffID = ?", (tariff_id,))
        if not exists:
            raise HTTPException(status_code=404, detail="Tariff not found")
        db_helper.execute_query("UPDATE app.Tariffs SET IsActive = 0, EffectiveTo = ISNULL(EffectiveTo, GETUTCDATE()), UpdatedAt = GETUTCDATE() WHERE TariffID = ?", (tariff_id,))
        db_helper.execute_stored_procedure("ops.sp_LogAuditEvent", {
            "@Action": "TariffDisabled",
            "@Details": f"Tariff {tariff_id} disabled by {current_user['username']}"
        })
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Delete tariff error: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete tariff")
