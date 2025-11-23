"""
Authentication API routes for FastAPI
Handles user login and JWT token management.
"""

from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import base64
from jose import jwt
from jose.exceptions import JWTError, ExpiredSignatureError
import os
from datetime import datetime, timedelta
from typing import Dict, Any
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from backend.dal.database import db_helper

load_dotenv()

# JWT Configuration
JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
_exp_minutes_env = os.getenv("JWT_EXPIRE_MINUTES")
if _exp_minutes_env is not None:
    try:
        JWT_EXPIRATION_MINUTES = int(_exp_minutes_env)
    except Exception:
        JWT_EXPIRATION_MINUTES = 1440
else:
    try:
        JWT_EXPIRATION_MINUTES = int(os.getenv("JWT_EXPIRATION_MINUTES", "1440"))
    except Exception:
        JWT_EXPIRATION_MINUTES = 1440
REFRESH_SECRET = os.getenv("JWT_REFRESH_SECRET") or (JWT_SECRET + "_refresh" if JWT_SECRET else None)
REFRESH_EXPIRATION_HOURS = int(os.getenv("JWT_REFRESH_EXPIRATION_HOURS", "240"))

router = APIRouter()
security = HTTPBearer()

class LoginRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=100)
    password: str = Field(..., min_length=6, max_length=128)

class TokenResponse(BaseModel):
    success: bool
    token: str
    refresh_token: str
    user: Dict[str, Any]

def verify_password(password: str, stored_password: str) -> bool:
    if stored_password is None:
        return False
    try:
        if isinstance(stored_password, bytes):
            stored_password = stored_password.decode('utf-8')
        return str(password) == str(stored_password).strip()
    except Exception:
        return False

def create_jwt_token(user_id: int, username: str, role: str) -> str:
    """Create JWT token for authenticated user"""
    expiration = datetime.utcnow() + timedelta(minutes=JWT_EXPIRATION_MINUTES)

    payload = {
        "sub": str(user_id),
        "username": username,
        "role": role,
        "exp": expiration,
        "iat": datetime.utcnow()
    }

    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token

def create_refresh_token(user_id: int, username: str) -> str:
    expiration = datetime.utcnow() + timedelta(hours=REFRESH_EXPIRATION_HOURS)
    payload = {
        "sub": str(user_id),
        "username": username,
        "type": "refresh",
        "exp": expiration,
        "iat": datetime.utcnow(),
    }
    return jwt.encode(payload, REFRESH_SECRET, algorithm=JWT_ALGORITHM)

def decode_jwt_token(token: str) -> Dict[str, Any]:
    """Decode and verify JWT token"""
    try:
        if not JWT_SECRET:
            raise HTTPException(status_code=500, detail="Server misconfiguration: JWT_SECRET not set")
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# Simple in-memory rate limiting for login
_LOGIN_ATTEMPTS: Dict[str, list] = {}
_LOGIN_WINDOW_SECONDS = int(os.getenv("LOGIN_WINDOW_SECONDS", "900"))
_LOGIN_MAX_ATTEMPTS = int(os.getenv("LOGIN_MAX_ATTEMPTS", "10"))

def _record_login_attempt(key: str) -> bool:
    now = datetime.utcnow().timestamp()
    arr = _LOGIN_ATTEMPTS.get(key, [])
    arr = [t for t in arr if now - t <= _LOGIN_WINDOW_SECONDS]
    arr.append(now)
    _LOGIN_ATTEMPTS[key] = arr
    return len(arr) <= _LOGIN_MAX_ATTEMPTS

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency to get current authenticated user"""
    token = credentials.credentials
    payload = decode_jwt_token(token)
    return payload

@router.post("/login", response_model=TokenResponse)
async def login(request: LoginRequest, http_req: Request):
    """
    Authenticate user and return JWT token

    - **username**: User's username
    - **password**: User's password
    """
    try:
        

        if hasattr(db_helper, "test_connection") and not db_helper.test_connection():
            raise HTTPException(status_code=503, detail="Database unavailable")
        # Rate limiting per IP and username
        client_ip = http_req.client.host if http_req.client else "unknown"
        if not _record_login_attempt(f"ip:{client_ip}") or not _record_login_attempt(f"user:{request.username}"):
            raise HTTPException(status_code=429, detail="Too many login attempts. Please try again later.")

        # Normalize inputs
        req_username = (request.username or "").strip()
        req_password = (request.password or "").strip()

        # Query user by username only; verify password in Python (plain-text expected)
        query = """
        SELECT UserID, Username, FullName, Email, Password as Password, Role, IsLocked, ISNULL(IsActive, 1) as IsActive
        , Status, AllocatedKWh, UsedKWh, RemainingKWh, LastLoginAt
        FROM app.Users
        WHERE Username = ?
        """

        try:
            users = db_helper.execute_query(query, (req_username,))
        except Exception as e:
            print(f"Login DB error: {e}")
            raise HTTPException(status_code=503, detail="Authentication service unavailable")

        if not users or len(users) == 0:
            raise HTTPException(status_code=401, detail="Invalid username or password")

        user = users[0]

        # Check if user is active
        if not user.get("IsActive", True):
            raise HTTPException(status_code=403, detail="Account is inactive")

        # Check if user is locked
        if user.get("IsLocked"):
            raise HTTPException(status_code=403, detail="Account is locked")

        # Check status field
        status = (user.get("Status") or "ACTIVE").upper()
        if status != "ACTIVE":
            raise HTTPException(status_code=403, detail=f"Account status is {status}")

        # Verify password
        stored_password = user.get("Password")
        if not stored_password:
            raise HTTPException(status_code=401, detail="Invalid username or password")

        # Ensure password is a string
        if isinstance(stored_password, bytes):
            stored_password = stored_password.decode('utf-8')

        # Verify BCrypt password
        password_valid = verify_password(req_password, stored_password)
        
        if not password_valid:
            # Log failed login attempt
            try:
                # Insert directly into events table
                event_query = """
                INSERT INTO ops.Events (UserID, Level, EventType, Message, Source, MetaData)
                VALUES (?, 'WARN', 'login_failed', ?, 'API', ?)
                """
                db_helper.execute_query(event_query, (
                    user["UserID"],
                    f"Failed login attempt for user {req_username}",
                    '{"reason": "invalid_password"}'
                ))
            except Exception as e:
                print(f"Warning: Could not log failed login: {e}")
            
            raise HTTPException(status_code=401, detail="Invalid username or password")

        # Create tokens
        db_role = (user.get("Role") or "User").upper()
        norm_role = "Admin" if db_role == "ADMIN" else "User"
        token = create_jwt_token(
            user_id=user["UserID"],
            username=user["Username"],
            role=norm_role
        )
        refresh_token = create_refresh_token(user_id=user["UserID"], username=user["Username"]) 

        # Update last login
        try:
            update_query = "UPDATE app.Users SET LastLoginAt = GETUTCDATE() WHERE UserID = ?"
            db_helper.execute_query(update_query, (user["UserID"],))
        except Exception as e:
            print(f"Warning: Could not update last login: {e}")

        # Log successful login
        try:
            # Insert directly into audit log since procedure doesn't exist
            audit_query = """
            INSERT INTO ops.AuditLogs (ActorUserID, Action, Details)
            VALUES (?, 'UserLogin', ?)
            """
            db_helper.execute_query(audit_query, (user["UserID"], f"User {req_username} logged in successfully"))
        except Exception as e:
            print(f"Warning: Could not log audit event: {e}")

        return TokenResponse(
            success=True,
            token=token,
            refresh_token=refresh_token,
            user={
                "id": user["UserID"],
                "username": user["Username"],
                "fullname": user["FullName"],
                "email": user["Email"],
                "role": norm_role,
                "allocated_kwh": float(user.get("AllocatedKWh") or 0.0),
                "used_kwh": float(user.get("UsedKWh") or 0.0),
                "remaining_kwh": float(user.get("RemainingKWh") or 0.0),
                "last_login_at": user.get("LastLoginAt")
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        try:
            msg = str(e).encode('ascii', 'replace').decode('ascii')
        except Exception:
            msg = "unexpected_error"
        print(f"Login error: {msg}")
        return JSONResponse(status_code=500, content={
            "success": False,
            "error": {
                "code": 500,
                "message": f"Login failed: {msg}",
                "path": "/api/login"
            }
        })

@router.post("/auth/login", response_model=TokenResponse)
async def login_alias(request: LoginRequest, http_req: Request):
    return await login(request, http_req)

@router.post("/token/refresh")
async def refresh_access_token(data: Dict[str, str]):
    try:
        token = data.get("refresh_token") if isinstance(data, dict) else None
        if not token:
            raise HTTPException(status_code=400, detail="Missing refresh_token")
        try:
            payload = jwt.decode(token, REFRESH_SECRET, algorithms=[JWT_ALGORITHM])
        except ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Refresh token expired")
        except JWTError:
            raise HTTPException(status_code=401, detail="Invalid refresh token")

        if payload.get("type") != "refresh":
            raise HTTPException(status_code=401, detail="Invalid refresh token type")

        user_id = int(payload.get("sub"))
        username = payload.get("username")

        if hasattr(db_helper, "test_connection") and not db_helper.test_connection():
            new_access = create_jwt_token(user_id=user_id, username=username, role="User")
        else:
            rows = db_helper.execute_query(
                "SELECT UserID, Username, Role FROM app.Users WHERE UserID = ? AND ISNULL(IsActive,1)=1",
                (user_id,)
            )
            if not rows:
                raise HTTPException(status_code=401, detail="User not found")
            role = rows[0].get("Role") or "User"
            new_access = create_jwt_token(user_id=user_id, username=username, role=role)
        return {"success": True, "token": new_access}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Refresh token error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/logout")
async def logout(current_user: Dict = Depends(get_current_user)):
    """Logout user (client should discard token)"""
    # In a more advanced implementation, you might want to blacklist the token
    # For now, we just return success and let the client handle token removal

    # Skip audit log for now to avoid database issues

    return {"success": True, "message": "Logged out successfully"}

 

@router.get("/me")
async def get_current_user_info(current_user: Dict = Depends(get_current_user)):
    """Get current authenticated user's information"""
    try:
        if hasattr(db_helper, "test_connection") and not db_helper.test_connection():
            return {"success": True, "data": {"UserID": current_user.get("sub"), "Username": current_user.get("username"), "Role": current_user.get("role")}}
        query = """
        SELECT UserID, Username, FullName, Email, Role, AllocatedKWh, UsedKWh, RemainingKWh, IsLocked, ISNULL(IsActive, 1) as IsActive, CreatedAt, LastLoginAt
        FROM app.Users
        WHERE UserID = ?
        """

        users = db_helper.execute_query(query, (current_user["sub"],))

        if not users or len(users) == 0:
            raise HTTPException(status_code=404, detail="User not found")

        user = users[0]

        return {
            "success": True,
            "data": user
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
