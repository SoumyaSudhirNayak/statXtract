from fastapi import FastAPI, UploadFile, File, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, Response, JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.openapi.utils import get_openapi
from contextlib import asynccontextmanager
import contextvars
from starlette.middleware.sessions import SessionMiddleware
import asyncio
import asyncpg
import json
import os
import zipfile
import tempfile
import difflib
import re
import bcrypt
from typing import Any, Dict, List
from datetime import timedelta, datetime
from dotenv import load_dotenv
from pathlib import Path
from watchgod import awatch, Change


# Import your custom modules
from utils.ingestion_pipeline import ingest_upload_file, process_dataset_zip, convert_and_ingest_nesstar_binary_study, discover_nesstar_converter_exe
from utils.db_init import ensure_core_tables, _to_pg_schema_name
from utils.metadata_helper import get_column_labels, apply_labels
from utils.job_manager import (
    create_job,
    get_job,
    update_job,
    list_jobs,
    JOB_STATUS_INITIALIZED,
    JOB_STATUS_QUEUED,
    JOB_STATUS_PROCESSING,
    JOB_STATUS_CONVERTING,
    JOB_STATUS_PARSING_DDI,
    JOB_STATUS_INGESTING,
    JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED,
)
from utils.watcher import IngestionWatcher
from fastapi import BackgroundTasks
from utils.db_utils import to_snake_case_identifier


# Import authentication modules
from auth.local.dependencies import get_current_user, get_current_active_user_with_role
from auth.local.schemas import TokenData
from auth.local.crud import get_user_by_email
from auth.local.utils import create_access_token
from auth.local.routes import router as local_auth_router


# Import query router
from query.query_data import router as query_router
from query.query_data import log_usage  # Add this for log_usage
from nada_routes import router as nada_router
from fastapi import HTTPException  # Add this for HTTPException
from datetime import date

# Central Security module imports
from security import check_user_access
from security.privacy_guard import check_columns_and_filters, apply_privacy_and_labeling

today = date.today().isoformat()
START_TIME = datetime.now()

from datetime import timezone

try:
    from zoneinfo import ZoneInfo
    DB_TZ = ZoneInfo("Asia/Kolkata")
except Exception:
    from datetime import timezone as dt_tz, timedelta
    DB_TZ = dt_tz(timedelta(hours=5, minutes=30))

def format_local_timestamp_to_utc_iso(dt) -> str:
    """Converts a database local naive timestamp to UTC and formats it with 'Z'."""
    if not dt:
        return None
    if isinstance(dt, str):
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S'):
            try:
                dt = datetime.strptime(dt.split('+')[0].split('Z')[0].strip(), fmt)
                break
            except ValueError:
                continue
        if isinstance(dt, str):
            if not dt.endswith('Z') and not '+' in dt:
                return dt + 'Z'
            return dt
    # If naive, assume it is in the database timezone (Asia/Kolkata)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=DB_TZ)
    # Convert to UTC and format
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def format_utc_timestamp_to_utc_iso(dt) -> str:
    """Converts a database UTC naive timestamp (like freeze_until) to UTC and formats it with 'Z'."""
    if not dt:
        return None
    if isinstance(dt, str):
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S'):
            try:
                dt = datetime.strptime(dt.split('+')[0].split('Z')[0].strip(), fmt)
                break
            except ValueError:
                continue
        if isinstance(dt, str):
            if not dt.endswith('Z') and not '+' in dt:
                return dt + 'Z'
            return dt
    # If naive, assume it is already in UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Convert to UTC and format
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

# ── Internal / metadata tables that should NOT appear in user-facing dropdowns ──
_INTERNAL_TABLE_NAMES = {
    "datasets", "dataset_files", "dataset_metadata", "dataset_registry", "dataset_tables",
    "variables", "variable_categories", "variable_statistics", "variable_missing_values",
}
_INTERNAL_TABLE_KEYWORDS = (
    "variable", "metadata", "category", "missing", "_error", "_stat",
)

def _is_internal_table(name: str) -> bool:
    """Returns True if the table is a system/metadata table that should be hidden from users."""
    low = (name or "").lower()
    if low in _INTERNAL_TABLE_NAMES:
        return True
    for kw in _INTERNAL_TABLE_KEYWORDS:
        if kw in low:
            return True
    return False


async def _resolve_registry_schema(conn: asyncpg.Connection, schema_value: str) -> dict | None:
    s = (schema_value or "").strip()
    if not s:
        return None
    return await conn.fetchrow(
        """
        SELECT display_name, db_name
        FROM schema_registry
        WHERE db_name = $1 OR lower(display_name) = lower($1)
        LIMIT 1
        """,
        s,
    )


# Load environment variables
load_dotenv()


# Configuration
DB_URL = os.getenv("DATABASE_URL")
SECRET_KEY = os.getenv("SECRET_KEY", "fallback_secret")
ALGORITHM = os.getenv("ALGORITHM", "HS256")
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# FastAPI lifespan management
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up...")
    app.state.db = await asyncpg.create_pool(dsn=DB_URL, min_size=1, max_size=5)
    async with app.state.db.acquire() as conn:
        await ensure_core_tables(conn)

    nesstar_exe = (os.getenv("NESSTAR_CONVERTER_EXE") or "").strip()
    if not nesstar_exe or not os.path.exists(nesstar_exe):
        nesstar_exe = discover_nesstar_converter_exe()
        if nesstar_exe:
            os.environ["NESSTAR_CONVERTER_EXE"] = nesstar_exe
    base_dir = os.path.dirname(os.path.abspath(__file__))
    default_nesstar_script = os.path.join(base_dir, "utils", "nesstar_convert.ps1")
    nesstar_script = (os.getenv("NESSTAR_CONVERTER_SCRIPT") or default_nesstar_script).strip()
    enabled = True
    if not nesstar_exe or not os.path.exists(nesstar_exe):
        print("❌ NESSTAR: NESSTAR_CONVERTER_EXE not set or path does not exist. .nesstar conversion is disabled.")
        enabled = False
    if not nesstar_script or not os.path.exists(nesstar_script):
        print("❌ NESSTAR: NESSTAR_CONVERTER_SCRIPT not found. .nesstar conversion is disabled.")
        enabled = False

    app.state.nesstar_enabled = enabled
    app.state.nesstar_exe = nesstar_exe
    app.state.nesstar_script = nesstar_script

    watcher_enabled = (os.getenv("UPLOADS_SAV_WATCHER_ENABLED") or "1").strip().lower() not in {"0", "false", "no", "off"}
    app.state.uploads_sav_watcher_task = None
    if watcher_enabled:
        app.state.uploads_sav_watcher_task = asyncio.create_task(_uploads_sav_watcher_loop())

    completion_watcher_enabled = (os.getenv("UPLOAD_COMPLETION_WATCHER_ENABLED") or "1").strip().lower() not in {"0", "false", "no", "off"}
    app.state.ingestion_watcher = None
    app.state.upload_completion_watcher_task = None
    if completion_watcher_enabled:
        watcher = IngestionWatcher()
        app.state.ingestion_watcher = watcher
        app.state.upload_completion_watcher_task = asyncio.create_task(watcher.start())

    yield
    print("🔻 Shutting down...")
    watcher_task = getattr(app.state, "uploads_sav_watcher_task", None)
    if watcher_task:
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    # Stop Ingestion Watcher
    ingestion_watcher = getattr(app.state, "ingestion_watcher", None)
    if ingestion_watcher:
        ingestion_watcher.stop()

    completion_task = getattr(app.state, "upload_completion_watcher_task", None)
    if completion_task:
        completion_task.cancel()
        try:
            await completion_task
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    await app.state.db.close()


# FastAPI app initialization
app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
os.makedirs("uploads/user_uploads", exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads/user_uploads"), name="user_uploads")
templates = Jinja2Templates(directory="templates")
app.add_middleware(SessionMiddleware, secret_key="yoursecretkey")


def _get_request_role(request: Request) -> str:
    try:
        from auth.local.dependencies import SECRET_KEY, ALGORITHM
        from jose import jwt
        from fastapi.security.utils import get_authorization_scheme_param
        token = None
        auth = request.headers.get("Authorization")
        if auth:
            scheme, param = get_authorization_scheme_param(auth)
            if scheme.lower() == "bearer":
                token = param
        if not token:
            token = request.cookies.get("access_token")
        if token:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            return str(payload.get("role", "3"))
    except Exception:
        pass
    return "3"


@app.exception_handler(HTTPException)
async def governance_http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail
    status_code = exc.status_code
    
    is_governance_err = False
    error_type = None
    message = None
    extra_fields = {}
    
    # Check if the exception detail or status matches a governance/security error
    # Case 1: Rate limit or daily limit exceeded
    if status_code == 429:
        is_governance_err = True
        detail_str = str(detail)
        if "daily query limit" in detail_str.lower() or "exceeded for today" in detail_str.lower():
            error_type = "QUERY_LIMIT_REACHED"
            message = "Daily query limit reached. Please upgrade your plan or try again tomorrow."
        elif "remaining daily limit" in detail_str.lower() or "rows" in detail_str.lower():
            error_type = "QUERY_LIMIT_REACHED"
            message = "Daily query limit reached. Your query would exceed your remaining daily limit of rows."
        else:
            error_type = "RATE_LIMIT"
            message = "Too many requests. Please wait a few seconds before trying again."
            
    # Case 2: Security Policy / Suspicious activity / Restricted Columns / Direct SQL
    elif status_code == 400:
        detail_str = str(detail)
        if (
            "security policy block" in detail_str.lower() 
            or "suspicious or malicious" in detail_str.lower()
            or "privacy violation" in detail_str.lower()
            or "access denied" in detail_str.lower()
            or "not configured as filterable" in detail_str.lower()
        ):
            is_governance_err = True
            error_type = "SENSITIVE_QUERY_BLOCKED"
            message = detail_str
            
    # Case 3: Cell Suppression or Blocked/Frozen/Unverified accounts
    elif status_code == 403:
        if isinstance(detail, dict) and detail.get("error") == "Cell Suppression Applied":
            is_governance_err = True
            error_type = "PRIVACY_SUPPRESSION"
            message = detail.get("detail", "Data suppressed due to privacy rules (less than minimum rows required).")
            extra_fields = {
                "minimum_rows_required": detail.get("minimum_rows_required", 5),
                "actual_rows": detail.get("actual_rows", 0)
            }
        elif isinstance(detail, str):
            detail_str = detail
            if detail_str == "Suppressed" or "suppress" in detail_str.lower():
                is_governance_err = True
                error_type = "PRIVACY_SUPPRESSION"
                message = "Data suppressed due to privacy rules (less than minimum rows required)."
                extra_fields = {
                    "minimum_rows_required": 5,
                    "actual_rows": 0
                }
            elif "access denied" in detail_str.lower() or "blocked" in detail_str.lower() or "frozen" in detail_str.lower() or "verification" in detail_str.lower():
                is_governance_err = True
                if "blocked" in detail_str.lower():
                    error_type = "ACCOUNT_BLOCKED"
                elif "frozen" in detail_str.lower():
                    error_type = "ACCOUNT_FROZEN"
                elif "verification" in detail_str.lower() or "pending" in detail_str.lower():
                    error_type = "VERIFICATION_PENDING"
                else:
                    error_type = "SENSITIVE_QUERY_BLOCKED"
                message = detail_str
                
    accept = request.headers.get("accept", "")
    is_html_request = "text/html" in accept

    if is_governance_err:
        if is_html_request:
            title = "Access Denied"
            if error_type == "ACCOUNT_BLOCKED":
                title = "Account Blocked"
            elif error_type == "ACCOUNT_FROZEN":
                title = "Account Temporarily Frozen"
            elif error_type == "VERIFICATION_PENDING":
                title = "Verification Pending"
            elif error_type == "QUERY_LIMIT_REACHED":
                title = "Usage Limit Exceeded"
            elif error_type == "RATE_LIMIT":
                title = "Rate Limit Exceeded"
                
            role = _get_request_role(request)
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "title": title,
                    "message": message,
                    "error_type": error_type,
                    "role": role
                },
                status_code=status_code
            )

        content = {
            "success": False,
            "error_type": error_type,
            "message": message,
            "error": message  # backward compatibility
        }
        if extra_fields:
            content.update(extra_fields)
        return JSONResponse(status_code=status_code, content=content)
        
    # Standard response for other HTTPExceptions
    if is_html_request:
        if status_code in (401, 403):
            path = request.url.path
            if path.startswith("/user/") and path not in ("/user/login", "/user/register"):
                return RedirectResponse(url="/user/login", status_code=302)
            elif path.startswith("/admin/") and path != "/login":
                return RedirectResponse(url="/login", status_code=302)
        role = _get_request_role(request)
        return templates.TemplateResponse(
            "error.html",
            {
                "request": request,
                "title": "Error Occurred",
                "message": str(detail),
                "error_type": "UNKNOWN",
                "role": role
            },
            status_code=status_code
        )


    if isinstance(detail, dict):
        return JSONResponse(status_code=status_code, content=detail)
    return JSONResponse(status_code=status_code, content={"detail": detail})


# Include routers
app.include_router(local_auth_router)
app.include_router(query_router)
app.include_router(nada_router)


# OAuth2 setup
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")


# Debug routes on startup
@app.on_event("startup")
async def debug_routes():
    print("🔍 Registered routes:")
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            print(f"  {list(route.methods)} {route.path}")


# =================== AUTHENTICATION ROUTES ===================


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root(request: Request):
    return templates.TemplateResponse("splashscreen.html", {"request": request})

@app.get("/module-selection", response_class=HTMLResponse)
async def module_selection(request: Request):
    return templates.TemplateResponse("module_selection.html", {"request": request})

@app.get("/user/module-selection", response_class=HTMLResponse)
async def user_module_selection(request: Request):
    return templates.TemplateResponse("user_module_selection.html", {"request": request})


@app.get("/login", response_class=HTMLResponse)
async def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.post("/login", response_class=HTMLResponse)
async def login_post(
    request: Request, form_data: OAuth2PasswordRequestForm = Depends()
):
    conn = request.app.state.db
    async with conn.acquire() as db:
        user = await get_user_by_email(db, form_data.username)
        if not user or not bcrypt.checkpw(
            form_data.password.encode(), user["hashed_password"].encode()
        ):
            return templates.TemplateResponse(
                "login.html", {"request": request, "error": "Invalid credentials"}
            )

        access_token = create_access_token(
            data={"sub": user["email"], "role": user["role_id"]}
        )
        response = RedirectResponse(
            url="/admin/dashboard" if str(user["role_id"]) == "1" else "/user/dashboard",
            status_code=302,
        )
        response.set_cookie("access_token", access_token, httponly=True)
        return response


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})


@app.post("/register")
async def register_user(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    role: str = Form("user"),
    org_type: str = Form(""),
    org_details: str = Form(""),
    phone: str = Form(""),
    verification_doc: UploadFile = File(None),
):
    pool = request.app.state.db

    # Determine if caller expects JSON (fetch) or HTML redirect
    accept = request.headers.get("accept", "")
    wants_json = "application/json" in accept

    async with pool.acquire() as conn:
        # Check if user already exists
        existing = await conn.fetchrow("SELECT * FROM users WHERE email = $1", email)
        if existing:
            if wants_json:
                return JSONResponse(
                    status_code=400,
                    content={"detail": "User with this email already exists"},
                )
            return templates.TemplateResponse(
                "register.html", {"request": request, "error": "User already exists"}
            )

        # 🔒 BLOCK ADMIN REGISTRATION
        if role.lower() == "admin":
            if wants_json:
                return JSONResponse(
                    status_code=400,
                    content={"detail": "Admin registration is disabled."},
                )
            return templates.TemplateResponse(
                "register.html",
                {
                    "request": request,
                    "error": "Admin registration is disabled. Only user accounts can be created.",
                },
            )

        # Only allow user registration
        role_row = await conn.fetchrow("SELECT id FROM roles WHERE name = $1", "user")
        if not role_row:
            if wants_json:
                return JSONResponse(
                    status_code=500,
                    content={"detail": "User role not found in database"},
                )
            return templates.TemplateResponse(
                "register.html",
                {"request": request, "error": "User role not found in database"},
            )

        role_id = role_row["id"]
        hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

        # Determine if we have a verification document
        has_doc = verification_doc is not None and verification_doc.filename

        await conn.execute(
            """INSERT INTO users (username, email, hashed_password, role_id, document_uploaded, org_type, org_details, phone)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""",
            username,
            email,
            hashed_pw,
            role_id,
            bool(has_doc),
            org_type,
            org_details,
            phone,
        )


        # ── Save verification document if provided ──
        if has_doc:
            # Get the newly created user's ID
            user_row = await conn.fetchrow(
                "SELECT id FROM users WHERE email = $1", email
            )
            user_id = user_row["id"] if user_row else 0

            # Create per-user upload directory
            user_upload_dir = os.path.join(
                "uploads", "user_uploads", str(user_id)
            )
            os.makedirs(user_upload_dir, exist_ok=True)

            # Save the file
            safe_filename = re.sub(
                r"[^\w\-.]", "_", verification_doc.filename
            )
            file_path = os.path.join(user_upload_dir, safe_filename)
            file_content = await verification_doc.read()
            with open(file_path, "wb") as f:
                f.write(file_content)

            # URL accessible via the /uploads static mount
            document_url = f"/uploads/{user_id}/{safe_filename}"

            # Build a descriptive document name
            doc_name = f"{org_type} verification" if org_type else safe_filename

            # Insert into user_documents table
            await conn.execute(
                """INSERT INTO user_documents
                       (user_email, document_name, document_url, status)
                   VALUES ($1, $2, $3, 'pending')""",
                email,
                doc_name,
                document_url,
            )

    if wants_json:
        return JSONResponse(
            status_code=200, content={"detail": "Account created successfully"}
        )
    return RedirectResponse("/", status_code=302)


@app.post("/auth/logout")
async def logout(response: Response):
    """Logout endpoint - clears authentication cookies"""
    try:
        # Delete the access token cookie
        response.delete_cookie(
            key="access_token",
            path="/",
            domain=None,
            secure=False,  # Set to True in production with HTTPS
            httponly=True,
            samesite="lax",
        )

        return {"message": "Logged out successfully", "status": "success"}
    except Exception as e:
        print(f"Logout error: {e}")
        return {
            "message": "Logout completed",
            "status": "success",
        }  # Always return success


# =================== USER DASHBOARD ROUTES ===================


@app.get("/user/login", response_class=HTMLResponse)
async def user_login_page(request: Request):
    return templates.TemplateResponse("USER_PAGES/user_login.html", {"request": request})


@app.get("/user/register", response_class=HTMLResponse)
async def user_register_page(request: Request):
    return templates.TemplateResponse("USER_PAGES/user_register.html", {"request": request})


async def get_user_template_context(request: Request, current_user_email: str) -> dict:
    pool = request.app.state.db
    async with pool.acquire() as conn:
        user_row = await conn.fetchrow(
            """
            SELECT u.*, r.name as role_name 
            FROM users u 
            LEFT JOIN roles r ON u.role_id = r.id 
            WHERE u.email = $1
            """,
            current_user_email
        )
        if not user_row:
            return {
                "username": "User",
                "email": current_user_email,
                "role": "Student",
                "org_type": "Student",
                "phone": "",
                "org_details": {},
                "created_at": None,
                "is_verified": False,
                "plan": "free",
                "credits": {},
                "total_queries": 0,
                "total_downloads": 0,
                "rows_accessed": 0,
                "member_since": "Jan 2026",
                "last_active": "N/A",
                "plan_expiry": "N/A"
            }
        
        org_type = user_row.get("org_type") or ""
        role_display = org_type
        if not role_display:
            role_name = user_row.get("role_name") or "user"
            if role_name == "admin":
                role_display = "Admin"
            elif role_name == "analyst":
                role_display = "Analyst"
            else:
                role_display = "Student"
                
        import json
        org_details = {}
        if user_row.get("org_details"):
            try:
                org_details = json.loads(user_row["org_details"])
            except Exception:
                pass

        # Total queries
        total_queries = await conn.fetchval(
            "SELECT COUNT(*) FROM usage_logs WHERE user_email = $1",
            current_user_email
        ) or 0

        # Total downloads
        total_downloads = await conn.fetchval(
            "SELECT COUNT(*) FROM download_logs WHERE user_email = $1",
            current_user_email
        ) or 0

        # Rows accessed
        rows_accessed = await conn.fetchval(
            "SELECT COALESCE(SUM(rows_returned), 0) FROM usage_logs WHERE user_email = $1",
            current_user_email
        ) or 0

        # Plan limits and usage credits
        from security.usage_tracker import get_daily_usage_credits
        user_role_str = str(user_row["role_id"]) if user_row else "3"
        plan_limits = {}
        usage_credits = {}
        try:
            plan_limits = await get_and_enforce_plan_limits(conn, current_user_email, user_role_str)
            usage_credits = await get_daily_usage_credits(conn, current_user_email, plan_limits)
        except Exception as e:
            print(f"Error getting plan limits or credits: {e}")

        # Format member_since
        member_since = "Jan 2026"
        if user_row and user_row.get("created_at"):
            member_since = user_row["created_at"].strftime("%B %Y")
            
        # Format last_active
        last_active = "N/A"
        if user_row and user_row.get("last_active"):
            last_active = user_row["last_active"].strftime("%d %b %Y, %I:%M %p")

        # Format plan expiry
        plan_expiry_str = "N/A"
        if user_row and user_row.get("plan_expiry"):
            plan_expiry_str = user_row["plan_expiry"].strftime("%d %b %Y")

        is_admin_user = (str(user_row.get("role_id")) == "1")
        return {
            "username": user_row.get("username") or "User",
            "email": user_row.get("email"),
            "role": role_display,
            "org_type": org_type,
            "phone": user_row.get("phone") or org_details.get("phone") or "",
            "org_details": org_details,
            "created_at": user_row.get("created_at"),
            "is_verified": True if is_admin_user else (user_row.get("is_verified") or False),
            "plan": user_row.get("plan") or "free",
            "is_blocked": user_row.get("is_blocked") or False,
            "total_queries": total_queries,
            "total_downloads": total_downloads,
            "rows_accessed": rows_accessed,
            "member_since": member_since,
            "last_active": last_active,
            "plan_expiry": plan_expiry_str,
            "credits": usage_credits
        }


@app.get("/user/dashboard", response_class=HTMLResponse)
async def user_dashboard_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        "USER_PAGES/user_dashboard.html",
        {
            "request": request,
            **ctx
        },
    )


@app.get("/user/profile", response_class=HTMLResponse)
async def user_profile_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        "USER_PAGES/user_profile.html", 
        {
            "request": request,
            **ctx
        }
    )


@app.get("/user/history", response_class=HTMLResponse)
async def user_history_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        "USER_PAGES/user_history.html", 
        {
            "request": request,
            **ctx
        }
    )


@app.get("/user/downloads", response_class=HTMLResponse)
async def user_downloads_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        "USER_PAGES/user_downloads.html", 
        {
            "request": request,
            **ctx
        }
    )


@app.get("/user/plans", response_class=HTMLResponse)
async def user_plans_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        "USER_PAGES/user_plans.html", 
        {
            "request": request,
            **ctx
        }
    )


@app.get("/user/settings", response_class=HTMLResponse)
async def user_settings_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    ctx = await get_user_template_context(request, current_user.username)
    
    # Extract login time from JWT
    from jose import jwt
    from auth.local.dependencies import SECRET_KEY, ALGORITHM
    token = request.cookies.get("access_token")
    login_time = None
    if token:
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            iat = payload.get("iat")
            if iat:
                from datetime import datetime, timezone
                login_time = datetime.fromtimestamp(iat, timezone.utc).isoformat()
        except Exception:
            pass

    return templates.TemplateResponse(
        "USER_PAGES/user_settings.html", 
        {
            "request": request,
            "login_time": login_time,
            "client_ip": request.client.host if request.client else "127.0.0.1",
            **ctx
        }
    )



@app.get("/user/usage", response_class=HTMLResponse)
async def user_usage_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        "USER_PAGES/user_usage.html", 
        {
            "request": request,
            **ctx
        }
    )


@app.get("/user/feedback", response_class=HTMLResponse)
async def user_feedback_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        "USER_PAGES/user_feedback.html", 
        {
            "request": request,
            **ctx
        }
    )



@app.post("/user/feedback/submit")
async def user_feedback_submit(
    request: Request,
    category: str = Form(...),
    title: str = Form(...),
    feedback_text: str = Form(...),
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_requests (user_email, request_type, category, title, message, status)
            VALUES ($1, 'feedback', $2, $3, $4, 'pending')
            """,
            current_user.username,
            category,
            title,
            feedback_text,
        )
    return RedirectResponse(url="/user/feedback?success=Feedback+submitted+successfully!", status_code=302)


@app.post("/user/dataset-request/submit")
async def user_dataset_request_submit(
    request: Request,
    dataset_name: str = Form(...),
    survey_name: str = Form(...),
    reason: str = Form(...),
    notes: str = Form(""),
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO user_requests (user_email, request_type, requested_dataset, survey_name, reason, message, status)
            VALUES ($1, 'dataset_request', $2, $3, $4, $5, 'pending')
            """,
            current_user.username,
            dataset_name,
            survey_name,
            reason,
            notes,
        )
    return RedirectResponse(url="/user/feedback?success=Dataset+request+submitted+successfully!", status_code=302)


@app.post("/user/revoke-all-sessions")
async def user_revoke_all_sessions(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    import datetime
    utc_now = datetime.datetime.utcnow()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET token_valid_after = $1 WHERE email = $2",
            utc_now,
            current_user.username
        )
        
    # Generate new token for current user so they stay logged in
    from auth.local.utils import create_access_token
    new_token = create_access_token({
        "sub": current_user.username,
        "role": str(current_user.role)
    })
    
    response = JSONResponse(content={"message": "All other sessions signed out successfully.", "status": "success"})
    response.set_cookie("access_token", new_token, httponly=True)
    return response


@app.post("/user/delete-account")
async def user_delete_account(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    try:
        data = await request.json()
        password = data.get("password")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid request payload")

    if not password:
        raise HTTPException(status_code=400, detail="Password is required to delete your account")

    pool = request.app.state.db
    async with pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE email = $1", current_user.username)
        if not user or not bcrypt.checkpw(password.encode(), user["hashed_password"].encode()):
            raise HTTPException(status_code=400, detail="Invalid password verification.")
        
        # Anonymize governance logs
        await conn.execute("UPDATE governance_logs SET user_email = 'deleted_user@statxtract.in' WHERE user_email = $1", current_user.username)
        await conn.execute("UPDATE usage_logs SET user_email = 'deleted_user@statxtract.in' WHERE user_email = $1", current_user.username)
        await conn.execute("UPDATE query_logs SET user_email = 'deleted_user@statxtract.in' WHERE user_email = $1", current_user.username)
        await conn.execute("UPDATE download_logs SET user_email = 'deleted_user@statxtract.in' WHERE user_email = $1", current_user.username)
        await conn.execute("DELETE FROM user_documents WHERE user_email = $1", current_user.username)
        await conn.execute("DELETE FROM user_requests WHERE user_email = $1", current_user.username)
        
        # Delete user account
        await conn.execute("DELETE FROM users WHERE email = $1", current_user.username)
        
    response = JSONResponse(content={"message": "Account deleted successfully.", "status": "success"})
    response.delete_cookie("access_token", path="/")
    return response


@app.post("/api/user/update-profile")
async def api_update_profile(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid request payload")
        
    full_name = data.get("full_name")
    phone = data.get("phone", "")
    org_type = data.get("org_type", "")
    institution = data.get("institution", "")
    
    if not full_name:
        raise HTTPException(status_code=400, detail="Name is required")

    pool = request.app.state.db
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT org_details FROM users WHERE email = $1", current_user.username)
        org_details = {}
        if row and row["org_details"]:
            try:
                import json
                org_details = json.loads(row["org_details"])
            except Exception:
                pass
        
        org_key = org_type.split(' ')[0] if org_type else ""
        if org_key == "Student" or org_key == "Researcher":
            org_details["institution"] = institution
        elif org_key == "Private":
            org_details["company"] = institution
        else:
            org_details["institution"] = institution

        import json
        await conn.execute(
            """
            UPDATE users 
            SET username = $1, phone = $2, org_type = $3, org_details = $4
            WHERE email = $5
            """,
            full_name,
            phone,
            org_type,
            json.dumps(org_details),
            current_user.username
        )
        
    return {"message": "Profile updated successfully", "status": "success"}


@app.post("/api/user/change-password")
async def user_change_password(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    try:
        data = await request.json()
        current_password = data.get("current_password")
        new_password = data.get("new_password")
        confirm_password = data.get("confirm_password")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid request payload")

    if not current_password or not new_password or not confirm_password:
        raise HTTPException(status_code=400, detail="Please fill in all password fields")

    if new_password != confirm_password:
        raise HTTPException(status_code=400, detail="New passwords do not match")

    if len(new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters long")

    pool = request.app.state.db
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT * FROM users WHERE email = $1", current_user.username
        )
        if not user or not bcrypt.checkpw(current_password.encode(), user["hashed_password"].encode()):
            raise HTTPException(status_code=400, detail="Current password is incorrect")

        new_hashed = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
        await conn.execute(
            "UPDATE users SET hashed_password = $1 WHERE email = $2",
            new_hashed,
            current_user.username,
        )

    return {"message": "Password updated successfully!", "status": "success"}



# =================== GOVERNANCE API ENDPOINTS ===================

from security.plan_enforcer import get_and_enforce_plan_limits, get_plan_export_formats
from security.usage_tracker import get_daily_usage_credits, log_file_download
from security.warning_manager import get_user_warnings, get_user_governance_notices
from security.suspicious_detector import check_behavioral_patterns


@app.get("/api/user/governance")
async def api_user_governance(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    """Full governance status for the current user — plan, limits, usage, warnings, restrictions."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        user_email = current_user.username
        user_role = str(current_user.role)

        from security.warning_manager import check_and_auto_unfreeze
        await check_and_auto_unfreeze(conn, user_email)

        # Get user record
        user_row = await conn.fetchrow(
            """
            SELECT 
                COALESCE(plan, 'free') AS plan,
                COALESCE(is_verified, FALSE) AS is_verified,
                COALESCE(is_blocked, FALSE) AS is_blocked,
                COALESCE(status, 'active') AS status,
                COALESCE(warning_count, 0) AS warning_count,
                COALESCE(suspicious_score, 0) AS suspicious_score,
                freeze_until,
                plan_expiry,
                blocked_reason,
                COALESCE(cancel_at_period_end, FALSE) AS cancel_at_period_end,
                org_type
            FROM users WHERE email = $1 LIMIT 1
            """,
            user_email
        )

        if not user_row:
            return {"error": "User not found"}

        # Get plan limits
        plan_limits = await get_and_enforce_plan_limits(conn, user_email, user_role)
        plan_name = plan_limits.get("plan", str(user_row["plan"]))

        # Get role name
        org_type = user_row.get("org_type") or ""
        role_name = org_type
        if not role_name:
            role_name = {"1": "admin", "2": "analyst", "3": "user"}.get(user_role, "user")
            if role_name == "admin":
                role_name = "Admin"
            elif role_name == "analyst":
                role_name = "Analyst"
            else:
                role_name = "Student"

        # Get usage credits
        credits = await get_daily_usage_credits(conn, user_email, plan_limits)

        # Get warnings
        warnings = await get_user_warnings(conn, user_email)

        # Get governance notices
        notices = await get_user_governance_notices(conn, user_email)

        # Get last successful payment details
        last_pay = await conn.fetchrow(
            """
            SELECT billing_cycle, expiry_date, payment_status
            FROM payments
            WHERE email = $1 AND payment_status = 'success'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            user_email
        )
        billing_cycle = last_pay["billing_cycle"] if last_pay else "N/A"
        renewal_date = last_pay["expiry_date"] if last_pay else user_row["plan_expiry"]
        payment_status = last_pay["payment_status"] if last_pay else "N/A"
        is_admin = str(current_user.role) == "1"

        return {
            "plan": plan_name,
            "role": role_name,
            "is_verified": True if is_admin else bool(user_row["is_verified"]),
            "is_blocked": bool(user_row["is_blocked"]),
            "status": str(user_row["status"]),
            "plan_expiry": format_local_timestamp_to_utc_iso(user_row["plan_expiry"]),
            "billing_cycle": billing_cycle,
            "renewal_date": format_local_timestamp_to_utc_iso(renewal_date),
            "payment_status": payment_status,
            "cancel_at_period_end": bool(user_row["cancel_at_period_end"]),
            "testing_mode": False,
            "limits": {
                "max_queries_per_day": plan_limits.get("max_queries_per_day", 1000),
                "max_rows_per_day": plan_limits.get("max_rows_per_day", 100000),
                "max_downloads_per_day": plan_limits.get("max_downloads_per_day", 5),
                "max_queries_per_month": plan_limits.get("max_queries_per_month", 200),
                "max_rows_per_month": plan_limits.get("max_rows_per_month", 5000),
                "max_downloads_per_month": plan_limits.get("max_downloads_per_month", 10),
                "max_ai_queries_per_month": plan_limits.get("max_ai_queries_per_month", 3),
                "downloads_allowed": plan_limits.get("downloads_allowed", False),
                "api_access": plan_limits.get("api_access", False),
                "advanced_analytics": plan_limits.get("advanced_analytics", False),
                "export_formats": get_plan_export_formats(plan_limits),
            },
            "usage_today": credits,
            "warnings": warnings,
            "warning_count": int(user_row["warning_count"]),
            "suspicious_score": int(user_row["suspicious_score"]),
            "freeze_until": format_utc_timestamp_to_utc_iso(user_row["freeze_until"]),
            "governance_notices": notices,
        }


@app.get("/api/user/query-history")
async def api_user_query_history(
    request: Request,
    page: int = 1,
    per_page: int = 20,
    q: str = "",
    status: str = "all",
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    """Real query history from usage_logs for the current user."""
    pool = request.app.state.db
    offset = (max(1, page) - 1) * per_page
    async with pool.acquire() as conn:
        user_email = current_user.username

        conditions = ["user_email = $1"]
        params = [user_email]

        if q:
            search_param = f"%{q}%"
            params.append(search_param)
            conditions.append(f"(schema_name ILIKE ${len(params)} OR table_name ILIKE ${len(params)} OR filters ILIKE ${len(params)} OR status ILIKE ${len(params)})")

        if status and status != "all":
            if status == "success":
                conditions.append("status IN ('success', 'completed')")
            elif status == "failed":
                conditions.append("(status ILIKE 'fail%' OR status = 'error')")
            else:
                params.append(status)
                conditions.append(f"status = ${len(params)}")

        where_clause = "WHERE " + " AND ".join(conditions)

        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM usage_logs {where_clause}",
            *params
        ) or 0

        params.append(per_page)
        limit_param = len(params)
        params.append(offset)
        offset_param = len(params)

        rows = await conn.fetch(
            f"""
            SELECT
                COALESCE(schema_name, '-') AS dataset,
                COALESCE(table_name, '-') AS table_name,
                COALESCE(filters, '') AS filters,
                COALESCE(rows_returned, 0) AS rows_returned,
                COALESCE(query_time_ms, 0) AS query_time_ms,
                COALESCE(status, 'success') AS status,
                queried_at AS timestamp,
                endpoint
            FROM usage_logs
            {where_clause}
            ORDER BY queried_at DESC
            LIMIT ${limit_param} OFFSET ${offset_param}
            """,
            *params
        )

        history_list = []
        for r in rows:
            d = dict(r)
            if "timestamp" in d:
                d["timestamp"] = format_local_timestamp_to_utc_iso(d["timestamp"])
            history_list.append(d)

        return {
            "total": int(total),
            "page": page,
            "per_page": per_page,
            "history": history_list,
        }


@app.get("/api/user/download-history")
async def api_user_download_history(
    request: Request,
    page: int = 1,
    per_page: int = 20,
    q: str = "",
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    """Real download history from download_logs for the current user."""
    pool = request.app.state.db
    offset = (max(1, page) - 1) * per_page
    async with pool.acquire() as conn:
        user_email = current_user.username

        conditions = ["user_email = $1"]
        params = [user_email]

        if q:
            search_param = f"%{q}%"
            params.append(search_param)
            conditions.append(f"(file_name ILIKE ${len(params)} OR dataset_schema ILIKE ${len(params)} OR export_format ILIKE ${len(params)} OR status ILIKE ${len(params)})")

        where_clause = "WHERE " + " AND ".join(conditions)

        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM download_logs {where_clause}",
            *params
        ) or 0

        params.append(per_page)
        limit_param = len(params)
        params.append(offset)
        offset_param = len(params)

        rows = await conn.fetch(
            f"""
            SELECT
                file_name,
                COALESCE(dataset_schema, '-') AS dataset,
                COALESCE(export_format, 'csv') AS export_format,
                COALESCE(rows_exported, 0) AS rows_exported,
                COALESCE(size_bytes, 0) AS size_bytes,
                COALESCE(status, 'success') AS status,
                created_at AS timestamp
            FROM download_logs
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${limit_param} OFFSET ${offset_param}
            """,
            *params
        )

        downloads_list = []
        for r in rows:
            d = dict(r)
            if "timestamp" in d:
                d["timestamp"] = format_local_timestamp_to_utc_iso(d["timestamp"])
            downloads_list.append(d)

        return {
            "total": int(total),
            "page": page,
            "per_page": per_page,
            "downloads": downloads_list,
        }


@app.get("/api/user/credits")
async def api_user_credits(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    """Used/remaining usage credits for the current user."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        user_email = current_user.username
        user_role = str(current_user.role)
        plan_limits = await get_and_enforce_plan_limits(conn, user_email, user_role)
        credits = await get_daily_usage_credits(conn, user_email, plan_limits)
        return credits


@app.get("/admin/users/governance/{email}")
async def admin_user_governance_detail(
    request: Request,
    email: str,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    """Detailed per-user governance data for admin dashboard."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        user_row = await conn.fetchrow(
            """
            SELECT
                u.username, u.email,
                COALESCE(r.name, 'user') AS role,
                u.org_type,
                COALESCE(u.plan, 'free') AS plan,
                COALESCE(u.is_verified, FALSE) AS is_verified,
                COALESCE(u.is_blocked, FALSE) AS is_blocked,
                COALESCE(u.status, 'active') AS status,
                COALESCE(u.warning_count, 0) AS warning_count,
                COALESCE(u.suspicious_score, 0) AS suspicious_score,
                u.freeze_until,
                u.last_active,
                u.created_at
            FROM users u
            LEFT JOIN roles r ON r.id = u.role_id
            WHERE u.email = $1
            LIMIT 1
            """,
            email
        )
        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")

        # Get usage today
        usage_today = await conn.fetchrow(
            """
            SELECT COUNT(*) AS queries, COALESCE(SUM(rows_returned), 0) AS rows
            FROM usage_logs
            WHERE user_email = $1 AND queried_at >= CURRENT_DATE
            """,
            email
        )

        downloads_today = await conn.fetchval(
            "SELECT COUNT(*) FROM download_logs WHERE user_email = $1 AND created_at >= CURRENT_DATE",
            email
        ) or 0

        # Recent queries
        recent_queries = await conn.fetch(
            """
            SELECT schema_name, table_name, rows_returned, COALESCE(query_time_ms, 0) AS query_time_ms,
                   COALESCE(status, 'success') AS status, queried_at
            FROM usage_logs WHERE user_email = $1
            ORDER BY queried_at DESC LIMIT 10
            """,
            email
        )

        # Recent downloads
        recent_downloads = await conn.fetch(
            """
            SELECT file_name, COALESCE(dataset_schema, '-') AS dataset, COALESCE(export_format, 'csv') AS format,
                   size_bytes, created_at
            FROM download_logs WHERE user_email = $1
            ORDER BY created_at DESC LIMIT 10
            """,
            email
        )

        # Warnings
        warnings = await get_user_warnings(conn, email)

        # Suspicious activity
        suspicious = await conn.fetch(
            """
            SELECT activity_type, risk_score, detail, created_at
            FROM suspicious_activity_logs WHERE user_email = $1
            ORDER BY created_at DESC LIMIT 10
            """,
            email
        )

        # Governance logs
        gov_logs = await conn.fetch(
            """
            SELECT event_type, detail, created_at
            FROM governance_logs WHERE user_email = $1
            ORDER BY created_at DESC LIMIT 10
            """,
            email
        )

        user_dict = dict(user_row)
        user_dict["role_display"] = user_dict.get("org_type") if user_dict.get("org_type") else user_dict["role"].capitalize()
        user_dict["freeze_until"] = format_utc_timestamp_to_utc_iso(user_dict["freeze_until"])
        user_dict["last_active"] = format_local_timestamp_to_utc_iso(user_dict["last_active"])
        user_dict["created_at"] = format_local_timestamp_to_utc_iso(user_dict["created_at"])

        recent_queries_list = []
        for r in recent_queries:
            d = dict(r)
            if "queried_at" in d:
                d["queried_at"] = format_local_timestamp_to_utc_iso(d["queried_at"])
            recent_queries_list.append(d)

        recent_downloads_list = []
        for r in recent_downloads:
            d = dict(r)
            if "created_at" in d:
                d["created_at"] = format_local_timestamp_to_utc_iso(d["created_at"])
            recent_downloads_list.append(d)

        suspicious_list = []
        for r in suspicious:
            d = dict(r)
            if "created_at" in d:
                d["created_at"] = format_local_timestamp_to_utc_iso(d["created_at"])
            suspicious_list.append(d)

        gov_logs_list = []
        for r in gov_logs:
            d = dict(r)
            if "created_at" in d:
                d["created_at"] = format_local_timestamp_to_utc_iso(d["created_at"])
            gov_logs_list.append(d)

        return {
            "user": user_dict,
            "usage_today": {
                "queries": int(usage_today["queries"] or 0) if usage_today else 0,
                "rows": int(usage_today["rows"] or 0) if usage_today else 0,
                "downloads": int(downloads_today),
            },
            "recent_queries": recent_queries_list,
            "recent_downloads": recent_downloads_list,
            "warnings": warnings,
            "suspicious_activity": suspicious_list,
            "governance_logs": gov_logs_list,
        }


@app.get("/admin/governance/suspicious")
async def admin_suspicious_activity_feed(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    """Suspicious activity feed for admin dashboard."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        activities = await conn.fetch(
            """
            SELECT user_email, activity_type, risk_score, detail, dataset_affected, created_at
            FROM suspicious_activity_logs
            ORDER BY created_at DESC
            LIMIT 50
            """
        )

        # Users with highest suspicious scores
        risky_users = await conn.fetch(
            """
            SELECT u.email, u.username, COALESCE(u.suspicious_score, 0) AS suspicious_score,
                   COALESCE(u.warning_count, 0) AS warning_count,
                   COALESCE(u.status, 'active') AS status,
                   COALESCE(u.plan, 'free') AS plan
            FROM users u
            WHERE COALESCE(u.suspicious_score, 0) > 0
            ORDER BY u.suspicious_score DESC
            LIMIT 20
            """
        )

        activities_list = []
        for r in activities:
            d = dict(r)
            if "created_at" in d:
                d["created_at"] = format_local_timestamp_to_utc_iso(d["created_at"])
            activities_list.append(d)

        return {
            "activities": activities_list,
            "risky_users": [dict(r) for r in risky_users],
        }


# =================== DASHBOARD ROUTES ===================


@app.get("/admin/dashboard", response_class=HTMLResponse)
async def admin_dashboard(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    print(
        f"DEBUG: Current user - Username: {current_user.username}, Role: {current_user.role}"
    )

    # Calculate uptime
    uptime_delta = datetime.now() - START_TIME
    days = uptime_delta.days
    hours, remainder = divmod(uptime_delta.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    
    uptime_str = f"{days}d {hours}h {minutes}m"
    if days == 0:
        uptime_str = f"{hours}h {minutes}m"

    pool = request.app.state.db
    async with pool.acquire() as conn:
        # 1. Total Datasets (Sum of all {schema}.datasets)
        # First get schemas that have a 'datasets' table
        schemas_rows = await conn.fetch("""
            SELECT table_schema 
            FROM information_schema.tables 
            WHERE table_name = 'datasets' 
            AND table_schema NOT IN ('information_schema', 'pg_catalog')
        """)
        
        total_datasets = 0
        for row in schemas_rows:
            schema = row['table_schema']
            try:
                # Use double quotes for schema name to handle special characters/case sensitivity
                count = await conn.fetchval(f'SELECT COUNT(*) FROM "{schema}".datasets')
                total_datasets += (count or 0)
            except Exception as e:
                print(f"Error counting datasets in {schema}: {e}")
                pass

        # 2. Active Users
        active_users = await conn.fetchval("SELECT COUNT(*) FROM users WHERE role_id != 1")

        # 3. Data Schemas (Count of schemas with 'datasets' table)
        data_schemas = len(schemas_rows)

    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,  # Since username is email in your case
            "role": current_user.role,
            "total_datasets": total_datasets,
            "active_users": active_users,
            "data_schemas": data_schemas,
            "uptime": uptime_str,
        },
    )


@app.get("/admin/survey-config", response_class=HTMLResponse)
async def survey_config_page(
    request: Request,
    current_user: TokenData = Depends(
        get_current_active_user_with_role(["1", "2"])
    ),
):
    return templates.TemplateResponse(
        "survey_config.html",
        {
            "request": request,
            "username": current_user.username,
            "role": current_user.role,
        },
    )


@app.get("/query", response_class=HTMLResponse)
async def query_page(
    request: Request,
    current_user: TokenData = Depends(
        get_current_active_user_with_role(["1", "2", "3"])
    ),
):
    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        "query_ui.html",
        {
            "request": request,
            **ctx
        },
    )



@app.get("/api/user/status")
async def get_user_status_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        from security.warning_manager import check_and_auto_unfreeze
        await check_and_auto_unfreeze(conn, current_user.username)
        
        row = await conn.fetchrow(
            """
            SELECT is_verified, document_uploaded, status
            FROM users
            WHERE email = $1
            LIMIT 1
            """,
            current_user.username
        )
        if not row:
            return {"is_verified": False, "document_uploaded": False, "status": "inactive"}
        is_admin = str(current_user.role) == "1"
        return {
            "is_verified": True if is_admin else bool(row["is_verified"]),
            "document_uploaded": bool(row["document_uploaded"]),
            "status": str(row["status"] or "active")
        }


@app.get("/upload", response_class=HTMLResponse)
async def upload_form_ui(
    request: Request,
    current_user=Depends(get_current_active_user_with_role(["1", "2"])),
):
    return templates.TemplateResponse("index.html", {"request": request})


# =================== BEAUTIFUL TABLE PAGES ===================


@app.get("/schemas-page", response_class=HTMLResponse)
async def schemas_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    return templates.TemplateResponse("schemas.html", {"request": request})


@app.get("/datasets-page", response_class=HTMLResponse)
async def datasets_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    return templates.TemplateResponse("datasets.html", {"request": request})


@app.get("/explorer", response_class=HTMLResponse)
async def explorer_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        "explorer.html",
        {
            "request": request,
            **ctx
        },
    )



@app.get("/metadata-detail/{schema}/{dataset}", response_class=HTMLResponse)
async def metadata_detail_page(
    request: Request,
    schema: str,
    dataset: str,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    return templates.TemplateResponse(
        "metadata_detail.html",
        {"request": request, "schema": schema, "dataset": dataset}
    )


@app.get("/admin/metadata-browser", response_class=HTMLResponse)
async def metadata_browser_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    return templates.TemplateResponse("metadata_browser.html", {"request": request})


@app.get("/admin/nada-import", response_class=HTMLResponse)
async def nada_import_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    return templates.TemplateResponse("nada_import.html", {"request": request})


# =================== API ROUTES ===================


@app.get("/schemas")
async def get_schemas(request: Request):
    """List schemas from schema_registry only."""
    try:
        pool = request.app.state.db
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT display_name, db_name
                FROM schema_registry
                ORDER BY display_name;
                """
            )

            schemas_with_info = [
                {
                    "schema": r["db_name"],
                    "name": r["display_name"],
                    "description": "Database schema containing tables and data structures",
                    "icon": "fas fa-database",
                    "category": "Data Schema",
                }
                for r in rows
            ]

            return schemas_with_info

    except Exception as e:
        print(f"ERROR in get_schemas: {e}")
        return {"error": str(e)}


def _format_dataset_suffix(suffix: str) -> str:
    s = (suffix or "").strip().strip("_")
    if not s:
        return ""
    return "-".join([p for p in s.split("_") if p])


def _dataset_display_from_schema(schema_name: str, survey_display: str, survey_db: str) -> str:
    sn = (schema_name or "").strip()
    if "__" in sn:
        ds = sn.split("__", 1)[1]
        suffix = _format_dataset_suffix(ds)
        return suffix.replace("-", " ").replace("_", " ").title() or survey_display

    if sn == survey_db:
        return survey_display

    if sn.startswith(survey_db + "_"):
        suffix = sn[len(survey_db) + 1 :]
        cleaned = _format_dataset_suffix(suffix)
        return f"{survey_display}({cleaned})" if cleaned else survey_display

    parts = sn.split("_", 1)
    if len(parts) == 2:
        cleaned = _format_dataset_suffix(parts[1])
        return f"{survey_display}({cleaned})" if cleaned else survey_display

    return survey_display


async def _list_non_system_schemas(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch(
        """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public')
        ORDER BY schema_name
        """
    )
    return [r["schema_name"] for r in rows]


async def _group_schemas_by_survey(conn: asyncpg.Connection) -> dict[str, dict[str, Any]]:
    reg_rows = await conn.fetch(
        """
        SELECT display_name, db_name
        FROM schema_registry
        ORDER BY display_name
        """
    )
    display_by_db = {r["db_name"]: r["display_name"] for r in reg_rows}
    db_by_display_lower = {str(r["display_name"]).strip().lower(): r["db_name"] for r in reg_rows}

    schema_names = await _list_non_system_schemas(conn)

    grouped: dict[str, dict[str, Any]] = {}
    for sn in schema_names:
        survey_db = None
        if "__" in sn:
            candidate = sn.split("__", 1)[0]
            if candidate in display_by_db:
                survey_db = candidate
        if not survey_db:
            if sn in display_by_db:
                survey_db = sn
        if not survey_db:
            for db in display_by_db.keys():
                if sn.startswith(db + "_"):
                    survey_db = db
                    break
        if not survey_db:
            base_token = sn.split("_", 1)[0].strip()
            survey_db = db_by_display_lower.get(base_token.lower())
            if not survey_db:
                candidate = _to_pg_schema_name(base_token)
                if candidate in display_by_db:
                    survey_db = candidate

        if not survey_db or survey_db not in display_by_db:
            continue

        survey_display = display_by_db[survey_db]
        group = grouped.get(survey_db)
        if not group:
            group = {"survey": survey_db, "display_name": survey_display, "datasets": []}
            grouped[survey_db] = group

        group["datasets"].append(
            {
                "schema": sn,
                "display_name": _dataset_display_from_schema(sn, survey_display, survey_db),
            }
        )

    for g in grouped.values():
        g["datasets"] = sorted(g["datasets"], key=lambda x: x["display_name"])
    return dict(sorted(grouped.items(), key=lambda x: x[1]["display_name"]))


_CFG_CONN: contextvars.ContextVar = contextvars.ContextVar("cfg_conn")
_CFG_FILTERS: contextvars.ContextVar = contextvars.ContextVar("cfg_filters", default=None)
_CFG_LABELS: contextvars.ContextVar = contextvars.ContextVar("cfg_labels", default=None)


class TableHidden(Exception):
    pass


def _set_apply_context(*, conn, filters=None, labels=None):
    t1 = _CFG_CONN.set(conn)
    t2 = _CFG_FILTERS.set(filters)
    t3 = _CFG_LABELS.set(labels)
    return (t1, t2, t3)


def _reset_apply_context(tokens):
    _CFG_CONN.reset(tokens[0])
    _CFG_FILTERS.reset(tokens[1])
    _CFG_LABELS.reset(tokens[2])


def _normalize_role(role_value: Any) -> str:
    raw = str(role_value or "").strip().lower()
    return {"1": "admin", "2": "analyst", "3": "user"}.get(raw, raw or "user")


def _extract_filter_columns(filter_expr: str, known_columns: list[str]) -> set[str]:
    if not filter_expr:
        return set()
    known = {c.lower(): c for c in known_columns}
    used = set()
    for m in re.finditer(r'("([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))', filter_expr):
        ident = m.group(2) or m.group(3)
        if not ident:
            continue
        hit = known.get(ident.lower())
        if hit:
            used.add(hit)
    return used


async def get_dataset_configs(schema: str) -> dict[str, dict]:
    conn = _CFG_CONN.get(None)
    if conn is None:
        raise RuntimeError("Missing DB connection for config context")
    has_show_col = await conn.fetchval(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'dataset_configs'
          AND column_name = 'show_table_to_users'
        LIMIT 1
        """
    )
    if has_show_col:
        rows = await conn.fetch(
            """
            SELECT table_name, show_table_to_users
            FROM dataset_configs
            WHERE schema_name = $1
            """,
            schema,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT table_name, TRUE AS show_table_to_users
            FROM dataset_configs
            WHERE schema_name = $1
            """,
            schema,
        )
    return {r["table_name"]: dict(r) for r in rows}


async def get_variable_configs(schema: str, table: str) -> dict[str, dict]:
    conn = _CFG_CONN.get(None)
    if conn is None:
        raise RuntimeError("Missing DB connection for config context")
    has_table_col = await conn.fetchval(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'variable_configs'
          AND column_name = 'table_name'
        LIMIT 1
        """
    )
    if has_table_col:
        rows = await conn.fetch(
            """
            SELECT *
            FROM variable_configs
            WHERE schema_name = $1
              AND table_name IN ($2, '*')
            ORDER BY (table_name <> '*') DESC, updated_at DESC
            """,
            schema,
            table,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT *
            FROM variable_configs
            WHERE schema_name = $1
            ORDER BY updated_at DESC
            """,
            schema,
        )
    out = {}
    for r in rows:
        vn = r["variable_name"]
        if vn not in out:
            out[vn] = dict(r)
    return out


async def _get_system_setting(conn: asyncpg.Connection, key: str, default: str) -> str:
    try:
        val = await conn.fetchval("SELECT value FROM system_settings WHERE key = $1 LIMIT 1", key)
        if val is None:
            return default
        return str(val)
    except Exception:
        return default


async def apply_admin_rules(conn: asyncpg.Connection, user, query_context: dict | None = None) -> dict:
    context = query_context or {}
    user_email = str(getattr(user, "username", "") or "")
    
    action = str(context.get("action") or "query").lower()
    if action == "dataset_access":
        action_type = "query"
    else:
        action_type = action

    requested_rows = int(context.get("requested_rows") or 0)
    filters = context.get("filters")

    # Delegate validation entirely to our Central Access Control Engine
    res = await check_user_access(
        conn=conn,
        user=user,
        action_type=action_type,
        rows_requested=requested_rows,
        filters=filters
    )
    return {
        "ok": res.get("allowed", False),
        "plan": res.get("plan"),
        "max_queries_per_day": res.get("limits", {}).get("max_queries_per_day", 1000),
        "max_rows_per_day": res.get("limits", {}).get("max_rows_per_day", 100000),
        "daily_queries": 0,  # Legacy return parameter
        "daily_rows": 0,     # Legacy return parameter
    }


async def apply_config(schema, table, user, columns, rows):
    conn = _CFG_CONN.get(None)
    filters = _CFG_FILTERS.get()
    labels = _CFG_LABELS.get() or {}
    if conn is None:
        raise RuntimeError("Missing DB connection for config context")

    user_role = getattr(user, "role", "user")

    # Table visibility check
    dataset_cfg = await get_dataset_configs(schema)
    table_cfg = dataset_cfg.get(table)
    if table_cfg is not None and table_cfg.get("show_table_to_users") is False:
        raise TableHidden()

    if not columns and rows is None:
        return [], None

    # Delegate column and filter security policies to the privacy guard
    allowed_columns = await check_columns_and_filters(
        conn=conn,
        schema=schema,
        table=table,
        user_role=user_role,
        columns=columns,
        filters=filters
    )

    if rows is None:
        return allowed_columns, None

    # Delegate cell suppression and value labels mapping to the privacy guard
    try:
        result = await apply_privacy_and_labeling(
            conn=conn,
            schema=schema,
            table=table,
            user_role=user_role,
            columns=allowed_columns,
            rows=rows,
            labels=labels
        )
    except HTTPException as e:
        if isinstance(e.detail, dict) and e.detail.get("error") == "Cell Suppression Applied":
            raise HTTPException(status_code=403, detail="Suppressed")
        raise

    return allowed_columns, result



@app.get("/surveys")
async def list_surveys(request: Request, current_user=Depends(get_current_user)):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "dataset_access"})
        grouped = await _group_schemas_by_survey(conn)
        out = []
        for v in grouped.values():
            datasets = v["datasets"]
            if len(datasets) > 1:
                datasets = [d for d in datasets if d["schema"] != v["survey"]]
            visible_count = 0
            for d in datasets:
                rows = await conn.fetch(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                      AND table_schema = $1
                    ORDER BY table_name
                    """,
                    d["schema"],
                )
                tokens = _set_apply_context(conn=conn)
                try:
                    has_visible = False
                    for r in rows:
                        tname = r["table_name"]
                        if _is_internal_table(tname):
                            continue
                        try:
                            await apply_config(d["schema"], tname, current_user, [], None)
                            has_visible = True
                            break
                        except TableHidden:
                            continue
                    if has_visible:
                        visible_count += 1
                finally:
                    _reset_apply_context(tokens)
            out.append({"survey": v["survey"], "display_name": v["display_name"], "dataset_count": visible_count})
        return out


@app.get("/surveys/{survey}/datasets")
async def list_survey_datasets(request: Request, survey: str, current_user=Depends(get_current_user)):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "dataset_access"})
        grouped = await _group_schemas_by_survey(conn)
        g = grouped.get(survey)
        if not g:
            raise HTTPException(status_code=404, detail="Survey not found")
        datasets = g["datasets"]
        if len(datasets) > 1:
            datasets = [d for d in datasets if d["schema"] != survey]

        allowed = []
        for d in datasets:
            rows = await conn.fetch(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND table_schema = $1
                ORDER BY table_name
                """,
                d["schema"],
            )
            tokens = _set_apply_context(conn=conn)
            try:
                has_visible = False
                for r in rows:
                    tname = r["table_name"]
                    if _is_internal_table(tname):
                        continue
                    try:
                        await apply_config(d["schema"], tname, current_user, [], None)
                        has_visible = True
                        break
                    except TableHidden:
                        continue
                if has_visible:
                    allowed.append(d)
            finally:
                _reset_apply_context(tokens)
        return allowed


@app.get("/surveys/{survey}/{dataset}/tables")
async def list_survey_tables(request: Request, survey: str, dataset: str, current_user=Depends(get_current_user)):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "dataset_access"})
        grouped = await _group_schemas_by_survey(conn)
        g = grouped.get(survey)
        if not g:
            raise HTTPException(status_code=404, detail="Survey not found")
        ds = [d for d in g["datasets"] if d["schema"] == dataset]
        if not ds:
            raise HTTPException(status_code=404, detail="Dataset not found")

        rows = await conn.fetch(
            """
            SELECT table_name,
                   (SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_name = t.table_name AND table_schema = t.table_schema) as column_count,
                   (SELECT reltuples::bigint FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = t.table_name AND n.nspname = t.table_schema) as row_count
            FROM information_schema.tables t
            WHERE table_type = 'BASE TABLE'
              AND table_schema = $1
            ORDER BY table_name
            """,
            dataset,
        )

        tokens = _set_apply_context(conn=conn)
        try:
            out = []
            for r in rows:
                name = r["table_name"]
                if _is_internal_table(name):
                    continue
                try:
                    await apply_config(dataset, name, current_user, [], None)
                except TableHidden:
                    continue
                out.append(
                    {
                        "table_name": name,
                        "row_count": r["row_count"] or 0,
                        "column_count": r["column_count"] or 0,
                    }
                )
            return out
        finally:
            _reset_apply_context(tokens)


@app.get("/surveys/{survey}/{dataset}/{table}/columns")
async def list_survey_columns(request: Request, survey: str, dataset: str, table: str, current_user=Depends(get_current_user)):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        grouped = await _group_schemas_by_survey(conn)
        g = grouped.get(survey)
        if not g:
            raise HTTPException(status_code=404, detail="Survey not found")
        ds = [d for d in g["datasets"] if d["schema"] == dataset]
        if not ds:
            raise HTTPException(status_code=404, detail="Dataset not found")

        cols = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            """,
            dataset,
            table,
        )
        all_cols = [c["column_name"] for c in cols]
        tokens = _set_apply_context(conn=conn)
        try:
            try:
                allowed_cols, _ = await apply_config(dataset, table, current_user, all_cols, None)
            except TableHidden:
                raise HTTPException(status_code=404, detail="Table not found")
            return allowed_cols
        finally:
            _reset_apply_context(tokens)


@app.get("/surveys/{survey}/{dataset}/{table}/filter-metadata")
async def get_filter_metadata(request: Request, survey: str, dataset: str, table: str, current_user=Depends(get_current_user)):
    """Returns per-column metadata for the interactive filter builder (types, categories, statistics)."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        # Validate survey/dataset
        grouped = await _group_schemas_by_survey(conn)
        g = grouped.get(survey)
        if not g:
            raise HTTPException(status_code=404, detail="Survey not found")
        ds = [d for d in g["datasets"] if d["schema"] == dataset]
        if not ds:
            raise HTTPException(status_code=404, detail="Dataset not found")

        # Get columns from information_schema
        cols = await conn.fetch(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            """,
            dataset,
            table,
        )

        tokens = _set_apply_context(conn=conn)
        try:
            try:
                allowed_cols, _ = await apply_config(dataset, table, current_user, [c["column_name"] for c in cols], None)
            except TableHidden:
                raise HTTPException(status_code=404, detail="Table not found")
        finally:
            _reset_apply_context(tokens)
        allowed_set = set(allowed_cols)
        cols = [c for c in cols if c["column_name"] in allowed_set]

        # Get DDI variable metadata
        try:
            variables = await conn.fetch(
                f'SELECT variable_name, label, ddi_type, final_type FROM "{dataset}".variables WHERE table_name = $1',
                table,
            )
        except Exception:
            variables = []

        # Get categories
        try:
            categories = await conn.fetch(
                f'SELECT variable_name, value, label FROM "{dataset}".variable_categories WHERE table_name = $1',
                table,
            )
        except Exception:
            categories = []

        # Get statistics
        try:
            stats = await conn.fetch(
                f'SELECT variable_name, mean, min, max, stddev, unique_count FROM "{dataset}".variable_statistics WHERE table_name = $1',
                table,
            )
        except Exception:
            stats = []

    # Build lookup maps
    var_map = {v["variable_name"]: dict(v) for v in variables}
    cat_map: Dict[str, List[Dict]] = {}
    for c in categories:
        cat_map.setdefault(c["variable_name"], []).append({"value": c["value"], "label": c["label"]})
    stat_map = {s["variable_name"]: dict(s) for s in stats}

    # Build per-column result
    result = []
    for c in cols:
        col_name = c["column_name"]
        pg_type = c["data_type"]
        var_info = var_map.get(col_name, {})
        col_cats = cat_map.get(col_name, [])
        col_stats = stat_map.get(col_name)

        # Determine filter type: numeric, categorical, text
        ddi_type = (var_info.get("ddi_type") or "").lower()
        final_type = (var_info.get("final_type") or "").lower()
        is_numeric = pg_type in ("integer", "bigint", "smallint", "numeric", "double precision", "real") or "numeric" in ddi_type or "int" in final_type or "float" in final_type
        is_categorical = len(col_cats) > 0

        if is_categorical:
            filter_type = "categorical"
        elif is_numeric:
            filter_type = "numeric"
        else:
            filter_type = "text"

        entry = {
            "column_name": col_name,
            "pg_type": pg_type,
            "label": var_info.get("label") or col_name,
            "filter_type": filter_type,
        }
        if col_cats:
            entry["categories"] = col_cats
        if col_stats:
            entry["statistics"] = {
                "min": col_stats.get("min"),
                "max": col_stats.get("max"),
                "mean": col_stats.get("mean"),
                "stddev": col_stats.get("stddev"),
                "unique_count": col_stats.get("unique_count"),
            }
        result.append(entry)

    return result

@app.get("/surveys/{survey}/{dataset}/{table}/query")
async def query_survey_table(
    request: Request,
    survey: str,
    dataset: str,
    table: str,
    columns: str = "",
    filters: str = "",
    limit: int = 100,
    offset: int = 0,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        grouped = await _group_schemas_by_survey(conn)
        g = grouped.get(survey)
        if not g:
            raise HTTPException(status_code=404, detail="Survey not found")
        ds = [d for d in g["datasets"] if d["schema"] == dataset]
        if not ds:
            raise HTTPException(status_code=404, detail="Dataset not found")

    return await query_table(
        request=request,
        schema=dataset,
        table=table,
        columns=columns,
        filters=filters,
        limit=limit,
        offset=offset,
        current_user=current_user,
    )


@app.get("/schemas/{schema}/datasets")
async def list_datasets_v2(
    request: Request,
    schema: str,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "dataset_access"})
        row = await _resolve_registry_schema(conn, schema)
        if not row:
            raise HTTPException(status_code=404, detail="Schema not found")
        schema_db = row["db_name"]

        rows = await conn.fetch(
            """
            SELECT dataset_schema, dataset_display_name
            FROM dataset_registry
            WHERE survey_schema = $1
            ORDER BY dataset_display_name
            """,
            schema_db,
        )

        out = []
        for r in rows:
            dataset_schema = r["dataset_schema"]
            trows = await conn.fetch(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND table_schema = $1
                ORDER BY table_name
                """,
                dataset_schema,
            )
            tokens = _set_apply_context(conn=conn)
            try:
                has_visible = False
                for tr in trows:
                    tn = tr["table_name"]
                    if _is_internal_table(tn):
                        continue
                    try:
                        await apply_config(dataset_schema, tn, current_user, [], None)
                        has_visible = True
                        break
                    except TableHidden:
                        continue
                if has_visible:
                    out.append({"dataset": dataset_schema, "display_name": r["dataset_display_name"]})
            finally:
                _reset_apply_context(tokens)
        return out


@app.get("/schemas/{schema}/{dataset}/tables")
async def list_tables_v2(
    request: Request,
    schema: str,
    dataset: str,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "dataset_access"})
        row = await _resolve_registry_schema(conn, schema)
        if not row:
            raise HTTPException(status_code=404, detail="Schema not found")
        schema_db = row["db_name"]

        ds = (dataset or "").strip()
        ds_row = await conn.fetchrow(
            """
            SELECT dataset_schema, dataset_display_name
            FROM dataset_registry
            WHERE survey_schema = $1
              AND (dataset_schema = $2 OR lower(dataset_display_name) = lower($2))
            LIMIT 1
            """,
            schema_db,
            ds,
        )
        if not ds_row:
            raise HTTPException(status_code=404, detail="Dataset not found")
        dataset_schema = ds_row["dataset_schema"]

        rows = await conn.fetch(
            """
            SELECT table_name,
                   (SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_name = t.table_name AND table_schema = t.table_schema) as column_count,
                   (SELECT reltuples::bigint FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = t.table_name AND n.nspname = t.table_schema) as row_count
            FROM information_schema.tables t
            WHERE table_type = 'BASE TABLE'
              AND table_schema = $1
            ORDER BY table_name
            """,
            dataset_schema,
        )

        hidden = {"dataset_metadata", "variables", "variable_categories", "variable_statistics"}
        tokens = _set_apply_context(conn=conn)
        try:
            out = []
            for r in rows:
                name = r["table_name"]
                if name in hidden:
                    continue
                try:
                    await apply_config(dataset_schema, name, current_user, [], None)
                except TableHidden:
                    continue
                out.append(
                    {
                        "table_name": name,
                        "row_count": r["row_count"] or 0,
                        "column_count": r["column_count"] or 0,
                    }
                )
            return out
        finally:
            _reset_apply_context(tokens)


@app.get("/metadata/{schema}/{dataset}")
async def get_metadata_v2(
    request: Request,
    schema: str,
    dataset: str,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "dataset_access"})
        row = await _resolve_registry_schema(conn, schema)
        if not row:
            raise HTTPException(status_code=404, detail="Schema not found")
        schema_db = row["db_name"]

        ds = (dataset or "").strip()
        ds_row = await conn.fetchrow(
            """
            SELECT dataset_schema, dataset_display_name
            FROM dataset_registry
            WHERE survey_schema = $1
              AND (dataset_schema = $2 OR lower(dataset_display_name) = lower($2))
            LIMIT 1
            """,
            schema_db,
            ds,
        )
        if not ds_row:
            raise HTTPException(status_code=404, detail="Dataset not found")
        dataset_schema = ds_row["dataset_schema"]

        try:
            meta = await conn.fetchrow(f'SELECT * FROM "{dataset_schema}".dataset_metadata ORDER BY id DESC LIMIT 1')
            variables = await conn.fetch(f'SELECT * FROM "{dataset_schema}".variables ORDER BY table_name, variable_name')
            categories = await conn.fetch(f'SELECT * FROM "{dataset_schema}".variable_categories')
            stats = await conn.fetch(f'SELECT * FROM "{dataset_schema}".variable_statistics')
        except Exception:
            # Sub-schema might not be initialized yet
            meta = None
            variables = []
            categories = []
            stats = []

    dataset_folder = dataset_schema.split("__", 1)[1] if "__" in dataset_schema else dataset_schema
    downloads_enabled = (await _get_system_setting(conn, "enable_downloads", await _get_system_setting(conn, "features.enable_downloads", "true"))).strip().lower() == "true"
    ddi_url = f"/downloads/{schema_db}/{dataset_schema}/ddi" if downloads_enabled else None
    micro_url = f"/downloads/{schema_db}/{dataset_schema}/microdata.zip" if downloads_enabled else None

    return {
        "survey": {"db_name": schema_db, "display_name": row["display_name"]},
        "dataset": {"schema": dataset_schema, "display_name": ds_row["dataset_display_name"], "folder": dataset_folder},
        "downloads": {"ddi": ddi_url, "microdata": micro_url},
        "study_description": dict(meta) if meta else None,
        "variables": [dict(v) for v in variables],
        "variable_categories": [dict(c) for c in categories],
        "variable_statistics": [dict(s) for s in stats],
    }


@app.get("/downloads/{schema}/{dataset}/ddi")
async def download_ddi(request: Request, schema: str, dataset: str, current_user=Depends(get_current_user)):
    survey_schema = (schema or "").strip()
    dataset_schema = (dataset or "").strip()
    if not survey_schema or not dataset_schema:
        raise HTTPException(status_code=400, detail="Invalid request")

    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "download", "requested_rows": 0})
    dataset_folder = dataset_schema.split("__", 1)[1] if "__" in dataset_schema else dataset_schema
    upload_root = Path(os.getenv("UPLOAD_DIR") or "uploads").resolve()
    ddi_path = upload_root / survey_schema / dataset_folder / "ddi.xml"
    if not ddi_path.exists():
        raise HTTPException(status_code=404, detail="DDI not found")
    try:
        async with pool.acquire() as conn:
            size_bytes = ddi_path.stat().st_size
            await conn.execute(
                "INSERT INTO download_logs (file_name, user_email, size_bytes, created_at) VALUES ($1, $2, $3, NOW())",
                "ddi.xml",
                current_user.username,
                size_bytes,
            )
            await log_usage(conn, current_user.username, "/downloads/ddi", schema, dataset, 0, size_bytes)
    except Exception:
        pass
    return FileResponse(str(ddi_path), filename="ddi.xml")


@app.get("/downloads/{schema}/{dataset}/microdata.zip")
async def download_microdata(request: Request, schema: str, dataset: str, current_user=Depends(get_current_user)):
    survey_schema = (schema or "").strip()
    dataset_schema = (dataset or "").strip()
    if not survey_schema or not dataset_schema:
        raise HTTPException(status_code=400, detail="Invalid request")

    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "download", "requested_rows": 0})
    dataset_folder = dataset_schema.split("__", 1)[1] if "__" in dataset_schema else dataset_schema
    upload_root = Path(os.getenv("UPLOAD_DIR") or "uploads").resolve()
    processed_dir = upload_root / survey_schema / dataset_folder / "processed"
    if not processed_dir.exists():
        raise HTTPException(status_code=404, detail="Processed data not found")

    tmp_dir = Path(tempfile.mkdtemp(prefix="statxtract_microdata_"))
    zip_path = tmp_dir / f"{dataset_folder}_microdata.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in processed_dir.rglob("*"):
            if p.is_file():
                zf.write(p, arcname=str(p.relative_to(processed_dir)))

    try:
        async with pool.acquire() as conn:
            size_bytes = zip_path.stat().st_size
            await conn.execute(
                "INSERT INTO download_logs (file_name, user_email, size_bytes, created_at) VALUES ($1, $2, $3, NOW())",
                zip_path.name,
                current_user.username,
                size_bytes,
            )
            await log_usage(conn, current_user.username, "/downloads/microdata.zip", schema, dataset, 0, size_bytes)
    except Exception:
        pass
    return FileResponse(str(zip_path), filename=zip_path.name)


@app.get("/schemas/{schema}/years")
async def list_years(
    request: Request,
    schema: str,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        row = await _resolve_registry_schema(conn, schema)
        if not row:
            raise HTTPException(status_code=404, detail="Schema not found")
        schema_db = row["db_name"]

    return []


@app.get("/schemas/{schema}/{year}/datasets")
async def list_datasets(
    request: Request,
    schema: str,
    year: str,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "dataset_access"})
        row = await _resolve_registry_schema(conn, schema)
        if not row:
            raise HTTPException(status_code=404, detail="Schema not found")
        schema_db = row["db_name"]

        rows = await conn.fetch(
            f"""
            SELECT dataset_display_name, dataset_db_name
            FROM "{schema_db}".dataset_registry
            WHERE year = $1
            ORDER BY dataset_display_name
            """,
            (year or "").strip(),
        )
        out = []
        for r in rows:
            dataset_db = r["dataset_db_name"]
            trows = await conn.fetch(
                f"""
                SELECT table_name
                FROM "{schema_db}".dataset_tables
                WHERE year = $1 AND dataset_db_name = $2
                ORDER BY table_name
                """,
                (year or "").strip(),
                dataset_db,
            )
            tokens = _set_apply_context(conn=conn)
            try:
                has_visible = False
                for tr in trows:
                    tn = tr["table_name"]
                    try:
                        await apply_config(schema_db, tn, current_user, [], None)
                        has_visible = True
                        break
                    except TableHidden:
                        continue
                if has_visible:
                    out.append({"display_name": r["dataset_display_name"], "db_name": dataset_db})
            finally:
                _reset_apply_context(tokens)
        return out


@app.get("/schemas/{schema}/{year}/{dataset}/tables")
async def list_tables(
    request: Request,
    schema: str,
    year: str,
    dataset: str,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "dataset_access"})
        row = await _resolve_registry_schema(conn, schema)
        if not row:
            raise HTTPException(status_code=404, detail="Schema not found")
        schema_db = row["db_name"]

        rows = await conn.fetch(
            f"""
            SELECT table_name, level_display_name, row_count, column_count
            FROM "{schema_db}".dataset_tables
            WHERE year = $1 AND dataset_db_name = $2
            ORDER BY table_name
            """,
            (year or "").strip(),
            (dataset or "").strip(),
        )
        tokens = _set_apply_context(conn=conn)
        try:
            out = []
            for r in rows:
                tname = r["table_name"]
                try:
                    await apply_config(schema_db, tname, current_user, [], None)
                except TableHidden:
                    continue
                out.append(
                    {
                        "table_name": tname,
                        "display_name": r["level_display_name"] or tname,
                        "row_count": r["row_count"] or 0,
                        "column_count": r["column_count"] or 0,
                    }
                )
            return out
        finally:
            _reset_apply_context(tokens)


def _parse_filters_param(filters: str) -> list[tuple[str, str, str]]:
    parts: list[tuple[str, str, str]] = []
    if not filters:
        return parts
    for chunk in (filters or "").split(";"):
        c = chunk.strip()
        if not c:
            continue
        m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(=|!=|>=|<=|>|<|IN|LIKE)\s*(.+)$", c, re.IGNORECASE)
        if not m:
            raise ValueError(f"Invalid filter: {c}")
        col, op, val = m.group(1), m.group(2).upper(), m.group(3).strip()
        parts.append((col, op, val))
    return parts


@app.get("/schemas/{schema}/{year}/{dataset}/{table}/query")
async def query_dataset_table(
    request: Request,
    schema: str,
    year: str,
    dataset: str,
    table: str,
    columns: str = "",
    filters: str = "",
    limit: int = 100,
    offset: int = 0,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await apply_admin_rules(conn, current_user, {"action": "query", "requested_rows": int(limit)})
        schema_row = await _resolve_registry_schema(conn, schema)
        if not schema_row:
            raise HTTPException(status_code=404, detail="Schema not found")
        schema_db = schema_row["db_name"]

        exists = await conn.fetchval(
            f"""
            SELECT 1
            FROM "{schema_db}".dataset_tables
            WHERE year = $1 AND dataset_db_name = $2 AND table_name = $3
            LIMIT 1
            """,
            (year or "").strip(),
            (dataset or "").strip(),
            (table or "").strip(),
        )
        if not exists:
            raise HTTPException(status_code=404, detail="Table not found")

        allowed_cols = await conn.fetch(
            f"""
            SELECT variable_name, final_type
            FROM "{schema_db}".variable_dictionary
            WHERE year = $1 AND dataset_db_name = $2 AND table_name = $3
            """,
            (year or "").strip(),
            (dataset or "").strip(),
            (table or "").strip(),
        )
        all_columns = [r["variable_name"] for r in allowed_cols]
        type_map = {r["variable_name"]: (r["final_type"] or "").upper() for r in allowed_cols}

        tokens = _set_apply_context(conn=conn, filters=filters)
        try:
            try:
                allowed_columns, _ = await apply_config(schema_db, table, current_user, all_columns, None)
            except TableHidden:
                raise HTTPException(status_code=404, detail="Table not found")
        finally:
            _reset_apply_context(tokens)

        allowed_set = set(allowed_columns)
        if not allowed_set:
            raise HTTPException(status_code=403, detail="No columns available")

        selected_columns: list[str] = []
        if columns and columns.strip() and columns.strip() != "*":
            for c in columns.split(","):
                col = c.strip()
                if not col:
                    continue
                if col not in allowed_set:
                    raise HTTPException(status_code=400, detail=f"Invalid column: {col}")
                selected_columns.append(col)
        else:
            selected_columns = allowed_columns

        select_cols = [f'"{c}"' for c in selected_columns]

        where_parts: list[str] = []
        values: list[Any] = []
        try:
            parsed = _parse_filters_param(filters)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        for col, op, val in parsed:
            if col not in allowed_set:
                raise HTTPException(status_code=400, detail=f"Invalid filter column: {col}")
            ftype = type_map.get(col, "")
            if op == "IN":
                cleaned = val.strip().strip("()[]")
                items = [x.strip().strip("\"'") for x in cleaned.split(",") if x.strip()]
                if not items:
                    raise HTTPException(status_code=400, detail=f"Empty IN list for {col}")
                placeholders = []
                for item in items:
                    if ftype in {"INTEGER", "FLOAT"}:
                        try:
                            values.append(float(item))
                        except Exception:
                            values.append(None)
                    else:
                        values.append(item)
                    placeholders.append(f"${len(values)}")
                where_parts.append(f'"{col}" IN ({", ".join(placeholders)})')
            elif op == "LIKE":
                values.append(val.strip().strip("\"'"))
                where_parts.append(f'"{col}" LIKE ${len(values)}')
            else:
                raw = val.strip().strip("\"'")
                if ftype == "INTEGER":
                    try:
                        values.append(int(float(raw)))
                    except Exception:
                        values.append(None)
                elif ftype == "FLOAT":
                    try:
                        values.append(float(raw))
                    except Exception:
                        values.append(None)
                else:
                    values.append(raw)
                where_parts.append(f'"{col}" {op} ${len(values)}')

        where_sql = "" if not where_parts else " WHERE " + " AND ".join(where_parts)
        max_limit = 1000
        default_row_limit = await _get_system_setting(conn, "default_row_limit", await _get_system_setting(conn, "platform.default_row_limit", "1000"))
        try:
            configured_limit = max(1, min(int(default_row_limit), max_limit))
        except Exception:
            configured_limit = max_limit
        role_name = _normalize_role(getattr(current_user, "role", "user"))
        if role_name == "admin":
            safe_limit = max(1, min(int(limit), max_limit))
        else:
            safe_limit = max(1, min(int(limit), configured_limit))
        safe_offset = max(0, int(offset))
        sql = f'SELECT {", ".join(select_cols)} FROM "{schema_db}"."{table}"{where_sql} LIMIT {safe_limit} OFFSET {safe_offset}'
        rows = await conn.fetch(sql, *values)

        labels = {}
        try:
            for col in selected_columns:
                cats = await conn.fetch(
                    f'SELECT value, label FROM "{schema_db}".variable_categories WHERE table_name = $1 AND variable_name = $2',
                    table,
                    col,
                )
                if cats:
                    labels[col] = {str(r["value"]): r["label"] for r in cats}
        except Exception:
            labels = {}

        data = [dict(r) for r in rows]
        tokens = _set_apply_context(conn=conn, labels=labels)
        try:
            _, filtered_data = await apply_config(schema_db, table, current_user, selected_columns, data)
        except HTTPException:
            await log_usage(conn, current_user.username, f"/schemas/{schema_db}/{year}/{dataset}/{table}/query", schema_db, table, 0, 0)
            raise
        finally:
            _reset_apply_context(tokens)

        await log_usage(
            conn,
            current_user.username,
            f"/schemas/{schema_db}/{year}/{dataset}/{table}/query",
            schema_db,
            table,
            len(filtered_data),
            len(json.dumps(filtered_data, default=str).encode()),
        )
        return filtered_data


@app.get("/metadata/{schema}/{year}/{dataset}")
async def get_dataset_metadata(
    request: Request,
    schema: str,
    year: str,
    dataset: str,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        schema_row = await _resolve_registry_schema(conn, schema)
        if not schema_row:
            raise HTTPException(status_code=404, detail="Schema not found")
        schema_db = schema_row["db_name"]

        meta = await conn.fetchrow(
            f"""
            SELECT *
            FROM "{schema_db}".dataset_metadata
            WHERE year = $1 AND dataset_db_name = $2
            """,
            (year or "").strip(),
            (dataset or "").strip(),
        )
        variables = await conn.fetch(
            f"""
            SELECT *
            FROM "{schema_db}".variable_dictionary
            WHERE year = $1 AND dataset_db_name = $2
            ORDER BY table_name, variable_name
            """,
            (year or "").strip(),
            (dataset or "").strip(),
        )
        categories = await conn.fetch(
            f"""
            SELECT *
            FROM "{schema_db}".variable_categories
            WHERE year = $1 AND dataset_db_name = $2
            """,
            (year or "").strip(),
            (dataset or "").strip(),
        )
        stats = await conn.fetch(
            f"""
            SELECT *
            FROM "{schema_db}".variable_statistics
            WHERE year = $1 AND dataset_db_name = $2
            """,
            (year or "").strip(),
            (dataset or "").strip(),
        )

    return {
        "schema": {"display_name": schema_row["display_name"], "db_name": schema_db},
        "year": (year or "").strip(),
        "dataset_db_name": (dataset or "").strip(),
        "study": dict(meta) if meta else None,
        "variables": [dict(v) for v in variables],
        "categories": [dict(c) for c in categories],
        "statistics": [dict(s) for s in stats],
    }


@app.get("/datasets")
async def list_schemas_and_tables(request: Request, current_user=Depends(get_current_user)):
    try:
        pool = request.app.state.db
        async with pool.acquire() as conn:
            await apply_admin_rules(conn, current_user, {"action": "dataset_access"})
            all_schemas = await conn.fetch("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
                ORDER BY schema_name;
            """)

            tables_data = await conn.fetch("""
                SELECT table_schema, table_name,
                       (SELECT COUNT(*) FROM information_schema.columns 
                        WHERE table_name = t.table_name AND table_schema = t.table_schema) as column_count,
                       (SELECT reltuples::bigint FROM pg_class c 
                        JOIN pg_namespace n ON n.oid = c.relnamespace 
                        WHERE c.relname = t.table_name AND n.nspname = t.table_schema) as row_count
                FROM information_schema.tables t
                WHERE table_type = 'BASE TABLE' 
                AND table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name;
            """)

            result = {schema_row["schema_name"]: [] for schema_row in all_schemas}
            tokens = _set_apply_context(conn=conn)
            try:
                for row in tables_data:
                    if _is_internal_table(row["table_name"]):
                        continue
                    try:
                        await apply_config(row["table_schema"], row["table_name"], current_user, [], None)
                    except TableHidden:
                        continue
                    result[row["table_schema"]].append(
                        {
                            "table_name": row["table_name"],
                            "row_count": row["row_count"] or 0,
                            "column_count": row["column_count"] or 0,
                        }
                    )
            finally:
                _reset_apply_context(tokens)
            print(f"DEBUG: Returning {len(result)} schemas (including empty ones): {list(result.keys())}")
            return result

    except Exception as e:
        print(f"ERROR in list_schemas_and_tables: {e}")
        return {"error": str(e)}


@app.get("/datasets/{schema}/{table}/columns")
async def get_columns(
    request: Request,
    schema: str,
    table: str,
    details: bool = False,
    current_user=Depends(get_current_user),
):
    """Returns list of columns for the given table with enhanced debugging and validation."""
    try:
        pool = request.app.state.db
        async with pool.acquire() as conn:
            # First, verify the table exists
            table_check = await conn.fetch(
                """
                SELECT table_name, table_schema 
                FROM information_schema.tables 
                WHERE table_schema = $1 AND table_name = $2
                AND table_type = 'BASE TABLE'
            """,
                schema,
                table,
            )

            if not table_check:
                print(f"DEBUG: Table {schema}.{table} not found in information_schema!")
                # Try case-insensitive search
                case_insensitive_check = await conn.fetch(
                    """
                    SELECT table_name, table_schema 
                    FROM information_schema.tables 
                    WHERE LOWER(table_schema) = LOWER($1) AND LOWER(table_name) = LOWER($2)
                    AND table_type = 'BASE TABLE'
                """,
                    schema,
                    table,
                )

                if case_insensitive_check:
                    actual_schema = case_insensitive_check[0]["table_schema"]
                    actual_table = case_insensitive_check[0]["table_name"]
                    print(
                        f"DEBUG: Found table with different case: {actual_schema}.{actual_table}"
                    )
                    schema, table = actual_schema, actual_table
                else:
                    return {"error": f"Table {schema}.{table} not found"}

            print(f"DEBUG: Table verified: {schema}.{table}")

            # Get columns with detailed information for debugging
            rows = await conn.fetch(
                """
                SELECT column_name, data_type, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position;
            """,
                schema,
                table,
            )

            if not rows:
                print(f"DEBUG: No columns found for {schema}.{table}")
                # Fallback: Try direct table query to get actual columns
                try:
                    sample_row = await conn.fetchrow(
                        f'SELECT * FROM "{schema}"."{table}" LIMIT 1'
                    )
                    if sample_row:
                        columns = list(sample_row.keys())
                        print(
                            f"DEBUG: Using direct query method, found columns: {columns}"
                        )
                        tokens = _set_apply_context(conn=conn)
                        try:
                            try:
                                allowed_cols, _ = await apply_config(schema, table, current_user, columns, None)
                            except TableHidden:
                                raise HTTPException(status_code=404, detail="Table not found")
                        finally:
                            _reset_apply_context(tokens)
                        if details:
                            allowed_set = set(allowed_cols)
                            return [
                                {"name": c, "type": None, "position": i + 1}
                                for i, c in enumerate(columns)
                                if c in allowed_set
                            ]
                        return allowed_cols
                    else:
                        print(f"DEBUG: Table {schema}.{table} exists but has no data")
                        return []
                except Exception as direct_error:
                    print(f"DEBUG: Direct query also failed: {direct_error}")
                    return {"error": f"Could not retrieve columns for {schema}.{table}"}

            columns = [r["column_name"] for r in rows]
            column_details = [
                {
                    "name": r["column_name"],
                    "type": r["data_type"],
                    "position": r["ordinal_position"],
                }
                for r in rows
            ]

            print(f"DEBUG: Found {len(columns)} columns for {schema}.{table}")
            print(f"DEBUG: Columns: {columns}")
            print(f"DEBUG: Column details: {column_details}")

            tokens = _set_apply_context(conn=conn)
            try:
                try:
                    allowed_cols, _ = await apply_config(schema, table, current_user, columns, None)
                except TableHidden:
                    raise HTTPException(status_code=404, detail="Table not found")
            finally:
                _reset_apply_context(tokens)

            if details:
                allowed_set = set(allowed_cols)
                return [d for d in column_details if d["name"] in allowed_set]
            return allowed_cols

    except Exception as e:
        print(f"ERROR in get_columns for {schema}.{table}: {e}")
        return {"error": str(e)}


def fix_filter_case_sensitivity(filters: str, column_map: dict, raw_cols_with_labels: set = None) -> str:
    """Fix case sensitivity issues and handle label redirection for WHERE clauses."""
    if not filters:
        return filters
    if raw_cols_with_labels is None:
        raw_cols_with_labels = set()

    fixed_filters = filters
    sorted_columns = sorted(column_map.items(), key=lambda x: len(x[0]), reverse=True)

    for lower_col, actual_col in sorted_columns:
        # If this column has a _label redirected display version,
        # we still want the WHERE clause to hit the original raw column.
        # We do NOT add quotes here if it's in raw_cols_with_labels to let it be treated normally
        # or we quote the actual raw column name.
        pattern = r'(?<!")' + re.escape(lower_col) + r'(?!")\b'
        fixed_filters = re.sub(
            pattern, f'"{actual_col}"', fixed_filters, flags=re.IGNORECASE
        )

    return fixed_filters


def smart_quote_filters(filters: str, col_type_map: dict = None) -> str:
    """
    Auto-quote unquoted string values in filter expressions.
    e.g. "status = active" -> "status = 'active'"
    Also quotes numbers if the target column is a text type.
    """
    if col_type_map is None:
        col_type_map = {}
        
    # Normalize map keys to lowercase
    col_type_map = {k.lower(): v.lower() for k, v in col_type_map.items()}

    SQL_KEYWORDS = {
        "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", 
        "IS", "IN", "LIKE", "BETWEEN", "ASC", "DESC"
    }
    
    TEXT_TYPES = {"text", "character varying", "varchar", "char", "character", "bpchar", "string"}

    def replace_func(match):
        col = match.group(1)
        op = match.group(2)
        val = match.group(3)
        
        # Clean column name (remove quotes if present)
        clean_col = col.replace('"', '').lower()
        col_type = col_type_map.get(clean_col, "")
        
        is_text_col = any(t in col_type for t in TEXT_TYPES)
        
        # Check if it's a number
        is_number = val.replace('.', '', 1).isdigit()
        
        # If it's a text column and value is number, FORCE QUOTES
        if is_text_col and is_number:
            return f"{col}{op}'{val}'"
            
        # Check if it's a number (and not a text column)
        if is_number:
            return match.group(0)
            
        # Check if it's a keyword
        if val.upper() in SQL_KEYWORDS:
            return match.group(0)
            
        return f"{col}{op}'{val}'"

    # Pattern: 
    # Group 1: Column (alphanumeric + underscore + optional quotes)
    # Group 2: Operator (=, !=, <>, >=, <=, <, >, LIKE) with surrounding spaces included
    # Group 3: Value (alphanumeric + dots)
    pattern = r'([a-zA-Z0-9_"]+)(\s*(?:=|!=|<>|>=|<=|<|>|LIKE)\s*)([a-zA-Z0-9_\.]+)(?=\s|$|;|AND|OR|\))'
    
    return re.sub(pattern, replace_func, filters, flags=re.IGNORECASE)


@app.get("/datasets/{schema}/{table}/query")
async def query_table(
    request: Request,
    schema: str,
    table: str,
    columns: str = "",
    filters: str = "",
    limit: int = 100,
    offset: int = 0,
    format: str | None = None,
    current_user=Depends(get_current_user),  # <-- Add user dependency!
):
    try:
        is_export = format in ["csv", "excel", "pdf", "json"]
        action_type = "export" if is_export else "query"
        export_fmt = format.lower() if is_export else None

        pool = request.app.state.db
        async with pool.acquire() as conn:
            await apply_admin_rules(
                conn, 
                current_user, 
                {
                    "action": action_type, 
                    "requested_rows": int(limit),
                    "filters": filters
                }
            )

            # Additional check for download limits if exporting
            if is_export:
                from security.plan_enforcer import get_and_enforce_plan_limits
                from security.usage_tracker import check_download_limits
                user_role = str(current_user.role)
                plan_limits = await get_and_enforce_plan_limits(conn, current_user.username, user_role)
                await check_download_limits(conn, current_user.username, plan_limits)
            table_check = await conn.fetch(
                """
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = $1 AND table_name = $2
            """,
                schema,
                table,
            )

            if not table_check:
                raise HTTPException(status_code=404, detail=f"Table {schema}.{table} not found")

            actual_columns = await conn.fetch(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position;
            """,
                schema,
                table,
            )

            column_map = {
                col["column_name"].lower(): col["column_name"] for col in actual_columns
            }
            column_type_map = {
                col["column_name"].lower(): col["data_type"] for col in actual_columns
            }
            print(f"DEBUG: Column map for case sensitivity: {column_map}")
            print(f"DEBUG: Column type map: {column_type_map}")

            all_cols = [c["column_name"] for c in actual_columns]
            label_cols = set(c for c in all_cols if c.endswith("_label"))
            raw_cols_with_labels = set(c[:-6] for c in label_cols)
            raw_cols = [c for c in all_cols if c not in label_cols]

            fixed_filters = None
            if filters:
                print(f"DEBUG: Original filters: {filters}")
                fixed_filters = fix_filter_case_sensitivity(filters, column_map, raw_cols_with_labels)
                fixed_filters = smart_quote_filters(fixed_filters, column_type_map)
                try:
                    meta_map = await get_column_labels(conn, table, schema)
                    def resolve_code_to_label(match):
                        col_full = match.group(1)
                        col_name_stripped = col_full.replace('"', '')
                        op = match.group(2)
                        val_quoted = match.group(3)
                        val = val_quoted.strip("'\"")
                        
                        col_type = column_type_map.get(col_name_stripped.lower(), "").lower()
                        is_text_db = any(t in col_type for t in ["text", "char", "string"])
                        if is_text_db:
                            cats = meta_map.get(col_name_stripped)
                            if cats:
                                val_norm = val
                                try: val_norm = str(int(float(val)))
                                except: pass
                                for code, label in cats.items():
                                    if not label: continue
                                    code_norm = str(code)
                                    try: code_norm = str(int(float(code)))
                                    except: pass
                                    if (str(code) == val or code_norm == val_norm) and val != "":
                                        label_esc = label.replace("'", "''")
                                        return f'"{col_name_stripped}"{op}\'{label_esc}\''
                        return match.group(0)

                    filter_pattern = r'("[^"]+"|[a-zA-Z0-9_]+)\s*(=|!=|<>)\s*(\'[^\']*\'|"[^"]*"|[a-zA-Z0-9_\.]+)'
                    fixed_filters = re.sub(filter_pattern, resolve_code_to_label, fixed_filters)
                except Exception as e:
                    print(f"⚠️ Fallback filter mapping failed: {e}")
                print(f"DEBUG: Fixed filters: {fixed_filters}")

            tokens = _set_apply_context(conn=conn, filters=fixed_filters)
            try:
                try:
                    allowed_columns, _ = await apply_config(schema, table, current_user, raw_cols, None)
                except TableHidden:
                    raise HTTPException(status_code=404, detail=f"Table {schema}.{table} not found")
            finally:
                _reset_apply_context(tokens)

            if not allowed_columns:
                raise HTTPException(status_code=403, detail="No columns available")

            allowed_set = set(allowed_columns)
            selected_columns = []
            if columns and columns.strip() and columns.strip() != "*":
                for col in columns.split(","):
                    col = col.strip()
                    if not col:
                        continue
                    col_case_matched = column_map.get(col.lower(), col)
                    if col_case_matched not in allowed_set:
                        raise HTTPException(status_code=400, detail=f"Column not allowed or invalid: {col_case_matched}")
                    selected_columns.append(col_case_matched)
            else:
                selected_columns = allowed_columns

            col_list = []
            for c in selected_columns:
                if c in raw_cols_with_labels:
                    col_list.append(f'"{c}_label" AS "{c}"')
                else:
                    col_list.append(f'"{c}"')

            query = f'SELECT {", ".join(col_list)} FROM "{schema}"."{table}"'
            if fixed_filters:
                query += f" WHERE {fixed_filters}"
            max_limit = 1000
            default_row_limit = await _get_system_setting(conn, "default_row_limit", await _get_system_setting(conn, "platform.default_row_limit", "1000"))
            try:
                configured_limit = max(1, min(int(default_row_limit), max_limit))
            except Exception:
                configured_limit = max_limit
            role_name = _normalize_role(getattr(current_user, "role", "user"))
            if role_name == "admin":
                safe_limit = max(1, min(int(limit), max_limit))
            else:
                safe_limit = max(1, min(int(limit), configured_limit))
            safe_offset = max(0, int(offset))
            query += f" LIMIT {safe_limit} OFFSET {safe_offset}"

            print(f"DEBUG: Executing query: {query}")
            rows = await conn.fetch(query)
            result = [dict(r) for r in rows]
            label_map = {}
            try:
                label_map = await get_column_labels(conn, table, schema)
            except Exception:
                label_map = {}

            labels = {c: (label_map.get(c) or {}) for c in selected_columns}
            tokens = _set_apply_context(conn=conn, labels=labels)
            try:
                _, filtered = await apply_config(schema, table, current_user, selected_columns, result)
            except HTTPException:
                from security.usage_tracker import log_api_usage
                await log_api_usage(
                    conn,
                    current_user.username,
                    f"/datasets/{schema}/{table}/query",
                    schema,
                    table,
                    0,
                    0,
                    status="failure",
                    filters=filters
                )
                raise
            finally:
                _reset_apply_context(tokens)

            from security.usage_tracker import log_api_usage
            await log_api_usage(
                conn,
                current_user.username,
                f"/datasets/{schema}/{table}/query",
                schema,
                table,
                len(filtered),
                len(json.dumps(filtered, default=str).encode()),
                filters=filters
            )
            print(f"DEBUG: Query returned {len(filtered)} rows")

            if is_export:
                # Log file download details to database
                from security.usage_tracker import log_file_download
                import io

                filename = f"{table}_query.{export_fmt}"
                if export_fmt == "csv":
                    import csv
                    output = io.StringIO()
                    writer = csv.DictWriter(output, fieldnames=filtered[0].keys() if filtered else [])
                    writer.writeheader()
                    writer.writerows(filtered)
                    output.seek(0)
                    content = output.getvalue().encode("utf-8")
                    media_type = "text/csv"
                elif export_fmt == "json":
                    content = json.dumps(filtered, default=str, indent=2).encode("utf-8")
                    media_type = "application/json"
                elif export_fmt == "excel":
                    import pandas as pd
                    df = pd.DataFrame(filtered)
                    output = io.BytesIO()
                    df.to_excel(output, index=False, engine='openpyxl')
                    output.seek(0)
                    content = output.getvalue()
                    media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    filename = f"{table}_query.xlsx"
                elif export_fmt == "pdf":
                    from reportlab.lib.pagesizes import letter, landscape
                    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
                    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                    from reportlab.lib import colors

                    output = io.BytesIO()
                    doc = SimpleDocTemplate(output, pagesize=landscape(letter), rightMargin=20, leftMargin=20, topMargin=20, bottomMargin=20)
                    elements = []

                    styles = getSampleStyleSheet()
                    title_style = ParagraphStyle(
                        'TitleStyle',
                        parent=styles['Heading1'],
                        fontSize=14,
                        spaceAfter=10
                    )
                    cell_style = ParagraphStyle(
                        'CellStyle',
                        parent=styles['Normal'],
                        fontSize=7,
                        leading=9
                    )
                    header_style = ParagraphStyle(
                        'HeaderStyle',
                        parent=styles['Normal'],
                        fontSize=8,
                        leading=10,
                        textColor=colors.whitesmoke,
                        fontName='Helvetica-Bold'
                    )

                    elements.append(Paragraph(f"Dataset Export: {table} (Schema: {schema})", title_style))
                    elements.append(Spacer(1, 10))

                    if filtered:
                        headers = list(filtered[0].keys())
                        headers_subset = headers[:12]

                        table_data = []
                        table_data.append([Paragraph(h, header_style) for h in headers_subset])

                        for row in filtered[:100]:
                            row_cells = []
                            for h in headers_subset:
                                val = str(row.get(h, ""))
                                if len(val) > 40:
                                    val = val[:37] + "..."
                                row_cells.append(Paragraph(val, cell_style))
                            table_data.append(row_cells)

                        col_width = 750 / len(headers_subset)
                        t = Table(table_data, colWidths=[col_width]*len(headers_subset))
                        t.setStyle(TableStyle([
                            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#2E5BBA')),
                            ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                            ('VALIGN', (0,0), (-1,-1), 'TOP'),
                            ('INNERGRID', (0,0), (-1,-1), 0.5, colors.grey),
                            ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#2E5BBA')),
                            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#F5F7FA')]),
                            ('TOPPADDING', (0,0), (-1,-1), 4),
                            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
                        ]))
                        elements.append(t)

                        if len(filtered) > 100:
                            elements.append(Spacer(1, 10))
                            elements.append(Paragraph(f"... and {len(filtered) - 100} more rows (total {len(filtered)} rows exported)", styles['Italic']))
                    else:
                        elements.append(Paragraph("No data found.", styles['Normal']))

                    doc.build(elements)
                    content = output.getvalue()
                    media_type = "application/pdf"

                size_bytes = len(content)
                await log_file_download(
                    conn=conn,
                    file_name=filename,
                    user_email=current_user.username,
                    size_bytes=size_bytes,
                    dataset_schema=schema,
                    export_format=export_fmt,
                    rows_exported=len(filtered),
                    status="success"
                )

                return StreamingResponse(
                    io.BytesIO(content),
                    media_type=media_type,
                    headers={"Content-Disposition": f"attachment; filename={filename}"}
                )

            return filtered

    except HTTPException as e:
        # --- Suspicious Activity Escalation ---
        try:
            pool = request.app.state.db
            async with pool.acquire() as conn:
                user_email = current_user.username
                score_inc = 0
                violation_type = None
                detail_msg = ""
                
                # Check for Cell Suppression (403, Cell Suppression Applied or Suppressed)
                is_cell_sup = False
                if e.status_code == 403:
                    if isinstance(e.detail, dict) and e.detail.get("error") == "Cell Suppression Applied":
                        is_cell_sup = True
                        detail_msg = f"Cell suppression threshold violated: {e.detail.get('detail')}"
                    elif isinstance(e.detail, str) and e.detail == "Suppressed":
                        is_cell_sup = True
                        detail_msg = "Cell suppression threshold violated: Privacy suppression applied to query results."

                if is_cell_sup:
                    score_inc = 15
                    violation_type = "cell_suppression_violation"
                # Check for Privacy Violation (400, contains Privacy Violation or not configured as filterable)
                elif e.status_code == 400 and isinstance(e.detail, str) and ("Privacy Violation" in e.detail or "not configured as filterable" in e.detail):
                    score_inc = 20
                    violation_type = "privacy_violation"
                    detail_msg = e.detail
                # Check for Limit Exceeded (429, Usage Limit Exceeded)
                elif e.status_code == 429 and isinstance(e.detail, str) and "Usage Limit Exceeded" in e.detail:
                    score_inc = 5
                    violation_type = "blocked_query_retry"
                    detail_msg = e.detail

                if score_inc > 0:
                    await conn.execute(
                        "UPDATE users SET suspicious_score = COALESCE(suspicious_score, 0) + $1 WHERE email = $2",
                        score_inc, user_email
                    )
                    await conn.execute(
                        """
                        INSERT INTO suspicious_activity_logs (user_email, activity_type, risk_score, detail, created_at)
                        VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                        """,
                        user_email, violation_type, score_inc, detail_msg[:500]
                    )
                    if violation_type == "cell_suppression_violation":
                        from security.warning_manager import issue_governance_warning
                        await issue_governance_warning(
                            conn, user_email,
                            violation_type=violation_type,
                            message=detail_msg
                        )
                    from security.warning_manager import check_and_escalate_suspicious_score
                    await check_and_escalate_suspicious_score(conn, user_email)
        except Exception as db_err:
            print(f"Error logging query failure escalation: {db_err}")
        raise
    except Exception as e:
        print(f"ERROR in query_table: {e}")
        return {"error": str(e)}


# =================== UPLOAD HANDLING ===================

async def _wait_for_stable_file(
    file_path: str,
    *,
    min_bytes: int,
    stable_checks: int,
    interval_sec: float,
    timeout_sec: float,
) -> bool:
    loop = asyncio.get_running_loop()
    start = loop.time()
    stable = 0
    last_sig = None

    while (loop.time() - start) < timeout_sec:
        try:
            st = os.stat(file_path)
        except FileNotFoundError:
            stable = 0
            last_sig = None
            await asyncio.sleep(interval_sec)
            continue
        except Exception:
            stable = 0
            last_sig = None
            await asyncio.sleep(interval_sec)
            continue

        if st.st_size < min_bytes:
            stable = 0
            last_sig = None
            await asyncio.sleep(interval_sec)
            continue

        try:
            with open(file_path, "rb") as f:
                f.read(8)
        except Exception:
            stable = 0
            last_sig = None
            await asyncio.sleep(interval_sec)
            continue

        sig = (st.st_size, st.st_mtime_ns)
        if sig == last_sig:
            stable += 1
        else:
            stable = 0
            last_sig = sig

        if stable >= stable_checks:
            return True

        await asyncio.sleep(interval_sec)

    return False


async def _uploads_sav_watcher_loop() -> None:
    watch_dir = Path((os.getenv("UPLOADS_SAV_WATCHER_DIR") or UPLOAD_DIR)).resolve()
    watch_dir.mkdir(parents=True, exist_ok=True)

    schema = (os.getenv("UPLOADS_SAV_WATCHER_SCHEMA") or "").strip()
    if not schema:
        print("⚠️ SAV watcher disabled: UPLOADS_SAV_WATCHER_SCHEMA is not configured")
        return

    year = (os.getenv("UPLOADS_SAV_WATCHER_YEAR") or "").strip()
    dataset_display = (os.getenv("UPLOADS_SAV_WATCHER_DATASET") or "").strip()
    if not year or not dataset_display:
        print("⚠️ SAV watcher disabled: set UPLOADS_SAV_WATCHER_YEAR and UPLOADS_SAV_WATCHER_DATASET")
        return
    dataset_db = to_snake_case_identifier(dataset_display) or "dataset"
    max_concurrency = int((os.getenv("UPLOADS_SAV_WATCHER_MAX_CONCURRENCY") or "1").strip() or "1")
    min_bytes = int((os.getenv("UPLOADS_SAV_MIN_BYTES") or "1024").strip() or "1024")
    stable_checks = int((os.getenv("UPLOADS_SAV_STABLE_CHECKS") or "3").strip() or "3")
    interval_sec = float((os.getenv("UPLOADS_SAV_STABLE_INTERVAL_SEC") or "1").strip() or "1")
    timeout_sec = float((os.getenv("UPLOADS_SAV_READY_TIMEOUT_SEC") or "300").strip() or "300")

    sem = asyncio.Semaphore(max_concurrency)
    inflight: set[str] = set()
    tasks: set[asyncio.Task] = set()

    def _track_task(t: asyncio.Task) -> None:
        tasks.add(t)
        t.add_done_callback(lambda done: tasks.discard(done))

    async def _handle_path(path_str: str) -> None:
        p = Path(path_str)
        if p.suffix.lower() != ".sav":
            return
        try:
            resolved = str(p.resolve())
        except Exception:
            resolved = str(p)

        if resolved in inflight:
            return

        inflight.add(resolved)
        job_id = None
        try:
            job_id = create_job(
                filename=p.name,
                schema=schema,
                schema_display_name=schema,
                year=year,
                dataset_display_name=dataset_display,
                dataset_db_name=dataset_db,
            )
            print(f"📥 SAV watcher: detected {p.name} -> job {job_id}")
            update_job(
                job_id,
                status="pending",
                progress=0,
                message=f"Detected .sav file: {p.name}",
                log=f"Detected file: {p}",
                schema=schema,
                job_type="sav_watcher",
                input_file_path=str(p),
            )

            ready = await _wait_for_stable_file(
                str(p),
                min_bytes=min_bytes,
                stable_checks=stable_checks,
                interval_sec=interval_sec,
                timeout_sec=timeout_sec,
            )
            if not ready:
                update_job(
                    job_id,
                    status="failed",
                    progress=100,
                    message=f"File not ready: {p.name}",
                    error="File did not become stable",
                    log="File did not become stable before timeout",
                )
                return

            update_job(job_id, status=JOB_STATUS_PROCESSING, progress=1, message="Starting ingestion...", log="Starting ingestion")
            async with sem:
                await run_ingestion_job(job_id, str(p), DB_URL)
        except asyncio.CancelledError:
            if job_id:
                update_job(job_id, status="failed", progress=100, message="Watcher cancelled", error="Watcher cancelled")
            raise
        except Exception as e:
            if job_id:
                update_job(job_id, status="failed", progress=100, message=str(e), error=str(e))
        finally:
            inflight.discard(resolved)

    for p in watch_dir.glob("*.sav"):
        t = asyncio.create_task(_handle_path(str(p)))
        _track_task(t)

    try:
        async for changes in awatch(str(watch_dir)):
            for change, fpath in changes:
                if change not in (Change.added, Change.modified):
                    continue
                if str(fpath).lower().endswith(".sav"):
                    t = asyncio.create_task(_handle_path(str(fpath)))
                    _track_task(t)
    except asyncio.CancelledError:
        for t in list(tasks):
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        raise





async def run_ingestion_job(job_id: str, zip_path: str, db_url: str):
    """Background task wrapper for ingestion pipeline."""
    try:
        update_job(job_id, status=JOB_STATUS_QUEUED, current_state=JOB_STATUS_QUEUED, message="Job queued")

        job = get_job(job_id) or {}
        schema = str(job.get("schema") or "").strip()
        year = str(job.get("year") or "").strip()
        dataset_display = str(job.get("dataset_display_name") or "").strip()
        dataset_db = str(job.get("dataset_db_name") or "").strip()

        if not schema:
            raise ValueError("Schema is required for ingestion")
        if not year:
            raise ValueError("Year is required for ingestion")
        if not dataset_display:
            raise ValueError("Dataset name is required for ingestion")
        if not dataset_db:
            raise ValueError("Dataset db name is required for ingestion")

        update_job(job_id, status=JOB_STATUS_PROCESSING, current_state=JOB_STATUS_PROCESSING, message="Preparing ingestion...")

        await ingest_upload_file(
            zip_path,
            db_url,
            schema=schema,
            year=year,
            dataset_display_name=dataset_display,
            dataset_db_name=dataset_db,
            job_id=job_id,
        )

    except Exception as e:
        print(f"❌ Job {job_id} failed: {e}")
        job = get_job(job_id)
        if job:
            # Always mark as FAILED
            update_job(job_id, status=JOB_STATUS_FAILED, current_state=JOB_STATUS_FAILED, message=str(e), error=str(e))
    finally:
        # Clean up
        if os.path.exists(zip_path):
            try:
                os.remove(zip_path)
                print(f"🧹 Deleted ZIP: {zip_path}")
            except Exception as e:
                print(f"⚠️ Failed to delete ZIP {zip_path}: {e}")

async def _schema_exists(conn, schema: str) -> bool:
    s = (schema or "").strip()
    if not s:
        return False
    row = await conn.fetchval(
        """
        SELECT 1
        FROM information_schema.schemata
        WHERE schema_name = $1
        LIMIT 1
        """,
        s,
    )
    return bool(row)

@app.get("/admin/upload/status/{job_id}")
async def get_upload_status(job_id: str, current_user=Depends(get_current_active_user_with_role(["1", "2"]))):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.get("/admin/upload/events/{job_id}")
async def upload_events(
    request: Request,
    job_id: str,
    current_user=Depends(get_current_active_user_with_role(["1", "2"])),
):
    async def event_gen():
        last_payload = None
        while True:
            if await request.is_disconnected():
                break
            job = get_job(job_id)
            if not job:
                payload = json.dumps({"error": "Job not found", "job_id": job_id})
                yield f"data: {payload}\n\n"
                break

            payload = json.dumps(job, default=str)
            if payload != last_payload:
                yield f"data: {payload}\n\n"
                last_payload = payload

            status = str(job.get("status") or "").strip().lower()
            if status in {"completed", "failed"}:
                break

            await asyncio.sleep(1)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

@app.get("/admin/nesstar/jobs/{job_id}")
async def get_nesstar_job_status(job_id: str, current_user=Depends(get_current_active_user_with_role(["1"]))):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.get("/upload/status/{job_id}")
async def get_upload_status(
    job_id: str,
    current_user=Depends(get_current_active_user_with_role(["1", "2"])),
):
    from utils.job_manager import get_job
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Return subset of job info for the UI
    return {
        "status": job.get("status", "unknown").lower(),
        "progress": job.get("progress", 0),
        "message": job.get("message", ""),
        "error": job.get("error"),
        "logs": job.get("logs", []),
        "processed_files": job.get("processed_files", [])
    }


@app.get("/upload/progress/{job_id}", response_class=HTMLResponse)
async def upload_progress_page(
    request: Request,
    job_id: str,
    current_user=Depends(get_current_active_user_with_role(["1", "2"])),
):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return templates.TemplateResponse(
        "upload_progress.html",
        {
            "request": request,
            "job_id": job_id,
            "filename": job.get("filename") or "Upload",
        },
    )

@app.post("/upload/", response_class=HTMLResponse)
async def upload_dataset(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    schema: str = Form(...),
    year: str = Form(...),
    dataset: str = Form(...),
    current_user=Depends(get_current_active_user_with_role(["1", "2"])),
):
    wants_json = request.query_params.get("response") == "json" or (
        (request.headers.get("x-requested-with") or "").lower() == "xmlhttprequest"
    ) or ("application/json" in (request.headers.get("accept") or "").lower())

    try:
        raw_schema = (schema or "").strip()
        year = (year or "").strip()
        dataset_display = (dataset or "").strip()

        if not raw_schema:
            msg = "Schema is required"
            if wants_json:
                return JSONResponse({"error": msg}, status_code=400)
            return f"<h3>❌ Upload failed: {msg}</h3>"

        if not year:
            msg = "Year is required"
            if wants_json:
                return JSONResponse({"error": msg}, status_code=400)
            return f"<h3>❌ Upload failed: {msg}</h3>"

        if not dataset_display:
            msg = "Dataset name is required"
            if wants_json:
                return JSONResponse({"error": msg}, status_code=400)
            return f"<h3>❌ Upload failed: {msg}</h3>"

        pool = request.app.state.db
        async with pool.acquire() as conn:
            schema_row = await conn.fetchrow(
                """
                SELECT display_name, db_name
                FROM schema_registry
                WHERE db_name = $1 OR lower(display_name) = lower($1)
                LIMIT 1
                """,
                raw_schema,
            )
        if not schema_row:
            msg = "Invalid schema"
            if wants_json:
                return JSONResponse({"error": msg}, status_code=400)
            return f"<h3>❌ Upload failed: {msg}</h3>"
        schema_display = schema_row["display_name"]
        schema = schema_row["db_name"]

        if (file.filename or "").lower().endswith(".nesstar") and not getattr(request.app.state, "nesstar_enabled", False):
            msg = "Nesstar conversion is disabled: configure NESSTAR_CONVERTER_EXE and NESSTAR_CONVERTER_SCRIPT"
            if wants_json:
                return JSONResponse({"error": msg}, status_code=400)
            return f"<h3>❌ Upload failed: {msg}</h3>"

        dataset_db = to_snake_case_identifier(dataset_display) or "dataset"

        job_id = create_job(
            filename=file.filename,
            schema=schema,
            schema_display_name=schema_display,
            year=year,
            dataset_display_name=dataset_display,
            dataset_db_name=dataset_db,
        )
        
        from utils.ingestion_pipeline import log_terminal
        log_terminal(f"Job initialized: {job_id} for file {file.filename}")

        update_job(
            job_id,
            schema=schema,
            schema_display_name=schema_display,
            year=year,
            dataset_display_name=dataset_display,
            dataset_db_name=dataset_db,
        )
        
        # Save file
        zip_filename = f"{job_id}_{file.filename}"
        zip_path = os.path.join(UPLOAD_DIR, zip_filename)

        from utils.ingestion_pipeline import log_terminal
        log_terminal(f"Receiving upload: {file.filename} for survey {schema_display}")

        with open(zip_path, "wb") as buffer:
            buffer.write(await file.read())
        
        log_terminal(f"File saved to disk: {zip_filename}", "success")
            
        # Start background task
        background_tasks.add_task(run_ingestion_job, job_id, zip_path, DB_URL)

        progress_url = f"/upload/progress/{job_id}"
        if wants_json:
            return JSONResponse({"job_id": job_id, "progress_url": progress_url})
        
        # Return page with polling logic
        return templates.TemplateResponse(
            "upload_progress.html",
            {"request": request, "job_id": job_id, "filename": file.filename},
        )

    except Exception as e:
        print(f"❌ Upload initiation failed: {e}")
        if wants_json:
            return JSONResponse({"error": str(e)}, status_code=400)
        return f"<h3>❌ Upload failed: {str(e)}</h3>"


# =================== ADMIN ROUTES ===================


@app.get("/admin/survey-config", response_class=HTMLResponse)
async def admin_survey_config_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    return templates.TemplateResponse(
        "survey_config.html",
        {
            "request": request,
        },
    )


@app.get("/admin/change-password", response_class=HTMLResponse)
async def admin_change_password_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    return templates.TemplateResponse(
        "admin_change_password.html",
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
        },
    )


@app.post("/admin/change-password")
async def admin_change_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_password: str = Form(...),
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    if new_password != confirm_password:
        return templates.TemplateResponse(
            "admin_change_password.html",
            {
                "request": request,
                "username": current_user.username,
                "email": current_user.username,
                "error": "New passwords do not match",
            },
        )

    if len(new_password) < 8:
        return templates.TemplateResponse(
            "admin_change_password.html",
            {
                "request": request,
                "username": current_user.username,
                "email": current_user.username,
                "error": "Password must be at least 8 characters long",
            },
        )

    pool = request.app.state.db
    async with pool.acquire() as conn:
        # Verify current password
        user = await conn.fetchrow(
            "SELECT * FROM users WHERE email = $1", current_user.username
        )
        if not user or not bcrypt.checkpw(
            current_password.encode(), user["hashed_password"].encode()
        ):
            return templates.TemplateResponse(
                "admin_change_password.html",
                {
                    "request": request,
                    "username": current_user.username,
                    "email": current_user.username,
                    "error": "Current password is incorrect",
                },
            )

        # Update password
        new_hashed = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
        await conn.execute(
            "UPDATE users SET hashed_password = $1 WHERE email = $2",
            new_hashed,
            current_user.username,
        )

    # Force re-login: clear cookie and redirect
    response = RedirectResponse(
        url="/login?success=Password+changed+successfully.+Please+log+in+again.",
        status_code=302,
    )
    response.delete_cookie(
        key="access_token",
        path="/",
        domain=None,
        secure=False,
        httponly=True,
        samesite="lax",
    )
    return response


@app.get("/admin/schemas/{schema}/variables")
async def get_schema_variables(
    request: Request,
    schema: str,
    table: str = "",
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    try:
        table_name = (table or "").strip()
        if not table_name:
            return {"error": "Table is required"}

        pool = request.app.state.db
        async with pool.acquire() as conn:
            t_exists = await conn.fetchval(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = $1
                  AND table_name = $2
                AND table_type = 'BASE TABLE'
                LIMIT 1
                """,
                schema,
                table_name,
            )
            if not t_exists:
                return {"error": f"Table '{table_name}' not found in schema '{schema}'"}

            cols = await conn.fetch(
                """
                SELECT column_name, data_type, is_nullable, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
                """,
                schema,
                table_name,
            )

            cfg_rows = await conn.fetch(
                """
                SELECT *
                FROM variable_configs
                WHERE schema_name = $1
                  AND table_name IN ($2, '*')
                ORDER BY (table_name <> '*') DESC, updated_at DESC
                """,
                schema,
                table_name,
            )
            cfg_map = {}
            for r in cfg_rows:
                vn = r["variable_name"]
                if vn not in cfg_map:
                    cfg_map[vn] = dict(r)

            table_cfg = await conn.fetchrow(
                """
                SELECT show_table_to_users
                FROM dataset_configs
                WHERE schema_name = $1 AND table_name = $2
                LIMIT 1
                """,
                schema,
                table_name,
            )

            variables = []
            for c in cols:
                vn = c["column_name"]
                cfg = cfg_map.get(vn, {})
                variables.append(
                    {
                        "variable_name": vn,
                        "label": cfg.get("label") or vn,
                        "data_type": c["data_type"],
                        "nullable": c["is_nullable"] == "YES",
                        "include_in_api": bool(cfg.get("include_in_api", True)),
                        "filterable": bool(cfg.get("filterable", False)),
                        "is_sensitive": bool(cfg.get("is_sensitive", False)),
                        "min_rows": int(cfg.get("min_rows") or 5),
                    }
                )

            return {
                "schema": schema,
                "table": table_name,
                "show_table_to_users": True if table_cfg is None else bool(table_cfg["show_table_to_users"]),
                "total_variables": len(variables),
                "variables": variables,
            }

    except Exception as e:
        print(f"Error getting schema variables: {e}")
        return {"error": str(e)}


@app.post("/admin/update-variable-config")
async def update_variable_config(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    try:
        body = await request.json()
        schema = (body.get("schema") or "").strip()
        table_name = (body.get("table") or "").strip()
        show_table_to_users = bool(body.get("show_table_to_users", True))
        configs = body.get("configs") or []
        if not schema:
            return {"error": "Schema parameter is required"}
        if not table_name:
            return {"error": "Table parameter is required"}

        pool = request.app.state.db
        async with pool.acquire() as conn:
            try:
                await conn.execute(
                    "ALTER TABLE dataset_configs ADD COLUMN IF NOT EXISTS show_table_to_users BOOLEAN DEFAULT TRUE"
                )
            except Exception:
                pass
            has_dataset_name = await conn.fetchval(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'dataset_configs'
                  AND column_name = 'dataset_name'
                LIMIT 1
                """
            )
            if has_dataset_name:
                upd_ds = await conn.execute(
                    """
                    UPDATE dataset_configs
                    SET show_table_to_users = $4, updated_at = NOW()
                    WHERE schema_name = $1 AND dataset_name = $2 AND table_name = $3
                    """,
                    schema,
                    "*",
                    table_name,
                    show_table_to_users,
                )
                if str(upd_ds).upper().endswith(" 0"):
                    await conn.execute(
                        """
                        INSERT INTO dataset_configs (schema_name, dataset_name, table_name, show_table_to_users, updated_at)
                        VALUES ($1, $2, $3, $4, NOW())
                        """,
                        schema,
                        "*",
                        table_name,
                        show_table_to_users,
                    )
            else:
                upd_ds = await conn.execute(
                    """
                    UPDATE dataset_configs
                    SET show_table_to_users = $3, updated_at = NOW()
                    WHERE schema_name = $1 AND table_name = $2
                    """,
                    schema,
                    table_name,
                    show_table_to_users,
                )
                if str(upd_ds).upper().endswith(" 0"):
                    await conn.execute(
                        """
                        INSERT INTO dataset_configs (schema_name, table_name, show_table_to_users, updated_at)
                        VALUES ($1, $2, $3, NOW())
                        """,
                        schema,
                        table_name,
                        show_table_to_users,
                    )

            keep_names = []
            updated = 0
            for cfg in configs:
                var_name = (cfg.get("variable_name") or "").strip()
                if not var_name:
                    continue
                keep_names.append(var_name)
                label = cfg.get("label") or var_name
                include_in_api = bool(cfg.get("include_in_api", True))
                filterable = bool(cfg.get("filterable", False))
                is_sensitive = bool(cfg.get("is_sensitive", False))
                try:
                    min_rows = int(cfg.get("min_rows") or 5)
                except Exception:
                    min_rows = 5
                min_rows = max(1, min_rows)
                upd_var = await conn.execute(
                    """
                    UPDATE variable_configs
                    SET label = $4,
                        include_in_api = $5,
                        filterable = $6,
                        is_sensitive = $7,
                        min_rows = $8,
                        updated_at = NOW()
                    WHERE schema_name = $1
                      AND table_name = $2
                      AND variable_name = $3
                    """,
                    schema,
                    table_name,
                    var_name,
                    label,
                    include_in_api,
                    filterable,
                    is_sensitive,
                    min_rows,
                )
                if str(upd_var).upper().endswith(" 0"):
                    await conn.execute(
                        """
                        INSERT INTO variable_configs
                            (schema_name, table_name, variable_name, label, include_in_api, filterable, is_sensitive, min_rows, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                        """,
                        schema,
                        table_name,
                        var_name,
                        label,
                        include_in_api,
                        filterable,
                        is_sensitive,
                        min_rows,
                    )
                updated += 1

            if keep_names:
                await conn.execute(
                    """
                    DELETE FROM variable_configs
                    WHERE schema_name = $1
                      AND table_name = $2
                      AND variable_name <> ALL($3::text[])
                    """,
                    schema,
                    table_name,
                    keep_names,
                )
            else:
                await conn.execute(
                    """
                    DELETE FROM variable_configs
                    WHERE schema_name = $1
                      AND table_name = $2
                    """,
                    schema,
                    table_name,
                )
        return {
            "message": f"Successfully saved configuration for table '{table_name}' in schema '{schema}'",
            "schema": schema,
            "table": table_name,
            "show_table_to_users": show_table_to_users,
            "updated_count": updated,
        }

    except Exception as e:
        import traceback

        print(f"ERROR in update_variable_config: {e}")
        traceback.print_exc()
        return {"error": f"Server error: {str(e)}"}


@app.get("/admin/usage-logs", response_class=HTMLResponse)
async def usage_logs_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    return templates.TemplateResponse(
        "usage.html",
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
        },
    )


@app.get("/admin/user-management", response_class=HTMLResponse)
async def user_management_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    return templates.TemplateResponse(
        "user_management.html",
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
        },
    )


@app.get("/admin/system-settings", response_class=HTMLResponse)
async def system_settings_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    return templates.TemplateResponse(
        "system_settings.html",
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
        },
    )


@app.get("/admin/usage/logs")
async def admin_usage_logs_api(
    request: Request,
    q: str = "",
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        usage_cols = {
            r["column_name"]
            for r in await conn.fetch(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'usage_logs'
                """
            )
        }
        query_cols = {
            r["column_name"]
            for r in await conn.fetch(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'query_logs'
                """
            )
        }
        download_cols = {
            r["column_name"]
            for r in await conn.fetch(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'download_logs'
                """
            )
        }

        usage_time_col = "queried_at" if "queried_at" in usage_cols else ("created_at" if "created_at" in usage_cols else None)
        usage_rows_col = "rows_returned" if "rows_returned" in usage_cols else None
        usage_endpoint_col = "endpoint" if "endpoint" in usage_cols else None
        usage_bytes_col = "bytes_sent" if "bytes_sent" in usage_cols else None
        usage_has_identity = "user_email" in usage_cols
        usage_has_dataset = "schema_name" in usage_cols and "table_name" in usage_cols

        query_time_col = "created_at" if "created_at" in query_cols else ("queried_at" if "queried_at" in query_cols else None)
        query_rows_col = "rows_returned" if "rows_returned" in query_cols else None
        query_has_identity = "user_email" in query_cols
        query_has_dataset = "dataset_name" in query_cols and "table_name" in query_cols
        query_has_filters = "filters" in query_cols

        download_time_col = "created_at" if "created_at" in download_cols else ("queried_at" if "queried_at" in download_cols else None)
        download_size_col = "size_bytes" if "size_bytes" in download_cols else ("bytes_sent" if "bytes_sent" in download_cols else None)
        download_has_file = "file_name" in download_cols
        download_has_identity = "user_email" in download_cols

        query_logs = []
        if usage_time_col and usage_has_identity:
            try:
                filters_col_expr = "COALESCE(filters, '-')" if "filters" in usage_cols else "'-'"
                status_col_expr = "COALESCE(status, 'success')" if "status" in usage_cols else "'success'"
                query_time_ms_expr = "COALESCE(query_time_ms, 0)" if "query_time_ms" in usage_cols else "0"
                where_clause = ""
                params = []
                if q:
                    where_clause = "WHERE user_email ILIKE $1 OR schema_name ILIKE $1 OR table_name ILIKE $1 OR filters ILIKE $1 OR status ILIKE $1"
                    params = [f"%{q}%"]
                query_logs = await conn.fetch(
                    f"""
                    SELECT
                        user_email,
                        CASE
                            WHEN COALESCE(NULLIF(schema_name, ''), '') = '' AND COALESCE(NULLIF(table_name, ''), '') = '' THEN COALESCE(endpoint, '-')
                            ELSE COALESCE(NULLIF(schema_name, ''), '-') || '/' || COALESCE(NULLIF(table_name, ''), '-')
                        END AS dataset_table,
                        {filters_col_expr} AS filters,
                        COALESCE({usage_rows_col or '0'}, 0) AS rows_returned,
                        {query_time_ms_expr} AS query_time_ms,
                        {status_col_expr} AS status,
                        {usage_time_col} AS time
                    FROM usage_logs
                    {where_clause}
                    ORDER BY {usage_time_col} DESC
                    LIMIT 100
                    """,
                    *params
                )
            except Exception:
                query_logs = []
        if not query_logs and query_time_col and query_has_identity:
            try:
                dataset_expr = "COALESCE(dataset_name, '-') || '/' || COALESCE(table_name, '-')" if query_has_dataset else "'-'"
                filters_expr = "COALESCE(filters, '-')" if query_has_filters else "'-'"
                where_clause = ""
                params = []
                if q:
                    where_clause = "WHERE user_email ILIKE $1 OR dataset_name ILIKE $1 OR table_name ILIKE $1 OR filters ILIKE $1"
                    params = [f"%{q}%"]
                query_logs = await conn.fetch(
                    f"""
                    SELECT
                        user_email,
                        {dataset_expr} AS dataset_table,
                        {filters_expr} AS filters,
                        COALESCE({query_rows_col or '0'}, 0) AS rows_returned,
                        0 AS query_time_ms,
                        'success' AS status,
                        {query_time_col} AS time
                    FROM query_logs
                    {where_clause}
                    ORDER BY {query_time_col} DESC
                    LIMIT 100
                    """,
                    *params
                )
            except Exception:
                query_logs = []

        usage_downloads = []
        if usage_time_col and usage_has_identity and usage_endpoint_col:
            try:
                where_clause = "WHERE endpoint ILIKE '/downloads/%'"
                params = []
                if q:
                    where_clause += " AND (user_email ILIKE $1 OR endpoint ILIKE $1 OR status ILIKE $1)"
                    params = [f"%{q}%"]
                usage_downloads = await conn.fetch(
                    f"""
                    SELECT
                        COALESCE(NULLIF(split_part(endpoint, '/', 4), ''), 'download') AS file_name,
                        user_email,
                        COALESCE({usage_bytes_col or '0'}, 0) AS size_bytes,
                        'csv' AS export_format,
                        0 AS rows_exported,
                        'success' AS status,
                        {usage_time_col} AS time
                    FROM usage_logs
                    {where_clause}
                    ORDER BY {usage_time_col} DESC
                    LIMIT 100
                    """,
                    *params
                )
            except Exception:
                usage_downloads = []

        table_downloads = []
        if download_time_col and download_has_file and download_has_identity:
            try:
                format_col_expr = "COALESCE(export_format, 'csv')" if "export_format" in download_cols else "'csv'"
                rows_col_expr = "COALESCE(rows_exported, 0)" if "rows_exported" in download_cols else "0"
                status_col_expr = "COALESCE(status, 'success')" if "status" in download_cols else "'success'"
                where_clause = ""
                params = []
                if q:
                    where_clause = "WHERE user_email ILIKE $1 OR file_name ILIKE $1 OR dataset_schema ILIKE $1 OR export_format ILIKE $1 OR status ILIKE $1"
                    params = [f"%{q}%"]
                table_downloads = await conn.fetch(
                    f"""
                    SELECT
                        file_name,
                        user_email,
                        COALESCE({download_size_col or '0'}, 0) AS size_bytes,
                        {format_col_expr} AS export_format,
                        {rows_col_expr} AS rows_exported,
                        {status_col_expr} AS status,
                        {download_time_col} AS time
                    FROM download_logs
                    {where_clause}
                    ORDER BY {download_time_col} DESC
                    LIMIT 100
                    """,
                    *params
                )
            except Exception:
                table_downloads = []

        merged_downloads = [dict(r) for r in usage_downloads]
        existing = {(d.get("file_name"), d.get("user_email"), str(d.get("time"))) for d in merged_downloads}
        for r in table_downloads:
            d = dict(r)
            key = (d.get("file_name"), d.get("user_email"), str(d.get("time")))
            if key not in existing:
                merged_downloads.append(d)

        total_queries = 0
        active_users = 0
        rows_accessed = 0
        queries_over_time = []
        queries_over_time_hourly = []
        top_users = []

        if usage_time_col and usage_has_identity:
            try:
                admin_emails_subquery = "SELECT email FROM users WHERE role_id = 1"
                total_queries = await conn.fetchval(f"SELECT COUNT(*) FROM usage_logs WHERE user_email NOT IN ({admin_emails_subquery})")
                active_users = await conn.fetchval(f"SELECT COUNT(DISTINCT user_email) FROM usage_logs WHERE user_email NOT IN ({admin_emails_subquery})")
                rows_accessed = await conn.fetchval(f"SELECT COALESCE(SUM({usage_rows_col or '0'}), 0) FROM usage_logs WHERE user_email NOT IN ({admin_emails_subquery})")
                queries_over_time = await conn.fetch(
                    f"""
                    SELECT TO_CHAR(DATE({usage_time_col}), 'YYYY-MM-DD') AS day, COUNT(*) AS count
                    FROM usage_logs
                    WHERE {usage_time_col} >= NOW() - INTERVAL '14 days'
                      AND user_email NOT IN ({admin_emails_subquery})
                    GROUP BY DATE({usage_time_col})
                    ORDER BY DATE({usage_time_col})
                    """
                )
                queries_over_time_hourly = await conn.fetch(
                    f"""
                    SELECT TO_CHAR(DATE_TRUNC('hour', {usage_time_col}), 'YYYY-MM-DD HH24:00') AS hour, COUNT(*) AS count
                    FROM usage_logs
                    WHERE {usage_time_col} >= NOW() - INTERVAL '24 hours'
                      AND user_email NOT IN ({admin_emails_subquery})
                    GROUP BY DATE_TRUNC('hour', {usage_time_col})
                    ORDER BY DATE_TRUNC('hour', {usage_time_col})
                    """
                )
                top_users = await conn.fetch(
                    f"""
                    SELECT 
                        COALESCE(u.username, split_part(u.email, '@', 1)) AS username,
                        u.email AS user_email,
                        COALESCE(q.queries_count, 0) AS queries_used,
                        COALESCE(q.rows_sum, 0) AS rows_accessed,
                        COALESCE(d.downloads_count, 0) AS downloads,
                        u.plan AS current_plan,
                        u.last_active
                    FROM users u
                    LEFT JOIN (
                        SELECT user_email, COUNT(*) AS queries_count, SUM(COALESCE(rows_returned, 0)) AS rows_sum
                        FROM usage_logs
                        GROUP BY user_email
                    ) q ON u.email = q.user_email
                    LEFT JOIN (
                        SELECT user_email, COUNT(*) AS downloads_count
                        FROM download_logs
                        GROUP BY user_email
                    ) d ON u.email = d.user_email
                    WHERE u.role_id != 1
                    ORDER BY queries_used DESC, u.email ASC
                    LIMIT 10
                    """
                )
            except Exception:
                total_queries = 0
                active_users = 0
                rows_accessed = 0
                queries_over_time = []
                queries_over_time_hourly = []
                top_users = []

        if (not total_queries) and query_time_col and query_has_identity:
            try:
                admin_emails_subquery = "SELECT email FROM users WHERE role_id = 1"
                total_queries = await conn.fetchval(f"SELECT COUNT(*) FROM query_logs WHERE user_email NOT IN ({admin_emails_subquery})")
                active_users = await conn.fetchval(f"SELECT COUNT(DISTINCT user_email) FROM query_logs WHERE user_email NOT IN ({admin_emails_subquery})")
                rows_accessed = await conn.fetchval(f"SELECT COALESCE(SUM({query_rows_col or '0'}), 0) FROM query_logs WHERE user_email NOT IN ({admin_emails_subquery})")
                queries_over_time = await conn.fetch(
                    f"""
                    SELECT TO_CHAR(DATE({query_time_col}), 'YYYY-MM-DD') AS day, COUNT(*) AS count
                    FROM query_logs
                    WHERE {query_time_col} >= NOW() - INTERVAL '14 days'
                      AND user_email NOT IN ({admin_emails_subquery})
                    GROUP BY DATE({query_time_col})
                    ORDER BY DATE({query_time_col})
                    """
                )
                queries_over_time_hourly = await conn.fetch(
                    f"""
                    SELECT TO_CHAR(DATE_TRUNC('hour', {query_time_col}), 'YYYY-MM-DD HH24:00') AS hour, COUNT(*) AS count
                    FROM query_logs
                    WHERE {query_time_col} >= NOW() - INTERVAL '24 hours'
                      AND user_email NOT IN ({admin_emails_subquery})
                    GROUP BY DATE_TRUNC('hour', {query_time_col})
                    ORDER BY DATE_TRUNC('hour', {query_time_col})
                    """
                )
                top_users = await conn.fetch(
                    f"""
                    SELECT 
                        COALESCE(u.username, split_part(u.email, '@', 1)) AS username,
                        u.email AS user_email,
                        COALESCE(q.queries_count, 0) AS queries_used,
                        COALESCE(q.rows_sum, 0) AS rows_accessed,
                        COALESCE(d.downloads_count, 0) AS downloads,
                        u.plan AS current_plan,
                        u.last_active
                    FROM users u
                    LEFT JOIN (
                        SELECT user_email, COUNT(*) AS queries_count, SUM(COALESCE(rows_returned, 0)) AS rows_sum
                        FROM query_logs
                        GROUP BY user_email
                    ) q ON u.email = q.user_email
                    LEFT JOIN (
                        SELECT user_email, COUNT(*) AS downloads_count
                        FROM download_logs
                        GROUP BY user_email
                    ) d ON u.email = d.user_email
                    WHERE u.role_id != 1
                    ORDER BY queries_used DESC, u.email ASC
                    LIMIT 10
                    """
                )
            except Exception:
                pass

        formatted_query_logs = []
        for r in query_logs:
            d = dict(r)
            if "time" in d:
                d["time"] = format_local_timestamp_to_utc_iso(d["time"])
            formatted_query_logs.append(d)

        formatted_download_logs = []
        for r in merged_downloads[:100]:
            d = dict(r)
            if "time" in d:
                d["time"] = format_local_timestamp_to_utc_iso(d["time"])
            formatted_download_logs.append(d)

        formatted_top_users = []
        for r in top_users:
            d = dict(r)
            if "last_active" in d and d["last_active"]:
                d["last_active"] = format_local_timestamp_to_utc_iso(d["last_active"])
            formatted_top_users.append(d)

    return {
        "query_logs": formatted_query_logs,
        "download_logs": formatted_download_logs,
        "metrics": {
            "total_queries": int(total_queries or 0),
            "active_users": int(active_users or 0),
            "rows_accessed": int(rows_accessed or 0),
        },
        "charts": {
            "queries_over_time": [dict(r) for r in queries_over_time],
            "queries_over_time_hourly": [dict(r) for r in queries_over_time_hourly],
            "top_users": formatted_top_users,
        },
    }


@app.get("/admin/users")
async def admin_users_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        users = await conn.fetch(
            """
            SELECT
                u.username,
                u.email,
                COALESCE(r.name, 'user') AS role,
                u.org_type,
                COALESCE(u.is_verified, FALSE) AS is_verified,
                COALESCE(u.document_uploaded, FALSE) AS document_uploaded,
                COALESCE(u.is_blocked, FALSE) AS is_blocked,
                COALESCE(u.status, CASE WHEN COALESCE(u.is_blocked, FALSE) THEN 'blocked' ELSE 'active' END) AS status,
                COALESCE(u.plan, 'free') AS plan,
                u.plan_expiry,
                COALESCE(u.max_queries_per_day, u.max_queries_day, 1000) AS max_queries_day,
                COALESCE(u.max_rows_per_day, u.max_rows_day, 100000) AS max_rows_day,
                COALESCE(u.blocked_reason, '') AS blocked_reason,
                COALESCE(u.warning_count, 0) AS warning_count,
                COALESCE(u.suspicious_score, 0) AS suspicious_score,
                u.last_active,
                u.created_at
            FROM users u
            LEFT JOIN roles r ON r.id = u.role_id
            ORDER BY u.created_at DESC
            """
        )
        enriched = []
        for u in users:
            ud = dict(u)
            ud["role_display"] = ud.get("org_type") if ud.get("org_type") else ud["role"].capitalize()
            ud["last_active"] = format_local_timestamp_to_utc_iso(ud["last_active"])
            ud["created_at"] = format_local_timestamp_to_utc_iso(ud["created_at"])
            ud["plan_expiry"] = format_local_timestamp_to_utc_iso(ud["plan_expiry"])
            # Compute effective plan limits for this user
            try:
                user_role_id = {"admin": "1", "analyst": "2", "user": "3"}.get(ud.get("role", "user"), "3")
                effective = await get_and_enforce_plan_limits(conn, ud["email"], user_role_id)
                ud["max_queries_day"] = effective.get("max_queries_per_month", ud["max_queries_day"])
                ud["max_rows_day"] = effective.get("max_rows_per_month", ud["max_rows_day"])
                ud["downloads_allowed"] = effective.get("downloads_allowed", False)
                ud["max_downloads"] = effective.get("max_downloads_per_month", 0)
                ud["rate_limit"] = effective.get("rate_limit", 5)
                ud["export_formats"] = effective.get("export_formats", ["csv"])
            except Exception:
                pass
            try:
                usage = await conn.fetchrow(
                    "SELECT COUNT(*) AS queries_today, COALESCE(SUM(rows_returned),0) AS rows_today FROM usage_logs WHERE user_email = $1 AND queried_at >= CURRENT_DATE",
                    ud["email"]
                )
                ud["queries_today"] = int(usage["queries_today"] or 0) if usage else 0
                ud["rows_today"] = int(usage["rows_today"] or 0) if usage else 0
            except Exception:
                ud["queries_today"] = 0
                ud["rows_today"] = 0
            try:
                dl = await conn.fetchval("SELECT COUNT(*) FROM download_logs WHERE user_email = $1 AND created_at >= CURRENT_DATE", ud["email"])
                ud["downloads_today"] = int(dl or 0)
            except Exception:
                ud["downloads_today"] = 0
            enriched.append(ud)
    return {"users": enriched}


@app.post("/admin/users/update-role")
async def admin_update_role_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    role = (body.get("role") or "").strip().lower()
    plan = (body.get("plan") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Invalid payload")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        if role:
            if role not in {"admin", "analyst", "user"}:
                raise HTTPException(status_code=400, detail="Invalid role")
            role_id = await conn.fetchval("SELECT id FROM roles WHERE lower(name) = $1 LIMIT 1", role)
            if not role_id:
                raise HTTPException(status_code=404, detail="Role not found")
            await conn.execute("UPDATE users SET role_id = $1 WHERE email = $2", role_id, email)
        if plan:
            await conn.execute("UPDATE users SET plan = $1 WHERE email = $2", plan, email)
    return {"ok": True}


@app.post("/admin/users/block")
async def admin_block_user_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    blocked = bool(body.get("blocked", True))
    reason = (body.get("reason") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    status = "blocked" if blocked else "active"
    pool = request.app.state.db
    async with pool.acquire() as conn:
        if blocked:
            await conn.execute(
                """
                UPDATE users
                SET status = $1,
                    is_blocked = $2,
                    blocked_reason = $3
                WHERE email = $4
                """,
                status,
                blocked,
                reason,
                email
            )
        else:
            await conn.execute(
                """
                UPDATE users
                SET status = $1,
                    is_blocked = $2,
                    blocked_reason = $3,
                    warning_count = 0,
                    freeze_until = NULL
                WHERE email = $4
                """,
                status,
                blocked,
                "",
                email
            )
    return {"ok": True}


@app.post("/admin/users/verify")
async def admin_verify_user_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    approved = bool(body.get("approved", False))
    document_id = body.get("document_id")
    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT COUNT(*) FROM users WHERE email = $1", email
        )
        if not exists:
            raise HTTPException(status_code=404, detail="User not found")
        await conn.execute("UPDATE users SET is_verified = $1 WHERE email = $2", approved, email)
        if document_id:
            await conn.execute(
                "UPDATE user_documents SET status = $1, updated_at = NOW() WHERE id = $2",
                "approved" if approved else "rejected",
                int(document_id),
            )
    return {"ok": True}


@app.post("/admin/users/set-limits")
async def admin_set_limits_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    max_queries_per_day = int(body.get("max_queries_per_day") or 0)
    max_rows_per_day = int(body.get("max_rows_per_day") or 0)
    if not email or max_queries_per_day <= 0 or max_rows_per_day <= 0:
        raise HTTPException(status_code=400, detail="Invalid payload")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE users
            SET max_queries_per_day = $1,
                max_rows_per_day = $2,
                max_queries_day = $1,
                max_rows_day = $2
            WHERE email = $3
            """,
            max_queries_per_day,
            max_rows_per_day,
            email,
        )
    return {"ok": True}


@app.post("/admin/users/assign-plan")
async def admin_assign_plan_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    plan = (body.get("plan") or "").strip().lower()
    plan_expiry = (body.get("plan_expiry") or "").strip()
    if not email or plan not in {"free", "pro", "enterprise"}:
        raise HTTPException(status_code=400, detail="Invalid payload")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        if plan_expiry:
            await conn.execute(
                "UPDATE users SET plan = $1, plan_expiry = $2::timestamp, cancel_at_period_end = FALSE WHERE email = $3",
                plan,
                plan_expiry,
                email,
            )
        else:
            await conn.execute(
                "UPDATE users SET plan = $1, cancel_at_period_end = FALSE WHERE email = $2",
                plan,
                email,
            )
    return {"ok": True}


@app.post("/admin/users/reset-usage")
async def admin_reset_usage_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")

    pool = request.app.state.db
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Clear usage records
            await conn.execute("DELETE FROM usage_logs WHERE user_email = $1", email)
            await conn.execute("DELETE FROM download_logs WHERE user_email = $1", email)
            
            # Reset user safety metrics and status in user table
            await conn.execute(
                """
                UPDATE users 
                SET warning_count = 0, 
                    suspicious_score = 0, 
                    status = 'active', 
                    freeze_until = NULL 
                WHERE email = $1
                """,
                email
            )
            
            # Log governance reset event
            await conn.execute(
                """
                INSERT INTO governance_logs (user_email, event_type, detail, created_at)
                VALUES ($1, 'usage_reset', 'Admin reset user usage limits and warnings', CURRENT_TIMESTAMP)
                """,
                email
            )
            
    return {"ok": True}


@app.post("/admin/users/unfreeze")
async def admin_unfreeze_user_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")

    pool = request.app.state.db
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                UPDATE users
                SET status = 'active',
                    freeze_until = NULL,
                    warning_count = 0
                WHERE email = $1
                """,
                email
            )
            await conn.execute(
                """
                INSERT INTO governance_logs (user_email, event_type, detail, created_at)
                VALUES ($1, 'manual_unfreeze', 'Admin manually unfroze the account', CURRENT_TIMESTAMP)
                """,
                email
            )
    return {"ok": True}


@app.post("/admin/users/delete")
async def admin_delete_user_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    
    # Do not allow deleting self
    if email == current_user.username:
        raise HTTPException(status_code=400, detail="Cannot delete your own admin account")

    pool = request.app.state.db
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Delete related logs first to avoid foreign key / dependency constraints (if any)
            await conn.execute("DELETE FROM usage_logs WHERE user_email = $1", email)
            await conn.execute("DELETE FROM download_logs WHERE user_email = $1", email)
            await conn.execute("DELETE FROM query_logs WHERE user_email = $1", email)
            await conn.execute("DELETE FROM suspicious_activity_logs WHERE user_email = $1", email)
            await conn.execute("DELETE FROM governance_warnings WHERE user_email = $1", email)
            await conn.execute("DELETE FROM governance_logs WHERE user_email = $1", email)
            await conn.execute("DELETE FROM user_documents WHERE user_email = $1", email)
            await conn.execute("DELETE FROM user_requests WHERE user_email = $1", email)
            await conn.execute("DELETE FROM payments WHERE user_email = $1", email)
            # Delete user
            await conn.execute("DELETE FROM users WHERE email = $1", email)
            
    return {"ok": True}


@app.get("/admin/requests")
async def admin_requests_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        try:
            dataset_requests = await conn.fetch(
                """
                SELECT 
                    r.id, 
                    r.user_email, 
                    r.requested_dataset, 
                    COALESCE(r.survey_name, '') AS survey_name, 
                    COALESCE(r.reason, '') AS reason, 
                    r.status, 
                    r.created_at,
                    COALESCE(u.username, split_part(r.user_email, '@', 1)) AS username
                FROM user_requests r
                LEFT JOIN users u ON u.email = r.user_email
                WHERE r.request_type = 'dataset_request'
                ORDER BY r.created_at DESC
                LIMIT 100
                """
            )
        except Exception:
            dataset_requests = []
        try:
            feedback = await conn.fetch(
                """
                SELECT 
                    r.id, 
                    r.user_email, 
                    COALESCE(r.category, 'Other') AS category, 
                    COALESCE(r.title, '') AS title, 
                    r.message, 
                    r.status, 
                    r.created_at,
                    COALESCE(u.username, split_part(r.user_email, '@', 1)) AS username,
                    COALESCE(u.plan, 'free') AS current_plan,
                    COALESCE(u.org_type, 'N/A') AS org_type
                FROM user_requests r
                LEFT JOIN users u ON u.email = r.user_email
                WHERE r.request_type = 'feedback'
                ORDER BY r.created_at DESC
                LIMIT 100
                """
            )
        except Exception:
            feedback = []
        try:
            docs = await conn.fetch(
                """
                SELECT 
                    d.id, 
                    d.user_email, 
                    d.document_name, 
                    d.document_url, 
                    d.status, 
                    d.created_at,
                    u.username,
                    COALESCE(u.org_type, '') AS org_type,
                    COALESCE(u.org_details, '{}') AS org_details,
                    u.created_at AS registration_timestamp,
                    COALESCE(u.status, 'active') AS account_status,
                    COALESCE(u.plan, 'free') AS current_plan,
                    COALESCE(r.name, 'user') AS role,
                    (SELECT COUNT(*) FROM usage_logs WHERE user_email = u.email) AS query_usage
                FROM user_documents d
                JOIN users u ON u.email = d.user_email
                LEFT JOIN roles r ON r.id = u.role_id
                WHERE COALESCE(u.is_verified, FALSE) = FALSE
                ORDER BY d.created_at DESC
                LIMIT 100
                """
            )
        except Exception:
            docs = []
        admin_emails_subquery = "SELECT email FROM users WHERE role_id = 1"
        try:
            suspicious = await conn.fetch(
                f"""
                SELECT 
                    l.id,
                    l.user_email, 
                    COALESCE(l.activity_type, 'unknown') AS activity_type,
                    COALESCE(l.detail, '') AS detail,
                    l.created_at,
                    COALESCE(l.risk_score, 0) AS risk_score,
                    COALESCE(u.warning_count, 0) AS warning_count,
                    COALESCE(u.status, 'active') AS auto_action
                FROM suspicious_activity_logs l
                JOIN users u ON u.email = l.user_email
                WHERE l.user_email NOT IN ({admin_emails_subquery})
                ORDER BY l.created_at DESC
                LIMIT 100
                """
            )
        except Exception as e:
            print(f"Error fetching suspicious activity logs: {e}")
            suspicious = []
    formatted_dataset_requests = []
    for r in dataset_requests:
        d = dict(r)
        if "created_at" in d:
            d["created_at"] = format_local_timestamp_to_utc_iso(d["created_at"])
        formatted_dataset_requests.append(d)

    formatted_feedback = []
    for r in feedback:
        d = dict(r)
        if "created_at" in d:
            d["created_at"] = format_local_timestamp_to_utc_iso(d["created_at"])
        formatted_feedback.append(d)

    formatted_docs = []
    for r in docs:
        d = dict(r)
        if "created_at" in d:
            d["created_at"] = format_local_timestamp_to_utc_iso(d["created_at"])
        if "registration_timestamp" in d:
            d["registration_timestamp"] = format_local_timestamp_to_utc_iso(d["registration_timestamp"])
        formatted_docs.append(d)

    formatted_suspicious = []
    for r in suspicious:
        d = dict(r)
        if "created_at" in d:
            d["created_at"] = format_local_timestamp_to_utc_iso(d["created_at"])
        formatted_suspicious.append(d)

    return {
        "dataset_requests": formatted_dataset_requests,
        "feedback": formatted_feedback,
        "documents": formatted_docs,
        "suspicious_activity": formatted_suspicious,
    }


@app.post("/admin/requests/action")
async def admin_requests_action_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    request_id = int(body.get("id") or 0)
    status = (body.get("status") or "").strip().lower()
    action_type = (body.get("action_type") or "dataset_request").strip().lower()
    if request_id <= 0 or status not in {"approved", "rejected", "pending", "reviewing", "resolved", "archived", "respond_later"}:
        raise HTTPException(status_code=400, detail="Invalid payload")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE user_requests SET status = $1, updated_at = NOW() WHERE id = $2",
            status,
            request_id,
        )
        if action_type == "dataset_request" and status == "approved":
            row = await conn.fetchrow(
                "SELECT requested_dataset FROM user_requests WHERE id = $1",
                request_id,
            )
            req_name = row["requested_dataset"] if row else f"request_{request_id}"
            create_job("dataset_request_ingestion", req_name)
    return {"ok": True}


@app.get("/admin/payments")
async def admin_payments_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        plans = await conn.fetch(
            """
            SELECT 
                u.email AS user_email, 
                COALESCE(u.plan, 'free') AS plan, 
                u.plan_expiry,
                COALESCE(u.cancel_at_period_end, FALSE) AS cancel_at_period_end,
                (
                    SELECT billing_cycle 
                    FROM payments 
                    WHERE email = u.email 
                    ORDER BY created_at DESC 
                    LIMIT 1
                ) AS billing_cycle,
                (
                    SELECT payment_status 
                    FROM payments 
                    WHERE email = u.email 
                    ORDER BY created_at DESC 
                    LIMIT 1
                ) AS payment_status,
                (
                    SELECT COALESCE(SUM(amount), 0) 
                    FROM payments 
                    WHERE email = u.email AND payment_status = 'success'
                ) AS total_paid
            FROM users u
            WHERE u.role_id != 1
            ORDER BY u.email
            LIMIT 500
            """
        )
        try:
            txns = await conn.fetch(
                """
                SELECT 
                    COALESCE(razorpay_payment_id, razorpay_order_id, '') AS transaction_id, 
                    email AS user_email, 
                    amount, 
                    payment_status AS status, 
                    billing_cycle,
                    created_at
                FROM payments
                ORDER BY created_at DESC
                LIMIT 200
                """
            )
        except Exception as e:
            print(f"Error fetching txns: {e}")
            txns = []
    formatted_plans = []
    for r in plans:
        d = dict(r)
        if "plan_expiry" in d:
            d["plan_expiry"] = format_utc_timestamp_to_utc_iso(d["plan_expiry"])
        formatted_plans.append(d)

    formatted_txns = []
    for r in txns:
        d = dict(r)
        if "created_at" in d:
            d["created_at"] = format_local_timestamp_to_utc_iso(d["created_at"])
        formatted_txns.append(d)

    return {"user_plans": formatted_plans, "transactions": formatted_txns}


@app.post("/admin/users/warn")
async def admin_warn_user_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    violation_type = (body.get("violation_type") or "manual_admin_warning").strip()
    message = (body.get("message") or "Admin issued a governance warning").strip()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        from security.warning_manager import issue_governance_warning
        await issue_governance_warning(conn, email, violation_type, message)
    return {"ok": True}


@app.post("/admin/users/freeze")
async def admin_freeze_user_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        async with conn.transaction():
            freeze_until = datetime.utcnow() + timedelta(days=1)
            await conn.execute(
                """
                UPDATE users
                SET status = 'frozen',
                    freeze_until = $1
                WHERE email = $2
                """,
                freeze_until,
                email
            )
            await conn.execute(
                """
                INSERT INTO governance_logs (user_email, event_type, detail, created_at)
                VALUES ($1, 'manual_freeze', 'Admin manually froze the account for 24 hours', CURRENT_TIMESTAMP)
                """,
                email
            )
    return {"ok": True}


@app.post("/admin/payments/cancel-subscription")
async def admin_cancel_subscription_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    email = (body.get("email") or "").strip()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        user_row = await conn.fetchrow(
            "SELECT plan, plan_expiry FROM users WHERE email = $1 LIMIT 1", email
        )
        if not user_row or user_row["plan"] == "free":
            raise HTTPException(status_code=400, detail="No active paid subscription found to cancel")
        await conn.execute("UPDATE users SET cancel_at_period_end = TRUE WHERE email = $1", email)
        await conn.execute(
            """
            INSERT INTO governance_logs (user_email, event_type, detail, created_at)
            VALUES ($1, 'subscription_cancelled', 'Admin manually cancelled the subscription', CURRENT_TIMESTAMP)
            """,
            email
        )
    return {"ok": True}


@app.post("/admin/payments/refund")
async def admin_refund_payment_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    transaction_id = (body.get("transaction_id") or "").strip()
    if not transaction_id:
        raise HTTPException(status_code=400, detail="transaction_id is required")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE payments
            SET payment_status = 'refunded'
            WHERE razorpay_payment_id = $1 OR razorpay_order_id = $1
            """,
            transaction_id
        )
    return {"ok": True}


@app.get("/admin/settings")
async def admin_settings_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    defaults = {
        "default_row_limit": "1000",
        "min_rows_threshold": "5",
        "default_rate_limit": "60",
        "enable_downloads": "true",
        "enable_charts": "true",
        "enable_sensitive_columns": "false",
        "platform.default_row_limit": "1000",
        "platform.default_query_limit": "100",
        "privacy.min_rows_threshold": "5",
        "privacy.enable_sensitive_columns": "false",
        "api.default_rate_limit": "60",
        "api.timeout_seconds": "30",
        "storage.max_upload_size_mb": "512",
        "storage.auto_delete_uploads": "false",
        "features.enable_charts": "true",
        "features.enable_downloads": "true",
        "payments.enabled": "false",
        "payments.default_plan_limits": "free:1000,pro:100000",
        "payments.pricing_config": "free=0,pro=99",
        # ── Plan Governance: per-plan limits (read by plan_enforcer.py) ──
        "plans.free.max_queries_per_month": "100",
        "plans.free.max_rows_per_month": "5000",
        "plans.free.max_downloads_per_month": "10",
        "plans.free.rate_limit": "5",
        "plans.free.export_formats": "csv",
        "plans.free.max_ai_queries_per_month": "3",
        "plans.free.api_access": "false",
        "plans.free.advanced_analytics": "false",
        "plans.free.downloads_allowed": "true",

        "plans.pro.max_queries_per_month": "1000",
        "plans.pro.max_rows_per_month": "100000",
        "plans.pro.max_downloads_per_month": "100",
        "plans.pro.rate_limit": "20",
        "plans.pro.export_formats": "csv,pdf,json",
        "plans.pro.max_ai_queries_per_month": "50",
        "plans.pro.api_access": "true",
        "plans.pro.advanced_analytics": "true",
        "plans.pro.downloads_allowed": "true",

        "plans.enterprise.max_queries_per_month": "999999",
        "plans.enterprise.max_rows_per_month": "999999999",
        "plans.enterprise.max_downloads_per_month": "999999",
        "plans.enterprise.rate_limit": "100",
        "plans.enterprise.export_formats": "csv,excel,pdf,json,api",
        "plans.enterprise.max_ai_queries_per_month": "999999",
        "plans.enterprise.api_access": "true",
        "plans.enterprise.advanced_analytics": "true",
        "plans.enterprise.downloads_allowed": "true",
    }
    pool = request.app.state.db
    async with pool.acquire() as conn:
        for k, v in defaults.items():
            await conn.execute(
                """
                INSERT INTO system_settings (key, value, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (key) DO NOTHING
                """,
                k,
                v,
            )
        try:
            rows = await conn.fetch("SELECT key, value, updated_at FROM system_settings ORDER BY key")
        except Exception:
            rows = []
    settings_map = {r["key"]: str(r["value"]) for r in rows}
    return {
        "settings": [dict(r) for r in rows],
        "effective": {
            "default_row_limit": settings_map.get("default_row_limit") or settings_map.get("platform.default_row_limit", "1000"),
            "min_rows_threshold": settings_map.get("min_rows_threshold") or settings_map.get("privacy.min_rows_threshold", "5"),
            "default_rate_limit": settings_map.get("default_rate_limit") or settings_map.get("api.default_rate_limit", "60"),
            "enable_downloads": settings_map.get("enable_downloads") or settings_map.get("features.enable_downloads", "true"),
            "enable_charts": settings_map.get("enable_charts") or settings_map.get("features.enable_charts", "true"),
            "plans": {
                "free": {
                    "max_queries_per_month": settings_map.get("plans.free.max_queries_per_month", "100"),
                    "max_rows_per_month": settings_map.get("plans.free.max_rows_per_month", "5000"),
                    "max_downloads_per_month": settings_map.get("plans.free.max_downloads_per_month", "10"),
                    "rate_limit": settings_map.get("plans.free.rate_limit", "5"),
                    "export_formats": settings_map.get("plans.free.export_formats", "csv"),
                    "max_ai_queries_per_month": settings_map.get("plans.free.max_ai_queries_per_month", "3"),
                    "api_access": settings_map.get("plans.free.api_access", "false"),
                    "advanced_analytics": settings_map.get("plans.free.advanced_analytics", "false"),
                },
                "pro": {
                    "max_queries_per_month": settings_map.get("plans.pro.max_queries_per_month", "1000"),
                    "max_rows_per_month": settings_map.get("plans.pro.max_rows_per_month", "100000"),
                    "max_downloads_per_month": settings_map.get("plans.pro.max_downloads_per_month", "100"),
                    "rate_limit": settings_map.get("plans.pro.rate_limit", "20"),
                    "export_formats": settings_map.get("plans.pro.export_formats", "csv,pdf,json"),
                    "max_ai_queries_per_month": settings_map.get("plans.pro.max_ai_queries_per_month", "50"),
                    "api_access": settings_map.get("plans.pro.api_access", "true"),
                    "advanced_analytics": settings_map.get("plans.pro.advanced_analytics", "true"),
                },
                "enterprise": {
                    "max_queries_per_month": settings_map.get("plans.enterprise.max_queries_per_month", "999999"),
                    "max_rows_per_month": settings_map.get("plans.enterprise.max_rows_per_month", "999999999"),
                    "max_downloads_per_month": settings_map.get("plans.enterprise.max_downloads_per_month", "999999"),
                    "rate_limit": settings_map.get("plans.enterprise.rate_limit", "100"),
                    "export_formats": settings_map.get("plans.enterprise.export_formats", "csv,excel,pdf,json,api"),
                    "max_ai_queries_per_month": settings_map.get("plans.enterprise.max_ai_queries_per_month", "999999"),
                    "api_access": settings_map.get("plans.enterprise.api_access", "true"),
                    "advanced_analytics": settings_map.get("plans.enterprise.advanced_analytics", "true"),
                },
            },
        },
    }


@app.post("/admin/settings/update")
async def admin_settings_update_api(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    body = await request.json()
    settings = body.get("settings")
    if settings is None:
        keys = ["default_row_limit", "min_rows_threshold", "default_rate_limit", "enable_downloads", "enable_charts"]
        settings = {k: body[k] for k in keys if k in body}
    settings = settings or {}
    if not isinstance(settings, dict):
        raise HTTPException(status_code=400, detail="settings must be an object")
    pool = request.app.state.db
    aliases = {
        "platform.default_row_limit": "default_row_limit",
        "default_row_limit": "platform.default_row_limit",
        "privacy.min_rows_threshold": "min_rows_threshold",
        "min_rows_threshold": "privacy.min_rows_threshold",
        "api.default_rate_limit": "default_rate_limit",
        "default_rate_limit": "api.default_rate_limit",
        "features.enable_downloads": "enable_downloads",
        "enable_downloads": "features.enable_downloads",
        "features.enable_charts": "enable_charts",
        "enable_charts": "features.enable_charts",
        "privacy.enable_sensitive_columns": "enable_sensitive_columns",
        "enable_sensitive_columns": "privacy.enable_sensitive_columns",
    }
    async with pool.acquire() as conn:
        for k, v in settings.items():
            key = str(k).strip()
            if not key:
                continue
            val = str(v)
            await conn.execute(
                """
                INSERT INTO system_settings (key, value, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = NOW()
                """,
                key,
                val,
            )
            mirror = aliases.get(key)
            if mirror:
                await conn.execute(
                    """
                    INSERT INTO system_settings (key, value, updated_at)
                    VALUES ($1, $2, NOW())
                    ON CONFLICT (key) DO UPDATE SET
                        value = EXCLUDED.value,
                        updated_at = NOW()
                    """,
                    mirror,
                    val,
                )
    return {"ok": True}


# =================== PAYMENTS / RAZORPAY INTEGRATION ===================
import hmac
import hashlib
import httpx
from datetime import datetime, timedelta

def _load_razorpay_keys_directly():
    key_id = os.getenv("Test Key ID") or os.getenv("RAZORPAY_KEY_ID") or ""
    key_secret = os.getenv("Test Key Secret") or os.getenv("RAZORPAY_KEY_SECRET") or ""
    if not key_id or not key_secret:
        try:
            with open(".env", "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip("'").strip('"')
                        if k == "Test Key ID":
                            key_id = v
                        elif k == "Test Key Secret":
                            key_secret = v
        except Exception:
            pass
    return key_id, key_secret

RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET = _load_razorpay_keys_directly()

@app.post("/api/payments/create-order")
async def api_payments_create_order(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    body = await request.json()
    plan = (body.get("plan") or "").strip().lower()
    billing_cycle = (body.get("billing_cycle") or "monthly").strip().lower()

    if plan not in {"pro", "enterprise"}:
        raise HTTPException(status_code=400, detail="Invalid plan selected")
    if billing_cycle not in {"monthly", "annual"}:
        raise HTTPException(status_code=400, detail="Invalid billing cycle")

    # Determine amount based on plan and billing cycle
    # Pro: ₹299 monthly, ₹239 annually equivalent (₹239 * 12 = ₹2868)
    # Enterprise: ₹999 monthly, ₹799 annually equivalent (₹799 * 12 = ₹9588)
    if plan == "pro":
        amount_in_rupees = 299 if billing_cycle == "monthly" else (239 * 12)
    else: # enterprise
        amount_in_rupees = 999 if billing_cycle == "monthly" else (799 * 12)

    amount_in_paise = amount_in_rupees * 100

    pool = request.app.state.db
    async with pool.acquire() as conn:
        # Get user id and username
        user_row = await conn.fetchrow(
            "SELECT id, username FROM users WHERE email = $1 LIMIT 1",
            current_user.username
        )
        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")
        
        user_id = user_row["id"]
        username = user_row["username"] or current_user.username

        # Create order in Razorpay using HTTP Basic auth
        auth_header = httpx.BasicAuth(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET)
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.post(
                    "https://api.razorpay.com/v1/orders",
                    auth=auth_header,
                    json={
                        "amount": amount_in_paise,
                        "currency": "INR",
                        "receipt": f"receipt_{user_id}_{int(datetime.now().timestamp())}",
                        "notes": {
                            "user_email": current_user.username,
                            "plan": plan,
                            "billing_cycle": billing_cycle
                        }
                    },
                    timeout=10.0
                )
                if resp.status_code != 200:
                    print(f"Razorpay API Error: {resp.status_code} - {resp.text}")
                    raise HTTPException(status_code=500, detail="Failed to initiate payment with Razorpay")
                order_data = resp.json()
            except Exception as e:
                print(f"Razorpay API Exception: {e}")
                raise HTTPException(status_code=500, detail="Payment gateway connection failed")

        order_id = order_data["id"]

        # Log pending payment in payments table
        await conn.execute(
            """
            INSERT INTO payments (
                user_id, username, email, current_plan, purchased_plan, 
                payment_provider, razorpay_order_id, amount, currency, 
                payment_status, billing_cycle, created_at
            ) VALUES ($1, $2, $3, (SELECT plan FROM users WHERE id = $1), $4, 'razorpay', $5, $6, 'INR', 'pending', $7, NOW())
            """,
            user_id,
            username,
            current_user.username,
            plan,
            order_id,
            amount_in_rupees,
            billing_cycle
        )

    return {
        "id": order_id,
        "amount": amount_in_paise,
        "currency": "INR",
        "key": RAZORPAY_KEY_ID
    }

@app.post("/api/payments/verify-payment")
async def api_payments_verify_payment(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    body = await request.json()
    order_id = (body.get("razorpay_order_id") or "").strip()
    payment_id = (body.get("razorpay_payment_id") or "").strip()
    signature = (body.get("razorpay_signature") or "").strip()

    if not order_id or not payment_id or not signature:
        raise HTTPException(status_code=400, detail="Missing required payment credentials")

    # Secure verification of Razorpay HMAC Signature
    msg = f"{order_id}|{payment_id}".encode("utf-8")
    key = RAZORPAY_KEY_SECRET.encode("utf-8")
    computed = hmac.new(key, msg, hashlib.sha256).hexdigest()

    pool = request.app.state.db
    async with pool.acquire() as conn:
        # Fetch matching pending payment record
        pay_row = await conn.fetchrow(
            "SELECT * FROM payments WHERE razorpay_order_id = $1 AND email = $2 LIMIT 1",
            order_id,
            current_user.username
        )
        if not pay_row:
            raise HTTPException(status_code=404, detail="Order record not found")

        if not hmac.compare_digest(computed, signature):
            # Signature mismatch: log failure in database
            await conn.execute(
                "UPDATE payments SET payment_status = 'failed', razorpay_payment_id = $1 WHERE razorpay_order_id = $2",
                payment_id,
                order_id
            )
            # Log suspicious activity/governance failure
            await conn.execute(
                """
                INSERT INTO suspicious_activity_logs (user_email, activity_type, risk_score, detail, created_at)
                VALUES ($1, 'PAYMENT_SIGNATURE_TAMPERING', 80, $2, NOW())
                """,
                current_user.username,
                f"Failed payment signature verification for Order ID {order_id}"
            )
            raise HTTPException(status_code=400, detail="Payment verification failed: invalid signature")

        # Success: calculate subscription dates
        billing_cycle = pay_row["billing_cycle"]
        purchased_plan = pay_row["purchased_plan"]
        
        start_date = datetime.now()
        if billing_cycle == "annual":
            expiry_date = start_date + timedelta(days=365)
        else:
            expiry_date = start_date + timedelta(days=30)

        # Update payments record
        await conn.execute(
            """
            UPDATE payments 
            SET payment_status = 'success', 
                razorpay_payment_id = $1, 
                start_date = $2, 
                expiry_date = $3 
            WHERE razorpay_order_id = $4
            """,
            payment_id,
            start_date,
            expiry_date,
            order_id
        )

        # Upgrade User's plan & plan_expiry in users table
        await conn.execute(
            """
            UPDATE users 
            SET plan = $1, 
                plan_expiry = $2,
                cancel_at_period_end = FALSE
            WHERE email = $3
            """,
            purchased_plan,
            expiry_date,
            current_user.username
        )

        # Immediately log a governance log event for the upgrade
        await conn.execute(
            """
            INSERT INTO governance_logs (user_email, event_type, detail, created_at)
            VALUES ($1, 'PLAN_UPGRADE', $2, NOW())
            """,
            current_user.username,
            f"User automatically upgraded to {purchased_plan.upper()} plan via Razorpay (Order ID: {order_id})"
        )

    return {"ok": True, "message": "Payment verified and plan upgraded successfully"}

@app.post("/api/payments/payment-failed")
async def api_payments_failed_notification(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    body = await request.json()
    order_id = (body.get("razorpay_order_id") or "").strip()
    error_reason = (body.get("reason") or "Payment failed/cancelled").strip()

    pool = request.app.state.db
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE payments SET payment_status = 'failed' WHERE razorpay_order_id = $1 AND email = $2",
            order_id,
            current_user.username
        )
        # Log to governance logs
        await conn.execute(
            """
            INSERT INTO governance_logs (user_email, event_type, detail, created_at)
            VALUES ($1, 'PAYMENT_FAILED', $2, NOW())
            """,
            current_user.username,
            f"Payment failed for Order ID {order_id}. Reason: {error_reason}"
        )
    return {"ok": True}

@app.post("/api/payments/cancel-subscription")
async def api_payments_cancel_subscription(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        user_row = await conn.fetchrow(
            "SELECT plan, plan_expiry FROM users WHERE email = $1 LIMIT 1",
            current_user.username
        )
        if not user_row or user_row["plan"] == "free":
            raise HTTPException(status_code=400, detail="No active paid subscription found to cancel")

        await conn.execute(
            "UPDATE users SET cancel_at_period_end = TRUE WHERE email = $1",
            current_user.username
        )

        await conn.execute(
            """
            INSERT INTO governance_logs (user_email, event_type, detail, created_at)
            VALUES ($1, 'SUBSCRIPTION_CANCELLED', $2, NOW())
            """,
            current_user.username,
            f"User cancelled their active subscription. Access remains until {user_row['plan_expiry']}"
        )
        
    return {"ok": True, "message": "Subscription cancelled successfully. You can use your remaining quota until the end of the billing period."}

@app.post("/api/payments/reactivate-subscription")
async def api_payments_reactivate_subscription(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        user_row = await conn.fetchrow(
            "SELECT plan, plan_expiry, cancel_at_period_end FROM users WHERE email = $1 LIMIT 1",
            current_user.username
        )
        if not user_row or not user_row["cancel_at_period_end"]:
            raise HTTPException(status_code=400, detail="No cancelled subscription found to reactivate")

        await conn.execute(
            "UPDATE users SET cancel_at_period_end = FALSE WHERE email = $1",
            current_user.username
        )

        await conn.execute(
            """
            INSERT INTO governance_logs (user_email, event_type, detail, created_at)
            VALUES ($1, 'SUBSCRIPTION_REACTIVATED', 'User reactivated their cancelled subscription', NOW())
            """,
            current_user.username
        )
        
    return {"ok": True, "message": "Subscription successfully reactivated!"}




# =================== OPENAPI CUSTOMIZATION ===================


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="Statathon API Gateway",
        version="1.0.0",
        description="Upload and query datasets securely",
        routes=app.routes,
    )

    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}
    }

    # Apply to all routes
    for path in openapi_schema["paths"].values():
        for method in path.values():
            method.setdefault("security", []).append({"BearerAuth": []})

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi
