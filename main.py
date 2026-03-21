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

today = date.today().isoformat()
START_TIME = datetime.now()

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
    print("🔄 Starting up...")
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
templates = Jinja2Templates(directory="templates")
app.add_middleware(SessionMiddleware, secret_key="yoursecretkey")


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


@app.post("/register", response_class=HTMLResponse)
async def register_user(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    role: str = Form("user"),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        # Check if user already exists
        existing = await conn.fetchrow("SELECT * FROM users WHERE email = $1", email)
        if existing:
            return templates.TemplateResponse(
                "register.html", {"request": request, "error": "User already exists"}
            )

        # 🔒 BLOCK ADMIN REGISTRATION
        if role.lower() == "admin":
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
            return templates.TemplateResponse(
                "register.html",
                {"request": request, "error": "User role not found in database"},
            )

        role_id = role_row["id"]
        hashed_pw = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

        await conn.execute(
            "INSERT INTO users (username, email, hashed_password, role_id) VALUES ($1, $2, $3, $4)",
            username,
            email,
            hashed_pw,
            role_id,
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


@app.get("/user/dashboard", response_class=HTMLResponse)
async def user_dashboard_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    return templates.TemplateResponse(
        "USER_PAGES/user_dashboard.html",
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
        },
    )


@app.get("/user/profile", response_class=HTMLResponse)
async def user_profile_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    return templates.TemplateResponse(
        "USER_PAGES/user_profile.html", 
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
        }
    )


@app.get("/user/history", response_class=HTMLResponse)
async def user_history_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    return templates.TemplateResponse(
        "USER_PAGES/user_history.html", 
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
        }
    )


@app.get("/user/downloads", response_class=HTMLResponse)
async def user_downloads_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    return templates.TemplateResponse(
        "USER_PAGES/user_downloads.html", 
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
        }
    )


@app.get("/user/plans", response_class=HTMLResponse)
async def user_plans_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    return templates.TemplateResponse(
        "USER_PAGES/user_plans.html", 
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
        }
    )


@app.get("/user/settings", response_class=HTMLResponse)
async def user_settings_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    return templates.TemplateResponse(
        "USER_PAGES/user_settings.html", 
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
        }
    )

@app.get("/user/usage", response_class=HTMLResponse)
async def user_usage_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    return templates.TemplateResponse(
        "USER_PAGES/user_usage.html", 
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
        }
    )


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
        active_users = await conn.fetchval("SELECT COUNT(*) FROM users")

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
    return templates.TemplateResponse(
        "query_ui.html",
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,  # Since username is email in your case
            "role": current_user.role,
        },
    )


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
    return templates.TemplateResponse(
        "explorer.html",
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "role": current_user.role,
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
    role_name = _normalize_role(getattr(user, "role", "user"))

    row = await conn.fetchrow(
        """
        SELECT
            COALESCE(is_blocked, FALSE) AS is_blocked,
            COALESCE(plan, 'free') AS plan,
            plan_expiry,
            COALESCE(max_queries_per_day, max_queries_day, 1000) AS max_queries_per_day,
            COALESCE(max_rows_per_day, max_rows_day, 100000) AS max_rows_per_day
        FROM users
        WHERE email = $1
        LIMIT 1
        """,
        user_email,
    )

    is_blocked = bool(row["is_blocked"]) if row else False
    if is_blocked:
        raise HTTPException(status_code=403, detail="User blocked")

    if role_name == "admin":
        return {"ok": True}

    plan = (str(row["plan"]) if row else "free").strip().lower()
    plan_expiry = row["plan_expiry"] if row else None
    max_queries = int(row["max_queries_per_day"] or 1000) if row else 1000
    max_rows = int(row["max_rows_per_day"] or 100000) if row else 100000

    if plan == "free":
        max_queries = min(max_queries, 200)
        max_rows = min(max_rows, 50000)
    elif plan == "pro":
        max_queries = max(max_queries, 5000)
        max_rows = max(max_rows, 500000)

    if plan_expiry and plan_expiry < datetime.utcnow():
        plan = "free"
        max_queries = min(max_queries, 200)
        max_rows = min(max_rows, 50000)

    default_rate_limit = await _get_system_setting(conn, "default_rate_limit", "1000")
    try:
        max_queries = min(max_queries, max(1, int(default_rate_limit)))
    except Exception:
        pass

    action = str(context.get("action") or "").lower()
    if action == "download":
        downloads_enabled = (await _get_system_setting(conn, "enable_downloads", "true")).strip().lower() == "true"
        if not downloads_enabled:
            raise HTTPException(status_code=403, detail="Downloads are disabled by system settings")

    daily = await conn.fetchrow(
        """
        SELECT
            COUNT(*) AS daily_queries,
            COALESCE(SUM(rows_returned), 0) AS daily_rows
        FROM usage_logs
        WHERE user_email = $1
          AND queried_at >= CURRENT_DATE
        """,
        user_email,
    )
    daily_queries = int(daily["daily_queries"] or 0) if daily else 0
    daily_rows = int(daily["daily_rows"] or 0) if daily else 0

    if action == "query":
        requested_rows = int(context.get("requested_rows") or 0)
        if daily_queries >= max_queries:
            raise HTTPException(status_code=429, detail="Daily query limit exceeded")
        if (daily_rows + max(0, requested_rows)) > max_rows:
            raise HTTPException(status_code=429, detail="Daily rows limit exceeded")

    return {
        "ok": True,
        "plan": plan,
        "max_queries_per_day": max_queries,
        "max_rows_per_day": max_rows,
        "daily_queries": daily_queries,
        "daily_rows": daily_rows,
    }


async def apply_config(schema, table, user, columns, rows):
    conn = _CFG_CONN.get(None)
    filters = _CFG_FILTERS.get()
    labels = _CFG_LABELS.get() or {}
    if conn is None:
        raise RuntimeError("Missing DB connection for config context")

    user_role = _normalize_role(getattr(user, "role", "user"))
    print("USER ROLE:", user_role)

    if user_role == "admin":
        print("FINAL COLUMNS:", columns)
        if rows is None:
            return columns, None
        return columns, [dict(r) for r in rows]

    dataset_cfg = await get_dataset_configs(schema)
    table_cfg = dataset_cfg.get(table)
    if table_cfg is not None and table_cfg.get("show_table_to_users") is False:
        raise TableHidden()

    if not columns and rows is None:
        return [], None

    var_cfg = await get_variable_configs(schema, table)
    sensitive_enabled = (
        await _get_system_setting(conn, "enable_sensitive_columns", await _get_system_setting(conn, "privacy.enable_sensitive_columns", "false"))
    ).strip().lower() == "true"
    global_min_rows_raw = await _get_system_setting(conn, "min_rows_threshold", await _get_system_setting(conn, "privacy.min_rows_threshold", "5"))
    try:
        global_min_rows = max(1, int(global_min_rows_raw))
    except Exception:
        global_min_rows = 5

    allowed_columns = []
    min_rows_required = 0
    for col in columns:
        cfg = var_cfg.get(col)
        if cfg and cfg.get("include_in_api") is False:
            continue
        if cfg and cfg.get("is_sensitive") and user_role != "admin" and not sensitive_enabled:
            continue
        if cfg and cfg.get("is_sensitive"):
            try:
                min_rows_required = max(min_rows_required, int(cfg.get("min_rows") or 5))
            except Exception:
                min_rows_required = max(min_rows_required, 5)
        allowed_columns.append(col)

    print("FINAL COLUMNS:", allowed_columns)

    if filters and columns:
        used_cols = _extract_filter_columns(filters, columns)
        allowed_set = set(allowed_columns)
        for c in used_cols:
            if c not in allowed_set:
                raise HTTPException(status_code=400, detail=f"Invalid or restricted filter column: {c}")
            cfg = var_cfg.get(c)
            if cfg and cfg.get("filterable") is False:
                raise HTTPException(status_code=400, detail=f"Column '{c}' is not configured as filterable.")

    if rows is None:
        return allowed_columns, None

    result = []
    for row in rows:
        row_dict = dict(row)
        filtered = {}
        for col in allowed_columns:
            val = row_dict.get(col)
            if val is not None:
                col_labels = labels.get(col) or {}
                if col_labels:
                    val = col_labels.get(str(val), val)
            filtered[col] = val
        result.append(filtered)

    min_rows_required = max(min_rows_required, global_min_rows)
    if user_role != "admin" and min_rows_required > 0 and len(result) < min_rows_required:
        raise HTTPException(status_code=403, detail="Suppressed")

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
    current_user=Depends(get_current_user),  # <-- Add user dependency!
):
    try:
        pool = request.app.state.db
        async with pool.acquire() as conn:
            await apply_admin_rules(conn, current_user, {"action": "query", "requested_rows": int(limit)})
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
                await log_usage(
                    conn,
                    current_user.username,
                    f"/datasets/{schema}/{table}/query",
                    schema,
                    table,
                    0,
                    0,
                )
                raise
            finally:
                _reset_apply_context(tokens)

            await log_usage(
                conn,
                current_user.username,
                f"/datasets/{schema}/{table}/query",
                schema,
                table,
                len(filtered),
                len(json.dumps(filtered, default=str).encode()),
            )
            print(f"DEBUG: Query returned {len(filtered)} rows")
            return filtered

    except HTTPException:
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

    return templates.TemplateResponse(
        "admin_change_password.html",
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,
            "success": "Password changed successfully!",
        },
    )


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
                query_logs = await conn.fetch(
                    f"""
                    SELECT
                        user_email,
                        CASE
                            WHEN COALESCE(NULLIF(schema_name, ''), '') = '' AND COALESCE(NULLIF(table_name, ''), '') = '' THEN COALESCE(endpoint, '-')
                            ELSE COALESCE(NULLIF(schema_name, ''), '-') || '/' || COALESCE(NULLIF(table_name, ''), '-')
                        END AS dataset_table,
                        '-' AS filters,
                        COALESCE({usage_rows_col or '0'}, 0) AS rows_returned,
                        {usage_time_col} AS time
                    FROM usage_logs
                    ORDER BY {usage_time_col} DESC
                    LIMIT 100
                    """
                )
            except Exception:
                query_logs = []
        if not query_logs and query_time_col and query_has_identity:
            try:
                dataset_expr = "COALESCE(dataset_name, '-') || '/' || COALESCE(table_name, '-')" if query_has_dataset else "'-'"
                filters_expr = "COALESCE(filters, '-')" if query_has_filters else "'-'"
                query_logs = await conn.fetch(
                    f"""
                    SELECT
                        user_email,
                        {dataset_expr} AS dataset_table,
                        {filters_expr} AS filters,
                        COALESCE({query_rows_col or '0'}, 0) AS rows_returned,
                        {query_time_col} AS time
                    FROM query_logs
                    ORDER BY {query_time_col} DESC
                    LIMIT 100
                    """
                )
            except Exception:
                query_logs = []

        usage_downloads = []
        if usage_time_col and usage_has_identity and usage_endpoint_col:
            try:
                usage_downloads = await conn.fetch(
                    f"""
                    SELECT
                        COALESCE(NULLIF(split_part(endpoint, '/', 4), ''), 'download') AS file_name,
                        user_email,
                        COALESCE({usage_bytes_col or '0'}, 0) AS size_bytes,
                        {usage_time_col} AS time
                    FROM usage_logs
                    WHERE endpoint ILIKE '/downloads/%'
                    ORDER BY {usage_time_col} DESC
                    LIMIT 100
                    """
                )
            except Exception:
                usage_downloads = []

        table_downloads = []
        if download_time_col and download_has_file and download_has_identity:
            try:
                table_downloads = await conn.fetch(
                    f"""
                    SELECT
                        file_name,
                        user_email,
                        COALESCE({download_size_col or '0'}, 0) AS size_bytes,
                        {download_time_col} AS time
                    FROM download_logs
                    ORDER BY {download_time_col} DESC
                    LIMIT 100
                    """
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
        top_users = []

        if usage_time_col and usage_has_identity:
            try:
                total_queries = await conn.fetchval("SELECT COUNT(*) FROM usage_logs")
                active_users = await conn.fetchval("SELECT COUNT(DISTINCT user_email) FROM usage_logs")
                rows_accessed = await conn.fetchval(f"SELECT COALESCE(SUM({usage_rows_col or '0'}), 0) FROM usage_logs")
                queries_over_time = await conn.fetch(
                    f"""
                    SELECT TO_CHAR(DATE({usage_time_col}), 'YYYY-MM-DD') AS day, COUNT(*) AS count
                    FROM usage_logs
                    WHERE {usage_time_col} >= NOW() - INTERVAL '14 days'
                    GROUP BY DATE({usage_time_col})
                    ORDER BY DATE({usage_time_col})
                    """
                )
                top_users = await conn.fetch(
                    """
                    SELECT user_email, COUNT(*) AS count
                    FROM usage_logs
                    GROUP BY user_email
                    ORDER BY count DESC
                    LIMIT 8
                    """
                )
            except Exception:
                total_queries = 0
                active_users = 0
                rows_accessed = 0
                queries_over_time = []
                top_users = []

        if (not total_queries) and query_time_col and query_has_identity:
            try:
                total_queries = await conn.fetchval("SELECT COUNT(*) FROM query_logs")
                active_users = await conn.fetchval("SELECT COUNT(DISTINCT user_email) FROM query_logs")
                rows_accessed = await conn.fetchval(f"SELECT COALESCE(SUM({query_rows_col or '0'}), 0) FROM query_logs")
                queries_over_time = await conn.fetch(
                    f"""
                    SELECT TO_CHAR(DATE({query_time_col}), 'YYYY-MM-DD') AS day, COUNT(*) AS count
                    FROM query_logs
                    WHERE {query_time_col} >= NOW() - INTERVAL '14 days'
                    GROUP BY DATE({query_time_col})
                    ORDER BY DATE({query_time_col})
                    """
                )
                top_users = await conn.fetch(
                    """
                    SELECT user_email, COUNT(*) AS count
                    FROM query_logs
                    GROUP BY user_email
                    ORDER BY count DESC
                    LIMIT 8
                    """
                )
            except Exception:
                pass

    return {
        "query_logs": [dict(r) for r in query_logs],
        "download_logs": merged_downloads[:100],
        "metrics": {
            "total_queries": int(total_queries or 0),
            "active_users": int(active_users or 0),
            "rows_accessed": int(rows_accessed or 0),
        },
        "charts": {
            "queries_over_time": [dict(r) for r in queries_over_time],
            "top_users": [dict(r) for r in top_users],
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
                COALESCE(u.is_verified, FALSE) AS is_verified,
                COALESCE(u.document_uploaded, FALSE) AS document_uploaded,
                COALESCE(u.is_blocked, FALSE) AS is_blocked,
                CASE WHEN COALESCE(u.is_blocked, FALSE) THEN 'blocked' ELSE 'active' END AS status,
                COALESCE(u.plan, 'free') AS plan,
                u.plan_expiry,
                COALESCE(u.max_queries_per_day, u.max_queries_day, 1000) AS max_queries_day,
                COALESCE(u.max_rows_per_day, u.max_rows_day, 100000) AS max_rows_day,
                COALESCE(u.blocked_reason, '') AS blocked_reason
            FROM users u
            LEFT JOIN roles r ON r.id = u.role_id
            ORDER BY u.created_at DESC
            """
        )
    return {"users": [dict(r) for r in users]}


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
            reason if blocked else "",
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
        can_verify = await conn.fetchval(
            """
            SELECT CASE
                     WHEN COALESCE(is_verified, FALSE) = FALSE
                      AND COALESCE(document_uploaded, FALSE) = TRUE
                     THEN TRUE ELSE FALSE
                   END
            FROM users
            WHERE email = $1
            LIMIT 1
            """,
            email,
        )
        if not can_verify:
            raise HTTPException(status_code=400, detail="User cannot be verified")
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
    if not email or plan not in {"free", "pro"}:
        raise HTTPException(status_code=400, detail="Invalid payload")
    pool = request.app.state.db
    async with pool.acquire() as conn:
        if plan_expiry:
            await conn.execute(
                "UPDATE users SET plan = $1, plan_expiry = $2::timestamp WHERE email = $3",
                plan,
                plan_expiry,
                email,
            )
        else:
            await conn.execute(
                "UPDATE users SET plan = $1 WHERE email = $2",
                plan,
                email,
            )
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
                SELECT id, user_email, requested_dataset, status, created_at
                FROM user_requests
                WHERE request_type = 'dataset_request'
                ORDER BY created_at DESC
                LIMIT 100
                """
            )
        except Exception:
            dataset_requests = []
        try:
            feedback = await conn.fetch(
                """
                SELECT id, user_email, message, created_at
                FROM user_requests
                WHERE request_type = 'feedback'
                ORDER BY created_at DESC
                LIMIT 100
                """
            )
        except Exception:
            feedback = []
        try:
            docs = await conn.fetch(
                """
                SELECT d.id, d.user_email, d.document_name, d.document_url, d.status, d.created_at
                FROM user_documents d
                JOIN users u ON u.email = d.user_email
                WHERE COALESCE(u.is_verified, FALSE) = FALSE
                  AND COALESCE(u.document_uploaded, FALSE) = TRUE
                ORDER BY d.created_at DESC
                LIMIT 100
                """
            )
        except Exception:
            docs = []
        suspicious = await conn.fetch(
            """
            SELECT user_email, COUNT(*) AS query_count, COALESCE(SUM(rows_returned), 0) AS rows_accessed
            FROM usage_logs
            WHERE endpoint ILIKE '%query%'
              AND queried_at >= NOW() - INTERVAL '24 hours'
            GROUP BY user_email
            HAVING COUNT(*) > 50
            ORDER BY query_count DESC
            LIMIT 30
            """
        )
    return {
        "dataset_requests": [dict(r) for r in dataset_requests],
        "feedback": [dict(r) for r in feedback],
        "documents": [dict(r) for r in docs],
        "suspicious_activity": [dict(r) for r in suspicious],
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
    if request_id <= 0 or status not in {"approved", "rejected", "pending"}:
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
            SELECT email AS user_email, COALESCE(plan, 'free') AS plan, plan_expiry
            FROM users
            ORDER BY email
            LIMIT 500
            """
        )
        try:
            txns = await conn.fetch(
                """
                SELECT transaction_id, user_email, amount, status, created_at
                FROM payments
                ORDER BY created_at DESC
                LIMIT 200
                """
            )
        except Exception:
            txns = []
    return {"user_plans": [dict(r) for r in plans], "transactions": [dict(r) for r in txns]}


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
