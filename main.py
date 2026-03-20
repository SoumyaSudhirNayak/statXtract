from fastapi import FastAPI, UploadFile, File, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, Response, JSONResponse, StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.openapi.utils import get_openapi
from contextlib import asynccontextmanager
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


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/login")


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
            url="/admin/dashboard" if str(user["role_id"]) == "1" else "/query",
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


@app.get("/surveys")
async def list_surveys(request: Request, current_user=Depends(get_current_user)):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        grouped = await _group_schemas_by_survey(conn)
    return [
        {"survey": v["survey"], "display_name": v["display_name"], "dataset_count": len(v["datasets"])}
        for v in grouped.values()
    ]


@app.get("/surveys/{survey}/datasets")
async def list_survey_datasets(request: Request, survey: str, current_user=Depends(get_current_user)):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        grouped = await _group_schemas_by_survey(conn)
    g = grouped.get(survey)
    if not g:
        raise HTTPException(status_code=404, detail="Survey not found")
    datasets = g["datasets"]
    if len(datasets) > 1:
        datasets = [d for d in datasets if d["schema"] != survey]
    return datasets


@app.get("/surveys/{survey}/{dataset}/tables")
async def list_survey_tables(request: Request, survey: str, dataset: str, current_user=Depends(get_current_user)):
    pool = request.app.state.db
    async with pool.acquire() as conn:
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

    out = []
    for r in rows:
        name = r["table_name"]
        if _is_internal_table(name):
            continue
        out.append(
            {
                "table_name": name,
                "row_count": r["row_count"] or 0,
                "column_count": r["column_count"] or 0,
            }
        )
    return out


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
    return [c["column_name"] for c in cols]


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

    return [{"dataset": r["dataset_schema"], "display_name": r["dataset_display_name"]} for r in rows]


@app.get("/schemas/{schema}/{dataset}/tables")
async def list_tables_v2(
    request: Request,
    schema: str,
    dataset: str,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
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
    out = []
    for r in rows:
        name = r["table_name"]
        if name in hidden:
            continue
        out.append(
            {
                "table_name": name,
                "row_count": r["row_count"] or 0,
                "column_count": r["column_count"] or 0,
            }
        )
    return out


@app.get("/metadata/{schema}/{dataset}")
async def get_metadata_v2(
    request: Request,
    schema: str,
    dataset: str,
    current_user=Depends(get_current_user),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
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
    ddi_url = f"/downloads/{schema_db}/{dataset_schema}/ddi"
    micro_url = f"/downloads/{schema_db}/{dataset_schema}/microdata.zip"

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
async def download_ddi(schema: str, dataset: str, current_user=Depends(get_current_user)):
    survey_schema = (schema or "").strip()
    dataset_schema = (dataset or "").strip()
    if not survey_schema or not dataset_schema:
        raise HTTPException(status_code=400, detail="Invalid request")

    dataset_folder = dataset_schema.split("__", 1)[1] if "__" in dataset_schema else dataset_schema
    upload_root = Path(os.getenv("UPLOAD_DIR") or "uploads").resolve()
    ddi_path = upload_root / survey_schema / dataset_folder / "ddi.xml"
    if not ddi_path.exists():
        raise HTTPException(status_code=404, detail="DDI not found")
    return FileResponse(str(ddi_path), filename="ddi.xml")


@app.get("/downloads/{schema}/{dataset}/microdata.zip")
async def download_microdata(schema: str, dataset: str, current_user=Depends(get_current_user)):
    survey_schema = (schema or "").strip()
    dataset_schema = (dataset or "").strip()
    if not survey_schema or not dataset_schema:
        raise HTTPException(status_code=400, detail="Invalid request")

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
    return [{"display_name": r["dataset_display_name"], "db_name": r["dataset_db_name"]} for r in rows]


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
    return [
        {
            "table_name": r["table_name"],
            "display_name": r["level_display_name"] or r["table_name"],
            "row_count": r["row_count"] or 0,
            "column_count": r["column_count"] or 0,
        }
        for r in rows
    ]


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
        allowed_set = {r["variable_name"] for r in allowed_cols}
        type_map = {r["variable_name"]: (r["final_type"] or "").upper() for r in allowed_cols}

        select_cols: list[str] = []
        if columns and columns.strip() and columns.strip() != "*":
            for c in columns.split(","):
                col = c.strip()
                if not col:
                    continue
                if col not in allowed_set:
                    raise HTTPException(status_code=400, detail=f"Invalid column: {col}")
                select_cols.append(f'"{col}"')
        else:
            select_cols = ["*"]

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
        safe_limit = max(1, min(int(limit), max_limit))
        safe_offset = max(0, int(offset))
        sql = f'SELECT {", ".join(select_cols)} FROM "{schema_db}"."{table}"{where_sql} LIMIT {safe_limit} OFFSET {safe_offset}'
        rows = await conn.fetch(sql, *values)

        user_role = str(getattr(current_user, "role", ""))
        if user_role != "1" and len(rows) < 5:
            await log_usage(conn, current_user.username, f"/schemas/{schema_db}/{year}/{dataset}/{table}/query", schema_db, table, 0, 0)
            raise HTTPException(status_code=403, detail="Data suppressed (less than 5 rows)")

        data = [dict(r) for r in rows]
        await log_usage(conn, current_user.username, f"/schemas/{schema_db}/{year}/{dataset}/{table}/query", schema_db, table, len(data), len(json.dumps(data, default=str).encode()))
        return data


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
async def list_schemas_and_tables(request: Request):
    """Lists ALL schemas and their tables - INCLUDING EMPTY SCHEMAS."""
    try:
        pool = request.app.state.db
        async with pool.acquire() as conn:
            # First, get ALL schemas (including empty ones)
            all_schemas = await conn.fetch("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
                ORDER BY schema_name;
            """)

            # Then get tables with their metadata
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

        # Initialize result with ALL schemas (empty arrays for schemas without tables)
        result = {}
        for schema_row in all_schemas:
            schema_name = schema_row["schema_name"]
            result[schema_name] = []

        # Add table data to schemas that have tables (excluding internal/metadata tables)
        for row in tables_data:
            if _is_internal_table(row["table_name"]):
                continue
            result[row["table_schema"]].append(
                {
                    "table_name": row["table_name"],
                    "row_count": row["row_count"] or 0,
                    "column_count": row["column_count"] or 0,
                }
            )

        print(
            f"DEBUG: Returning {len(result)} schemas (including empty ones): {list(result.keys())}"
        )
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
                        if details:
                            return [
                                {"name": c, "type": None, "position": i + 1}
                                for i, c in enumerate(columns)
                            ]
                        return columns
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

            return column_details if details else columns

    except Exception as e:
        print(f"ERROR in get_columns for {schema}.{table}: {e}")
        return {"error": str(e)}


def fix_filter_case_sensitivity(filters: str, column_map: dict) -> str:
    """Fix case sensitivity issues in filter expressions."""
    if not filters:
        return filters

    fixed_filters = filters

    # Sort by length (longest first) to avoid partial matches
    sorted_columns = sorted(column_map.items(), key=lambda x: len(x[0]), reverse=True)

    for lower_col, actual_col in sorted_columns:
        # Use word boundaries to match whole column names only
        # Match column names that are not already quoted
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
            # Validate table exists first
            table_check = await conn.fetch(
                """
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = $1 AND table_name = $2
            """,
                schema,
                table,
            )

            if not table_check:
                return {"error": f"Table {schema}.{table} not found"}

            # Get actual column names for case-insensitive filter handling
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

            selected_cols = columns or "*"

            # Build query with proper escaping
            if selected_cols == "*":
                query = f'SELECT * FROM "{schema}"."{table}"'
            else:
                # Escape column names
                col_list = [
                    f'"{col.strip()}"'
                    for col in selected_cols.split(",")
                    if col.strip()
                ]
                query = f'SELECT {", ".join(col_list)} FROM "{schema}"."{table}"'

            if filters:
                # Fix case sensitivity in filters
                print(f"DEBUG: Original filters: {filters}")
                fixed_filters = fix_filter_case_sensitivity(filters, column_map)
                
                # Auto-quote string values if missing quotes
                fixed_filters = smart_quote_filters(fixed_filters, column_type_map)
                
                print(f"DEBUG: Fixed filters: {fixed_filters}")
                query += f" WHERE {fixed_filters}"

            query += f" LIMIT {limit} OFFSET {offset}"

            print(f"DEBUG: Executing query: {query}")
            rows = await conn.fetch(query)
            result = [dict(r) for r in rows]
            row_count = len(result)
            
            # Apply metadata labels
            try:
                label_map = await get_column_labels(conn, table)
                result = apply_labels(result, label_map)
            except Exception as e:
                print(f"⚠️ Failed to apply labels in query_table: {e}")

            # Cell suppression logic
            user_role = str(current_user.role)
            print(f"DEBUG: role={current_user.role}, row_count={row_count}")
            if user_role != "1" and row_count < 5:
                print("DEBUG: Suppression triggered!")
                await log_usage(
                    conn, current_user.username,
                    f"/datasets/{schema}/{table}/query", schema, table,
                    0, 0
                )
                raise HTTPException(
                    status_code=403,
                    detail={
                        "error": "Data Access Restricted",
                        "reason": "Fewer than 5 rows returned. Cell suppression applied.",
                        "actual_rows": row_count,
                        "role": user_role,
                    }
                )

            print(f"DEBUG: Query returned {row_count} rows")
            return result

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
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    """Get all variables/columns from all tables in a schema"""
    try:
        pool = request.app.state.db
        async with pool.acquire() as conn:
            # Get all tables in the schema
            tables = await conn.fetch(
                """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = $1 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """,
                schema,
            )

            if not tables:
                return {"error": f"No tables found in schema '{schema}'"}

            # Get all unique columns across all tables in the schema
            all_variables = set()
            table_variables = {}

            for table_row in tables:
                table_name = table_row["table_name"]
                if _is_internal_table(table_name):
                    continue

                # Get columns for this table
                columns = await conn.fetch(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                    ORDER BY ordinal_position
                """,
                    schema,
                    table_name,
                )

                table_vars = []
                for col in columns:
                    var_info = {
                        "variable_name": col["column_name"],
                        "data_type": col["data_type"],
                        "nullable": col["is_nullable"] == "YES",
                        "table": table_name,
                    }
                    table_vars.append(var_info)
                    all_variables.add(col["column_name"])

                table_variables[table_name] = table_vars

            # Create a comprehensive variable list
            variables_list = []
            for var_name in sorted(all_variables):
                # Find which tables contain this variable
                containing_tables = []
                data_types = set()

                for table_name, vars_list in table_variables.items():
                    for var in vars_list:
                        if var["variable_name"] == var_name:
                            containing_tables.append(table_name)
                            data_types.add(var["data_type"])

                # Determine if it should be filterable based on data type
                is_filterable = any(
                    dt
                    in [
                        "integer",
                        "bigint",
                        "numeric",
                        "text",
                        "varchar",
                        "character varying",
                        "date",
                        "timestamp",
                    ]
                    for dt in data_types
                )

                variables_list.append(
                    {
                        "variable_name": var_name,
                        "label": var_name.replace(
                            "_", " "
                        ).title(),  # Auto-generate label
                        "data_types": list(data_types),
                        "tables": containing_tables,
                        "include_in_api": True,  # Default to true
                        "filterable": is_filterable,
                        "table_count": len(containing_tables),
                    }
                )

            return {
                "schema": schema,
                "total_tables": len(tables),
                "total_variables": len(variables_list),
                "variables": variables_list,
                "table_details": table_variables,
            }

    except Exception as e:
        print(f"Error getting schema variables: {e}")
        return {"error": str(e)}


@app.post("/admin/update-variable-config")
async def update_variable_config(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1"])),
):
    """Update variable configuration for a schema - Fixed for checkbox handling"""
    try:
        # Get raw form data
        form_data = await request.form()
        schema = form_data.get("schema")

        if not schema:
            return {"error": "Schema parameter is required"}

        # Debug: Print received form data
        print("=== RECEIVED FORM DATA ===")
        form_dict = dict(form_data)
        for key, value in form_dict.items():
            print(f"{key}: {value}")
        print("==========================")

        # Extract all variable names from label fields
        variable_names = set()
        for key in form_data.keys():
            if key.startswith("label_"):
                var_name = key.replace("label_", "")
                variable_names.add(var_name)

        if not variable_names:
            return {"error": "No variables found in form data"}

        print(f"Processing {len(variable_names)} variables: {list(variable_names)}")

        # Process each variable
        config_updates = []
        for var_name in sorted(variable_names):
            # Get label (always present)
            label = form_data.get(f"label_{var_name}", var_name)

            # Handle checkboxes: KEY EXISTS = checked, KEY MISSING = unchecked
            include_checked = f"include_{var_name}" in form_data
            filter_checked = f"filter_{var_name}" in form_data

            config = {
                "variable_name": var_name,
                "label": label,
                "include_in_api": include_checked,
                "filterable": filter_checked,
            }
            config_updates.append(config)

            # Debug output
            print(
                f"  {var_name}: label='{label}', include={include_checked}, filter={filter_checked}"
            )

        # Here you would save to your database
        # For now, we'll just return success

        return {
            "message": f"Successfully updated configuration for {len(config_updates)} variables in schema '{schema}'",
            "schema": schema,
            "updated_count": len(config_updates),
            "configurations": config_updates,
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
    """Display usage logs page for admin"""
    try:
        pool = request.app.state.db
        async with pool.acquire() as conn:
            # Mock usage data - replace with your actual usage logs table
            # You'll need to create this table structure based on your needs

            # For now, returning mock data
            mock_logs = [
                 
        {"user_email": "statathon12@gmail.com", "day": today, "rows": 1378},
        {"user_email": "statathon12@gmail.com", "day": today, "rows": 2805},
    
                
            ]

            # In production, replace with actual database query:
            # logs = await conn.fetch("""
            #     SELECT
            #         u.email as user_email,
            #         DATE(created_at) as day,
            #         SUM(rows_returned) as rows
            #     FROM usage_logs ul
            #     JOIN users u ON ul.user_id = u.id
            #     GROUP BY u.email, DATE(created_at)
            #     ORDER BY day DESC, u.email
            #     LIMIT 100
            # """)

            usage_data = mock_logs

    except Exception as e:
        print(f"Error fetching usage logs: {e}")
        usage_data = []

    return templates.TemplateResponse(
        "usage.html", {"request": request, "logs": usage_data}
    )


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
