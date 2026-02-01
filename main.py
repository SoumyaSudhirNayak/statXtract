from fastapi import FastAPI, UploadFile, File, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.openapi.utils import get_openapi
from contextlib import asynccontextmanager
from starlette.middleware.sessions import SessionMiddleware
import asyncpg
import os
import zipfile
import difflib
import re
import bcrypt
from datetime import timedelta, datetime
from dotenv import load_dotenv


# Import your custom modules
from utils.ingestion_pipeline import process_dataset_zip, convert_and_ingest_nesstar_binary_study, discover_nesstar_converter_exe
from utils.db_init import ensure_core_tables
from utils.metadata_helper import get_column_labels, apply_labels
from utils.job_manager import create_job, get_job, update_job
from fastapi import BackgroundTasks


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

    yield
    print("🔻 Shutting down...")
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

    return templates.TemplateResponse(
        "admin_dashboard.html",
        {
            "request": request,
            "username": current_user.username,
            "email": current_user.username,  # Since username is email in your case
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


# =================== API ROUTES ===================


@app.get("/schemas")
async def get_schemas(request: Request):
    """Get list of all schemas with enhanced information"""
    try:
        pool = request.app.state.db
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schema_name;
            """)

            # Schema name to full name mapping
            schema_descriptions = {
                "asi": {
                    "name": "Annual Survey of Industries",
                    "description": "Comprehensive data on industrial production, employment, and economic indicators",
                    "icon": "fas fa-industry",
                    "category": "Industrial Survey",
                },
                "ec": {
                    "name": "Economic Census",
                    "description": "Complete enumeration of all economic enterprises and establishments",
                    "icon": "fas fa-chart-line",
                    "category": "Economic Survey",
                },
                "es": {
                    "name": "Employment Survey",
                    "description": "Data on employment patterns, job market trends, and workforce statistics",
                    "icon": "fas fa-users",
                    "category": "Labour Survey",
                },
                "eu": {
                    "name": "Enterprise Units Survey",
                    "description": "Information about business enterprises, their structure and operations",
                    "icon": "fas fa-building",
                    "category": "Enterprise Survey",
                },
                "hce": {
                    "name": "Household Consumption Expenditure",
                    "description": "Consumer spending patterns and household economic behavior data",
                    "icon": "fas fa-home",
                    "category": "Household Survey",
                },
                "iip": {
                    "name": "Index of Industrial Production",
                    "description": "Monthly industrial production indices and manufacturing statistics",
                    "icon": "fas fa-chart-bar",
                    "category": "Production Index",
                },
                "llhs": {
                    "name": "Land and Livestock Holding Survey",
                    "description": "Agricultural land holdings, livestock data, and rural economic indicators",
                    "icon": "fas fa-seedling",
                    "category": "Agricultural Survey",
                },
                "others": {
                    "name": "Other Surveys and Data",
                    "description": "Miscellaneous surveys and supplementary statistical data",
                    "icon": "fas fa-folder-open",
                    "category": "General Data",
                },
                "pg_toast": {
                    "name": "PostgreSQL System Tables",
                    "description": "Database system tables for large object storage",
                    "icon": "fas fa-database",
                    "category": "System",
                },
                "plfs": {
                    "name": "Periodic Labour Force Survey",
                    "description": "Quarterly employment, unemployment, and labour market statistics",
                    "icon": "fas fa-briefcase",
                    "category": "Labour Survey",
                },
            }

            # Return enhanced schema information
            schemas_with_info = []
            for row in rows:
                schema_name = row["schema_name"]
                info = schema_descriptions.get(
                    schema_name,
                    {
                        "name": f"{schema_name.upper()} Survey",
                        "description": "Database schema containing tables and data structures",
                        "icon": "fas fa-database",
                        "category": "Data Schema",
                    },
                )

                schemas_with_info.append(
                    {
                        "schema": schema_name,
                        "name": info["name"],
                        "description": info["description"],
                        "icon": info["icon"],
                        "category": info["category"],
                    }
                )

            print(f"DEBUG: Found {len(schemas_with_info)} schemas")
            return schemas_with_info

    except Exception as e:
        print(f"ERROR in get_schemas: {e}")
        return {"error": str(e)}


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

        # Add table data to schemas that have tables
        for row in tables_data:
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
async def get_columns(request: Request, schema: str, table: str):
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

            return columns

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
                SELECT column_name
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
            print(f"DEBUG: Column map for case sensitivity: {column_map}")

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

async def run_ingestion_job(job_id: str, zip_path: str, db_url: str, schema: str):
    """Background task wrapper for ingestion pipeline."""
    try:
        ext = os.path.splitext(zip_path)[1].lower()
        size = None
        try:
            size = os.path.getsize(zip_path)
        except Exception:
            size = None

        threshold = int(os.getenv("NESSTAR_BINARY_THRESHOLD_BYTES") or str(100 * 1024 * 1024))
        if ext == ".nesstar" and size is not None and size > threshold:
            await convert_and_ingest_nesstar_binary_study(zip_path, db_url, schema=schema, job_id=job_id)
        else:
            try:
                await process_dataset_zip(zip_path, db_url, schema=schema, job_id=job_id)
            except Exception as e:
                if ext == ".nesstar":
                    await convert_and_ingest_nesstar_binary_study(zip_path, db_url, schema=schema, job_id=job_id)
                else:
                    raise e
    except Exception as e:
        print(f"❌ Job {job_id} failed: {e}")
        job = get_job(job_id)
        if job:
            if job.get("status") != "failed":
                update_job(job_id, status="failed")
            if str(e) and str(e) not in (job.get("errors") or []):
                update_job(job_id, message=str(e), error=str(e))
    finally:
        # Clean up
        if os.path.exists(zip_path):
            try:
                os.remove(zip_path)
                print(f"🧹 Deleted ZIP: {zip_path}")
            except Exception as e:
                print(f"⚠️ Failed to delete ZIP {zip_path}: {e}")

@app.get("/admin/upload/status/{job_id}")
async def get_upload_status(job_id: str, current_user=Depends(get_current_active_user_with_role(["1", "2"]))):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.get("/admin/nesstar/jobs/{job_id}")
async def get_nesstar_job_status(job_id: str, current_user=Depends(get_current_active_user_with_role(["1"]))):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

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
    schema: str = Form("public"),
    current_user=Depends(get_current_active_user_with_role(["1", "2"])),
):
    wants_json = request.query_params.get("response") == "json" or (
        (request.headers.get("x-requested-with") or "").lower() == "xmlhttprequest"
    ) or ("application/json" in (request.headers.get("accept") or "").lower())

    try:
        if (file.filename or "").lower().endswith(".nesstar") and not getattr(request.app.state, "nesstar_enabled", False):
            msg = "Nesstar conversion is disabled: configure NESSTAR_CONVERTER_EXE and NESSTAR_CONVERTER_SCRIPT"
            if wants_json:
                return JSONResponse({"error": msg}, status_code=400)
            return f"<h3>❌ Upload failed: {msg}</h3>"

        # Create Job
        job_id = create_job(filename=file.filename)
        
        # Save file
        zip_filename = f"{job_id}_{file.filename}"
        zip_path = os.path.join(UPLOAD_DIR, zip_filename)

        with open(zip_path, "wb") as buffer:
            buffer.write(await file.read())
            
        # Start background task
        background_tasks.add_task(run_ingestion_job, job_id, zip_path, DB_URL, schema)

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
