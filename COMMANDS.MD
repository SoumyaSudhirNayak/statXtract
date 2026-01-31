# Statathon API Gateway — Commands (Windows)

This repo is a single **FastAPI** application that serves both:
- Backend REST APIs (JSON/CSV)
- A minimal “frontend” (server-rendered HTML via Jinja templates under `templates/`)

## 0) Prerequisites

- Python 3.10+ recommended
- PostgreSQL reachable from your machine

## 1) Setup (venv + install deps)

```bat
cd /d "e:\STATATHON 2025 LOCAL\Statathon_API_Gateway"
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

## 2) Environment variables

This project uses `python-dotenv`, so it reads `.env` automatically.

Minimum required keys:
- `DATABASE_URL=postgresql://...`
- `SECRET_KEY=...`
- `ALGORITHM=HS256` (optional; defaults to HS256)

Example (do not paste real secrets into shared repos):
```env
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DBNAME
SECRET_KEY=change_me
ALGORITHM=HS256
```

## 3) Required folders

The app mounts `/static` from a local `static\` folder. Ensure it exists:

```bat
cd /d "e:\STATATHON 2025 LOCAL\Statathon_API_Gateway"
mkdir static
```

## 4) Start the backend (and UI)

Run FastAPI with Uvicorn:

```bat
cd /d "e:\STATATHON 2025 LOCAL\Statathon_API_Gateway"
.\.venv\Scripts\activate
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

Open in browser:
- Login UI: http://127.0.0.1:8000/login
- Query UI: http://127.0.0.1:8000/query
- Admin dashboard: http://127.0.0.1:8000/admin/dashboard

## 5) Swagger / OpenAPI docs

- Swagger UI: http://127.0.0.1:8000/docs
- ReDoc: http://127.0.0.1:8000/redoc
- OpenAPI JSON: http://127.0.0.1:8000/openapi.json

## 6) Create an admin user (one-time helper)

This script reads `DATABASE_URL` from `.env`:

```bat
cd /d "e:\STATATHON 2025 LOCAL\Statathon_API_Gateway"
.\.venv\Scripts\activate
py .\ADMIN_ACCOUNT\create_admin.py
```

## 7) Core API endpoints (quick reference)

### Auth

- `POST /auth/register` (JSON)
- `POST /auth/token` (form-data; OAuth2PasswordRequestForm)

### Dataset discovery

- `GET /schemas`
- `GET /datasets`
- `GET /datasets/{schema}/{table}/columns`

### Query

- `GET /datasets/{schema}/{table}/query?columns=A,B&filters=X=1;Y>2&limit=100&offset=0`
- `GET /datasets/{table_name}/query?columns=A,B&filters=X=1;Y>2&limit=100&offset=0` (public schema variant)
- `POST /query` (executes SQL provided in JSON body)

### Upload (admin/analyst)

- `POST /upload/` (multipart form-data: `file` ZIP + `schema` string)

### NADA admin integration

Prefix: `/admin/nada`
- `GET /admin/nada/datasets`
- `GET /admin/nada/datasets/{dataset_id}/fileslist`
- `POST /admin/nada/datasets/{dataset_id}/download`
- `POST /admin/nada/datasets/{dataset_id}/ingest`
- `GET /admin/nada/jobs/{job_id}`

## 8) Example API calls (PowerShell)

### Get token

```powershell
$base = "http://127.0.0.1:8000"
$resp = Invoke-RestMethod "$base/auth/token" -Method Post -ContentType "application/x-www-form-urlencoded" -Body @{
  username = "YOUR_EMAIL"
  password = "YOUR_PASSWORD"
}
$token = $resp.access_token
```

### Query a table (JSON)

```powershell
$schema = "public"
$table  = "YOUR_TABLE"
$url = "$base/datasets/$schema/$table/query?limit=10&offset=0"
Invoke-RestMethod $url -Headers @{ Authorization = "Bearer $token" }
```

### Query a table (CSV)

```powershell
$schema = "public"
$table  = "YOUR_TABLE"
$url = "$base/datasets/$schema/$table/query?limit=10&offset=0"
Invoke-WebRequest $url -Headers @{
  Authorization = "Bearer $token"
  Accept        = "text/csv"
} | Select-Object -ExpandProperty Content
```

## 9) Serve MkDocs locally (optional)

```bat
cd /d "e:\STATATHON 2025 LOCAL\Statathon_API_Gateway\statathon-docs-only"
mkdocs serve -a 127.0.0.1:8001
```

Open: http://127.0.0.1:8001/

