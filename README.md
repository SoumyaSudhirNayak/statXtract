# StatXtract – Statathon Dataset API Gateway

StatXtract is a FastAPI-based gateway for exploring official survey microdata in a safe and structured way.  
It focuses on:
- DDI-based ingestion of ZIP datasets into PostgreSQL
- A role-aware query API for filtered SQL access
- Usage metering, rate limits, and cell suppression for privacy
- A simple web UI for admins and users

> Live documentation (MkDocs): https://SoumyaSudhirNayak.github.io/STATATHON_2025/

---

## Features

- **Secure Authentication**
  - JWT-based login with roles (`admin`, `user`)
  - Protected endpoints using Bearer tokens

- **Dataset Ingestion**
  - Upload ZIP files containing `.csv`, `.txt` and `.xml` (DDI)
  - Ingest into a chosen PostgreSQL schema (e.g. `public`, `plfs`)
  - Nesstar-based conversion support for `.sav` and related formats

- **Query Engine**
  - Column selection, filters, pagination
  - Role-based access control
  - Cell suppression for queries returning fewer than 5 rows (for non-admins)
  - Daily row limits per user via `usage_logs`

- **Admin Dashboard & UI**
  - Glass-style admin dashboard with:
    - Total datasets
    - Active users
    - Data schemas
    - System uptime
  - Query UI for interactive filtering and charting

---

## Project Structure

Some key paths in this repository:

- [main.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/main.py) – FastAPI application entrypoint (routes, admin dashboard, schema-aware querying)
- [auth/local/](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/auth/local) – Local auth (register, login, JWT utilities, role checks)
- [query/](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/query) – Query-related routers (safe query endpoints, suppression, logging)
- [utils/](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/utils) – Ingestion pipeline, CSV/Excel/SAV conversion, metadata helpers, ingestion watcher
- [templates/](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/templates) – HTML templates for login, admin dashboard, query UI, datasets view, upload progress
- [tests/](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/tests) – Pytest suite for ingestion pipeline, watcher, and related helpers
- [statathon-docs-only/](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only) – MkDocs configuration and standalone documentation site

For a more narrative overview, see:
- [Docs Home](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/index.md)
- [API Reference](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/api.md)
- [Authentication](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/auth.md)
- [Usage Metering](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/usage.md)

---

## Getting Started

### 1. Clone and Install

```bash
git clone https://github.com/SoumyaSudhirNayak/statXtract.git
cd Statathon_API_Gateway

python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt
```

### 2. Environment Configuration

Create a `.env` file in the project root with at least:

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/statathon
SECRET_KEY=change_me
ALGORITHM=HS256
```

If you plan to use Nesstar-based `.sav` conversion, you will also need:

- `NESSTAR_CONVERTER_EXE`
- `NESSTAR_CONVERTER_SCRIPT`

(See `COMMANDS.MD` and the utils in [utils/](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/utils) for more details.)

### 3. Initialize Core Tables

Core tables (`users`, `datasets`, `usage_logs`, metadata tables, etc.) are created automatically on application startup via:

- [utils/db_init.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/utils/db_init.py)

Make sure your PostgreSQL instance is running and accessible via `DATABASE_URL`.

### 4. Run the App

```bash
uvicorn main:app --reload
```

Then open:

- Swagger UI: http://localhost:8000/docs  
- Web UI: http://localhost:8000/login

---

## Authentication Flow (Summary)

1. **Register a user**
   - `POST /auth/register` with JSON body:
     - `username`, `email`, `password`, `role_id` (`1` = admin, `3` = user)
2. **Obtain an access token**
   - `POST /auth/token` (form-encoded `username` + `password`)
   - Response contains `access_token` and `token_type`
3. **Call protected APIs**
   - Include header:
     ```http
     Authorization: Bearer <JWT_TOKEN>
     ```

See the detailed auth docs in [auth.md](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/auth.md).

---

## Core APIs

### Query Dataset

High-level query endpoint (public schema-focused router):

- `GET /datasets/{table_name}/query`
  - `columns` – comma-separated list of columns
  - `filters` – SQL-style filter expression (`col = 1`, `col IN (...)`, `col > 10`, etc.)
  - `limit`, `offset` – pagination
  - `Accept: application/json` or `Accept: text/csv`

Example:

```http
GET /datasets/block_5_2_usual_subsidiary_economic_activity_particulars_of_hou/query?limit=10&offset=0&columns=Round_Centre_Code,FSU_Serial_No
Authorization: Bearer <ADMIN_JWT>
Accept: application/json
```

There is also a **schema-aware** query endpoint mounted in [main.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/main.py):

- `GET /datasets/{schema}/{table}/query`

### Upload Dataset (Admin)

- `POST /upload/`
  - multipart form-data:
    - `file` – ZIP with `.csv` / `.txt` and `.xml` (DDI)
    - `schema` – target schema name

After upload, the ingestion pipeline in [utils/ingestion_pipeline.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/utils/ingestion_pipeline.py) creates tables, loads data, and populates metadata tables.

---

## Privacy and Usage Controls

- **Cell Suppression**
  - For non-admin users, queries returning fewer than 5 rows are suppressed.
- **Row Caps**
  - Default daily limit for role `3` (users) is 100,000 rows.
- **Usage Logging**
  - Every query is logged to `usage_logs` with:
    - user email
    - endpoint
    - schema and table
    - rows returned
    - bytes sent

See [usage.md](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/usage.md) for details.

---

## Documentation

This repo includes a standalone MkDocs site under `statathon-docs-only/`:

- Site URL: https://SoumyaSudhirNayak.github.io/STATATHON_2025/
- Config: [mkdocs.yml](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/mkdocs.yml)
- Content:
  - [Home](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/index.md)
  - [API Reference](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/api.md)
  - [Authentication](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/auth.md)
  - [Usage Metering](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/usage.md)

---

## Contributing

1. Fork the repo
2. Create a feature branch
3. Run tests locally (e.g. `pytest`)
4. Open a Pull Request on GitHub

Issues and suggestions are welcome via the GitHub issues page.
