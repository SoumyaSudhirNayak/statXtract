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
  - Client-side SHA256 password hashing at form submission (login, register, admin change password) — passwords are never sent in plaintext over the network
  - CAPTCHA verification on all authentication forms

- **Dataset Ingestion**
  - Upload ZIP files containing `.csv`, `.txt` and `.xml` (DDI)
  - Ingest into a chosen PostgreSQL schema (e.g. `public`, `plfs`)
  - Nesstar-based conversion support for `.sav` and related formats

- **Query Engine**
  - Column selection, filters, pagination
  - Role-based access control
  - Cell suppression for queries returning fewer than 5 rows (for non-admins)
  - Daily row limits per user via `usage_logs`

- **Advanced SQL Editor (Governed)**
  - Full SQL editor with syntax highlighting for custom SELECT/CTE queries
  - Governed pipeline applying the same security controls as the Query Builder:
    - SQL validation (read-only enforcement, write-operation blocking)
    - Rate limiting (30/200/unlimited queries per hour by role)
    - Query cost protection via EXPLAIN plan analysis
    - Variable configuration enforcement (hidden/sensitive column filtering)
    - Cell suppression (5-row threshold for non-aggregated queries)
    - Aggregation cell suppression (per-group count threshold)
    - Automatic LIMIT 1000 injection for non-admin users
  - Full audit logging to `usage_logs` and `governance_logs`
  - Schema-qualified metadata queries (`public.variable_configs`) for reliable cross-schema access

- **Governed Dataset Explorer**
  - Hierarchical logical survey -> dataset -> table navigation tree.
  - Variable profile schemas (label, decimal width, concepts) and descriptive DDI metadata card renderers.
  - Pro/Enterprise tier limits restricting advanced stats (mean, min, max, stddev) and frequency distributions.
  - Tabular governed previews (Free: 5, Pro: 50, Enterprise/Admin: 100 rows) with auto-labeling and suppression.

- **AI Query Engine (Governed NLP Layer)**
  - English prompt query translation into secure Postgres aggregations, filter expressions, and groupings.
  - Credit tracking limits per subscription level (e.g., Free plan restricted to 3 monthly credits).
  - Risk guardian classifier blocking queries scoring high for identity identification patterns.
  - Interactive Chart.js plotting and text translation summaries of query output.

- **Admin Dashboard & UI**
  - Glass-style admin dashboard with:
    - Total datasets
    - Active users
    - Data schemas
    - System uptime
  - Navy blue icon theme in light mode, gold/amber icons in dark mode
  - Query UI for interactive filtering and charting
  - Integrated clear logs capabilities, including an admin-only "Clear Payment Logs" button

- **Governance & Security Controls**
  - **Rate-Limit Temporary Freeze**: Accounts are automatically temporarily frozen (Free: 5m, Pro: 2m, Enterprise: 30s) upon consecutive rate limit violations, featuring real-time countdown display widgets.
  - **Dynamic API status badges**: Real-time status indicators ("API Access Enabled/Restricted") in the User Profile based on user limits.
  - **Standardized Auto-Refresh**: Background polling timer updating stats and indicators every 9 seconds silently across portal views.
  - **Theme Support**: Fixed dark mode integration for consistent user exploration layout styles.

- **Testing**
  - SQL governance unit tests verifying rate limiting, write-op blocking, variable filtering, and suppression (`tests/test_sql_governance.py`)
  - AI query parsing and risk classification tests (`tests/test_ai_query.py`)

---

## Project Structure

Some key paths in this repository:

- [main.py](main.py) – FastAPI application entrypoint (routes, admin dashboard, schema-aware querying, SQL editor governance)
- [auth/local/](auth/local) – Local auth (register, login, JWT utilities, role checks, SHA256 password hashing)
- [ai_query/](ai_query) – NLP parsing engine, risk checks, and API routes for Governed AI Queries
- [security/](security) – Central Security Layer (access control, plan enforcer, usage tracker, warning manager, suspicious detector, privacy guard)
- [query/](query) – Query-related routers (safe query endpoints, suppression, logging, user explore data endpoints)
- [utils/](utils) – Ingestion pipeline, CSV/Excel/SAV conversion, metadata helpers, ingestion watcher
- [templates/](templates) – HTML templates for login, admin dashboard, query UI, datasets view, upload progress, user explore panel, and user AI query panel
- [tests/](tests) – Pytest suite for ingestion pipeline, watcher, AI query parsing, SQL governance, and related helpers
- [statathon-docs-only/](statathon-docs-only) – MkDocs configuration and standalone documentation site

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

Core tables (`users`, `datasets`, `usage_logs`, `auto_temporary_freezes`, metadata tables, etc.) are created automatically on application startup via:

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

### Dataset Explorer (User Explore APIs)

- `GET /api/user/explore/tree` – returns hierarchical list of surveys, datasets, and allowed tables.
- `GET /api/user/explore/metadata` – returns general abstract metadata and variables dictionary; Pro/Enterprise plans also receive statistics and categories.
- `GET /api/user/explore/preview` – governed table data preview (Free: 5, Pro: 50, Enterprise/Admin: 100 rows).

### Governed AI Query (Natural Language Query APIs)

- `POST /api/ai-query/parse` – parses natural language prompts into structured intent and filters without executing SQL.
- `POST /api/ai-query/execute` – parses, validates governance, checks credits/risk score, executes SQL, applies suppression, and returns results + AI summary.
- `GET /api/ai-query/credits` – checks monthly AI credit usage against plan limits.
- `GET /api/ai-query/history` – returns personal NLP query history logs.

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
- **Row & Query Caps**
  - Daily query count and row count limits enforced per subscription plan (Free, Pro, Enterprise) with per-user overrides.
- **Auto Temporary Freeze**
  - Triggers when a user exceeds request-rate limits consecutively. Freezes the account for plan-specific intervals (Free: 5m, Pro: 2m, Enterprise: 30s) and displays a real-time countdown in the UI.
- **Usage Logging**
  - Every query is logged to `usage_logs` with:
    - user email
    - endpoint
    - schema and table
    - rows returned
    - bytes sent

See [usage.md](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/statathon-docs-only/docs/usage.md) for details.

---

## Navigation Flow (Back Buttons)

- **Main dashboard page (`/admin/dashboard`)**
  - No back button is shown on the dashboard interface.
- **All other template back buttons**
  - Unified behavior redirects to `/admin/dashboard`.
  - This applies to upload, progress, query, schema, dataset, usage, settings, metadata/admin utility pages that expose a back control.
- **Explorer exception**
  - The back button in [explorer.html](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/templates/explorer.html) keeps its existing behavior by design.

This keeps navigation predictable for admins by standardizing back actions to the dashboard as the primary return point.

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
