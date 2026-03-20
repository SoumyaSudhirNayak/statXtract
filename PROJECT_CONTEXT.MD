# Statathon API Gateway — Complete Context

## 1) Problem statement (as implemented)

Create an API gateway to run SQL queries on Survey Datasets and retrieve results in a user-friendly form like JSON.

Context: National Statistics Office (MoSPI) hosts anonymized unit-level microdata (e.g., PLFS, HCES) traditionally available as full downloadable files (CSV/Excel/SPSS/Stata). Users currently need manual local filtering/processing. The goal is a scalable, configurable, privacy-aware gateway that supports role-based access, usage limits, and developer-friendly outputs + optional visualization and payment simulation.

## 2) What this prototype already does (current repo status)

### Implemented (working features in codebase)

- Ingests survey datasets into **PostgreSQL** via file upload (ZIP containing `.csv`, `.xlsx`, `.sav`, `.txt` + `.xml` DDI).
- Partial DDI parsing to extract variable names/labels.
- Provides REST-style endpoints to:
  - discover schemas/tables/columns
  - query datasets (filters/columns/limit/offset)
  - return JSON and (for one query endpoint) CSV output
- Minimal “frontend” UI served by the same backend using **Jinja2 templates**.
- Admin dashboard with real-time stats: total datasets, active users, data schemas, system uptime.
- Rich query UI for schema/table discovery, column selection, filters, pagination, and Chart.js-based visualizations.
- Authentication + role checks using **JWT**.
- Daily usage tracking via a `usage_logs` table (row-count metering).
- Basic privacy rule: suppress results for non-admin if the response would return fewer than 5 rows.
- NADA-style admin endpoints for downloading/ingesting datasets into the DB via background jobs.
- Swagger/OpenAPI documentation at `/docs` (FastAPI auto-generated) plus a separate MkDocs doc site under `statathon-docs-only/`.

Nesstar-specific ingest path:

- Supports **Nesstar binary studies** (.nesstar packages, .nsdstat metadata + .sav/.por data).
- Uses a Windows-only pipeline combining:
  - `NESSTAR_CONVERTER_EXE` (Nesstar Explorer / Publisher binary)
  - `utils/nesstar_convert.ps1` (PowerShell orchestration script)
  - `utils/nesstar_export.au3` (AutoIt script that drives the Nesstar GUI: open study, trigger “Export all datasets…”, choose SPSS/SAV export, handle Save dialogs, wait for files to stabilize).
- Tracks internal Nesstar stages (QUEUED, CONVERTING_WITH_NESSTAR, EXPORT_DIALOG_OPENED, EXPORTING_ALL_DATASETS, SAVING_DATASETS, VALIDATING_EXPORTED_FILES, EXPORT_COMPLETE, CONVERSION_COMPLETE, INGESTING, COMPLETED, FAILED) and maps them to job states via `_set_nesstar_stage()`.
- Validates converted outputs (`_validate_converted_outputs`) by:
  - Checking file size and column counts from `.sav` / `.por` using `pyreadstat` or CSV header parsing.
  - Comparing number of DDI variables vs. data columns with a configurable tolerance.
- Handles retries and timeouts for flaky GUI automation, with exponential backoff and detailed job logs.

### Known gaps (important)

- SQL safety: there are endpoints that build SQL strings directly; raw SQL execution exists. These are not safe for untrusted users.
- `variable_configs` exists in DB, but query endpoints do not enforce it yet; the admin update endpoint currently does not write to DB.
- Request-rate limiting (requests/time) is not implemented; only daily row caps exist.
- Privacy controls are minimal (cell suppression by row count only).
- Duplicate query endpoints exist (schema-aware and public-only variants).

## 3) Architecture overview

This is a single FastAPI service that serves:
- Backend REST APIs (JSON/CSV)
- Frontend pages via HTML templates

Core pieces:
- FastAPI app entrypoint: `main.py`
- Auth: `auth/local/*`
- Query router: `query/query_data.py`
- Ingestion utilities: `utils/*`
- Background watchers: file-system watcher for `.sav` uploads and ingestion-completion watcher for jobs (see `utils/watcher.py` and `IngestionWatcher` usage in `main.py`).
- NADA admin integration: `nada_routes.py`
- HTML templates: `templates/*`

Database access:
- Uses `asyncpg` pool in FastAPI lifespan (startup/shutdown)
- Ingestion to Postgres uses `pandas` + SQLAlchemy engine (`df.to_sql`)

## 4) How to run (Windows)

See [COMMANDS.MD](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/COMMANDS.MD) for the full command list.

Quick start:

1. Create venv + install deps:
```bat
cd /d "e:\STATATHON 2025 LOCAL\Statathon_API_Gateway"
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

2. Ensure folders:
```bat
mkdir static
```

3. Set `.env` (do not commit secrets):
- `DATABASE_URL=postgresql://...`
- `SECRET_KEY=...`
- `ALGORITHM=HS256` (optional)

4. Start server:
```bat
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

URLs:
- Login UI: http://127.0.0.1:8000/login
- Query UI: http://127.0.0.1:8000/query
- Swagger UI: http://127.0.0.1:8000/docs
- OpenAPI JSON: http://127.0.0.1:8000/openapi.json

## 5) How auth/authorization works

### JWT token creation

- `POST /auth/token` returns:
  - `access_token` (JWT)
  - `token_type: bearer`

Implemented in: [routes.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/auth/local/routes.py#L37-L52)

Token payload fields:
- `sub`: user email
- `role`: role_id as string

Token expiry: 30 minutes default ([utils.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/auth/local/utils.py#L15-L29))

### How requests authenticate

Most protected endpoints depend on `get_current_user`, which accepts token from:
- `Authorization: Bearer <token>` header
- OR `access_token` cookie (set by HTML login)

Implementation: [dependencies.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/auth/local/dependencies.py#L16-L44)

### Roles

Roles are stored in DB table `roles` and seeded on startup:
- `admin` (role_id = 1)
- `analyst` (role_id = 2)
- `user` (role_id = 3)

Created by: [db_init.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/utils/db_init.py#L4-L33)

Role enforcement helper: `get_current_active_user_with_role([...])` ([dependencies.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/auth/local/dependencies.py#L46-L52))

## 6) Swagger UI (/docs) — how to use

Swagger UI is auto-generated by FastAPI and enhanced by a global BearerAuth scheme:
- OpenAPI customization: [main.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/main.py#L1126-L1153)

### Authorize in Swagger

1. Call `POST /auth/token` in Swagger.
2. Copy the `access_token`.
3. Click **Authorize** (top-right).
4. In **BearerAuth** → paste the token into the Value field.

After authorizing, Swagger sends:
- `Authorization: Bearer <token>`

## 7) Main UI pages (served by backend)

These are HTML templates under `templates/`:
- `/login` (login page)
- `/register` (register page)
- `/admin/dashboard` (admin UI)
- `/query` (query UI)
- `/upload` (upload page)

Entry routes: [main.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/main.py#L102-L260)

Notes:
- The admin dashboard now shows live statistics (total datasets across schemas, active users, number of data schemas, and system uptime) computed from the database and application `START_TIME`.
- The query UI supports:
  - dynamic schema → table → column discovery
  - checkbox-based column selection and “select all / clear all”
  - free-text filters that are normalized on the backend (case-insensitive column names and automatic quoting for string values)
  - table view plus Chart.js-powered visualizations (bar/line/pie/doughnut/scatter) based on selected X/Y axes.

## 8) API endpoints by area (from code)

### A) Auth routes (`/auth/*`)

Router: [auth/local/routes.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/auth/local/routes.py)

- `POST /auth/register` (JSON; creates user)
- `POST /auth/token` (form; returns JWT token)
- `POST /auth/login` (form; sets cookie + redirects)

### B) Schema / table discovery

Defined in: [main.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/main.py)

- `GET /schemas`
- `GET /datasets`
- `GET /datasets/{schema}/{table}/columns`

### C) Query endpoints

There are currently two “query styles”:

1) Schema-aware query (main app)
- `GET /datasets/{schema}/{table}/query`
  - params: `columns`, `filters`, `limit`, `offset`
  - returns: JSON list of rows
  - privacy: suppress non-admin if row_count < 5
  - filters support case-insensitive column names and automatic quoting of string values via helper functions `fix_filter_case_sensitivity` and `smart_quote_filters`
  - applies metadata labels from `utils/metadata_helper` before returning rows

Source: [main.py query_table](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/main.py#L705-L809)

2) Public schema query + CSV (router)
- `GET /datasets/{table_name}/query`
  - params: `columns`, `filters`, `limit`, `offset`
  - accepts header: `Accept: text/csv` to get CSV download
  - usage: daily row cap + logging + cell suppression
  - filters parsed by `parse_filters()` supporting `=`, `!=`, `>`, `<`, `>=`, `<=`, `IN`, `LIKE`

Source: [query_data.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/query/query_data.py#L94-L208)

3) Raw SQL execution
- `POST /query` with JSON body: `{"query": "SELECT ..."}`
  - role 3 is “aggregation-only” (COUNT/SUM/AVG/MIN/MAX) check
  - still executes SQL directly

Source: [query_data.py run_query](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/query/query_data.py#L52-L92)

### D) Upload / ingestion

- `POST /upload/` (multipart form-data: file + schema + dataset_name)
- Supported formats: `.zip`, `.csv`, `.xlsx`, `.sav`, `.txt`, `.nesstar`
- Extracts/converts and loads data into PostgreSQL

Source: [ingestion_pipeline.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/utils/ingestion_pipeline.py)

#### Unified Ingestion Workflow (ZIP + Nesstar):
All upload types converge into a **single, unified ingestion block** in `ingest_upload_file()`:

1. **File Preparation:**
   - **ZIP:** Extract to `raw_dir/extracted/`, copy data files to `processed_dir/`, copy DDI XML to `dataset_root/ddi.xml`.
   - **Nesstar:** AutoIt exports datasets + DDI to `uploads/export_nesstar/`, then code copies data files to `processed_dir/` and DDI to `dataset_root/ddi.xml`. *(Same as ZIP — just a different extraction source.)*
   - **Single file:** Copy directly to `processed_dir/`.
2. **DDI Parsing:** Parse `ddi_path` via `parse_ddi_xml()` → extract `ddi_meta` (title, abstract, keywords, coverage, variables).
3. **Dataset Registry:** Register in `public.dataset_registry` under parent survey schema.
4. **Dataset Metadata:** Store rich metadata (abstract, keywords, coverage, methodology, producer, etc.) in `{dataset_schema}.dataset_metadata`.
5. **Per-file Processing:**
   - Load via `_load_data_file()` (supports CSV, XLSX, SAV, POR, fixed-width TXT).
   - Apply DDI category labels via `_apply_ddi_labels()`.
   - Infer + convert column types via `_infer_and_convert_column()`.
   - Write to PostgreSQL via `df.to_sql()` into `{dataset_schema}.{table_name}`.
   - Populate `variables`, `variable_statistics`, `variable_categories` tables with per-column metadata.
6. **Hierarchy Registration:** Dataset tables registered in the survey schema's `dataset_tables`.

#### File Structuring Methodology:
- **Physical Storage:** `uploads/{survey_schema}/{dataset_folder}/raw_files` and `uploads/{survey_schema}/{dataset_folder}/processed`.
- **Nesstar Export Storage:** `uploads/export_nesstar/` (temporary; contents cleared after copy, folder preserved).
- **Naming Convention:** Dataset schemas use `{survey_schema}__{dataset_name}` (double-underscore delimiter).
- **DDI Mapping:** DDI XML files must be present in the ZIP or export folder; detected by `.xml`/`.nsdstat` extension.

### E) Logical Hierarchy & Schema Mapping

The system implements a **Survey → Dataset → Table** logical hierarchy without altering the physical flat schema structure of the database.

#### Schema Mapping Implementations:
- **`_group_schemas_by_survey` ([main.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/main.py)):** A centralized function that scans all database schemas and groups them based on prefixes registered in `schema_registry`.
- **Supported Surveys:**
  - Annual Survey of Industries (`annual_survey_of_industries`)
  - Index of Industrial Production (`index_of_industrial_production`)
  - Household Consumption Expenditure (`household_consumption_expenditure`)
  - Economic Census (`economic_census`)
  - Enterprises Surveys (`enterprises_surveys`)
  - Periodic Labour Force Survey (`periodic_labour_force_survey`)
  - Employment and Unemployment (`employment_and_unemployment`)
  - Land and Livestock Holding Surveys (`land_and_livestock_holding_surveys`)
  - Other Surveys (`other_surveys`)
- **Mapping Logic:**
  - Uses `__` as a delimiter (e.g., `annual_survey_of_industries__2023_24` maps to survey `annual_survey_of_industries`).
  - Falls back to `_` prefix matching if no delimiter is found.
  - Resolves display names from `schema_registry` or `SURVEY_SCHEMA_DISPLAY_NAMES`.
- **API Endpoints:**
  - `/surveys`: Returns logical survey groups with dataset counts.
  - `/surveys/{survey}/datasets`: Returns datasets belonging to a survey.
  - `/surveys/{survey}/{dataset}/tables`: Lists tables within a specific dataset.

### F) Variable Dictionary & Configurations

#### Variable Dictionary ([db_utils.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/utils/db_utils.py)):
- Every dataset schema contains a `variables` table (created by `ensure_dataset_schema_tables()`).
- **Fields:** `table_name`, `variable_name`, `label`, `ddi_type`, `width`, `interval`, `valid_count`, `invalid_count`, `final_type`.
- **Unique constraint:** `(table_name, variable_name)` — used for upsert during ingestion.
- Automatically populated during ingestion from DDI metadata and data file profiling.

#### Related tables per dataset schema:
- `variable_categories` — stores DDI value labels: `(table_name, variable_name, value, label, frequency)`.
- `variable_statistics` — stores per-column stats: `(table_name, variable_name, mean, min, max, stddev, unique_count)`.
- `variable_missing_values` — stores DDI missing value codes: `(variable_id, missing_value)`.
- `dataset_metadata` — stores rich DDI metadata per dataset.

#### Schema Migration (auto-healing):
- `ensure_dataset_schema_tables()` checks `information_schema.columns` for old `variable_id`-based `variable_categories` tables and auto-drops + recreates them with the correct `(table_name, variable_name)` schema.
- `ensure_metadata_tables()` runs the same migration via `DROP TABLE IF EXISTS ... CASCADE`.

#### Variable Configurations ([db_init.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/utils/db_init.py)):
- **Table:** `variable_configs` in the `public` schema.
- **Governance:** Admins can configure `include_in_api` (visibility) and `filterable` (query capability) per variable.
- **UI:** The "Configure Variables" section in the admin dashboard provides a grid for managing these settings.

### G) Metadata Integration Processes

- **DDI Parsing:** Uses `utils/ddi_parser.py` to extract `abstract`, `keywords`, `geographic_coverage`, and `industrial_coverage`.
- **Metadata Cards:**
  - Displays rich dataset metadata in the UI.
  - Metadata is stored in `{dataset_schema}.dataset_metadata`.
  - UI renders these as searchable cards to provide context before querying.
  - **Example:** A card for "PLFS 2023-24" would show its abstract from the DDI and a list of key variables.
- **Label Application:** Before returning query results, the system fetches labels from the variable dictionary and applies them to the JSON output for better readability.
- **Privacy Enforcement:** Suppression rules (e.g., `< 5 rows`) are applied after label application but before the response is returned.

### E) NADA admin integration

Router prefix: `/admin/nada` ([nada_routes.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/nada_routes.py))

- `GET /admin/nada/datasets`
- `GET /admin/nada/datasets/{dataset_id}/fileslist`
- `POST /admin/nada/datasets/{dataset_id}/download`
- `POST /admin/nada/datasets/{dataset_id}/ingest`
- `GET /admin/nada/jobs/{job_id}`

## 9) Database schema (core tables)

Created on startup in `ensure_core_tables()`:
- `roles`
- `users`
- `usage_logs`
- `variable_configs`
- `dataset_registry` — maps `(survey_schema, dataset_schema, dataset_display_name)`.

Source: [db_init.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/utils/db_init.py#L4-L62)

#### Per-dataset schema tables (created by `ensure_dataset_schema_tables()` in `db_utils.py`):
- `dataset_metadata` — DDI-derived rich metadata (title, abstract, keywords, coverage, methodology, etc.)
- `variables` — per-column metadata: `(table_name, variable_name, label, ddi_type, width, valid_count, invalid_count, final_type)`
- `variable_categories` — DDI value labels: `(table_name, variable_name, value, label, frequency)`
- `variable_statistics` — column stats: `(table_name, variable_name, mean, min, max, stddev, unique_count)`
- `variable_missing_values` — DDI missing value codes

#### Per-survey schema tables (created by `ensure_survey_metadata_tables()` in `db_utils.py`):
- `dataset_registry` — lists datasets within the survey: `(year, dataset_display_name, dataset_db_name)`
- `dataset_tables` — lists tables within each dataset

#### Internal table filtering:
All table-listing API endpoints (`/datasets`, `/surveys/.../tables`, `/admin/schemas/.../variables`) filter out internal tables using `_is_internal_table()` in `main.py`. Hidden tables include: `datasets`, `dataset_files`, `dataset_metadata`, `variables`, `variable_categories`, `variable_statistics`, `variable_missing_values`, and any table containing keywords like `variable`, `metadata`, `category`, `missing`, `_error`, `_stat`.

Notes:
- `usage_logs` stores `user_email`, `endpoint`, `schema_name`, `table_name`, `rows_returned`, `bytes_sent`, `queried_at`.
- `variable_configs` is intended for survey config governance (`include_in_api`, `filterable`) but is not enforced in query endpoints yet.

## 10) Documentation in repo

### Swagger/OpenAPI
- Served by FastAPI at runtime (`/docs`, `/openapi.json`)

### MkDocs site
- Source: `statathon-docs-only/docs/*.md`
- Static build: `statathon-docs-only/site/`
- Local serve:
```bat
cd /d "e:\STATATHON 2025 LOCAL\Statathon_API_Gateway\statathon-docs-only"
mkdocs serve -a 127.0.0.1:8001
```

## 11) Prioritized safety “fix list” (keeping your design)

1) Enforce SELECT-only + parameterized filters
- Remove/lock down raw SQL endpoint for non-admin.
- Stop concatenating filters directly into SQL; build parameterized queries.

2) Enforce `variable_configs` allowlists
- Deny columns where `include_in_api=false`.
- Deny filters on non-`filterable` columns (for non-admin roles).

3) Unify query endpoints
- Keep one canonical query route and make the other a wrapper.

4) Add request-rate limiting
- Implement per-user requests/minute (in-memory for prototype, Redis for production).

5) Strengthen privacy beyond `<5 rows`
- For role 3: aggregation-only, per-cell suppression (HAVING COUNT >= k), limit group-by shapes.

## 12) Current runtime status (local dev)

When started with:
```bat
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```
Uvicorn reports:
- “Application startup complete.”
- “Uvicorn running on http://127.0.0.1:8000”

## 13) Files to know (module map)

- App entrypoint: [main.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/main.py)
- Auth:
  - [dependencies.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/auth/local/dependencies.py)
  - [routes.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/auth/local/routes.py)
  - [crud.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/auth/local/crud.py)
  - [utils.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/auth/local/utils.py)
- Query router: [query_data.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/query/query_data.py)
- NADA admin: [nada_routes.py](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/nada_routes.py)
- Ingestion utilities: `utils/*.py` (DDI parsing, file conversion, ingestion pipeline, Nesstar conversion helpers)
  - Nesstar discovery and conversion entry points:
    - `utils/ingestion_pipeline.py`:
      - `discover_nesstar_converter_exe()` – finds Nesstar binaries via env and Windows registry.
      - `_run_nesstar_converter_streaming()` – launches PowerShell + AutoIt pipeline, streams logs, tracks STAGE markers.
      - `_set_nesstar_stage()` – maps Nesstar stages to job `status`/`current_state` (`QUEUED`, `CONVERTING`, `EXPORTING`, `INGESTING`, `COMPLETED`, `FAILED`).
      - `convert_and_ingest_nesstar_binary_study()` – high-level orchestration for Nesstar binary studies, including retries, validation, and ingestion via `process_directory()`.
  - Windows automation scripts:
    - `utils/nesstar_convert.ps1` – PowerShell wrapper that:
      - Validates `NESSTAR_CONVERTER_EXE`, `InputStudy`, output directories.
      - Resolves AutoIt executable and script (`NESSTAR_AUTOIT_EXE`, `NESSTAR_AUTOIT_SCRIPT`, fallback to `nesstar_export.au3`).
      - Emits `LOG` and `STAGE` lines consumed by `_run_nesstar_converter_streaming()`.
      - Orchestrates Nesstar launch and hands control to AutoIt; waits for exported files to stabilize; copies them into the conversion output directory.
    - `utils/nesstar_export.au3` – AutoIt script that:
      - Waits for Nesstar main window, opens the study, drives menus (`Alt+F`, `O`, `Ctrl+Shift+E`).
      - Navigates export dialogs, picks SPSS/SAV format, drives “Save As” flows, handles overwrite prompts.
      - Waits until dataset files reach minimum size and are stable before exiting.
- Commands: [COMMANDS.MD](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/COMMANDS.MD)
- Commands: [COMMANDS.MD](file:///e:/STATATHON%202025%20LOCAL/Statathon_API_Gateway/COMMANDS.MD)
