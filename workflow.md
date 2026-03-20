# End-to-End Workflow

## 1. Overview

- This system is an API Gateway for survey microdata.
- It supports ingestion of survey files, metadata parsing, querying, and configuration-driven response control.
- The goal is to let users discover and query survey data safely through APIs and UI without manual file processing.

## 2. Data Ingestion Workflow

- User uploads a dataset package (`ZIP` / `CSV` / `SAV` / `Nesstar`).
- Files are extracted and routed into ingestion folders.
- If upload is Nesstar:
  - Nesstar export pipeline converts to `SAV/CSV` + `DDI XML`.
- Data files are cleaned and prepared:
  - column normalization
  - type inference/conversion
  - category label preparation
- Prepared data is loaded into PostgreSQL tables.
- Schema mapping is applied as:
  - `survey -> dataset -> table`

## 3. Metadata Integration (DDI Processing)

- DDI XML is parsed during ingestion.
- The parser extracts:
  - Study Description (metadata)
  - Variable information
  - Categories (`value <-> label`)
- Metadata is stored in database metadata tables.
- Metadata is used for:
  - column naming
  - type context
  - label mapping in responses

## 4. Database Structure

- Data is organized in PostgreSQL by:
  - schema (survey/dataset namespace)
  - tables (dataset levels/blocks)
- Metadata tables include:
  - `variables`
  - `variable_categories`
  - `dataset_metadata`

## 5. Query Workflow

- User selects:
  - survey -> dataset -> table
- User applies filters from UI or API.
- Backend query flow:
  - validates inputs
  - builds SQL
  - applies pagination
- Response formats:
  - JSON
  - CSV (where supported)
- Label mapping is applied when configured/available.

## 6. Filter System

- Filter UI is generated dynamically from metadata.
- Supported filter types include:
  - numeric (range/slider style constraints)
  - categorical (dropdown/list selections)
- Backend converts filter input into SQL safely using validated column/operator logic.

## 7. Survey Configuration Workflow

- Admin opens Survey Configuration Panel.
- Admin selects:
  - survey -> dataset -> table
- Table-level config:
  - Show/Hide table for users
- Variable-level config:
  - `include_in_api` -> controls variable visibility
  - `filterable` -> controls if variable can be used in filters
  - `display_mode` -> label/value (legacy concept; current panel uses standardized label mapping behavior)
  - privacy:
    - `is_sensitive`
    - `min_rows`

## 8. Configuration Enforcement (Backend)

- A central function applies config before returning user-facing results.
- Enforcement is used in:
  - query APIs
  - schema/dataset/table listing APIs
- Core logic:
  - remove excluded columns
  - block restricted filters
  - apply label/value display behavior
  - hide sensitive columns for non-admin users
  - suppress small results based on `min_rows`

## 9. User Access Flow

- User logs in with JWT-based authentication.
- Role is resolved from token/user context.
- Configuration checks run before data is returned.
- Admin bypasses user-facing restrictions for operational/admin workflows.

## 10. Final Output

- Users get a clean dataset view.
- Query results are filtered and policy-compliant.
- UI can render charts/tables from response data.
- API output stays developer-friendly and structured.

## 11. Key Design Principle

- Metadata-driven system behavior.
- Configuration controls output behavior instead of hardcoded endpoint logic.
- Clear separation of concerns:
  - ingestion layer
  - metadata layer
  - query layer
  - configuration/enforcement layer
