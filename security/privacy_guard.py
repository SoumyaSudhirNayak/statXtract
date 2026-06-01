# security/privacy_guard.py
import re
from fastapi import HTTPException

# Compile basic regex to identify column names in SQL filter expressions
IDENTIFIER_REGEX = re.compile(r'("([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))')

def _normalize_role(role_value) -> str:
    raw = str(role_value or "").strip().lower()
    return {"1": "admin", "2": "analyst", "3": "user"}.get(raw, raw or "user")

def extract_filter_columns(filter_expr: str, known_columns: list[str]) -> set[str]:
    """
    Extracts columns referenced in filter expression that match known columns of the table.
    """
    if not filter_expr:
        return set()
    known = {c.lower(): c for c in known_columns}
    used = set()
    for m in IDENTIFIER_REGEX.finditer(filter_expr):
        ident = m.group(2) or m.group(3)
        if not ident:
            continue
        hit = known.get(ident.lower())
        if hit:
            used.add(hit)
    return used

async def get_system_setting(conn, key: str, default: str) -> str:
    """Helper to fetch a setting value from system_settings."""
    try:
        val = await conn.fetchval("SELECT value FROM system_settings WHERE key = $1 LIMIT 1", key)
        if val is None:
            return default
        return str(val)
    except Exception:
        return default

async def get_variable_configs(conn, schema: str, table: str) -> dict[str, dict]:
    """
    Fetches the configured variable restrictions for the given schema and table.
    """
    # Check if table_name column exists in variable_configs to stay backward compatible
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

async def check_columns_and_filters(conn, schema: str, table: str, user_role: str, columns: list[str], filters: str = None) -> list[str]:
    """
    Verifies that requested columns and filter parameters align with privacy policies.
    - Filters out columns that are not included in the API.
    - Blocks sensitive columns if they are globally disabled.
    - Prevents filtering on non-filterable variables.
    Returns the list of allowed columns.
    """
    role_name = _normalize_role(user_role)

    # Admins bypass column checks
    if role_name == "admin":
        return columns

    # 1. Fetch configs
    var_configs = await get_variable_configs(conn, schema, table)
    sensitive_enabled = (await get_system_setting(conn, "enable_sensitive_columns", "false")).strip().lower() == "true"

    allowed_columns = []
    for col in columns:
        cfg = var_configs.get(col)
        # Check if included in API
        if cfg and cfg.get("include_in_api") is False:
            continue
        # Check if sensitive and globally disabled
        if cfg and cfg.get("is_sensitive") and not sensitive_enabled:
            continue
        allowed_columns.append(col)

    # 2. Check filters if any
    if filters and columns:
        used_cols = extract_filter_columns(filters, columns)
        allowed_set = set(allowed_columns)
        for c in used_cols:
            if c not in allowed_set:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Privacy Violation: Columns used in filters are either sensitive or restricted: {c}"
                )
            cfg = var_configs.get(c)
            if cfg and cfg.get("filterable") is False:
                raise HTTPException(
                    status_code=400,
                    detail=f"Access Denied: Column '{c}' is not configured as filterable for public/user queries."
                )

    return allowed_columns

async def apply_privacy_and_labeling(conn, schema: str, table: str, user_role: str, columns: list[str], rows: list, labels: dict = None, is_aggregated: bool = False) -> list:
    """
    Enforces cell suppression thresholds and maps raw codes to readable descriptive labels.
    """
    role_name = _normalize_role(user_role)
    labels = labels or {}

    # Admin bypass cell suppression
    if role_name == "admin":
        # Format rows but bypass minimum count restriction
        formatted = []
        for r in rows:
            row_dict = dict(r)
            item = {}
            for col in columns:
                val = row_dict.get(col)
                if val is not None and col in labels:
                    val = labels[col].get(str(val), val)
                item[col] = val
            formatted.append(item)
        return formatted

    # 1. Resolve variable configs & min rows
    var_configs = await get_variable_configs(conn, schema, table)
    global_min_rows_raw = await get_system_setting(conn, "min_rows_threshold", "5")
    try:
        global_min_rows = max(1, int(global_min_rows_raw))
    except Exception:
        global_min_rows = 5

    min_rows_required = global_min_rows

    # 2. Process rows and replace labels
    formatted = []
    for r in rows:
        row_dict = dict(r)
        item = {}
        for col in columns:
            val = row_dict.get(col)
            # Map code value to label if available
            if val is not None and col in labels:
                val = labels[col].get(str(val), val)
            item[col] = val
            
            # Check if this column raises our suppression threshold
            cfg = var_configs.get(col)
            if cfg and cfg.get("is_sensitive"):
                try:
                    col_min = int(cfg.get("min_rows") or 5)
                    min_rows_required = max(min_rows_required, col_min)
                except Exception:
                    min_rows_required = max(min_rows_required, 5)
        formatted.append(item)

    # 3. Check Cell Suppression (fewer rows than threshold)
    if not is_aggregated:
        row_count = len(formatted)
        if min_rows_required > 0 and row_count < min_rows_required:
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "Cell Suppression Applied",
                    "detail": f"Privacy Rule Violation: Result contains {row_count} rows, which is below the minimum threshold of {min_rows_required} rows required for privacy protection.",
                    "code": "CELL_SUPPRESSION_APPLIED",
                    "minimum_rows_required": min_rows_required,
                    "actual_rows": row_count
                }
            )

    return formatted

