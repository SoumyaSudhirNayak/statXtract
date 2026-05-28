# security/usage_tracker.py
from datetime import datetime
from fastapi import HTTPException
import time


async def get_qualified_table(conn, table_name: str) -> str:
    """
    Dynamically resolves the schema for a table and returns its schema-qualified reference.
    Falls back to current_schema() if the table or schema does not exist.
    """
    schema = await conn.fetchval(
        """
        SELECT table_schema 
        FROM information_schema.tables 
        WHERE LOWER(table_name) = LOWER($1) 
        LIMIT 1
        """,
        table_name
    )
    if schema:
        schema_ok = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
            schema
        )
        if schema_ok:
            return f'"{schema}"."{table_name}"'
            
    fallback_schema = await conn.fetchval("SELECT current_schema()") or "public"
    return f'"{fallback_schema}"."{table_name}"'


async def check_daily_usage(conn, user_email: str, limits: dict, rows_requested: int = 0):
    """
    Checks the user's monthly usage logs from usage_logs.
    Raises HTTPException 429 if the user has exceeded query or row limits for this month.
    """
    # Fetch this month's query count and row count sum
    usage_table = await get_qualified_table(conn, "usage_logs")
    monthly = await conn.fetchrow(
        f"""
        SELECT 
            COUNT(*) AS monthly_queries,
            COALESCE(SUM(rows_returned), 0) AS monthly_rows
        FROM {usage_table}
        WHERE user_email = $1
          AND queried_at >= DATE_TRUNC('month', CURRENT_DATE)
        """,
        user_email
    )

    monthly_queries = int(monthly["monthly_queries"] or 0)
    monthly_rows = int(monthly["monthly_rows"] or 0)

    # Use monthly settings if available, else daily fallbacks
    max_queries = limits.get("max_queries_per_month", limits.get("max_queries_per_day", 200))
    max_rows = limits.get("max_rows_per_month", limits.get("max_rows_per_day", 5000))

    # 1. Enforce query count limit
    if monthly_queries >= max_queries:
        raise HTTPException(
            status_code=429,
            detail=f"Usage Limit Exceeded: Monthly query limit of {max_queries} reached for your current plan."
        )

    # 2. Enforce rows count limit
    if (monthly_rows + max(0, rows_requested)) > max_rows:
        raise HTTPException(
            status_code=429,
            detail=f"Usage Limit Exceeded: This request would exceed your remaining monthly limit of {max_rows - monthly_rows} rows (Current this month: {monthly_rows})."
        )

    return {
        "daily_queries": monthly_queries,
        "daily_rows": monthly_rows
    }


async def get_daily_usage_credits(conn, user_email: str, limits: dict) -> dict:
    """
    Returns the user's credit usage for this month: queries, rows, downloads.
    """
    usage_table = await get_qualified_table(conn, "usage_logs")
    download_table = await get_qualified_table(conn, "download_logs")

    # Query and row usage
    monthly = await conn.fetchrow(
        f"""
        SELECT 
            COUNT(*) AS monthly_queries,
            COALESCE(SUM(rows_returned), 0) AS monthly_rows
        FROM {usage_table}
        WHERE user_email = $1
          AND queried_at >= DATE_TRUNC('month', CURRENT_DATE)
        """,
        user_email
    )
    monthly_queries = int(monthly["monthly_queries"] or 0) if monthly else 0
    monthly_rows = int(monthly["monthly_rows"] or 0) if monthly else 0

    # Download usage
    monthly_downloads = await conn.fetchval(
        f"""
        SELECT COUNT(*) FROM {download_table}
        WHERE user_email = $1 AND created_at >= DATE_TRUNC('month', CURRENT_DATE)
        """,
        user_email
    ) or 0

    # AI query usage
    ai_queries_used = await conn.fetchval(
        f"""
        SELECT COUNT(*) FROM {usage_table}
        WHERE user_email = $1 
          AND endpoint LIKE '%/ai%'
          AND queried_at >= DATE_TRUNC('month', CURRENT_DATE)
        """,
        user_email
    ) or 0

    max_queries = limits.get("max_queries_per_month", limits.get("max_queries_per_day", 200))
    max_rows = limits.get("max_rows_per_month", limits.get("max_rows_per_day", 5000))
    max_downloads = limits.get("max_downloads_per_month", limits.get("max_downloads_per_day", 10))
    max_ai = limits.get("max_ai_queries_per_month", 3)

    return {
        "queries_used": monthly_queries,
        "queries_remaining": max(0, max_queries - monthly_queries),
        "queries_limit": max_queries,
        "rows_accessed": monthly_rows,
        "rows_remaining": max(0, max_rows - monthly_rows),
        "rows_limit": max_rows,
        "downloads_used": int(monthly_downloads),
        "downloads_remaining": max(0, max_downloads - int(monthly_downloads)),
        "downloads_limit": max_downloads,
        "ai_queries_used": int(ai_queries_used),
        "ai_queries_remaining": max(0, max_ai - int(ai_queries_used)),
        "ai_queries_limit": max_ai,
        "api_access": limits.get("api_access", False),
        "api_access_enabled": limits.get("api_access", False),
    }



async def log_api_usage(conn, user_email: str, endpoint: str, schema_name: str, table_name: str, rows_returned: int, bytes_sent: int, query_time_ms: int = 0, status: str = "success", filters: str = None):
    """
    Logs API queries to the usage_logs database table.
    """
    usage_table = await get_qualified_table(conn, "usage_logs")
    try:
        await conn.execute(
            f"""
            INSERT INTO {usage_table} (user_email, endpoint, schema_name, table_name, rows_returned, bytes_sent, query_time_ms, status, filters, queried_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)
            """,
            user_email,
            endpoint,
            schema_name or "unknown",
            table_name or "unknown",
            rows_returned,
            bytes_sent,
            query_time_ms,
            status,
            filters
        )
    except Exception as e:
        # Fallback: try without the new columns (for backward compatibility)
        try:
            await conn.execute(
                f"""
                INSERT INTO {usage_table} (user_email, endpoint, schema_name, table_name, rows_returned, bytes_sent, queried_at)
                VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
                """,
                user_email,
                endpoint,
                schema_name or "unknown",
                table_name or "unknown",
                rows_returned,
                bytes_sent
            )
        except Exception as e2:
            print(f"❌ security/usage_tracker - Failed to log usage: {e2}")


async def log_file_download(conn, file_name: str, user_email: str, size_bytes: int, dataset_schema: str = None, export_format: str = "csv", rows_exported: int = 0, status: str = "success"):
    """
    Logs file downloads to the download_logs database table with governance columns.
    """
    download_table = await get_qualified_table(conn, "download_logs")
    try:
        await conn.execute(
            f"""
            INSERT INTO {download_table} (file_name, user_email, size_bytes, dataset_schema, export_format, rows_exported, status, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
            """,
            file_name,
            user_email,
            size_bytes,
            dataset_schema,
            export_format,
            rows_exported,
            status
        )
    except Exception as e:
        # Fallback without new columns
        try:
            await conn.execute(
                f"""
                INSERT INTO {download_table} (file_name, user_email, size_bytes, created_at)
                VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                """,
                file_name,
                user_email,
                size_bytes
            )
        except Exception as e2:
            print(f"❌ security/usage_tracker - Failed to log download: {e2}")


async def check_download_limits(conn, user_email: str, limits: dict):
    """
    Check if the user has remaining download credits for this month.
    Raises HTTPException 429 if exceeded.
    """
    max_downloads = limits.get("max_downloads_per_month", limits.get("max_downloads_per_day", 10))
    downloads_are_allowed = (
        limits.get("downloads_allowed", False) or 
        max_downloads > 0 or 
        len(limits.get("export_formats", [])) > 0
    )
    if not downloads_are_allowed:
        raise HTTPException(
            status_code=403,
            detail="Access Denied: File downloads and data exports are restricted on your current subscription plan. Please upgrade."
        )
    download_table = await get_qualified_table(conn, "download_logs")
    monthly_downloads = await conn.fetchval(
        f"""
        SELECT COUNT(*) FROM {download_table}
        WHERE user_email = $1 AND created_at >= DATE_TRUNC('month', CURRENT_DATE)
        """,
        user_email
    ) or 0

    if int(monthly_downloads) >= max_downloads:
        raise HTTPException(
            status_code=429,
            detail="Monthly download limit reached for your current plan."
        )

    return {"downloads_used": int(monthly_downloads), "downloads_remaining": max_downloads - int(monthly_downloads)}
