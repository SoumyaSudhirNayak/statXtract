from fastapi import APIRouter, HTTPException, Request
import asyncpg

router = APIRouter()

@router.get("/datasets")
async def list_datasets(request: Request):
    pool: asyncpg.Pool = request.app.state.db

    try:
        async with pool.acquire() as conn:
            # Fetch registered schemas
            reg_rows = await conn.fetch("SELECT db_name FROM schema_registry")
            registered_schemas = [r["db_name"] for r in reg_rows]
            
            # Fetch default schema
            def_schema = await conn.fetchval("SELECT current_schema()")
            schemas_to_scan = list(set(registered_schemas + ([def_schema] if def_schema else [])))
            
            tables = await conn.fetch("""
                SELECT tablename, schemaname
                FROM pg_tables
                WHERE schemaname = ANY($1::text[])
                   OR (schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND $2::boolean = true);
            """, schemas_to_scan, True)
            
            result = []
            for t in tables:
                table_name = t["tablename"]
                schema_name = t["schemaname"]
                
                # Skip internal or log tables to avoid cluttering datasets list
                if schema_name == def_schema and table_name in {
                    "users", "roles", "schema_registry", "usage_logs", "query_logs",
                    "download_logs", "ai_query_logs", "suspicious_activity_logs", 
                    "user_requests", "user_documents", "governance_logs", "governance_warnings", "payments"
                }:
                    continue
                
                try:
                    row_count = await conn.fetchval(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
                    result.append({
                        "table_name": table_name,
                        "schema_name": schema_name,
                        "row_count": row_count
                    })
                except Exception:
                    continue  # skip system tables or erroring views
            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch datasets: {e}")

@router.get("/datasets/{table_name}/columns")
async def get_columns(table_name: str, request: Request):
    pool: asyncpg.Pool = request.app.state.db
    
    try:
        async with pool.acquire() as conn:
            # Resolve correct schema for table_name dynamically
            schema_row = await conn.fetchrow(
                """
                SELECT table_schema
                FROM information_schema.tables 
                WHERE LOWER(table_name) = LOWER($1) 
                LIMIT 1
                """,
                table_name
            )
            schema_name = schema_row["table_schema"] if schema_row else (await conn.fetchval("SELECT current_schema()") or "public")
            
            columns = await conn.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = $1 AND table_schema = $2;
            """, table_name, schema_name)
            return [col["column_name"] for col in columns]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get columns: {e}")
