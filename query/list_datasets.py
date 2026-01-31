from fastapi import APIRouter, HTTPException, Request
import asyncpg

router = APIRouter()

@router.get("/datasets")
async def list_datasets(request: Request):
    pool: asyncpg.Pool = request.app.state.db

    try:
        async with pool.acquire() as conn:
            tables = await conn.fetch("""
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public';
            """)
            result = []
            for t in tables:
                table_name = t["tablename"]
                try:
                    row_count = await conn.fetchval(f'SELECT COUNT(*) FROM "{table_name}"')
                    result.append({
                        "table_name": table_name,
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
            columns = await conn.fetch(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = $1 AND table_schema = 'public';
            """, table_name)
            return [col["column_name"] for col in columns]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get columns: {e}")
