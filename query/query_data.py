from fastapi import APIRouter, Request, Query, HTTPException, Depends
from typing import Optional
from starlette.responses import JSONResponse, StreamingResponse
import asyncpg
import io
import csv
import re
from datetime import datetime
import json

from auth.local.dependencies import get_current_user
from utils.metadata_helper import get_column_labels, apply_labels

router = APIRouter()

# Supported SQL operators
OPERATORS = {
    "=": "=",
    "!=": "!=",
    ">": ">",
    "<": "<",
    ">=": ">=",
    "<=": "<=",
    "IN": "IN",
    "LIKE": "LIKE",
}

def parse_filters(filter_str: str) -> str:
    conditions = []
    for f in filter_str.split(";"):
        f = f.strip()
        match = re.match(r"(\w+)\s*(=|!=|>=|<=|>|<|IN|LIKE)\s*(.+)", f, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid filter format: {f}")
        col, op, val = match.groups()
        op = op.upper()

        if op == "IN":
            val = val.strip("()[]")
            in_values = [v.strip().strip("'\"") for v in val.split(",")]
            val_str = "(" + ", ".join(f"'{v}'" for v in in_values) + ")"
            condition = f'"{col}" IN {val_str}'
        else:
            val = val.strip().strip("'\"")
            if not val.replace(".", "", 1).isdigit():
                val = f"'{val}'"
            condition = f'"{col}" {op} {val}'

        conditions.append(condition)

    return " AND ".join(conditions)

@router.post("/query")
async def run_query(request: Request, query: dict, current_user=Depends(get_current_user)):
    pool: asyncpg.Pool = request.app.state.db
    sql = query.get("query")
    if not sql:
        raise HTTPException(status_code=400, detail="Missing query")

    # Aggregation-only mode for normal users (role="3")
    user_role = str(current_user.role)
    if user_role == "3" and not is_aggregation_query(sql):
        raise HTTPException(
            status_code=403,
            detail={
                "error": "Aggregation Only",
                "reason": "Normal users can only perform aggregation queries (COUNT, SUM, AVG, MIN, MAX).",
                "role": user_role,
            }
        )

    try:
        async with pool.acquire() as conn:
            stmt = await conn.prepare(sql)
            records = await stmt.fetch()
            rows = [dict(record) for record in records]
            row_count = len(rows)

            # Suppress if user is not admin and result is < 5
            if user_role != "1" and row_count < 5:
                await log_usage(conn, current_user.username, "/query", "unknown", "direct_query", 0, 0)
                raise HTTPException(status_code=403, detail="Data suppressed (less than 5 rows)")

            await log_usage(
                conn, current_user.username, "/query", "unknown", "direct_query",
                row_count, len(json.dumps(rows).encode())
            )
            return {"columns": [attr.name for attr in stmt.get_attributes()], "rows": rows}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {e}")

@router.get("/datasets/{table_name}/query")
async def query_data(
    request: Request,
    table_name: str,
    columns: Optional[str] = None,
    filters: Optional[str] = None,
    limit: int = Query(100, ge=1),
    offset: int = Query(0, ge=0),
    current_user=Depends(get_current_user),
):
    pool: asyncpg.Pool = request.app.state.db

    async with pool.acquire() as conn:
        if await has_exceeded_usage_limit(conn, current_user.username):
            raise HTTPException(status_code=429, detail="Daily usage limit exceeded")

        col_sql = "*"
        if columns:
            col_sql = ", ".join(f'"{col.strip()}"' for col in columns.split(","))

        where_clause = ""
        if filters:
            try:
                where_clause = "WHERE " + parse_filters(filters)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

        sql = f'SELECT {col_sql} FROM "{table_name}" {where_clause} LIMIT {limit} OFFSET {offset}'

        try:
            rows = await conn.fetch(sql)
            row_count = len(rows)
        except asyncpg.UndefinedTableError:
            raise HTTPException(status_code=404, detail="Table not found")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Query failed: {e}")

        # Apply cell suppression for non-admin users
        user_role = str(current_user.role)
        print(f"DEBUG: role={current_user.role}, row_count={row_count}")
        if user_role != "1" and row_count < 5:
            print("DEBUG: Suppression triggered!")
            await log_usage(
                conn=conn,
                user_email=current_user.username,
                endpoint=f"/datasets/{table_name}/query",
                schema="public",
                table=table_name,
                row_count=0,
                bytes_sent=0
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

        data = [dict(row) for row in rows]
        
        # Apply metadata labels
        try:
            label_map = await get_column_labels(conn, table_name, schema="public")
            data = apply_labels(data, label_map)
        except Exception as e:
            print(f"⚠️ Failed to apply labels: {e}")

        await log_usage(
            conn, current_user.username,
            f"/datasets/{table_name}/query", "public", table_name,
            row_count, len(json.dumps(data, default=str).encode())
        )

        accept = request.headers.get("accept", "")
        if "text/csv" in accept:
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=data[0].keys() if data else [])
            writer.writeheader()
            writer.writerows(data)
            output.seek(0)
            return StreamingResponse(
                output,
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={table_name}_query.csv"}
            )

        return JSONResponse(content=data)

# ---------- Helpers ----------

async def has_exceeded_usage_limit(conn, user_email, daily_limit_rows=100000):
    today = datetime.utcnow().date()
    result = await conn.fetchval(
        """
        SELECT COALESCE(SUM(rows_returned), 0)
        FROM usage_logs
        WHERE user_email = $1 AND queried_at::date = $2
        """,
        user_email,
        today,
    )
    return result >= daily_limit_rows

async def log_usage(conn, user_email, endpoint, schema, table, row_count, bytes_sent):
    try:
        await conn.execute(
            """
            INSERT INTO usage_logs (user_email, endpoint, schema_name, table_name, rows_returned, bytes_sent)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            user_email, endpoint, schema, table, row_count, bytes_sent
        )
    except Exception as e:
        print(f"❌ Failed to log usage: {e}")

def is_aggregation_query(sql: str) -> bool:
    """Basic check for SQL aggregation queries."""
    sql_upper = sql.upper()
    return any(func in sql_upper for func in ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX("])
