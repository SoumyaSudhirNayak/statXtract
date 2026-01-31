# utils/usage_tracker.py
from datetime import datetime
import json

async def log_usage(conn, user_email, endpoint, schema, table, row_count, data):
    bytes_sent = len(json.dumps(data).encode())
    await conn.execute("""
        INSERT INTO usage_logs (user_email, endpoint, schema_name, table_name, rows_returned, bytes_sent)
        VALUES ($1, $2, $3, $4, $5, $6)
    """, user_email, endpoint, schema, table, row_count, bytes_sent)

async def has_exceeded_usage_limit(conn, user_email, daily_limit_rows=100000):
    today = datetime.utcnow().date()
    result = await conn.fetchval("""
        SELECT COALESCE(SUM(rows_returned), 0)
        FROM usage_logs
        WHERE user_email = $1 AND queried_at::date = $2
    """, user_email, today)
    return result >= daily_limit_rows
