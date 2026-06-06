"""
ai_query/routes.py
API routes for the AI Query (Governed Natural Language Query Layer).

All queries pass through the existing governance, RBAC, privacy, and usage systems.
This module only adds AI-specific parsing and credit management on top.
"""

from fastapi import APIRouter, Request, Depends, HTTPException, Query
from starlette.responses import JSONResponse
from typing import Optional
import json
import time
import re

from auth.local.dependencies import get_current_user
from auth.local.schemas import TokenData
from security import check_user_access
from security.privacy_guard import check_columns_and_filters, apply_privacy_and_labeling, get_variable_configs
from security.usage_tracker import log_api_usage
from security.plan_enforcer import get_and_enforce_plan_limits
from utils.metadata_helper import get_column_labels, apply_labels

from .nlp_engine import NLPQueryEngine, classify_query_risk, generate_summary

router = APIRouter(prefix="/api/ai-query", tags=["AI Query"])


async def check_ai_credits(conn, user_email: str, plan_limits: dict) -> dict:
    """Check if user has remaining AI query credits for this month."""
    max_ai = plan_limits.get("max_ai_queries_per_month", 3)
    
    ai_used = await conn.fetchval(
        """
        SELECT COUNT(*) FROM usage_logs
        WHERE user_email = $1 
          AND endpoint LIKE '%/ai%'
          AND queried_at >= DATE_TRUNC('month', CURRENT_DATE)
        """,
        user_email
    ) or 0
    
    return {
        "used": int(ai_used),
        "remaining": max(0, max_ai - int(ai_used)),
        "limit": max_ai
    }


async def log_ai_query(conn, user_email: str, prompt: str, parsed: dict, 
                        risk: dict, sql: str, rows_returned: int, 
                        execution_time_ms: int, status: str = "completed",
                        table_name: str = "unknown", schema_name: str = "public"):
    """Log AI query details to ai_query_logs table."""
    try:
        await conn.execute(
            """
            INSERT INTO ai_query_logs 
                (user_email, prompt, parsed_intent, parsed_filters, generated_sql, 
                 risk_level, risk_score, risk_factors, query_type, 
                 rows_returned, execution_time_ms, status,
                 table_name, schema_name, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, CURRENT_TIMESTAMP)
            """,
            user_email,
            prompt,
            parsed.get("intent", "exploratory"),
            json.dumps(parsed.get("filters", []), default=str),
            sql,
            risk.get("level", "low"),
            risk.get("score", 0),
            json.dumps(risk.get("factors", []), default=str),
            parsed.get("intent", "exploratory"),
            rows_returned,
            execution_time_ms,
            status,
            table_name,
            schema_name
        )
    except Exception as e:
        print(f"⚠️ AI Query log failed: {e}")


@router.post("/parse")
async def parse_ai_query(
    request: Request,
    body: dict,
    current_user: TokenData = Depends(get_current_user),
):
    """
    Parse a natural language prompt and return interpreted filters,
    aggregations, and risk assessment WITHOUT executing.
    """
    prompt = body.get("prompt", "").strip()
    table_name = body.get("table_name", "").strip()
    schema_name = body.get("schema_name", "public").strip()
    
    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt is required")
    if not table_name:
        raise HTTPException(status_code=400, detail="Table name is required")
    
    pool = request.app.state.db
    async with pool.acquire() as conn:
        user_role = str(current_user.role)
        
        # Resolve correct schema for table_name dynamically if not specified or left as public
        if not schema_name or schema_name == "public":
            schema_row = await conn.fetchrow(
                """
                SELECT table_schema
                FROM information_schema.tables 
                WHERE LOWER(table_name) = LOWER($1) 
                LIMIT 1
                """,
                table_name
            )
            if schema_row:
                schema_name = schema_row["table_schema"]
            else:
                schema_name = await conn.fetchval("SELECT current_schema()") or "public"

        # Validate that the schema exists in the database
        schema_ok = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
            schema_name
        )
        if not schema_ok:
            schema_name = await conn.fetchval("SELECT current_schema()") or "public"
        
        # Get available columns for the table
        actual_columns = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
            schema_name, table_name
        )
        all_cols = [c["column_name"] for c in actual_columns]
        # Exclude label columns from display
        visible_cols = [c for c in all_cols if not c.endswith("_label")]
        
        # Get variable configs for risk assessment
        var_configs = await get_variable_configs(conn, schema_name, table_name)
        
        # Parse the prompt
        engine = NLPQueryEngine(available_columns=visible_cols, variable_configs=var_configs)
        parsed = engine.parse(prompt)
        
        # Assess risk
        risk = classify_query_risk(parsed, var_configs)
        
        return {
            "parsed": parsed,
            "risk": risk,
            "available_columns": visible_cols,
            "table_name": table_name,
            "schema_name": schema_name,
        }


@router.post("/execute")
async def execute_ai_query(
    request: Request,
    body: dict,
    current_user: TokenData = Depends(get_current_user),
):
    """
    Execute a governed AI query:
    1. Parse NL prompt
    2. Validate through governance
    3. Build safe SQL
    4. Execute with privacy controls
    5. Generate summary
    6. Log everything
    """
    start_time = time.time()
    
    prompt = body.get("prompt", "").strip()
    table_name = body.get("table_name", "").strip()
    schema_name = body.get("schema_name", "public").strip()
    limit = min(body.get("limit", 100), 1000)
    
    if not prompt:
        raise HTTPException(status_code=400, detail="Prompt is required")
    if not table_name:
        raise HTTPException(status_code=400, detail="Table name is required")
    
    pool = request.app.state.db
    async with pool.acquire() as conn:
        user_email = current_user.username
        user_role = str(current_user.role)
        
        # Resolve correct schema for table_name dynamically if not specified or left as public
        if not schema_name or schema_name == "public":
            schema_row = await conn.fetchrow(
                """
                SELECT table_schema
                FROM information_schema.tables 
                WHERE LOWER(table_name) = LOWER($1) 
                LIMIT 1
                """,
                table_name
            )
            if schema_row:
                schema_name = schema_row["table_schema"]
            else:
                schema_name = await conn.fetchval("SELECT current_schema()") or "public"

        # Validate that the schema exists in the database
        schema_ok = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
            schema_name
        )
        if not schema_ok:
            schema_name = await conn.fetchval("SELECT current_schema()") or "public"
        
        # ─── 1. CHECK AI CREDITS ───
        plan_limits = await get_and_enforce_plan_limits(conn, user_email, user_role)
        credits = await check_ai_credits(conn, user_email, plan_limits)
        
        if credits["remaining"] <= 0 and user_role != "1":
            await log_ai_query(conn, user_email, prompt, {"intent": "blocked"}, 
                             {"level": "low", "score": 0, "factors": []}, 
                             "", 0, 0, status="credits_exhausted",
                             table_name=table_name, schema_name=schema_name)
            raise HTTPException(
                status_code=429, 
                detail=f"AI Query credit limit reached. Used {credits['used']}/{credits['limit']} this month. Upgrade your plan for more credits."
            )
        
        # ─── 2. GOVERNANCE CHECK ───
        try:
            await check_user_access(
                conn=conn,
                user=current_user,
                action_type="query",
                schema_name=schema_name,
                table_name=table_name,
                rows_requested=limit,
                filters=prompt  # Pass prompt for suspicious activity detection
            )
        except HTTPException as e:
            await log_ai_query(conn, user_email, prompt, {"intent": "blocked"}, 
                             {"level": "high", "score": 100, "factors": ["Governance blocked"]}, 
                             "", 0, 0, status="governance_blocked",
                             table_name=table_name, schema_name=schema_name)
            raise e
        
        # ─── 3. GET TABLE METADATA ───
        actual_columns = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
            schema_name, table_name
        )
        all_cols = [c["column_name"] for c in actual_columns]
        visible_cols = [c for c in all_cols if not c.endswith("_label")]
        label_cols = set(c for c in all_cols if c.endswith("_label"))
        raw_cols_with_labels = set(c[:-6] for c in label_cols)
        
        if not all_cols:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found or has no columns")
        
        # Get variable configs
        var_configs = await get_variable_configs(conn, schema_name, table_name)
        
        # ─── 4. PARSE PROMPT ───
        engine = NLPQueryEngine(available_columns=visible_cols, variable_configs=var_configs)
        parsed = engine.parse(prompt)
        
        # ─── 5. RISK ASSESSMENT ───
        risk = classify_query_risk(parsed, var_configs)
        
        if risk["level"] == "high" and user_role != "1":
            await log_ai_query(conn, user_email, prompt, parsed, risk, 
                             "", 0, 0, status="risk_blocked",
                             table_name=table_name, schema_name=schema_name)
            raise HTTPException(
                status_code=403, 
                detail={
                    "error": "High-Risk Query Blocked",
                    "detail": f"This query was classified as high-risk (score: {risk['score']}/100). " +
                              "Risk factors: " + "; ".join(risk["factors"]),
                    "risk": risk
                }
            )
        
        # ─── 6. APPLY COLUMN PRIVACY FILTERING ───
        # Determine which columns to select
        if parsed["select_columns"]:
            requested_cols = parsed["select_columns"]
        else:
            requested_cols = visible_cols
        
        allowed_cols = await check_columns_and_filters(
            conn=conn,
            schema=schema_name,
            table=table_name,
            user_role=user_role,
            columns=requested_cols,
            filters=None  # We handle filter validation separately
        )
        
        # ─── 7. BUILD SAFE SQL ───
        # Build column list (use labels where available)
        col_list = []
        for c in allowed_cols:
            if c in raw_cols_with_labels:
                col_list.append(f'"{c}_label" AS "{c}"')
            else:
                col_list.append(f'"{c}"')
        
        col_sql = ", ".join(col_list) if col_list else "*"
        
        # Build WHERE clause from parsed filters (only using allowed columns)
        where_parts = []
        allowed_set = {c.lower(): c for c in allowed_cols}
        
        for f in parsed.get("filters", []):
            col_name = f["column"]
            # Find actual column name (case-insensitive)
            actual_col = allowed_set.get(col_name.lower())
            if not actual_col:
                parsed["warnings"].append(f"Filter column '{col_name}' not available or restricted")
                continue
            
            op = f["operator"]
            val = f["value"]
            
            if op == "BETWEEN":
                where_parts.append(f'"{actual_col}" BETWEEN {val}')
            elif op == "=":
                # Check if value is numeric
                if str(val).replace(".", "", 1).replace("-", "", 1).isdigit():
                    where_parts.append(f'"{actual_col}" = {val}')
                else:
                    # Check for label column
                    if actual_col in raw_cols_with_labels:
                        where_parts.append(f'"{actual_col}_label" = \'{val}\'')
                    else:
                        where_parts.append(f'"{actual_col}"::text ILIKE \'%{val}%\'')
            elif op in (">=", "<=", ">", "<", "!="):
                where_parts.append(f'"{actual_col}" {op} {val}')
        
        where_clause = ""
        if where_parts:
            where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build aggregation SQL
        if parsed["aggregations"] and parsed.get("group_by"):
            # Aggregation with GROUP BY
            agg_parts = []
            for agg in parsed["aggregations"]:
                func = agg["function"]
                col = agg["column"]
                if col == "*":
                    agg_parts.append(f"{func}(*) AS \"{func.lower()}_all\"")
                else:
                    actual_agg_col = allowed_set.get(col.lower(), col)
                    agg_parts.append(f"{func}(\"{actual_agg_col}\") AS \"{func.lower()}_{actual_agg_col}\"")
            
            gb_cols = []
            for gb in parsed["group_by"]:
                actual_gb = allowed_set.get(gb.lower())
                if actual_gb:
                    if actual_gb in raw_cols_with_labels:
                        gb_cols.append(f'"{actual_gb}_label" AS "{actual_gb}"')
                    else:
                        gb_cols.append(f'"{actual_gb}"')
            
            if gb_cols:
                select_part = ", ".join(gb_cols + agg_parts)
                gb_part = ", ".join([f'"{allowed_set.get(gb.lower(), gb)}_label"' 
                                    if allowed_set.get(gb.lower(), gb) in raw_cols_with_labels 
                                    else f'"{allowed_set.get(gb.lower(), gb)}"' 
                                    for gb in parsed["group_by"] 
                                    if allowed_set.get(gb.lower())])
                sql = f'SELECT {select_part} FROM "{schema_name}"."{table_name}" {where_clause} GROUP BY {gb_part} LIMIT {limit}'
            else:
                sql = f'SELECT {col_sql} FROM "{schema_name}"."{table_name}" {where_clause} LIMIT {limit}'
        elif parsed["aggregations"]:
            # Aggregation without GROUP BY
            agg_parts = []
            for agg in parsed["aggregations"]:
                func = agg["function"]
                col = agg["column"]
                if col == "*":
                    agg_parts.append(f"{func}(*) AS \"{func.lower()}_count\"")
                else:
                    actual_agg_col = allowed_set.get(col.lower(), col)
                    agg_parts.append(f"{func}(\"{actual_agg_col}\") AS \"{func.lower()}_{actual_agg_col}\"")
            
            select_part = ", ".join(agg_parts)
            sql = f'SELECT {select_part} FROM "{schema_name}"."{table_name}" {where_clause}'
        else:
            # Standard SELECT query
            sql = f'SELECT {col_sql} FROM "{schema_name}"."{table_name}" {where_clause} LIMIT {limit}'
        
        # ─── 8. EXECUTE QUERY ───
        try:
            rows = await conn.fetch(sql)
            row_count = len(rows)
        except Exception as e:
            execution_time = int((time.time() - start_time) * 1000)
            await log_ai_query(conn, user_email, prompt, parsed, risk, 
                             sql, 0, execution_time, status="sql_error",
                             table_name=table_name, schema_name=schema_name)
            raise HTTPException(status_code=400, detail=f"AI Query execution failed: {str(e)}")
        
        data = [dict(row) for row in rows]
        
        # ─── 9. APPLY PRIVACY (suppression, labeling) ───
        result_columns = list(data[0].keys()) if data else allowed_cols
        
        try:
            label_map = await get_column_labels(conn, table_name, schema=schema_name)
            labels = {c: (label_map.get(c) or {}) for c in result_columns}
        except Exception:
            labels = {}
        
        if not parsed["aggregations"]:
            # Only apply suppression to non-aggregated queries
            try:
                filtered_data = await apply_privacy_and_labeling(
                    conn=conn,
                    schema=schema_name,
                    table=table_name,
                    user_role=user_role,
                    columns=result_columns,
                    rows=data,
                    labels=labels
                )
            except HTTPException as e:
                execution_time = int((time.time() - start_time) * 1000)
                await log_ai_query(conn, user_email, prompt, parsed, risk, 
                                 sql, 0, execution_time, status="suppressed",
                                 table_name=table_name, schema_name=schema_name)
                raise e
        else:
            filtered_data = data
        
        # ─── 10. GENERATE SUMMARY ───
        summary = generate_summary(parsed, filtered_data, result_columns)
        
        # ─── 11. LOG EVERYTHING ───
        execution_time = int((time.time() - start_time) * 1000)
        
        await log_ai_query(conn, user_email, prompt, parsed, risk, 
                         sql, len(filtered_data), execution_time, status="completed",
                         table_name=table_name, schema_name=schema_name)
        
        # Also log to usage_logs (for credit tracking)
        await log_api_usage(
            conn, user_email,
            f"/api/ai-query/execute", schema_name, table_name,
            len(filtered_data), len(json.dumps(filtered_data, default=str).encode()),
            query_time_ms=execution_time,
            status="completed",
            filters=prompt
        )
        
        # Update credits
        updated_credits = await check_ai_credits(conn, user_email, plan_limits)
        
        return {
            "results": filtered_data,
            "columns": result_columns,
            "row_count": len(filtered_data),
            "parsed": parsed,
            "risk": risk,
            "sql_preview": sql,
            "summary": summary,
            "execution_time_ms": execution_time,
            "credits": updated_credits,
        }


@router.get("/credits")
async def get_ai_credits(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
):
    """Get current AI query credit usage for the authenticated user."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        user_email = current_user.username
        user_role = str(current_user.role)
        plan_limits = await get_and_enforce_plan_limits(conn, user_email, user_role)
        credits = await check_ai_credits(conn, user_email, plan_limits)
        return credits


@router.get("/history", include_in_schema=False)
async def get_ai_query_history(
    request: Request,
    page: int = 1,
    per_page: int = 20,
    current_user: TokenData = Depends(get_current_user),
):
    """Get AI query history for the current user."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        offset = (page - 1) * per_page
        
        rows = await conn.fetch(
            """
            SELECT id, prompt, parsed_intent, risk_level, risk_score, 
                   rows_returned, execution_time_ms, status, 
                   table_name, schema_name, created_at
            FROM ai_query_logs
            WHERE user_email = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
            """,
            current_user.username, per_page, offset
        )
        
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM ai_query_logs WHERE user_email = $1",
            current_user.username
        ) or 0
        
        return {
            "history": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "per_page": per_page,
        }


@router.get("/admin/analytics", include_in_schema=False)
async def get_ai_analytics(
    request: Request,
    current_user: TokenData = Depends(get_current_user),
):
    """Admin-only: Get AI Query analytics for dashboard."""
    if str(current_user.role) != "1":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    pool = request.app.state.db
    async with pool.acquire() as conn:
        # Fetch admin clear cutoff for AI query logs
        cutoff = await conn.fetchval(
            "SELECT cleared_at FROM admin_log_clear_timestamps WHERE log_type = $1",
            "ai_query_logs"
        )

        # AI queries today
        if cutoff:
            today_count = await conn.fetchval(
                """SELECT COUNT(*) FROM ai_query_logs 
                   WHERE created_at >= CURRENT_DATE AND created_at > $1""",
                cutoff
            ) or 0
        else:
            today_count = await conn.fetchval(
                """SELECT COUNT(*) FROM ai_query_logs 
                   WHERE created_at >= CURRENT_DATE"""
            ) or 0
        
        # Total AI queries this month
        if cutoff:
            month_count = await conn.fetchval(
                """SELECT COUNT(*) FROM ai_query_logs 
                   WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE) AND created_at > $1""",
                cutoff
            ) or 0
        else:
            month_count = await conn.fetchval(
                """SELECT COUNT(*) FROM ai_query_logs 
                   WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)"""
            ) or 0
        
        # Blocked queries
        if cutoff:
            blocked_count = await conn.fetchval(
                """SELECT COUNT(*) FROM ai_query_logs 
                   WHERE status IN ('risk_blocked', 'governance_blocked', 'credits_exhausted')
                   AND created_at >= DATE_TRUNC('month', CURRENT_DATE) AND created_at > $1""",
                cutoff
            ) or 0
        else:
            blocked_count = await conn.fetchval(
                """SELECT COUNT(*) FROM ai_query_logs 
                   WHERE status IN ('risk_blocked', 'governance_blocked', 'credits_exhausted')
                   AND created_at >= DATE_TRUNC('month', CURRENT_DATE)"""
            ) or 0
        
        # High-risk prompts
        if cutoff:
            high_risk = await conn.fetch(
                """SELECT prompt, risk_score, risk_level, user_email, created_at
                   FROM ai_query_logs 
                   WHERE risk_level = 'high' AND created_at > $1
                   ORDER BY created_at DESC LIMIT 10""",
                cutoff
            )
        else:
            high_risk = await conn.fetch(
                """SELECT prompt, risk_score, risk_level, user_email, created_at
                   FROM ai_query_logs 
                   WHERE risk_level = 'high'
                   ORDER BY created_at DESC LIMIT 10"""
            )
        
        # Top AI users
        if cutoff:
            top_users = await conn.fetch(
                """SELECT user_email, COUNT(*) as query_count, 
                          AVG(risk_score) as avg_risk
                   FROM ai_query_logs 
                   WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE) AND created_at > $1
                   GROUP BY user_email
                   ORDER BY query_count DESC LIMIT 10""",
                cutoff
            )
        else:
            top_users = await conn.fetch(
                """SELECT user_email, COUNT(*) as query_count, 
                          AVG(risk_score) as avg_risk
                   FROM ai_query_logs 
                   WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)
                   GROUP BY user_email
                   ORDER BY query_count DESC LIMIT 10"""
            )
        
        # Most common prompts
        if cutoff:
            common_prompts = await conn.fetch(
                """SELECT prompt, COUNT(*) as usage_count
                   FROM ai_query_logs
                   WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE) AND created_at > $1
                   GROUP BY prompt
                   ORDER BY usage_count DESC LIMIT 10""",
                cutoff
            )
        else:
            common_prompts = await conn.fetch(
                """SELECT prompt, COUNT(*) as usage_count
                   FROM ai_query_logs
                   WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)
                   GROUP BY prompt
                   ORDER BY usage_count DESC LIMIT 10"""
            )
        
        # Risk distribution
        if cutoff:
            risk_dist = await conn.fetch(
                """SELECT risk_level, COUNT(*) as count
                   FROM ai_query_logs
                   WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE) AND created_at > $1
                   GROUP BY risk_level""",
                cutoff
            )
        else:
            risk_dist = await conn.fetch(
                """SELECT risk_level, COUNT(*) as count
                   FROM ai_query_logs
                   WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE)
                   GROUP BY risk_level"""
            )
        
        return {
            "ai_queries_today": int(today_count),
            "ai_queries_month": int(month_count),
            "blocked_queries": int(blocked_count),
            "high_risk_prompts": [dict(r) for r in high_risk],
            "top_users": [dict(r) for r in top_users],
            "common_prompts": [dict(r) for r in common_prompts],
            "risk_distribution": {r["risk_level"]: r["count"] for r in risk_dist},
        }
