from fastapi import APIRouter, Request, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
import asyncpg
from typing import List, Optional, Dict, Any
import json

from auth.local.dependencies import get_current_active_user_with_role
from auth.local.schemas import TokenData
from security.access_control import check_user_access
from security.plan_enforcer import get_and_enforce_plan_limits
from security.privacy_guard import check_columns_and_filters, apply_privacy_and_labeling
from security.usage_tracker import log_api_usage, get_qualified_table
from query.query_data import parse_filters

router = APIRouter()

@router.get("/user/explore-data", response_class=HTMLResponse, include_in_schema=False)
async def user_explore_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    from main import get_user_template_context, templates
    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        request=request,
        name="USER_PAGES/user_explore.html",
        context={
            "request": request,
            **ctx
        },
    )

@router.get("/api/user/explore/tree")
async def get_explore_tree(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    from main import (
        _group_schemas_by_survey,
        _is_internal_table,
        apply_config,
        _set_apply_context,
        _reset_apply_context,
        TableHidden
    )

    async with pool.acquire() as conn:
        # Enforce check_user_access to verify basic eligibility and status
        await check_user_access(conn, current_user, "query", limit=1)

        grouped = await _group_schemas_by_survey(conn)
        tree = []
        
        tokens = _set_apply_context(conn=conn)
        try:
            for survey_db, survey_info in grouped.items():
                survey_node = {
                    "survey": survey_db,
                    "display_name": survey_info["display_name"],
                    "datasets": []
                }
                
                datasets = survey_info["datasets"]
                # If there are multiple datasets, filter out the base survey DB schema itself
                if len(datasets) > 1:
                    datasets = [d for d in datasets if d["schema"] != survey_db]
                    
                for d in datasets:
                    dataset_schema = d["schema"]
                    
                    # Fetch tables
                    rows = await conn.fetch(
                        """
                        SELECT table_name,
                               (SELECT COUNT(*) FROM information_schema.columns
                                WHERE table_name = t.table_name AND table_schema = t.table_schema) as column_count,
                               (SELECT reltuples::bigint FROM pg_class c
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE c.relname = t.table_name AND n.nspname = t.table_schema) as row_count
                        FROM information_schema.tables t
                        WHERE table_type = 'BASE TABLE'
                          AND table_schema = $1
                        ORDER BY table_name
                        """,
                        dataset_schema,
                    )
                    
                    dataset_tables = []
                    for r in rows:
                        tname = r["table_name"]
                        if _is_internal_table(tname):
                            continue
                        try:
                            # Verify table is not hidden
                            await apply_config(dataset_schema, tname, current_user, [], None)
                        except TableHidden:
                            continue
                        
                        dataset_tables.append({
                            "table_name": tname,
                            "row_count": max(0, r["row_count"] or 0),
                            "column_count": r["column_count"] or 0
                        })
                        
                    if dataset_tables:
                        survey_node["datasets"].append({
                            "schema": dataset_schema,
                            "display_name": d["display_name"],
                            "tables": dataset_tables
                        })
                        
                if survey_node["datasets"]:
                    tree.append(survey_node)
        finally:
            _reset_apply_context(tokens)
            
        return tree

@router.get("/api/user/explore/metadata")
async def get_explore_metadata(
    request: Request,
    schema: str,
    table: str,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    from main import (
        apply_config,
        _set_apply_context,
        _reset_apply_context,
        TableHidden
    )

    async with pool.acquire() as conn:
        # Enforce check_user_access (limits rates, checks user block/freeze and daily quota limits)
        await check_user_access(conn, current_user, "query", limit=1)

        # Get plan settings
        plan_limits = await get_and_enforce_plan_limits(conn, current_user.username, current_user.role)
        plan_name = plan_limits.get("plan", "free")

        # 1. Fetch table columns
        cols = await conn.fetch(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            """,
            schema,
            table,
        )
        if not cols:
            raise HTTPException(status_code=404, detail="Table not found")

        all_cols = [c["column_name"] for c in cols]
        
        # Enforce privacy filters on columns visibility
        tokens = _set_apply_context(conn=conn)
        try:
            try:
                allowed_cols, _ = await apply_config(schema, table, current_user, all_cols, None)
            except TableHidden:
                raise HTTPException(status_code=404, detail="Table not found")
        finally:
            _reset_apply_context(tokens)

        allowed_set = set(allowed_cols)
        filtered_cols = [c for c in cols if c["column_name"] in allowed_set]

        # 2. Fetch dataset-level metadata (basic metadata allowed to all users)
        try:
            meta_row = await conn.fetchrow(
                f"""
                SELECT title, abstract, keywords, geographic_coverage, industrial_coverage, 
                       product_coverage, weighting, frequency, methodology, collection_mode, 
                       time_method, procedures, producer, file_case_count, file_variable_count
                FROM "{schema}".dataset_metadata
                LIMIT 1
                """
            )
            dataset_meta = dict(meta_row) if meta_row else {}
        except Exception:
            dataset_meta = {}

        # 3. Fetch variable descriptions (Filtered to allowed columns only)
        try:
            has_variables = await conn.fetchval(
                f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_name = 'variables'
                )
                """,
                schema
            )
            if has_variables:
                variables_rows = await conn.fetch(
                    f"""
                    SELECT column_name AS variable_name, label, ddi_type, width, decimals, concept, universe, question_text
                    FROM "{schema}".variables
                    WHERE table_name = $1
                    """,
                    table
                )
            else:
                variables_rows = await conn.fetch(
                    f"""
                    SELECT variable_name, label, ddi_type, width, interval, valid_count, invalid_count, final_type
                    FROM "{schema}".variable_dictionary
                    WHERE table_name = $1
                    """,
                    table
                )
            variables_list = [dict(v) for v in variables_rows if v["variable_name"] in allowed_set]
        except Exception:
            variables_list = []

        # 4. Fetch Categories & Statistics (Premium restricted - Pro and Enterprise only)
        categories_list = []
        statistics_list = []

        if plan_name != "free":
            try:
                cat_rows = await conn.fetch(
                    f"""
                    SELECT variable_name, value, label, frequency
                    FROM "{schema}".variable_categories
                    WHERE table_name = $1
                    """,
                    table
                )
                categories_list = [dict(c) for c in cat_rows if c["variable_name"] in allowed_set]
            except Exception:
                categories_list = []

            try:
                stat_rows = await conn.fetch(
                    f"""
                    SELECT variable_name, mean, min, max, stddev, unique_count
                    FROM "{schema}".variable_statistics
                    WHERE table_name = $1
                    """,
                    table
                )
                statistics_list = [dict(s) for s in stat_rows if s["variable_name"] in allowed_set]
            except Exception:
                statistics_list = []

        response_data = {
            "schema": schema,
            "table": table,
            "columns": [{"name": c["column_name"], "type": c["data_type"]} for c in filtered_cols],
            "dataset_metadata": dataset_meta,
            "variables": variables_list,
            "categories": categories_list,
            "statistics": statistics_list
        }

        # Log metadata access request (Deducts credits / logs usage statistics)
        response_bytes = json.dumps(response_data, default=str).encode("utf-8")
        await log_api_usage(
            conn=conn,
            user_email=current_user.username,
            endpoint="/api/user/explore/metadata",
            schema_name=schema,
            table_name=table,
            rows_returned=0,
            bytes_sent=len(response_bytes)
        )

        return response_data

@router.get("/api/user/explore/preview")
async def get_explore_preview(
    request: Request,
    schema: str,
    table: str,
    columns: Optional[str] = None,
    filters: Optional[str] = None,
    limit: int = Query(5, ge=1),
    offset: int = Query(0, ge=0),
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    user_role = str(current_user.role)

    async with pool.acquire() as conn:
        # Enforce plan-based preview row caps
        plan_limits = await get_and_enforce_plan_limits(conn, current_user.username, user_role)
        plan_name = plan_limits.get("plan", "free")

        if plan_name == "free":
            max_preview = 5
        elif plan_name == "pro":
            max_preview = 50
        else:  # enterprise / admin
            max_preview = 100

        limit = min(limit, max_preview)

        # Enforce Central Security governance (rate limits, warnings, query limits, row count limits)
        await check_user_access(
            conn=conn,
            user=current_user,
            action_type="query",
            schema_name=schema,
            table_name=table,
            rows_requested=limit,
            filters=filters,
            limit=limit
        )

        # Retrieve table columns
        actual_columns = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
            schema,
            table
        )
        if not actual_columns:
            raise HTTPException(status_code=404, detail="Table not found")

        all_cols = [c["column_name"] for c in actual_columns]
        label_cols = set(c for c in all_cols if c.endswith("_label"))
        raw_cols_with_labels = set(c[:-6] for c in label_cols)

        # Run privacy guard column-level security filters
        requested_cols = [c.strip() for c in (columns.split(",") if columns else all_cols) if c.strip()]
        allowed_cols = await check_columns_and_filters(
            conn=conn,
            schema=schema,
            table=table,
            user_role=user_role,
            columns=requested_cols,
            filters=filters
        )

        col_list = []
        for c in allowed_cols:
            if c in raw_cols_with_labels:
                col_list.append(f'"{c}_label" AS "{c}"')
            else:
                col_list.append(f'"{c}"')

        col_sql = ", ".join(col_list) if col_list else "*"

        where_clause = ""
        if filters:
            try:
                where_clause = "WHERE " + parse_filters(filters)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

        sql = f'SELECT {col_sql} FROM "{schema}"."{table}" {where_clause} LIMIT {limit} OFFSET {offset}'

        try:
            rows = await conn.fetch(sql)
            row_count = len(rows)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Query failed: {e}")

        data = [dict(row) for row in rows]

        # Enforce Cell Suppression (<5 rows result set restriction)
        if user_role != "1" and row_count > 0 and row_count < 5:
            await log_api_usage(
                conn=conn,
                user_email=current_user.username,
                endpoint="/api/user/explore/preview",
                schema_name=schema,
                table_name=table,
                rows_returned=0,
                bytes_sent=0,
                status="suppressed",
                filters=filters
            )
            raise HTTPException(status_code=403, detail="Data suppressed (less than 5 rows match the query)")

        # Retrieve value labeling maps
        from utils.metadata_helper import get_column_labels
        try:
            label_map = await get_column_labels(conn, table, schema=schema)
            labels = {c: (label_map.get(c) or {}) for c in allowed_cols}
        except Exception:
            labels = {}

        # Enforce cell-level and label-level privacy protection
        filtered_data = await apply_privacy_and_labeling(
            conn=conn,
            schema=schema,
            table=table,
            user_role=user_role,
            columns=allowed_cols,
            rows=data,
            labels=labels
        )

        # Log preview query to monthly credits usage
        response_bytes = json.dumps(filtered_data, default=str).encode("utf-8")
        await log_api_usage(
            conn=conn,
            user_email=current_user.username,
            endpoint="/api/user/explore/preview",
            schema_name=schema,
            table_name=table,
            rows_returned=len(filtered_data),
            bytes_sent=len(response_bytes),
            status="completed",
            filters=filters
        )

        return {
            "columns": allowed_cols,
            "rows": filtered_data,
            "total_preview_limit": max_preview
        }
