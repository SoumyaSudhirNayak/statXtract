from fastapi import APIRouter, Request, Depends, HTTPException, Query, Form
from fastapi.responses import HTMLResponse, JSONResponse
import asyncpg
import json
from typing import List, Optional, Dict, Any
from datetime import datetime

from auth.local.dependencies import get_current_active_user_with_role
from auth.local.schemas import TokenData
from security.access_control import check_user_access
from security.plan_enforcer import get_and_enforce_plan_limits
from security.privacy_guard import check_columns_and_filters, apply_privacy_and_labeling
from security.usage_tracker import log_api_usage, get_qualified_table

router = APIRouter()

# Helper to execute query with privacy controls
async def execute_widget_query(
    conn: asyncpg.Connection,
    user_email: str,
    role: str,
    schema: str,
    table: str,
    columns_str: str,
    filters_str: str,
    widget_type: str | None = None,
    chart_config: dict | None = None,
):
    # Enforce check_user_access
    user_token = TokenData(username=user_email, role=int(role))
    await check_user_access(
        conn=conn,
        user=user_token,
        action_type="query",
        schema_name=schema,
        table_name=table,
        limit=100
    )

    # Fetch columns from schema
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

    column_map = {c["column_name"].lower(): c["column_name"] for c in cols}
    column_type_map = {c["column_name"].lower(): c["data_type"] for c in cols}
    all_cols = [c["column_name"] for c in cols]
    label_cols = set(c for c in all_cols if c.endswith("_label"))
    raw_cols_with_labels = set(c[:-6] for c in label_cols)

    # Check security for requested columns
    requested_cols = [c.strip() for c in (columns_str.split(",") if columns_str else all_cols) if c.strip()]
    allowed_cols = await check_columns_and_filters(
        conn=conn,
        schema=schema,
        table=table,
        user_role=str(role),
        columns=requested_cols,
        filters=filters_str
    )

    allowed_set = set(allowed_cols)
    where_clause = ""
    if filters_str:
        from query.query_data import parse_filters
        try:
            where_clause = "WHERE " + parse_filters(filters_str)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    is_chart = widget_type in ["bar", "pie", "line", "area"]
    group_by_col = None
    agg_func = None
    agg_col = None

    if is_chart:
        cc = chart_config or {}
        label_col = cc.get("label_column")
        if not label_col and columns_str:
            parts = [c.strip() for c in columns_str.split(",") if c.strip()]
            if parts:
                label_col = parts[0]

        val_col = cc.get("value_column")
        if not val_col and columns_str:
            parts = [c.strip() for c in columns_str.split(",") if c.strip()]
            if len(parts) > 1:
                val_col = parts[1]
            elif parts:
                val_col = parts[0]

        agg_val = cc.get("aggregation", "none")
        if str(agg_val).lower() == "none" or not agg_val:
            agg_func = None
        else:
            agg_func = str(agg_val).upper()

        if label_col:
            group_by_col = column_map.get(label_col.lower(), label_col)
            if val_col:
                agg_col = column_map.get(val_col.lower(), val_col)
            else:
                agg_col = group_by_col

            if group_by_col in allowed_set:
                if not agg_func:
                    col_type = column_type_map.get(agg_col.lower(), "").lower()
                    is_numeric = any(t in col_type for t in ["int", "precision", "numeric", "real", "double", "float"])
                    if is_numeric and agg_col.lower() != group_by_col.lower():
                        agg_func = "SUM"
                    else:
                        agg_func = "COUNT"
                        agg_col = "*"

                if agg_col != "*" and agg_col not in allowed_set:
                    agg_col = "*"
                    agg_func = "COUNT"

                if agg_func in ["SUM", "AVG", "MIN", "MAX"] and agg_col == "*":
                    agg_func = "COUNT"

                if agg_func in ["SUM", "AVG", "MIN", "MAX"]:
                    col_type = column_type_map.get(agg_col.lower(), "").lower()
                    is_numeric = any(t in col_type for t in ["int", "precision", "numeric", "real", "double", "float"])
                    if not is_numeric:
                        agg_func = "COUNT"
                        agg_col = "*"

    if group_by_col and group_by_col in allowed_set:
        agg_alias = agg_col if agg_col != "*" else "count"
        if group_by_col.lower() == agg_alias.lower():
            agg_alias = f"{agg_alias}_value"

        agg_expr = f'{agg_func}("{agg_col}")' if agg_col != "*" else 'COUNT(*)'

        col_list = []
        if group_by_col in raw_cols_with_labels:
            col_list.append(f'"{group_by_col}_label" AS "{group_by_col}"')
            group_by_sql = f'"{group_by_col}", "{group_by_col}_label"'
        else:
            col_list.append(f'"{group_by_col}"')
            group_by_sql = f'"{group_by_col}"'

        col_list.append(f'{agg_expr} AS "{agg_alias}"')
        selected_columns = [group_by_col, agg_alias]

        sql = f'SELECT {", ".join(col_list)} FROM "{schema}"."{table}" {where_clause} GROUP BY {group_by_sql} LIMIT 1000'
    else:
        col_list = []
        for c in allowed_cols:
            if c in raw_cols_with_labels:
                col_list.append(f'"{c}_label" AS "{c}"')
            else:
                col_list.append(f'"{c}"')

        col_sql = ", ".join(col_list) if col_list else "*"
        selected_columns = allowed_cols
        sql = f'SELECT {col_sql} FROM "{schema}"."{table}" {where_clause} LIMIT 100'

    try:
        rows = await conn.fetch(sql)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Widget query failed: {e}")

    data = [dict(row) for row in rows]

    # Apply cell suppression and value labeling
    from utils.metadata_helper import get_column_labels
    try:
        label_map = await get_column_labels(conn, table, schema=schema)
        labels = {c: (label_map.get(c) or {}) for c in selected_columns}
    except Exception:
        labels = {}

    filtered_data = await apply_privacy_and_labeling(
        conn=conn,
        schema=schema,
        table=table,
        user_role=str(role),
        columns=selected_columns,
        rows=data,
        labels=labels,
        is_aggregated=bool(group_by_col)
    )

    return {
        "columns": selected_columns,
        "rows": filtered_data
    }


# ── PAGES ──

@router.get("/user/dashboards", response_class=HTMLResponse, include_in_schema=False)
async def dashboards_list_page(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    from main import get_user_template_context, templates
    pool = request.app.state.db
    async with pool.acquire() as conn:
        plan_limits = await get_and_enforce_plan_limits(conn, current_user.username, str(current_user.role))
        plan_name = plan_limits.get("plan", "free").lower()
        if plan_name == "free":
            raise HTTPException(status_code=403, detail="Dashboard Studio is only available for Pro and Enterprise plans.")

    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        request=request,
        name="USER_PAGES/user_dashboards.html",
        context={
            "request": request,
            **ctx
        },
    )

@router.get("/user/dashboard-editor/{dashboard_id}", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_editor_page(
    request: Request,
    dashboard_id: int,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    from main import get_user_template_context, templates
    pool = request.app.state.db
    async with pool.acquire() as conn:
        plan_limits = await get_and_enforce_plan_limits(conn, current_user.username, str(current_user.role))
        plan_name = plan_limits.get("plan", "free").lower()
        if plan_name == "free":
            raise HTTPException(status_code=403, detail="Dashboard Studio is only available for Pro and Enterprise plans.")

        # Ensure dashboard exists and is owned by the user
        db_row = await conn.fetchrow("SELECT * FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        # Parse designer settings
        designer_settings = db_row.get("designer_settings")
        if isinstance(designer_settings, str):
            try:
                designer_settings = json.loads(designer_settings)
            except Exception:
                designer_settings = {}
        elif designer_settings is None:
            designer_settings = {}
        designer_settings_json = json.dumps(designer_settings)

    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        request=request,
        name="USER_PAGES/user_dashboard_editor.html",
        context={
            "request": request,
            "dashboard_id": dashboard_id,
            "dashboard_name": db_row["name"],
            "canvas_preset": db_row["canvas_preset"] or "dashboard_lg",
            "designer_settings_json": designer_settings_json,
            **ctx
        },
    )

@router.get("/user/dashboard-view/{dashboard_id}", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_view_page(
    request: Request,
    dashboard_id: int,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    from main import get_user_template_context, templates
    pool = request.app.state.db
    async with pool.acquire() as conn:
        plan_limits = await get_and_enforce_plan_limits(conn, current_user.username, str(current_user.role))
        plan_name = plan_limits.get("plan", "free").lower()
        if plan_name == "free":
            raise HTTPException(status_code=403, detail="Dashboard Studio is only available for Pro and Enterprise plans.")

        db_row = await conn.fetchrow("SELECT * FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        # Parse designer settings
        designer_settings = db_row.get("designer_settings")
        if isinstance(designer_settings, str):
            try:
                designer_settings = json.loads(designer_settings)
            except Exception:
                designer_settings = {}
        elif designer_settings is None:
            designer_settings = {}
        designer_settings_json = json.dumps(designer_settings)

    ctx = await get_user_template_context(request, current_user.username)
    return templates.TemplateResponse(
        request=request,
        name="USER_PAGES/user_dashboard_viewer.html",
        context={
            "request": request,
            "dashboard_id": dashboard_id,
            "dashboard_name": db_row["name"],
            "canvas_preset": db_row["canvas_preset"] or "dashboard_lg",
            "designer_settings_json": designer_settings_json,
            **ctx
        },
    )

# ── API ENDPOINTS ──

@router.get("/api/user/dashboards")
async def api_get_dashboards(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        plan_limits = await get_and_enforce_plan_limits(conn, current_user.username, str(current_user.role))
        plan_name = plan_limits.get("plan", "free").lower()
        if plan_name == "free":
            return JSONResponse(status_code=403, content={"detail": "Forbidden"})

        rows = await conn.fetch(
            """
            SELECT d.id, d.name, d.created_at, d.updated_at,
                   (SELECT COUNT(*) FROM dashboard_widgets WHERE dashboard_id = d.id) as widget_count
            FROM dashboards d
            WHERE d.user_email = $1 AND NOT COALESCE(d.is_deleted, FALSE)
            ORDER BY d.updated_at DESC
            """,
            current_user.username
        )
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "widget_count": r["widget_count"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None
            } for r in rows
        ]

@router.post("/api/user/dashboards")
async def api_create_dashboard(
    request: Request,
    payload: Dict[str, Any],
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    name = payload.get("name", "New Dashboard").strip()
    canvas_preset = payload.get("canvas_preset", "dashboard_lg").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Dashboard name cannot be empty.")

    async with pool.acquire() as conn:
        plan_limits = await get_and_enforce_plan_limits(conn, current_user.username, str(current_user.role))
        plan_name = plan_limits.get("plan", "free").lower()
        if plan_name == "free":
            raise HTTPException(status_code=403, detail="Forbidden")

        count = await conn.fetchval("SELECT COUNT(*) FROM dashboards WHERE user_email = $1", current_user.username) or 0
        limit = await get_dashboard_limit(conn, plan_name)
        if limit >= 0 and count >= limit:
            raise HTTPException(status_code=403, detail=f"{plan_name.capitalize()} plan limit of {limit} dashboards reached. Please upgrade.")

        db_id = await conn.fetchval(
            "INSERT INTO dashboards (user_email, name, canvas_preset) VALUES ($1, $2, $3) RETURNING id",
            current_user.username,
            name,
            canvas_preset
        )
        return {"id": db_id, "name": name, "message": "Dashboard created successfully."}

@router.put("/api/user/dashboards/{dashboard_id}")
async def api_rename_dashboard(
    request: Request,
    dashboard_id: int,
    payload: Dict[str, Any],
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    name = payload.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Dashboard name cannot be empty.")

    async with pool.acquire() as conn:
        db_row = await conn.fetchrow("SELECT id FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        await conn.execute("UPDATE dashboards SET name = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2", name, dashboard_id)
        return {"message": "Dashboard renamed successfully."}

@router.delete("/api/user/dashboards/{dashboard_id}")
async def api_delete_dashboard(
    request: Request,
    dashboard_id: int,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        db_row = await conn.fetchrow("SELECT id FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        await conn.execute("UPDATE dashboards SET is_deleted = TRUE, updated_at = CURRENT_TIMESTAMP WHERE id = $1", dashboard_id)
        return {"message": "Dashboard deleted successfully."}

# ── WIDGETS API ──

@router.get("/api/user/dashboards/{dashboard_id}/widgets")
async def api_get_widgets(
    request: Request,
    dashboard_id: int,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        db_row = await conn.fetchrow("SELECT id FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        rows = await conn.fetch(
            "SELECT * FROM dashboard_widgets WHERE dashboard_id = $1 ORDER BY layout_config->>'order' ASC, id ASC",
            dashboard_id
        )
        return [
            {
                "id": r["id"],
                "dashboard_id": r["dashboard_id"],
                "widget_type": r["widget_type"],
                "title": r["title"],
                "schema_name": r["schema_name"],
                "table_name": r["table_name"],
                "columns": r["columns"],
                "filters": r["filters"],
                "sql_query": r["sql_query"],
                "chart_config": json.loads(r["chart_config"]) if isinstance(r["chart_config"], str) else r["chart_config"],
                "layout_config": json.loads(r["layout_config"]) if isinstance(r["layout_config"], str) else r["layout_config"],
                "cached_at": r["cached_at"].isoformat() if r["cached_at"] else None
            } for r in rows
        ]

@router.post("/api/user/dashboards/{dashboard_id}/widgets")
async def api_add_widget(
    request: Request,
    dashboard_id: int,
    payload: Dict[str, Any],
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    wtype = payload.get("widget_type", "").strip()
    title = payload.get("title", "New Widget").strip()
    schema = payload.get("schema_name", "")
    table = payload.get("table_name", "")
    columns = payload.get("columns", "")
    filters = payload.get("filters", "")
    sql = payload.get("sql_query", "")
    chart_config = payload.get("chart_config", {})
    layout_config = payload.get("layout_config", {"width": 2, "height": 1, "order": 99})

    if not wtype:
        raise HTTPException(status_code=400, detail="Widget type cannot be empty.")

    async with pool.acquire() as conn:
        db_row = await conn.fetchrow("SELECT id FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        widget_id = await conn.fetchval(
            """
            INSERT INTO dashboard_widgets 
            (dashboard_id, widget_type, title, schema_name, table_name, columns, filters, sql_query, chart_config, layout_config)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING id
            """,
            dashboard_id, wtype, title, schema, table, columns, filters, sql,
            json.dumps(chart_config), json.dumps(layout_config)
        )

        # Trigger asynchronous data load and cache it
        try:
            if schema and table:
                res = await execute_widget_query(
                    conn, current_user.username, str(current_user.role), 
                    schema, table, columns, filters,
                    wtype, chart_config
                )
                await conn.execute(
                    "UPDATE dashboard_widgets SET cached_data = $1, cached_at = CURRENT_TIMESTAMP WHERE id = $2",
                    json.dumps(res, default=str), widget_id
                )
        except Exception as e:
            # Let it save but log issue
            print(f"Initial query fetch failed for widget {widget_id}: {e}")

        await conn.execute("UPDATE dashboards SET updated_at = CURRENT_TIMESTAMP WHERE id = $1", dashboard_id)

        return {"id": widget_id, "message": "Widget added successfully."}

@router.put("/api/user/dashboards/{dashboard_id}/widgets/{widget_id}")
async def api_update_widget(
    request: Request,
    dashboard_id: int,
    widget_id: int,
    payload: Dict[str, Any],
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    title = payload.get("title", "").strip()
    schema = payload.get("schema_name", "")
    table = payload.get("table_name", "")
    columns = payload.get("columns", "")
    filters = payload.get("filters", "")
    sql = payload.get("sql_query", "")
    chart_config = payload.get("chart_config")
    layout_config = payload.get("layout_config")

    async with pool.acquire() as conn:
        # Auth check
        db_row = await conn.fetchrow("SELECT id FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        widget = await conn.fetchrow("SELECT * FROM dashboard_widgets WHERE id = $1 AND dashboard_id = $2", widget_id, dashboard_id)
        if not widget:
            raise HTTPException(status_code=404, detail="Widget not found.")

        # Build updates dynamically
        update_fields = []
        params = []
        i = 1

        if title:
            update_fields.append(f"title = ${i}")
            params.append(title)
            i += 1
        if schema is not None:
            update_fields.append(f"schema_name = ${i}")
            params.append(schema)
            i += 1
        if table is not None:
            update_fields.append(f"table_name = ${i}")
            params.append(table)
            i += 1
        if columns is not None:
            update_fields.append(f"columns = ${i}")
            params.append(columns)
            i += 1
        if filters is not None:
            update_fields.append(f"filters = ${i}")
            params.append(filters)
            i += 1
        if sql is not None:
            update_fields.append(f"sql_query = ${i}")
            params.append(sql)
            i += 1
        if chart_config is not None:
            update_fields.append(f"chart_config = ${i}")
            params.append(json.dumps(chart_config))
            i += 1
        if layout_config is not None:
            update_fields.append(f"layout_config = ${i}")
            params.append(json.dumps(layout_config))
            i += 1

        if update_fields:
            params.append(widget_id)
            sql_stmt = f"UPDATE dashboard_widgets SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP WHERE id = ${i}"
            await conn.execute(sql_stmt, *params)

        # Trigger data refresh if query parameters changed
        if schema or table or columns or filters:
            try:
                eff_schema = schema or widget["schema_name"]
                eff_table = table or widget["table_name"]
                eff_columns = columns if columns is not None else widget["columns"]
                eff_filters = filters if filters is not None else widget["filters"]
                eff_wtype = widget["widget_type"]
                eff_cc_raw = chart_config if chart_config is not None else widget["chart_config"]
                eff_cc = json.loads(eff_cc_raw) if isinstance(eff_cc_raw, str) else (eff_cc_raw or {})
                res = await execute_widget_query(
                    conn, current_user.username, str(current_user.role), 
                    eff_schema, eff_table, eff_columns, eff_filters,
                    eff_wtype, eff_cc
                )
                await conn.execute(
                    "UPDATE dashboard_widgets SET cached_data = $1, cached_at = CURRENT_TIMESTAMP WHERE id = $2",
                    json.dumps(res, default=str), widget_id
                )
            except Exception as e:
                print(f"Data refresh failed: {e}")

        await conn.execute("UPDATE dashboards SET updated_at = CURRENT_TIMESTAMP WHERE id = $1", dashboard_id)
        return {"message": "Widget updated successfully."}

@router.delete("/api/user/dashboards/{dashboard_id}/widgets/{widget_id}")
async def api_delete_widget(
    request: Request,
    dashboard_id: int,
    widget_id: int,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        db_row = await conn.fetchrow("SELECT id FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        await conn.execute("DELETE FROM dashboard_widgets WHERE id = $1 AND dashboard_id = $2", widget_id, dashboard_id)
        await conn.execute("UPDATE dashboards SET updated_at = CURRENT_TIMESTAMP WHERE id = $1", dashboard_id)
        return {"message": "Widget deleted successfully."}

@router.put("/api/user/dashboards/{dashboard_id}/designer-settings")
async def api_save_designer_settings(
    request: Request,
    dashboard_id: int,
    payload: Dict[str, Any],
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    designer_settings = payload.get("designer_settings", {})
    async with pool.acquire() as conn:
        db_row = await conn.fetchrow("SELECT id FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")
        
        page_size = designer_settings.get("page_size", "dashboard_lg")
        await conn.execute(
            "UPDATE dashboards SET designer_settings = $1, canvas_preset = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $3",
            json.dumps(designer_settings), page_size, dashboard_id
        )
        return {"message": "Designer settings saved successfully."}

@router.post("/api/user/dashboards/{dashboard_id}/layout")
async def api_save_layout(
    request: Request,
    dashboard_id: int,
    payload: List[Dict[str, Any]],
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        db_row = await conn.fetchrow("SELECT id FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        # Update each widget's layout config
        async with conn.transaction():
            for item in payload:
                wid = item.get("id")
                lconfig = item.get("layout_config")
                if wid and lconfig:
                    await conn.execute(
                        "UPDATE dashboard_widgets SET layout_config = $1 WHERE id = $2 AND dashboard_id = $3",
                        json.dumps(lconfig), wid, dashboard_id
                    )

        await conn.execute("UPDATE dashboards SET updated_at = CURRENT_TIMESTAMP WHERE id = $1", dashboard_id)
        return {"message": "Dashboard layouts saved successfully."}

@router.get("/api/user/dashboards/{dashboard_id}/widgets/{widget_id}/data")
async def api_get_widget_data(
    request: Request,
    dashboard_id: int,
    widget_id: int,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        db_row = await conn.fetchrow("SELECT id FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        widget = await conn.fetchrow("SELECT * FROM dashboard_widgets WHERE id = $1 AND dashboard_id = $2", widget_id, dashboard_id)
        if not widget:
            raise HTTPException(status_code=404, detail="Widget not found.")

        schema = widget.get("schema_name")
        table = widget.get("table_name")
        
        columns_raw = widget.get("columns")
        columns = columns_raw
        if isinstance(columns_raw, str) and columns_raw.strip():
            try:
                columns = json.loads(columns_raw)
            except Exception:
                columns = columns_raw
        else:
            columns = columns_raw

        filters_raw = widget.get("filters")
        filters = filters_raw
        if isinstance(filters_raw, str) and filters_raw.strip():
            try:
                filters = json.loads(filters_raw)
            except Exception:
                filters = filters_raw
        else:
            filters = filters_raw

        if not schema or not table:
            return JSONResponse(status_code=400, content={"error": "Widget is missing required fields: schema_name or table_name"})

        wtype = widget["widget_type"]
        cc_raw = widget["chart_config"]
        cc = json.loads(cc_raw) if isinstance(cc_raw, str) else (cc_raw or {})

        print("WIDGET DEBUG")
        print("schema =", schema)
        print("table =", table)
        print("columns =", columns)
        print("filters =", filters)
        print("widget_type =", wtype)

        res = await execute_widget_query(
            conn, current_user.username, str(current_user.role), 
            schema, table, columns, filters,
            wtype, cc
        )
        await conn.execute(
            "UPDATE dashboard_widgets SET cached_data = $1, cached_at = CURRENT_TIMESTAMP WHERE id = $2",
            json.dumps(res, default=str), widget_id
        )
        return res

@router.post("/api/user/dashboards/{dashboard_id}/widgets/{widget_id}/refresh")
async def api_refresh_widget_data(
    request: Request,
    dashboard_id: int,
    widget_id: int,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    pool = request.app.state.db
    async with pool.acquire() as conn:
        db_row = await conn.fetchrow("SELECT id FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        widget = await conn.fetchrow("SELECT * FROM dashboard_widgets WHERE id = $1 AND dashboard_id = $2", widget_id, dashboard_id)
        if not widget:
            raise HTTPException(status_code=404, detail="Widget not found.")

        schema = widget.get("schema_name")
        table = widget.get("table_name")
        
        columns_raw = widget.get("columns")
        columns = columns_raw
        if isinstance(columns_raw, str) and columns_raw.strip():
            try:
                columns = json.loads(columns_raw)
            except Exception:
                columns = columns_raw
        else:
            columns = columns_raw

        filters_raw = widget.get("filters")
        filters = filters_raw
        if isinstance(filters_raw, str) and filters_raw.strip():
            try:
                filters = json.loads(filters_raw)
            except Exception:
                filters = filters_raw
        else:
            filters = filters_raw

        if not schema or not table:
            return JSONResponse(status_code=400, content={"error": "Widget is missing required fields: schema_name or table_name"})

        wtype = widget["widget_type"]
        cc_raw = widget["chart_config"]
        cc = json.loads(cc_raw) if isinstance(cc_raw, str) else (cc_raw or {})

        print("WIDGET DEBUG")
        print("schema =", schema)
        print("table =", table)
        print("columns =", columns)
        print("filters =", filters)
        print("widget_type =", wtype)

        res = await execute_widget_query(
            conn, current_user.username, str(current_user.role), 
            schema, table, columns, filters,
            wtype, cc
        )
        await conn.execute(
            "UPDATE dashboard_widgets SET cached_data = $1, cached_at = CURRENT_TIMESTAMP WHERE id = $2",
            json.dumps(res, default=str), widget_id
        )
        return res



# ── SMART AUTOPOPULATE API (V2) ──

async def get_dashboard_limit(conn: asyncpg.Connection, plan_name: str) -> int:
    """Get dashboard limit from system_settings, fall back to defaults."""
    key = f"dashboard_limit_{plan_name}"
    row = await conn.fetchrow("SELECT value FROM system_settings WHERE key = $1", key)
    if row:
        try:
            return int(row["value"])
        except (ValueError, TypeError):
            pass
    # Defaults
    defaults = {"free": 0, "pro": 3, "enterprise": -1, "admin": -1}
    return defaults.get(plan_name, 2)


@router.get("/api/user/dashboard-studio/schemas")
async def api_get_schemas(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    """List all dataset schemas the user has access to."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT dr.dataset_schema, dr.dataset_display_name
            FROM dataset_registry dr
            ORDER BY dr.dataset_display_name ASC
            """
        )
        return [
            {"schema": r["dataset_schema"], "display_name": r["dataset_display_name"]}
            for r in rows
        ]


@router.get("/api/user/dashboard-studio/tables")
async def api_get_tables(
    request: Request,
    schema: str = Query(...),
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    """List all tables in a schema, respecting dataset_configs visibility."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1 AND table_type = 'BASE TABLE'
            ORDER BY table_name ASC
            """,
            schema,
        )
        table_names = [r["table_name"] for r in rows]

        # Filter out hidden tables
        visible_tables = []
        for tbl in table_names:
            cfg = await conn.fetchrow(
                "SELECT show_table_to_users FROM dataset_configs WHERE schema_name = $1 AND table_name = $2",
                schema, tbl,
            )
            if cfg and not cfg["show_table_to_users"]:
                continue
            visible_tables.append(tbl)

        return [{"table_name": t} for t in visible_tables]


@router.get("/api/user/dashboard-studio/columns")
async def api_get_columns(
    request: Request,
    schema: str = Query(...),
    table: str = Query(...),
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    """List all columns in a table, filtering out restricted columns based on RBAC."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        cols = await conn.fetch(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            """,
            schema, table,
        )
        if not cols:
            return []

        all_col_names = [c["column_name"] for c in cols]

        # Check column-level security
        try:
            allowed = await check_columns_and_filters(
                conn=conn,
                schema=schema,
                table=table,
                user_role=str(current_user.role),
                columns=all_col_names,
                filters=""
            )
        except Exception:
            allowed = all_col_names

        col_types = {c["column_name"]: c["data_type"] for c in cols}
        return [
            {"column_name": c, "data_type": col_types.get(c, "text")}
            for c in allowed
        ]


@router.get("/api/user/dashboard-studio/values")
async def api_get_distinct_values(
    request: Request,
    schema: str = Query(...),
    table: str = Query(...),
    column: str = Query(...),
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    """Get distinct values for a column (capped at 200 for filter dropdowns)."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        # Verify column exists
        col_check = await conn.fetchval(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
            """,
            schema, table, column,
        )
        if not col_check:
            return []

        try:
            rows = await conn.fetch(
                f'SELECT DISTINCT "{column}" AS val FROM "{schema}"."{table}" WHERE "{column}" IS NOT NULL ORDER BY "{column}" LIMIT 200'
            )
            return [{"value": str(r["val"])} for r in rows]
        except Exception:
            return []


@router.get("/api/user/dashboard-studio/limits")
async def api_get_dashboard_limits(
    request: Request,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    """Get dashboard creation limits for the current user based on system_settings."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        plan_limits = await get_and_enforce_plan_limits(conn, current_user.username, str(current_user.role))
        plan_name = plan_limits.get("plan", "free").lower()

        limit = await get_dashboard_limit(conn, plan_name)
        count = await conn.fetchval("SELECT COUNT(*) FROM dashboards WHERE user_email = $1", current_user.username) or 0

        return {
            "plan": plan_name,
            "limit": limit,
            "used": count,
            "remaining": max(0, limit - count) if limit >= 0 else -1,
            "unlimited": limit < 0,
        }


@router.get("/api/user/dashboard-studio/export/{dashboard_id}")
async def api_export_dashboard_csv(
    request: Request,
    dashboard_id: int,
    current_user: TokenData = Depends(get_current_active_user_with_role(["1", "2", "3"])),
):
    """Export all widget data from a dashboard as a JSON bundle for client-side CSV/PDF generation."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        db_row = await conn.fetchrow("SELECT * FROM dashboards WHERE id = $1 AND user_email = $2 AND NOT COALESCE(is_deleted, FALSE)", dashboard_id, current_user.username)
        if not db_row:
            raise HTTPException(status_code=404, detail="Dashboard not found or access denied.")

        widgets = await conn.fetch(
            "SELECT * FROM dashboard_widgets WHERE dashboard_id = $1 ORDER BY id",
            dashboard_id
        )

        export_data = {
            "dashboard_name": db_row["name"],
            "exported_at": datetime.utcnow().isoformat(),
            "widgets": []
        }

        for w in widgets:
            cached = w["cached_data"]
            if cached and isinstance(cached, str):
                cached = json.loads(cached)

            export_data["widgets"].append({
                "title": w["title"],
                "widget_type": w["widget_type"],
                "schema_name": w["schema_name"],
                "table_name": w["table_name"],
                "columns": w["columns"],
                "data": cached or {"columns": [], "rows": []}
            })

        return export_data
