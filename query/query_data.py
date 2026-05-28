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

# Central Security module imports
from security import check_user_access
from security.privacy_guard import check_columns_and_filters, apply_privacy_and_labeling
from security.usage_tracker import log_api_usage

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

    user_role = str(current_user.role)
    if user_role != "1":
        raise HTTPException(status_code=403, detail="Direct SQL execution is restricted to admins")

    async with pool.acquire() as conn:
        # Screen SQL using central detector
        await check_user_access(
            conn=conn,
            user=current_user,
            action_type="query",
            filters=sql
        )

        try:
            stmt = await conn.prepare(sql)
            records = await stmt.fetch()
            rows = [dict(record) for record in records]
            row_count = len(rows)

            # Suppress if user is not admin and result is < 5
            if user_role != "1" and row_count < 5:
                await log_api_usage(conn, current_user.username, "/query", "unknown", "direct_query", 0, 0)
                raise HTTPException(status_code=403, detail="Data suppressed (less than 5 rows)")

            await log_api_usage(
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
    format: Optional[str] = None,
    current_user=Depends(get_current_user),
):
    pool: asyncpg.Pool = request.app.state.db
    user_role = str(current_user.role)

    async with pool.acquire() as conn:
        # Resolve correct schema for table_name dynamically
        schema_row = await conn.fetchrow(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables 
            WHERE LOWER(table_name) = LOWER($1) 
            LIMIT 1
            """,
            table_name
        )
        if schema_row:
            schema_name = schema_row["table_schema"]
            table_name = schema_row["table_name"]
        else:
            schema_name = await conn.fetchval("SELECT current_schema()") or "public"

        # Validate that the schema exists in the database
        schema_ok = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
            schema_name
        )
        if not schema_ok:
            schema_name = await conn.fetchval("SELECT current_schema()") or "public"

        accept = request.headers.get("accept", "")
        is_export = "text/csv" in accept or (format in ["csv", "excel", "pdf", "json"])
        action_type = "export" if is_export else "query"
        export_fmt = (format.lower() if format else "csv") if is_export else None

        try:
            # Enforce all security limits using Central Security Layer
            await check_user_access(
                conn=conn,
                user=current_user,
                action_type=action_type,
                schema_name=schema_name,
                table_name=table_name,
                rows_requested=limit,
                export_type=export_fmt,
                filters=filters,
                limit=limit
            )

            # Additional check for download limits if exporting
            if is_export:
                from security.plan_enforcer import get_and_enforce_plan_limits
                from security.usage_tracker import check_download_limits
                plan_limits = await get_and_enforce_plan_limits(conn, current_user.username, user_role)
                await check_download_limits(conn, current_user.username, plan_limits)

            # Discover label columns for substitution
            actual_columns = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
                schema_name,
                table_name
            )
            all_cols = [c["column_name"] for c in actual_columns]
            label_cols = set(c for c in all_cols if c.endswith("_label"))
            raw_cols_with_labels = set(c[:-6] for c in label_cols)

            # Filter out disallowed columns via central privacy guard
            requested_cols = [c.strip() for c in (columns.split(",") if columns else all_cols) if c.strip()]
            allowed_cols = await check_columns_and_filters(
                conn=conn,
                schema=schema_name,
                table=table_name,
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

            sql = f'SELECT {col_sql} FROM "{schema_name}"."{table_name}" {where_clause} LIMIT {limit} OFFSET {offset}'

            try:
                rows = await conn.fetch(sql)
                row_count = len(rows)
            except asyncpg.UndefinedTableError:
                raise HTTPException(status_code=404, detail="Table not found")
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Query failed: {e}")

            data = [dict(row) for row in rows]
            
            # Enforce cell suppression and value mapping via privacy guard
            try:
                label_map = await get_column_labels(conn, table_name, schema=schema_name)
                labels = {c: (label_map.get(c) or {}) for c in allowed_cols}
            except Exception:
                labels = {}

            filtered_data = await apply_privacy_and_labeling(
                conn=conn,
                schema=schema_name,
                table=table_name,
                user_role=user_role,
                columns=allowed_cols,
                rows=data,
                labels=labels
            )

            # Log as completed
            await log_api_usage(
                conn, current_user.username,
                f"/datasets/{table_name}/query", schema_name, table_name,
                len(filtered_data), len(json.dumps(filtered_data, default=str).encode()),
                status="completed",
                filters=filters
            )

            if is_export:
                # Log file download details to database
                from security.usage_tracker import log_file_download
                import io

                filename = f"{table_name}_query.{export_fmt}"
                if export_fmt == "csv":
                    output = io.StringIO()
                    writer = csv.DictWriter(output, fieldnames=filtered_data[0].keys() if filtered_data else [])
                    writer.writeheader()
                    writer.writerows(filtered_data)
                    output.seek(0)
                    content = output.getvalue().encode("utf-8")
                    media_type = "text/csv"
                elif export_fmt == "json":
                    content = json.dumps(filtered_data, default=str, indent=2).encode("utf-8")
                    media_type = "application/json"
                elif export_fmt == "excel":
                    import pandas as pd
                    df = pd.DataFrame(filtered_data)
                    output = io.BytesIO()
                    df.to_excel(output, index=False, engine='openpyxl')
                    output.seek(0)
                    content = output.getvalue()
                    media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    filename = f"{table_name}_query.xlsx"
                elif export_fmt == "pdf":
                    from reportlab.lib.pagesizes import letter, landscape
                    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
                    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                    from reportlab.lib import colors

                    output = io.BytesIO()
                    doc = SimpleDocTemplate(output, pagesize=landscape(letter), rightMargin=20, leftMargin=20, topMargin=20, bottomMargin=20)
                    elements = []

                    styles = getSampleStyleSheet()
                    title_style = ParagraphStyle(
                        'TitleStyle',
                        parent=styles['Heading1'],
                        fontSize=14,
                        spaceAfter=10
                    )
                    cell_style = ParagraphStyle(
                        'CellStyle',
                        parent=styles['Normal'],
                        fontSize=7,
                        leading=9
                    )
                    header_style = ParagraphStyle(
                        'HeaderStyle',
                        parent=styles['Normal'],
                        fontSize=8,
                        leading=10,
                        textColor=colors.whitesmoke,
                        fontName='Helvetica-Bold'
                    )

                    elements.append(Paragraph(f"Dataset Export: {table_name} (Schema: {schema_name})", title_style))
                    elements.append(Spacer(1, 10))

                    if filtered_data:
                        headers = list(filtered_data[0].keys())
                        headers_subset = headers[:12]

                        table_data = []
                        table_data.append([Paragraph(h, header_style) for h in headers_subset])

                        for row in filtered_data[:100]:
                            row_cells = []
                            for h in headers_subset:
                                val = str(row.get(h, ""))
                                if len(val) > 40:
                                    val = val[:37] + "..."
                                row_cells.append(Paragraph(val, cell_style))
                            table_data.append(row_cells)

                        col_width = 750 / len(headers_subset)
                        t = Table(table_data, colWidths=[col_width]*len(headers_subset))
                        t.setStyle(TableStyle([
                            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#2E5BBA')),
                            ('ALIGN', (0,0), (-1,-1), 'LEFT'),
                            ('VALIGN', (0,0), (-1,-1), 'TOP'),
                            ('INNERGRID', (0,0), (-1,-1), 0.5, colors.grey),
                            ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#2E5BBA')),
                            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#F5F7FA')]),
                            ('TOPPADDING', (0,0), (-1,-1), 4),
                            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
                        ]))
                        elements.append(t)

                        if len(filtered_data) > 100:
                            elements.append(Spacer(1, 10))
                            elements.append(Paragraph(f"... and {len(filtered_data) - 100} more rows (total {len(filtered_data)} rows exported)", styles['Italic']))
                    else:
                        elements.append(Paragraph("No data found.", styles['Normal']))

                    doc.build(elements)
                    content = output.getvalue()
                    media_type = "application/pdf"

                size_bytes = len(content)
                await log_file_download(
                    conn=conn,
                    file_name=filename,
                    user_email=current_user.username,
                    size_bytes=size_bytes,
                    dataset_schema=schema_name,
                    export_format=export_fmt,
                    rows_exported=len(filtered_data),
                    status="success"
                )

                return StreamingResponse(
                    io.BytesIO(content),
                    media_type=media_type,
                    headers={"Content-Disposition": f"attachment; filename={filename}"}
                )

            return JSONResponse(content=filtered_data)

        except HTTPException as he:
            # Connected status mapping
            status = "failed"
            detail_lower = str(he.detail).lower()
            if he.status_code == 429:
                status = "rate_limited"
            elif he.status_code == 403:
                if "suppress" in detail_lower:
                    status = "suppressed"
                elif "suspicious" in detail_lower or "security" in detail_lower:
                    status = "suspicious"
                else:
                    status = "blocked"
            elif "suppress" in detail_lower:
                status = "suppressed"

            # Check suspicious score
            try:
                user_info = await conn.fetchrow("SELECT suspicious_score FROM users WHERE email = $1", current_user.username)
                if user_info and user_info["suspicious_score"] > 80:
                    status = "suspicious"
            except Exception:
                pass

            # Log to usage_logs
            await log_api_usage(
                conn, current_user.username,
                f"/datasets/{table_name}/query", schema_name, table_name,
                0, 0, status=status, filters=filters
            )
            if is_export:
                from security.usage_tracker import log_file_download
                await log_file_download(
                    conn=conn,
                    file_name=f"{table_name}_query.{export_fmt}",
                    user_email=current_user.username,
                    size_bytes=0,
                    dataset_schema=schema_name,
                    export_format=export_fmt,
                    rows_exported=0,
                    status=status
                )
            raise he
        except Exception as e:
            await log_api_usage(
                conn, current_user.username,
                f"/datasets/{table_name}/query", schema_name, table_name,
                0, 0, status="failed", filters=filters
            )
            if is_export:
                from security.usage_tracker import log_file_download
                await log_file_download(
                    conn=conn,
                    file_name=f"{table_name}_query.{export_fmt}",
                    user_email=current_user.username,
                    size_bytes=0,
                    dataset_schema=schema_name,
                    export_format=export_fmt,
                    rows_exported=0,
                    status="failed"
                )
            raise HTTPException(status_code=400, detail=f"Query failed: {e}")

def is_aggregation_query(sql: str) -> bool:
    """Basic check for SQL aggregation queries."""
    sql_upper = sql.upper()
    return any(func in sql_upper for func in ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX("])

async def log_usage(conn, user_email, endpoint, schema, table, row_count, bytes_sent):
    """Legacy wrapper delegating to centralized usage_tracker log_api_usage."""
    await log_api_usage(conn, user_email, endpoint, schema, table, row_count, bytes_sent)
