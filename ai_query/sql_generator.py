import re
from typing import List, Dict, Any, Set

def clean_value_quotes(val: Any) -> str:
    """Sanitize values by escaping single quotes."""
    return str(val).replace("'", "''")

def get_simplified_type(db_type: str) -> str:
    if not db_type:
        return "TEXT"
    t = db_type.lower().strip()
    if t in ("text", "character varying", "character", "varchar", "char", "bpchar", "name"):
        return "TEXT"
    elif t in ("numeric", "decimal"):
        return "NUMERIC"
    elif t in ("integer", "bigint", "smallint", "int", "int2", "int4", "int8", "serial", "bigserial"):
        return "INTEGER"
    elif t in ("real", "double precision", "float", "float4", "float8"):
        return "FLOAT"
    elif "date" in t or "timestamp" in t or "time" in t:
        return "DATE"
    elif t in ("boolean", "bool"):
        return "BOOLEAN"
    else:
        return "TEXT"

def is_numeric_value(val_str: str) -> bool:
    if not val_str:
        return False
    s = str(val_str).strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1].strip()
    if not s:
        return False
    if s.startswith('-') or s.startswith('+'):
        s = s[1:]
    if '.' in s:
        parts = s.split('.')
        if len(parts) == 2:
            p0 = parts[0]
            p1 = parts[1]
            return (p0.isdigit() or p0 == '') and p1.isdigit()
        return False
    return s.isdigit()

def should_quote_val(col_type: str, val_str: str, was_cast: bool) -> bool:
    if col_type is None:
        return not is_numeric_value(val_str)
    if was_cast:
        return not is_numeric_value(val_str)
    if col_type in ("NUMERIC", "INTEGER", "FLOAT"):
        return not is_numeric_value(val_str)
    if col_type == "BOOLEAN":
        s = val_str.lower().strip()
        if s in ("true", "false", "1", "0", "t", "f"):
            return False
        return True
    return True

def format_value_literal(col_type: str, val_str: Any, was_cast: bool) -> str:
    cleaned = clean_value_quotes(val_str)
    if should_quote_val(col_type, cleaned, was_cast):
        return f"'{cleaned}'"
    else:
        if col_type == "BOOLEAN":
            s = cleaned.lower().strip()
            if s in ("true", "1", "t"):
                return "TRUE"
            elif s in ("false", "0", "f"):
                return "FALSE"
        return cleaned

def generate_sql_from_plan(
    schema: str,
    table: str,
    plan: Dict[str, Any],
    allowed_cols: List[str],
    raw_cols_with_labels: Set[str],
    col_types: Dict[str, str] = None
) -> str:
    """
    Translates a structured Query Plan dict into a safe SQL query string.
    """
    allowed_set = {c.lower(): c for c in allowed_cols}
    
    # 1. Build SELECT Projection based on query_type & target_columns
    query_type = plan.get("query_type") or "record_lookup"
    target_columns = plan.get("target_columns") or ["*"]
    
    col_parts = []
    
    # Construct base columns list matching requested targets (or all allowed if "*" or empty)
    requested_cols = []
    if not target_columns or "*" in target_columns:
        requested_cols = allowed_cols
    else:
        for c in target_columns:
            c_lower = c.lower()
            if c_lower in allowed_set:
                requested_cols.append(allowed_set[c_lower])
                
    # Build WHERE clause
    where_parts = []
    filters = plan.get("filters") or []
    for i, f in enumerate(filters):
        col_name = f.get("column")
        if not col_name:
            continue
        c_lower = col_name.lower()
        if c_lower not in allowed_set:
            continue
            
        actual_col = allowed_set[c_lower]
        op = f.get("operator") or "="
        val = f.get("value")
        logic = f.get("logic") or "AND"
        
        has_label_col = actual_col in raw_cols_with_labels
        col_ref = f'"{actual_col}_label"' if has_label_col else f'"{actual_col}"'
        
        # Determine column datatype
        col_type = None
        if col_types is not None:
            db_type = col_types.get(actual_col.lower())
            if db_type:
                col_type = get_simplified_type(db_type)
            else:
                col_type = "TEXT"
                
        # Check if this is a numeric operation on the column
        is_numeric_op = False
        if op.upper() in (">", "<", ">=", "<="):
            is_numeric_op = is_numeric_value(str(val))
        elif op.upper() == "BETWEEN":
            parts = re.split(r'\s+AND\s+', str(val), flags=re.IGNORECASE)
            if len(parts) == 2:
                p1_num = is_numeric_value(parts[0].strip())
                p2_num = is_numeric_value(parts[1].strip())
                is_numeric_op = p1_num or p2_num
            else:
                is_numeric_op = is_numeric_value(str(val))
        elif op.upper() in ("IN", "NOT IN"):
            val_str = str(val).strip().lstrip('(').rstrip(')')
            list_items = [item.strip().strip("'").strip('"') for item in val_str.split(',')]
            is_numeric_op = any(is_numeric_value(item) for item in list_items)
        elif op.upper() in ("=", "!="):
            is_numeric_op = is_numeric_value(str(val))
            
        # Determine if we should cast the TEXT column
        was_cast = False
        if col_types is not None and col_type == "TEXT" and is_numeric_op:
            col_expr = f"CAST(NULLIF({col_ref}, '-') AS NUMERIC)"
            was_cast = True
        else:
            col_expr = col_ref
            
        cond_str = ""
        if op.upper() == "BETWEEN":
            parts = re.split(r'\s+AND\s+', str(val), flags=re.IGNORECASE)
            if len(parts) == 2:
                p1 = parts[0].strip()
                p2 = parts[1].strip()
                p1_val = format_value_literal(col_type, p1, was_cast)
                p2_val = format_value_literal(col_type, p2, was_cast)
                cond_str = f'{col_expr} BETWEEN {p1_val} AND {p2_val}'
            else:
                p_val = format_value_literal(col_type, str(val), was_cast)
                cond_str = f'{col_expr} BETWEEN {p_val}'
        elif op.upper() in ("IN", "NOT IN"):
            val_str = str(val).strip().lstrip('(').rstrip(')')
            list_items = [item.strip().strip("'").strip('"') for item in val_str.split(',')]
            in_vals = []
            for item in list_items:
                in_vals.append(format_value_literal(col_type, item, was_cast))
            in_str = ", ".join(in_vals)
            cond_str = f'{col_expr} {op.upper()} ({in_str})'
        elif op.upper() == "LIKE":
            escaped_val = clean_value_quotes(val) if val is not None else ""
            cond_str = f'{col_expr}::text ILIKE \'%{escaped_val}%\''
        elif op.upper() == "IS NULL":
            cond_str = f'{col_expr} IS NULL'
        elif op.upper() == "IS NOT NULL":
            cond_str = f'{col_expr} IS NOT NULL'
        else:
            # =, !=, >, <, >=, <=
            val_str = format_value_literal(col_type, val, was_cast)
            cond_str = f'{col_expr} {op} {val_str}'
        
        if cond_str:
            if not where_parts:
                where_parts.append(cond_str)
            else:
                where_parts.append(f"{logic.upper()} {cond_str}")

    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " ".join(where_parts)

    # Build Group By and Aggregations
    group_by = plan.get("group_by") or []
    aggregations = plan.get("aggregations") or []
    
    gb_parts = []
    for gb in group_by:
        gb_lower = gb.lower()
        if gb_lower in allowed_set:
            actual_gb = allowed_set[gb_lower]
            gb_parts.append(actual_gb)

    # 1. Distinct Query Type
    if query_type == "distinct_values":
        # Target column is the first requested column or first allowed column
        target_col = requested_cols[0] if requested_cols else (allowed_cols[0] if allowed_cols else "*")
        if target_col != "*":
            if target_col in raw_cols_with_labels:
                proj_sql = f'DISTINCT "{target_col}_label" AS "{target_col}"'
            else:
                proj_sql = f'DISTINCT "{target_col}"'
        else:
            proj_sql = "DISTINCT *"
        sql = f'SELECT {proj_sql} FROM "{schema}"."{table}" {where_clause}'

    # 2. Count Query Type
    elif query_type == "count_records":
        sql = f'SELECT COUNT(*) FROM "{schema}"."{table}" {where_clause}'

    # 3. Aggregations (with or without GROUP BY)
    elif aggregations:
        agg_sql_parts = []
        for agg in aggregations:
            func = agg.get("function") or "COUNT"
            col = agg.get("column") or "*"
            if col == "*":
                agg_sql_parts.append(f'{func}(*) AS "{func.lower()}_all"')
            else:
                c_lower = col.lower()
                actual_col = allowed_set.get(c_lower, col)
                agg_sql_parts.append(f'{func}("{actual_col}") AS "{func.lower()}_{actual_col.lower()}"')
        
        if gb_parts:
            # GROUP BY Select Projection
            gb_select_parts = []
            for col in gb_parts:
                if col in raw_cols_with_labels:
                    gb_select_parts.append(f'"{col}_label" AS "{col}"')
                else:
                    gb_select_parts.append(f'"{col}"')
            
            proj_sql = ", ".join(gb_select_parts + agg_sql_parts)
            
            # Group by expression must use the label columns if that's what we select
            gb_exp_parts = []
            for col in gb_parts:
                if col in raw_cols_with_labels:
                    gb_exp_parts.append(f'"{col}_label"')
                else:
                    gb_exp_parts.append(f'"{col}"')
            gb_clause = "GROUP BY " + ", ".join(gb_exp_parts)
            sql = f'SELECT {proj_sql} FROM "{schema}"."{table}" {where_clause} {gb_clause}'
        else:
            proj_sql = ", ".join(agg_sql_parts)
            sql = f'SELECT {proj_sql} FROM "{schema}"."{table}" {where_clause}'
            
    # 4. Standard Record lookup / show columns
    else:
        proj_parts = []
        for c in requested_cols:
            if c in raw_cols_with_labels:
                proj_parts.append(f'"{c}_label" AS "{c}"')
            else:
                proj_parts.append(f'"{c}"')
        proj_sql = ", ".join(proj_parts) if proj_parts else "*"
        sql = f'SELECT {proj_sql} FROM "{schema}"."{table}" {where_clause}'

    # Build ORDER BY Clause (Sorting)
    sorting = plan.get("sorting") or []
    if sorting:
        sort_parts = []
        for s in sorting:
            col_name = s.get("column")
            direction = s.get("direction") or "ASC"
            
            if col_name:
                c_lower = col_name.lower()
                # Check if it is an aggregate function sorting like AVG(Income)
                agg_match = re.match(r'^(avg|sum|count|min|max)\((.+?)\)$', c_lower, re.IGNORECASE)
                if agg_match:
                    func = agg_match.group(1).upper()
                    inner_col = agg_match.group(2).strip()
                    if inner_col != "*":
                        actual_inner = allowed_set.get(inner_col.lower(), inner_col)
                        sort_parts.append(f'{func}("{actual_inner}") {direction}')
                    else:
                        sort_parts.append(f'{func}(*) {direction}')
                else:
                    actual_col = allowed_set.get(c_lower, col_name)
                    if actual_col in raw_cols_with_labels:
                        sort_parts.append(f'"{actual_col}_label" {direction}')
                    else:
                        sort_parts.append(f'"{actual_col}" {direction}')
            else:
                # If column name is omitted but sorting is present, default to the first aggregate or first col
                if aggregations:
                    func = aggregations[0].get("function") or "COUNT"
                    col = aggregations[0].get("column") or "*"
                    if col == "*":
                        sort_parts.append(f'{func}(*) {direction}')
                    else:
                        actual_col = allowed_set.get(col.lower(), col)
                        sort_parts.append(f'{func}("{actual_col}") {direction}')
                elif requested_cols:
                    sort_parts.append(f'"{requested_cols[0]}" {direction}')
                else:
                    sort_parts.append(f'1 {direction}')
                    
        if sort_parts:
            sql += " ORDER BY " + ", ".join(sort_parts)

    # Build LIMIT Clause (default limit 200, cap at 1000)
    limit = plan.get("limit") or 200
    try:
        limit_val = min(int(limit), 1000)
    except (ValueError, TypeError):
        limit_val = 200
        
    # DISTINCT and record lookups usually get LIMIT, COUNT does not
    if query_type != "count_records" and not (aggregations and not gb_parts):
        sql += f" LIMIT {limit_val}"

    # Print detected column types if col_types is provided
    if col_types:
        filters = plan.get("filters") or []
        filter_cols = []
        for f in filters:
            col_name = f.get("column")
            if col_name:
                c_lower = col_name.lower()
                if c_lower in allowed_set:
                    actual_col = allowed_set[c_lower]
                    filter_cols.append(actual_col)
        if filter_cols:
            print("Detected Column Type:")
            for col in filter_cols:
                db_type = col_types.get(col.lower(), "text")
                simplified = get_simplified_type(db_type)
                print(f"{col} -> {simplified}")
            print()
            
    print("Generated SQL:")
    print(sql)

    return sql
