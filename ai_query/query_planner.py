import os
import json
import re
import asyncio
import httpx
from typing import Optional, List, Dict, Any
from difflib import SequenceMatcher

LM_STUDIO_URL = os.getenv("LM_STUDIO_URL", "http://127.0.0.1:1234")
cached_model_name = None

async def get_model_name() -> str:
    global cached_model_name
    if cached_model_name:
        return cached_model_name
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{LM_STUDIO_URL}/v1/models")
            if resp.status_code == 200:
                data = resp.json()
                models = data.get("data", [])
                if models:
                    for m in models:
                        m_id = m.get("id")
                        if m_id and "llama" in m_id.lower():
                            cached_model_name = m_id
                            return cached_model_name
                    m_id = models[0].get("id")
                    if m_id:
                        cached_model_name = m_id
                        return cached_model_name
    except Exception as e:
        print(f"Error auto-detecting model name from {LM_STUDIO_URL}/v1/models: {e}")
    return "meta-llama-3.1-8b-instruct"

async def fetch_column_distinct_values(conn, schema: str, table: str, col: str) -> List[Any]:
    """Query up to 10 distinct values for a column from the database."""
    try:
        rows = await conn.fetch(
            f'SELECT DISTINCT "{col}" FROM "{schema}"."{table}" WHERE "{col}" IS NOT NULL LIMIT 10'
        )
        return [r[col] for r in rows]
    except Exception as e:
        print(f"Error fetching distinct values for {schema}.{table}.{col}: {e}")
        return []

def select_columns_to_profile(question: str, visible_cols: List[str], labels: Dict[str, str] = None) -> List[str]:
    """Identify columns mentioned in the question and prioritize them."""
    if labels is None:
        labels = {}
    words = re.findall(r'\b\w+\b', question.lower())
    words_set = set(words)
    
    prioritized = []
    others = []
    for col in visible_cols:
        col_lower = col.lower()
        label_lower = labels.get(col_lower, "").lower()
        
        matched = False
        if col_lower in words_set:
            matched = True
        else:
            for w in words_set:
                if len(w) > 3 and (w in col_lower or col_lower in w):
                    matched = True
                    break
            if not matched and label_lower:
                for w in words_set:
                    if len(w) > 3 and (w in label_lower or label_lower in w):
                        matched = True
                        break
                        
        if matched:
            prioritized.append(col)
        else:
            others.append(col)
            
    return prioritized + others

async def collect_metadata(conn, schema: str, table: str, visible_cols: List[str], question: str = "") -> Dict[str, Any]:
    """
    Collect table name, columns with data types, DDI labels, and up to 10 distinct values for each.
    Prioritizes columns referenced in the query, profiling up to 50 columns.
    """
    db_types = {}
    try:
        rows = await conn.fetch(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = $1 AND table_name = $2
            """,
            schema, table
        )
        for r in rows:
            db_types[r["column_name"].lower()] = r["data_type"]
    except Exception as e:
        print(f"Error fetching column db types in collect_metadata: {e}")

    # Fetch DDI labels
    labels = {}
    try:
        has_variables = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = $1 AND table_name = 'variables'
            )
            """,
            schema
        )
        if has_variables:
            var_cols = await conn.fetch(
                """
                SELECT column_name FROM information_schema.columns 
                WHERE table_schema = $1 AND table_name = 'variables'
                """,
                schema
            )
            var_col_names = {c["column_name"].lower() for c in var_cols}
            id_col = "variable_name" if "variable_name" in var_col_names else ("column_name" if "column_name" in var_col_names else None)
            if id_col:
                var_rows = await conn.fetch(
                    f"""
                    SELECT "{id_col}" AS variable_name, label
                    FROM "{schema}".variables
                    WHERE table_name = $1
                    """,
                    table
                )
            else:
                var_rows = []
        else:
            has_dict = await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_name = 'variable_dictionary'
                )
                """,
                schema
            )
            if has_dict:
                var_cols = await conn.fetch(
                    """
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_schema = $1 AND table_name = 'variable_dictionary'
                    """,
                    schema
                )
                var_col_names = {c["column_name"].lower() for c in var_cols}
                id_col = "variable_name" if "variable_name" in var_col_names else ("column_name" if "column_name" in var_col_names else None)
                if id_col:
                    var_rows = await conn.fetch(
                        f"""
                        SELECT "{id_col}" AS variable_name, label
                        FROM "{schema}".variable_dictionary
                        WHERE table_name = $1
                        """,
                        table
                    )
                else:
                    var_rows = []
            else:
                var_rows = []
        for r in var_rows:
            vname = r.get("variable_name") or r.get("column_name")
            if vname:
                labels[vname.lower()] = r.get("label") or ""
    except Exception as e:
        print(f"Error fetching variable DDI labels: {e}")

    # Prioritize columns mentioned in the question
    profile_colnames = select_columns_to_profile(question, visible_cols, labels)
    
    # We fetch distinct sample values for the top 50 columns
    cols_to_profile = []
    for col in profile_colnames[:50]:
        col_lower = col.lower()
        if col_lower in db_types:
            cols_to_profile.append((col, db_types[col_lower]))

    # Fetch distinct sample values sequentially (asyncpg connection is not concurrent-safe)
    results = []
    for col in cols_to_profile:
        vals = await fetch_column_distinct_values(conn, schema, table, col[0])
        results.append(vals)

    # Format output metadata context (up to 50 columns)
    columns_meta = []
    for col in profile_colnames[:50]:
        col_lower = col.lower()
        if col_lower in db_types:
            columns_meta.append({
                "name": col,
                "type": db_types[col_lower],
                "label": labels.get(col_lower, "")
            })
            
    # Sample values for the profiled columns
    samples = {}
    for (col_name, col_type), vals in zip(cols_to_profile, results):
        samples[col_name] = [str(v) for v in vals]

    return {
        "table": table,
        "columns": columns_meta,
        "sample_values": samples
    }

PLANNER_SYSTEM_PROMPT = """You are a database query planner.
Given a user's natural language question and the metadata of the target table (including columns, types, DDI labels, and sample values), you must generate a structured Query Plan in JSON format.

Your output must be ONLY a valid JSON object. Do not include markdown code block syntax (such as ```json) or explanations outside the JSON. Do not generate SQL.

Supported query_type values:
- "record_lookup" (standard row retrieval matching filter criteria)
- "distinct_values" (retrieving unique values of one or more columns)
- "count_records" (counting matching records)
- "aggregation" (performing AVG, SUM, MIN, MAX without GROUP BY)
- "group_by" (aggregations grouped by one or more columns)
- "ranking" (retrieving ordered elements, e.g. top N)
- "comparison" (comparing values across categories)
- "distribution" (frequency distribution of variables)
- "exists_check" (checking if a condition exists)
- "column_discovery" (identifying columns)
- "metadata_lookup" (metadata or column details lookup)

Supported operators in filters:
- "=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "BETWEEN", "LIKE", "IS NULL", "IS NOT NULL"

Expected JSON Schema:
{
  "query_type": "record_lookup" | "distinct_values" | "count_records" | "aggregation" | "group_by" | "ranking" | "comparison" | "distribution" | "exists_check" | "column_discovery" | "metadata_lookup",
  "target_columns": ["column_name"] or ["*"],
  "filters": [
    {
      "column": "column_name",
      "operator": "=" | "!=" | ">" | "<" | ">=" | "<=" | "IN" | "NOT IN" | "BETWEEN" | "LIKE" | "IS NULL" | "IS NOT NULL",
      "value": "value_or_expression" | null,
      "logic": "AND" | "OR"
    }
  ],
  "group_by": ["column_name"],
  "aggregations": [
    {
      "function": "AVG" | "COUNT" | "SUM" | "MIN" | "MAX",
      "column": "column_name"
    }
  ],
  "sorting": [
    {
      "column": "column_name",
      "direction": "ASC" | "DESC"
    }
  ],
  "limit": 100
}

Instructions:
1. ONLY use columns that exist in the provided columns metadata list. Never invent columns.
2. Examine the "sample_values" to map filter conditions to actual values exactly.
3. Logical term mappings:
   - "AND": Set "logic": "AND" (or omit since it's the default). Add separate filter objects.
   - "OR" (e.g. "Sector is Urban or State is Karnataka"): You must set "logic": "OR" on the filter object that is combined via OR. Example: [{"column": "Sector", "operator": "=", "value": "Urban"}, {"column": "State", "operator": "=", "value": "Karnataka", "logic": "OR"}]
   - "NOT", "EXCEPT", "EXCLUDING", "OTHER THAN": Map these concepts to "!=" or "NOT IN" operators.
   - "IS NULL", "IS NOT NULL" (for missing/empty/not empty/exists): Use the appropriate operator (with value: null).
4. If the user question requires columns or concepts not present in the table, return a validation failure JSON:
{
  "success": false,
  "error": "Column not found"
}
"""

def normalize_plan_value(value: Any, sample_vals: List[str]) -> str:
    """Case-insensitive and fuzzy match against sample distinct values."""
    if not sample_vals or value is None:
        return str(value)
    
    val_str = str(value).strip()
    val_lower = val_str.lower()
    
    # 1. Try case-insensitive exact match
    for sv in sample_vals:
        if sv.lower().strip() == val_lower:
            return sv
            
    # 2. Try fuzzy matching
    best_match = None
    best_score = 0.0
    for sv in sample_vals:
        score = SequenceMatcher(None, val_lower, sv.lower().strip()).ratio()
        if score > best_score and score >= 0.6:
            best_match = sv
            best_score = score
            
    if best_match:
        return best_match
    return val_str

def clean_json_comments(content: str) -> str:
    # Remove single line comments starting with // (avoiding URL colons)
    content = re.sub(r'(?<!:)\/\/.*$', '', content, flags=re.MULTILINE)
    # Strip /* ... */ block comments
    content = re.sub(r'\/\*.*?\*\/', '', content, flags=re.DOTALL)
    return content

async def generate_query_plan(conn, prompt: str, schema: str, table: str, visible_cols: List[str]) -> Dict[str, Any]:
    """
    1. Collect metadata & sample values.
    2. Prompt LM Studio to generate a plan JSON.
    3. Validate and normalize the plan.
    """
    # 1. Fetch metadata context
    meta = await collect_metadata(conn, schema, table, visible_cols, prompt)
    
    # 2. Build model prompt context
    user_payload = {
        "metadata": meta,
        "question": prompt
    }
    
    model = await get_model_name()
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": PLANNER_SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(user_payload, indent=2)}
        ],
        "temperature": 0.1,
        "max_tokens": 500
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(f"{LM_STUDIO_URL}/v1/chat/completions", json=payload)
            if response.status_code != 200:
                raise Exception(f"LM Studio status: {response.status_code}")
            
            result = response.json()
            content = result["choices"][0]["message"]["content"].strip()
            print("RAW LM Studio Response content:", content)
            
            # Clean Markdown if needed
            if "```" in content:
                m = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', content, re.DOTALL)
                if m:
                    content = m.group(1)
                else:
                    content = content.replace("```json", "").replace("```", "").strip()
            
            content = clean_json_comments(content)
            plan = json.loads(content)
            
            # V1 to V2 schema translation for robustness
            if isinstance(plan, dict):
                if "query_type" not in plan and "intent" in plan:
                    plan["query_type"] = plan["intent"]
                if "target_columns" not in plan and "columns" in plan:
                    plan["target_columns"] = plan["columns"]
                if "aggregations" not in plan:
                    metric = plan.get("metric") or plan.get("aggregation")
                    if metric and metric != "none":
                        func_map = {
                            "count": "COUNT",
                            "avg": "AVG",
                            "average": "AVG",
                            "sum": "SUM",
                            "min": "MIN",
                            "max": "MAX"
                        }
                        func = func_map.get(str(metric).lower(), "COUNT")
                        agg_col = "*"
                        cols = plan.get("columns") or plan.get("target_columns") or []
                        if func != "COUNT" and cols:
                            group_by_set = set(g.lower() for g in plan.get("group_by", []))
                            for c in cols:
                                if c.lower() not in group_by_set:
                                    agg_col = c
                                    break
                            if agg_col == "*":
                                agg_col = cols[0]
                        plan["aggregations"] = [{"function": func, "column": agg_col}]
                
                # Convert filters dict to V2 list of dicts if needed
                if "filters" in plan and isinstance(plan["filters"], dict):
                    v2_filters = []
                    for col_key, val_val in plan["filters"].items():
                        v2_filters.append({
                            "column": col_key,
                            "operator": "=",
                            "value": val_val
                        })
                    plan["filters"] = v2_filters
            
            # Check for validation failure in plan
            if plan.get("success") is False:
                return {
                    "success": False,
                    "error": plan.get("error") or plan.get("message") or "Column not found"
                }
            
            # 3. Validate column names
            visible_lower = {c.lower(): c for c in visible_cols}
            
            # Validate target columns
            target_cols = plan.get("target_columns") or ["*"]
            if not isinstance(target_cols, list):
                target_cols = [target_cols]
            
            cleaned_targets = []
            for col in target_cols:
                if col == "*":
                    cleaned_targets.append("*")
                else:
                    col_lower = col.lower()
                    if col_lower not in visible_lower:
                        return {"success": False, "error": f"Column not found: {col}"}
                    cleaned_targets.append(visible_lower[col_lower])
            
            # Validate filters columns and normalize values
            filters = plan.get("filters") or []
            cleaned_filters = []
            for f in filters:
                col_name = f.get("column")
                if not col_name:
                    continue
                col_lower = col_name.lower()
                if col_lower not in visible_lower:
                    return {"success": False, "error": f"Column not found: {col_name}"}
                
                # Normalize values using sample profiling
                actual_col = visible_lower[col_lower]
                val = f.get("value")
                samples = meta["sample_values"].get(actual_col, [])
                normalized_val = normalize_plan_value(val, samples)
                
                cleaned_filters.append({
                    "column": actual_col,
                    "operator": f.get("operator") or "=",
                    "value": normalized_val,
                    "logic": f.get("logic") or "AND"
                })
            
            # Validate group by
            group_by = plan.get("group_by") or []
            cleaned_groupby = []
            for col in group_by:
                col_lower = col.lower()
                if col_lower not in visible_lower:
                    return {"success": False, "error": f"Column not found: {col}"}
                cleaned_groupby.append(visible_lower[col_lower])
                
            # Validate aggregations
            aggregations = plan.get("aggregations") or []
            cleaned_aggregations = []
            for agg in aggregations:
                col_name = agg.get("column")
                if col_name and col_name != "*":
                    col_lower = col_name.lower()
                    if col_lower not in visible_lower:
                        return {"success": False, "error": f"Column not found: {col_name}"}
                    agg_col = visible_lower[col_lower]
                else:
                    agg_col = "*"
                cleaned_aggregations.append({
                    "function": agg.get("function") or "COUNT",
                    "column": agg_col
                })
                
            # Validate sorting
            sorting = plan.get("sorting") or []
            cleaned_sorting = []
            for sort in sorting:
                col_name = sort.get("column")
                if col_name:
                    col_lower = col_name.lower()
                    # It might sort on an aggregated expression like AVG(Income), check that
                    agg_match = re.match(r'^(avg|sum|count|min|max)\((.+?)\)$', col_lower, re.IGNORECASE)
                    if agg_match:
                        func = agg_match.group(1).upper()
                        inner_col = agg_match.group(2).strip()
                        if inner_col != "*":
                            if inner_col.lower() not in visible_lower:
                                return {"success": False, "error": f"Column not found: {inner_col}"}
                            inner_col = visible_lower[inner_col.lower()]
                        sort_col = f"{func}({inner_col})"
                    else:
                        if col_lower not in visible_lower:
                            return {"success": False, "error": f"Column not found: {col_name}"}
                        sort_col = visible_lower[col_lower]
                else:
                    sort_col = None
                    
                cleaned_sorting.append({
                    "column": sort_col,
                    "direction": sort.get("direction") or "ASC"
                })
            
            return {
                "success": True,
                "query_type": plan.get("query_type") or "record_lookup",
                "target_columns": cleaned_targets,
                "filters": cleaned_filters,
                "group_by": cleaned_groupby,
                "aggregations": cleaned_aggregations,
                "sorting": cleaned_sorting,
                "limit": plan.get("limit") or 100,
                "explanation": plan.get("explanation") or "",
                "parsed_json": plan,
                "raw_response": result
            }
            
    except Exception as e:
        print(f"Error generating query plan in query_planner: {e}")
        return {
            "success": False,
            "error": "Plan generation failed"
        }
