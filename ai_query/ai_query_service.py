"""
ai_query_service.py
Isolated service module to interpret natural language questions into structured query plans.
Uses the local LM Studio model integration (auto-detecting loaded model).
"""

import os
import json
import re
import httpx
from typing import Optional, List, Dict, Any

LM_STUDIO_URL = os.getenv("LM_STUDIO_URL", "http://127.0.0.1:1234")
cached_model_name = None

# Prompt Template
PROMPT_TEMPLATE = """You are a data analyst assistant for a database.
Given a user's natural language question, you must analyze it and return a structured query plan in JSON format.
Only use columns that exist in the table. Never invent or guess column names.
If a requested column or criteria does not exist in the table, say so by returning:
{{
  "success": false,
  "message": "Column not found"
}}

Table: {table}

Columns:
{columns_context}

User Question: "{question}"

Determine:
1. User Intent (Must be exactly one of: "distinct_values", "filter_records", "list_records", "count_records", "aggregate", "group_by", "show_columns")
2. Required Columns (Only columns present in the table columns list above)
3. Filter Conditions (List of column and value mapping objects)
4. Target Column (Set "column" value to the specific column if intent is "distinct_values" or focusing on a single column)

Return a JSON object only. Do not include markdown code block syntax (like ```json) or explanations outside the JSON.

Expected JSON schema:
{{
  "success": true,
  "intent": "distinct_values" | "filter_records" | "list_records" | "count_records" | "aggregate" | "group_by" | "show_columns",
  "column": "column_name",
  "columns": ["column1", "column2"],
  "filters": [
    {{
      "column": "column1",
      "value": "value"
    }}
  ],
  "group_by": ["column3"],
  "metric": "count" | "avg" | "sum" | "min" | "max" | "none",
  "confidence": <confidence_score_between_0_and_1>,
  "explanation": "Short interpretation of what query does"
}}
"""

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
                    # Look for Llama or similar case-insensitive match
                    for m in models:
                        m_id = m.get("id")
                        if m_id and "llama" in m_id.lower():
                            cached_model_name = m_id
                            return cached_model_name
                    # Otherwise, use the first available model
                    m_id = models[0].get("id")
                    if m_id:
                        cached_model_name = m_id
                        return cached_model_name
    except Exception as e:
        print(f"Error auto-detecting model name from {LM_STUDIO_URL}/v1/models: {e}")
        
    return "meta-llama-3.1-8b-instruct"

async def fetch_table_metadata(conn, schema: str, table: str, visible_cols: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Queries variables and categories tables in the given schema/dataset to provide
    context to the AI service, alongside actual DB column types.
    """
    variables_info = {}
    
    # Fetch database data types from information_schema.columns
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
        print(f"Error fetching columns db types: {e}")

    try:
        # Check if variables table exists in schema
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
            rows = await conn.fetch(
                f"""
                SELECT column_name AS variable_name, label, ddi_type, question_text
                FROM "{schema}".variables
                WHERE table_name = $1
                """,
                table
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT variable_name, label, ddi_type
                FROM "{schema}".variable_dictionary
                WHERE table_name = $1
                """,
                table
            )
        for r in rows:
            vname = r.get("variable_name")
            if vname:
                variables_info[vname.lower()] = {
                    "label": r.get("label") or "",
                    "ddi_type": r.get("ddi_type") or "",
                    "question_text": r.get("question_text") or ""
                }
    except Exception as e:
        print(f"Error fetching variables metadata: {e}")

    categories_info = {}
    try:
        cat_rows = await conn.fetch(
            f"""
            SELECT variable_name, value, label
            FROM "{schema}".variable_categories
            WHERE table_name = $1
            """,
            table
        )
        for r in cat_rows:
            vname = r["variable_name"].lower()
            if vname not in categories_info:
                categories_info[vname] = []
            categories_info[vname].append(f"{r['value']} = {r['label']}")
    except Exception as e:
        print(f"Error fetching variable categories: {e}")

    metadata = {}
    for col in visible_cols:
        col_lower = col.lower()
        var_meta = variables_info.get(col_lower, {})
        cats = categories_info.get(col_lower, [])
        db_type = db_types.get(col_lower) or var_meta.get("ddi_type") or "unknown"
        metadata[col] = {
            "label": var_meta.get("label", ""),
            "type": db_type,
            "question_text": var_meta.get("question_text", ""),
            "categories": cats
        }
    return metadata

async def interpret_query_with_ai(conn, question: str, schema: str, table: str, visible_cols: List[str]) -> Dict[str, Any]:
    """
    Sends natural language prompt and schema details to the query planner and retrieves plan.
    Falls back to rule-based NLP parser on failure.
    """
    from .query_planner import generate_query_plan
    from .nlp_engine import NLPQueryEngine

    # 1. Try metadata-aware query planner
    try:
        plan = await generate_query_plan(conn, question, schema, table, visible_cols)
        if plan.get("success"):
            # Provide aliases for backward compatibility (V1 tests)
            mapped_intent = plan.get("query_type") or "record_lookup"
            # Extract metric from aggregations
            metric = "none"
            if plan.get("aggregations"):
                metric = plan["aggregations"][0].get("function", "count").lower()
            
            # Convert V2 filters list to dict for V1 compatibility
            filters_dict = {}
            for f in plan.get("filters") or []:
                if isinstance(f, dict) and "column" in f and "value" in f:
                    filters_dict[f["column"]] = f["value"]
            
            return {
                "success": True,
                "query_type": mapped_intent,
                "intent": mapped_intent,
                "target_columns": plan.get("target_columns"),
                "columns": plan.get("target_columns"),
                "filters": filters_dict,
                "group_by": plan.get("group_by"),
                "aggregations": plan.get("aggregations"),
                "sorting": plan.get("sorting"),
                "limit": plan.get("limit"),
                "metric": metric,
                "confidence": plan.get("confidence") if plan.get("confidence") is not None else plan.get("parsed_json", {}).get("confidence", 0.95),
                "explanation": plan.get("explanation"),
                "parsed_json": plan.get("parsed_json"),
                "raw_response": plan.get("raw_response")
            }
        
        # Check if column validation failed (Task 10)
        error_msg = plan.get("error") or ""
        if "Column not found" in error_msg:
            return {
                "success": False,
                "message": "Column not found"
            }
        print(f"V2 Planner error: {error_msg}. Falling back to NLP engine.")
    except Exception as e:
        print(f"Exception in V2 Planner: {e}. Falling back to NLP engine.")

    # 2. Fallback to Rule-Based NLP engine (Task 14)
    try:
        engine = NLPQueryEngine(available_columns=visible_cols)
        nlp_parsed = engine.parse(question)
        
        # Map NLP structure to Query Plan JSON format
        mapped_plan = map_nlp_to_query_plan(nlp_parsed)
        
        # Convert V2 filters list to dict for V1 compatibility
        filters_dict_nlp = {}
        for f in mapped_plan.get("filters") or []:
            if isinstance(f, dict) and "column" in f and "value" in f:
                filters_dict_nlp[f["column"]] = f["value"]
        
        return {
            "success": True,
            "query_type": mapped_plan["query_type"],
            "intent": mapped_plan["query_type"],
            "target_columns": mapped_plan["target_columns"],
            "columns": mapped_plan["target_columns"],
            "filters": filters_dict_nlp,
            "group_by": mapped_plan["group_by"],
            "aggregations": mapped_plan["aggregations"],
            "sorting": mapped_plan["sorting"],
            "limit": mapped_plan["limit"],
            "metric": nlp_parsed.get("intent") or "none",
            "confidence": nlp_parsed.get("confidence", 0.75),
            "explanation": mapped_plan["explanation"],
            "parsed_json": mapped_plan,
            "raw_response": {"fallback_nlp": True, "nlp_parsed": nlp_parsed}
        }
    except Exception as nlp_err:
        print(f"NLP Fallback failed: {nlp_err}")
        return {
            "success": False,
            "message": "Column not found"
        }

def map_nlp_to_query_plan(nlp_parsed: Dict[str, Any]) -> Dict[str, Any]:
    """Map the rule-based NLP parse output to the V2 Query Plan schema."""
    intent = nlp_parsed.get("intent") or "exploratory"
    query_type = "record_lookup"
    if intent == "aggregation":
        query_type = "aggregation"
        if nlp_parsed.get("group_by"):
            query_type = "group_by"
    elif intent == "comparison":
        query_type = "comparison"
    elif intent == "statistical":
        query_type = "distribution"
    elif nlp_parsed.get("filters"):
        query_type = "record_lookup"
        
    aggs = []
    for a in nlp_parsed.get("aggregations") or []:
        aggs.append({
            "function": a.get("function") or "COUNT",
            "column": a.get("column") or "*"
        })
        
    filters = []
    for f in nlp_parsed.get("filters") or []:
        filters.append({
            "column": f.get("column"),
            "operator": f.get("operator") or "=",
            "value": f.get("value")
        })
        
    return {
        "query_type": query_type,
        "target_columns": nlp_parsed.get("select_columns") or ["*"],
        "filters": filters,
        "group_by": nlp_parsed.get("group_by") or [],
        "aggregations": aggs,
        "sorting": [],
        "limit": nlp_parsed.get("limit") or 100,
        "explanation": nlp_parsed.get("interpretation") or "Fell back to rule-based NLP parser."
    }

async def generate_ai_explanation(question: str, sql: str, results: List[Dict[str, Any]]) -> str:
    """
    Sends the user question, generated SQL, and returned results to LM Studio
    to generate an explanation.
    """
    truncated_results = results[:15]
    
    prompt = f"""You are a data analyst explaining query results to a user.

User Question: "{question}"
Generated SQL: {sql}
Returned Results (truncated to first 15 rows):
{json.dumps(truncated_results, indent=2, default=str)}

Generate an explanation of the results answering the user's question.
You must follow these rules:
1. Explain:
   - What was requested
   - What data was found
   - Key observations
   - Important notes
2. Do not exceed 5 bullet points.
3. Return only the bullet points, no conversational introduction or conclusion.
"""
    
    model_name = await get_model_name()
    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "system",
                "content": "You are a data analyst. Output maximum 5 bullet points explaining the query results."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.3,
        "max_tokens": 500
    }
    
    try:
        print("=== AI QUERY PAYLOAD ===")
        print(json.dumps(payload, indent=2))
        print("========================")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(f"{LM_STUDIO_URL}/v1/chat/completions", json=payload)
            print("LM STUDIO STATUS:", response.status_code)
            print("LM STUDIO RESPONSE:", response.text)

            if response.status_code == 200:
                result = response.json()
                explanation = result["choices"][0]["message"]["content"].strip()
                return explanation
    except Exception as e:
        print(f"Error generating AI explanation: {e}")
        
    return ""

def map_ai_plan_to_parsed(plan: Dict[str, Any], prompt: str) -> Dict[str, Any]:
    """
    Translates the V2 Query Plan or Fallback Plan to the backward-compatible layout.
    """
    parsed = {
        "original_prompt": prompt,
        "intent": plan.get("query_type") or plan.get("intent") or "record_lookup",
        "filters": [],
        "aggregations": [],
        "group_by": plan.get("group_by") or [],
        "select_columns": plan.get("target_columns") or plan.get("columns") or [],
        "order_by": plan.get("sorting") or None,
        "limit": plan.get("limit") or 100,
        "confidence": plan.get("confidence", 0.95),
        "explanation": plan.get("explanation") or "",
        "interpretation": plan.get("explanation") or "",
        "warnings": [],
        "raw_response": plan.get("raw_response"),
        "parsed_json": plan.get("parsed_json") or plan
    }
    
    # Map filters (supporting V1 dict format and V2 list of dicts format)
    filters_input = []
    if plan.get("parsed_json") and isinstance(plan["parsed_json"], dict) and plan["parsed_json"].get("filters"):
        filters_input = plan["parsed_json"]["filters"]
    else:
        filters_input = plan.get("filters") or []
        
    if isinstance(filters_input, list):
        for f in filters_input:
            if isinstance(f, dict) and "column" in f and "value" in f:
                parsed["filters"].append({
                    "column": f["column"],
                    "operator": f.get("operator") or "=",
                    "value": f["value"],
                    "logic": f.get("logic") or "AND"
                })
    elif isinstance(filters_input, dict):
        for col, val in filters_input.items():
            val_str = str(val).strip()
            
            # Detect between, e.g. "18-25"
            between_match = re.match(r'^(\d+)\s*[-–to]+\s*(\d+)$', val_str, re.IGNORECASE)
            if between_match:
                low, high = between_match.group(1), between_match.group(2)
                parsed["filters"].append({
                    "column": col,
                    "operator": "BETWEEN",
                    "value": f"{low} AND {high}"
                })
                continue
                
            # Detect comparisons (e.g. >= 60, >60, etc.)
            op_match = re.match(r'^(>=|<=|>|<|!=|=)\s*(\d+(\.\d+)?)$', val_str)
            if op_match:
                parsed["filters"].append({
                    "column": col,
                    "operator": op_match.group(1),
                    "value": op_match.group(2)
                })
                continue
                
            # Default equality
            parsed["filters"].append({
                "column": col,
                "operator": "=",
                "value": val_str
            })
            
    # Map aggregations
    if plan.get("aggregations"):
        parsed["aggregations"] = plan.get("aggregations")
    else:
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
            cols = plan.get("columns") or plan.get("target_columns")
            if func != "COUNT" and cols:
                group_by_set = set(g.lower() for g in plan.get("group_by", []))
                for c in cols:
                    if c.lower() not in group_by_set:
                        agg_col = c
                        break
                if agg_col == "*":
                    agg_col = cols[0]
            
            parsed["aggregations"].append({
                "function": func,
                "column": agg_col
            })
            
    return parsed

