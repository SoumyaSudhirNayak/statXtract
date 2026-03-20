
from typing import Dict, Any, List
import asyncpg
from collections import defaultdict

async def get_column_labels(conn: asyncpg.Connection, table_name: str, schema: str) -> Dict[str, Dict[str, str]]:
    """
    Fetches category labels for a given table.
    Returns a dict: { "column_name": { "code": "label", ... } }
    """
    # Sanitize table name (strip schema if present in table_name arg, though usually passed separately)
    # The variables table stores 'table_name' usually as the safe name (e.g. 'plfs_2023_data') without schema
    # But let's check how it was stored in ingestion_pipeline.py:
    # full_table_name = f"{schema}.{table_name}" -> stored in uploaded_tables
    # variables table update: SET table_name = :tname (where tname is just the table name, not full path)
    
    # So we query by table_name (without schema prefix usually)
    # If the input table_name has "public.", strip it
    
    simple_table_name = table_name.split('.')[-1]

    # In the current architecture, category labels are stored in the
    # '{schema}'.variable_categories table, keyed by table_name and variable_name.
    try:
        rows = await conn.fetch(
            f"""
            SELECT variable_name as column_name, value as category_code, label as category_label
            FROM "{schema}".variable_categories
            WHERE table_name = $1
            """,
            simple_table_name,
        )
    except Exception as e:
        print(f"Warning: Could not fetch categorical labels for {schema}.{table_name} - {e}")
        return defaultdict(dict)
    
    label_map = defaultdict(dict)
    for r in rows:
        # Store as string for reliable lookup, assuming data might be int or string
        code = str(r["category_code"]).strip()
        label = r["category_label"]
        col = r["column_name"]
        label_map[col][code] = label
        
    return label_map

def apply_labels(rows: List[Dict[str, Any]], label_map: Dict[str, Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Replaces codes with labels in the result rows.
    """
    if not rows or not label_map:
        return rows
    
    transformed_rows = []
    for row in rows:
        new_row = row.copy() # Shallow copy to avoid mutating original if needed elsewhere
        for col, val in row.items():
            if col in label_map and val is not None:
                # Convert val to string for lookup
                val_str = str(val).strip()
                # Determine if val is a float ending in .0 (common in pandas/postgres numeric)
                if val_str.endswith('.0') and '.' in val_str:
                     # Try looking up "1" instead of "1.0"
                     val_str_int = val_str[:-2]
                     if val_str_int in label_map[col]:
                         new_row[col] = label_map[col][val_str_int]
                         continue

                if val_str in label_map[col]:
                    new_row[col] = label_map[col][val_str]
        transformed_rows.append(new_row)
        
    return transformed_rows
