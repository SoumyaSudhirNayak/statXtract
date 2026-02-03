
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

    rows = await conn.fetch(
        """
        WITH latest_dataset AS (
            SELECT v.dataset_id
            FROM variables v
            JOIN datasets d ON d.dataset_id = v.dataset_id
            WHERE v.table_name = $1
            ORDER BY d.created_at DESC
            LIMIT 1
        )
        SELECT v.column_name, vc.category_code, vc.category_label
        FROM variables v
        JOIN variable_categories vc ON v.variable_id = vc.variable_id
        WHERE v.table_name = $1
          AND v.dataset_id = (SELECT dataset_id FROM latest_dataset)
    """,
        simple_table_name,
    )
    
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
