import os
import hashlib
import re
from itertools import combinations
from datetime import datetime
import pandas as pd
from sqlalchemy import text

class DuplicateDatasetException(Exception):
    def __init__(self, message: str, duplicate_info: dict):
        super().__init__(message)
        self.message = message
        self.duplicate_info = duplicate_info

def detect_primary_key_forward(df: pd.DataFrame, max_composite_size: int = 3):
    total_rows = len(df)
    if total_rows == 0:
        return None
    
    # Pre-filter out any columns containing NULL values (Invalid for PKs)
    valid_cols = [col for col in df.columns if not df[col].isnull().any()]
    
    # Loop upward through combination sizes (1 column, then 2, then 3...)
    for size in range(1, max_composite_size + 1):
        for combo in combinations(valid_cols, size):
            combo_list = list(combo)
            try:
                # Check uniqueness using value grouping
                if df.groupby(combo_list).ngroups == total_rows:
                    return combo_list
            except Exception:
                pass
    return None

def clean_cell_val(cell) -> str:
    if cell is None or pd.isna(cell):
        return ""
    val = str(cell).strip()
    if val.endswith(".0"):
        try:
            return str(int(float(val)))
        except ValueError:
            pass
    return val

def compute_dataset_fingerprint_and_pk(df: pd.DataFrame):
    # 1. Normalize Column Names (lowercase and strip)
    df = df.rename(columns=lambda x: str(x).lower().strip())
    sorted_cols = sorted(df.columns)
    df = df[sorted_cols]
    
    # 2. Clean Cells to standardize values
    for col in df.columns:
        df[col] = df[col].apply(clean_cell_val)
        
    # 3. Detect PK using the forward PK discovery algorithm
    pk = detect_primary_key_forward(df, max_composite_size=3)
    
    # 4. Sort rows using PK if found, otherwise sorted columns
    if pk:
        df = df.sort_values(by=pk).reset_index(drop=True)
    else:
        df = df.sort_values(by=sorted_cols).reset_index(drop=True)
        
    # 5. Generate fingerprint from the standard CSV representation
    csv_data = df.to_csv(index=False, lineterminator='\n')
    fingerprint = hashlib.sha256(csv_data.encode('utf-8')).hexdigest()
    
    return fingerprint, pk

def ensure_redundancy_table(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dataset_redundancy_registry (
                id SERIAL PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                survey_name TEXT NOT NULL,
                year TEXT NOT NULL,
                dataset_name TEXT NOT NULL,
                block_name TEXT NOT NULL,
                candidate_key TEXT,
                fingerprint TEXT,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_type TEXT NOT NULL
            );
        """))

def check_dataset_duplicate(engine, survey_name: str, year: str, dataset_name: str, block_name: str, fingerprint: str, candidate_key: list) -> dict:
    ensure_redundancy_table(engine)
    
    with engine.connect() as conn:
        # 1. Check existing fingerprint match
        fp_match = conn.execute(text("""
            SELECT dataset_id, survey_name, year, dataset_name, block_name, upload_date, source_type
            FROM dataset_redundancy_registry
            WHERE fingerprint = :fp
            LIMIT 1
        """), {"fp": fingerprint}).fetchone()
        
        if fp_match:
            return {
                "status": "duplicate",
                "reason": "Fingerprint Match",
                "existing": dict(fp_match._mapping) if hasattr(fp_match, "_mapping") else dict(fp_match)
            }
            
        # 2. Check candidate key and identity match
        pk_str = ",".join(candidate_key) if candidate_key else None
        if pk_str:
            pk_match = conn.execute(text("""
                SELECT dataset_id, survey_name, year, dataset_name, block_name, upload_date, source_type
                FROM dataset_redundancy_registry
                WHERE candidate_key = :pk
                  AND survey_name = :survey
                  AND year = :year
                  AND block_name = :block
                LIMIT 1
            """), {"pk": pk_str, "survey": survey_name, "year": year, "block": block_name}).fetchone()
            
            if pk_match:
                return {
                    "status": "possible_duplicate",
                    "reason": "Primary Key Match",
                    "existing": dict(pk_match._mapping) if hasattr(pk_match, "_mapping") else dict(pk_match)
                }
                
        # 3. Check identity match only (survey/year/block)
        id_match = conn.execute(text("""
            SELECT dataset_id, survey_name, year, dataset_name, block_name, upload_date, source_type
            FROM dataset_redundancy_registry
            WHERE survey_name = :survey
              AND year = :year
              AND block_name = :block
            LIMIT 1
        """), {"survey": survey_name, "year": year, "block": block_name}).fetchone()
        
        if id_match:
            return {
                "status": "possible_duplicate",
                "reason": "Survey/Year/Block Match",
                "existing": dict(id_match._mapping) if hasattr(id_match, "_mapping") else dict(id_match)
            }
            
        return {
            "status": "new",
            "reason": None,
            "existing": None
        }

def save_redundancy_metadata(engine, dataset_id: str, survey_name: str, year: str, dataset_name: str, block_name: str, candidate_key: list, fingerprint: str, source_type: str):
    ensure_redundancy_table(engine)
    pk_str = ",".join(candidate_key) if candidate_key else None
    
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dataset_redundancy_registry (
                dataset_id, survey_name, year, dataset_name, block_name, candidate_key, fingerprint, source_type
            ) VALUES (
                :did, :survey, :year, :dname, :block, :pk, :fp, :source
            )
        """), {
            "did": dataset_id,
            "survey": survey_name,
            "year": year,
            "dname": dataset_name,
            "block": block_name,
            "pk": pk_str,
            "fp": fingerprint,
            "source": source_type
        })
