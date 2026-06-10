import os
import re
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Tuple
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# Terms to look for in filenames or sheet names to indicate a reference/mapping resource
REF_FILENAME_PATTERNS = [
    "state", "district", "village", "town", "nic", "nco", "occupation", "industry",
    "classification", "crosswalk", "lookup", "mapping", "master", "codebook", 
    "category", "bstrm", "bstream", "auxiliary", "reference"
]

def check_reference_mapping_confidence(file_path: Path) -> float:
    """
    Computes a confidence score (from 0.0 to 1.0) indicating whether a file
    is a reference mapping or lookup resource rather than a survey data table.
    Uses filename, sheet name, column headers, shape, and uniqueness constraints.
    """
    ext = file_path.suffix.lower()
    if ext not in {".csv", ".xlsx", ".xls"}:
        return 0.0
        
    name_lower = file_path.name.lower()
    score = 0.0
    
    # 1. Filename heuristic
    if any(term in name_lower for term in REF_FILENAME_PATTERNS):
        score += 0.4
        
    try:
        # 2. Analyze sheet names for spreadsheets
        if ext != ".csv":
            excel_file = pd.ExcelFile(file_path)
            if not excel_file.sheet_names:
                return 0.0
            sheet_names = [s.lower() for s in excel_file.sheet_names]
            if any(any(term in s for term in REF_FILENAME_PATTERNS) for s in sheet_names):
                score += 0.2
            df = pd.read_excel(file_path, sheet_name=excel_file.sheet_names[0], nrows=5000, keep_default_na=False)
        else:
            try:
                df = pd.read_csv(file_path, nrows=5000, keep_default_na=False)
            except Exception:
                df = pd.read_csv(file_path, nrows=5000, keep_default_na=False, encoding="latin1")
                
        if df.empty or len(df.columns) < 2:
            return 0.0
            
        cols = list(df.columns)
        num_rows = len(df)
        
        # 3. Reference mappings are typically smaller lookup sets (less than 50,000 records)
        if num_rows < 50000:
            score += 0.1
            
        # 4. Key-value column headers check
        has_key_like_col = False
        has_label_like_col = False
        for col in cols:
            col_lower = str(col).lower()
            if any(x in col_lower for x in ["code", "id", "value", "val", "key", "state", "dist", "bstrm", "htrm", "strm", "sstrm"]) or col_lower.endswith(("_code", "_cd", "code", "cd")):
                has_key_like_col = True
            if any(x in col_lower for x in ["name", "desc", "label", "description", "title"]) or col_lower.endswith(("_name", "_label", "_desc", "_description", "name", "desc", "label")):
                has_label_like_col = True
                
        if has_key_like_col:
            score += 0.15
        if has_label_like_col:
            score += 0.15
            
        # 5. Repeated code-value pattern (uniqueness constraint)
        # In a reference mapping, the key/code column is 100% unique
        max_uniqueness_ratio = 0.0
        for col in cols:
            series = df[col]
            non_null = series[series != ""]
            if len(non_null) > 5:
                uniq_ratio = len(non_null.unique()) / len(non_null)
                if uniq_ratio > max_uniqueness_ratio:
                    max_uniqueness_ratio = uniq_ratio
                    
        if max_uniqueness_ratio > 0.95:
            score += 0.2
            
    except Exception as e:
        logger.debug(f"Failed to check reference confidence for {file_path.name}: {e}")
        
    return min(1.0, score)

def is_reference_mapping_file(file_path: Path) -> bool:
    """
    Decides if the file is classified as a reference mapping resource (score >= 0.5).
    """
    confidence = check_reference_mapping_confidence(file_path)
    return confidence >= 0.5

def parse_sheet_mappings(df: pd.DataFrame, file_stem: str = None) -> Tuple[str, str, Dict[str, str]]:
    """
    Identifies key (code) and label (name) columns and builds key-value mappings.
    Prioritizes exact patterns such as Code|Name, NIC|NIC_Description, State_Code|State_Name, etc.
    """
    cols = list(df.columns)
    if len(cols) < 2:
        raise ValueError("Mapping sheet must have at least 2 columns")
        
    key_col = None
    lbl_col = None
    subj = None
    if file_stem:
        subj = get_core_subject(file_stem)
        
    # 1. Search for key column matching file's core subject first
    if subj:
        for c in cols:
            c_lower = str(c).lower().strip()
            if check_subject_match(subj, c):
                if c_lower in ["code", "id", "key", "nic", "nco", "value", "val", subj] or c_lower.endswith(("_code", "_cd", "code", "cd")) or any(x in c_lower for x in ["code", "cd", "id"]):
                    key_col = c
                    break
        if not key_col:
            for c in cols:
                if check_subject_match(subj, c):
                    key_col = c
                    break
                    
    # Fallback key search
    if not key_col:
        for c in cols:
            c_lower = str(c).lower().strip()
            if c_lower in ["code", "id", "key", "nic", "nco", "value", "val"] or c_lower.endswith(("_code", "_cd", "code", "cd")):
                key_col = c
                break
            
    if not key_col:
        for c in cols:
            c_lower = str(c).lower()
            if any(x in c_lower for x in ["code", "id", "value", "val", "key"]):
                key_col = c
                break
                
    if not key_col:
        key_col = cols[0]
        
    # 2. Search for label column matching file's core subject first
    if subj:
        for c in cols:
            if c == key_col:
                continue
            c_lower = str(c).lower().strip()
            if check_subject_match(subj, c):
                if c_lower in ["name", "description", "desc", "label", "title"] or c_lower.endswith(("_name", "_label", "_desc", "_description", "name", "desc", "label")) or any(x in c_lower for x in ["name", "label", "desc", "description"]):
                    lbl_col = c
                    break
        if not lbl_col:
            for c in cols:
                if c == key_col:
                    continue
                if check_subject_match(subj, c):
                    lbl_col = c
                    break
                    
    # Fallback label search
    if not lbl_col:
        for c in cols:
            if c == key_col:
                continue
            c_lower = str(c).lower().strip()
            if c_lower in ["name", "description", "desc", "label", "title"] or c_lower.endswith(("_name", "_label", "_desc", "_description", "name", "desc", "label")):
                lbl_col = c
                break
            
    if not lbl_col:
        for c in cols:
            if c == key_col:
                continue
            c_lower = str(c).lower()
            if any(x in c_lower for x in ["name", "desc", "label", "description", "title"]):
                lbl_col = c
                break
                
    if not lbl_col:
        lbl_col = cols[1] if cols[1] != key_col else (cols[0] if cols[0] != key_col else cols[1])
        
    # 3. Identify hierarchical State Code column if this is a district/bstrm mapping
    state_col = None
    if subj in ["district", "bstrm"] or check_subject_match("district", key_col) or check_subject_match("bstrm", key_col):
        for c in cols:
            if c == key_col or c == lbl_col:
                continue
            if check_subject_match("state", c):
                state_col = c
                break
        
    # Build mapping dictionary
    mappings = {}
    for _, row in df.iterrows():
        k = row[key_col]
        v = row[lbl_col]
        if pd.isna(k) or pd.isna(v):
            continue
        
        # Normalize code (strip .0 from floats, strip whitespace)
        k_str = str(k).strip()
        if not k_str or k_str.lower() in ["nan", "none", "null", ""]:
            continue
        if k_str.endswith(".0") and "." in k_str:
            k_str = k_str[:-2]
            
        val_str = str(v).strip()
        if not val_str or val_str.lower() in ["nan", "none", "null", ""]:
            continue
        
        if state_col:
            st_val = row[state_col]
            if not pd.isna(st_val):
                st_str = str(st_val).strip()
                if st_str.endswith(".0") and "." in st_str:
                    st_str = st_str[:-2]
                st_norm = st_str.lstrip('0') or '0'
                k_norm = k_str.lstrip('0') or '0'
                if st_norm and k_norm:
                    composite_key = f"{st_norm}_{k_norm}"
                    mappings[composite_key] = val_str
                
        mappings[k_str] = val_str
        
    return str(key_col), str(lbl_col), mappings

def store_reference_mapping(db_url: str, survey_schema: str, dataset_schema: str, file_path: Path):
    """
    Parses a reference mapping file and stores its mapping definition in the database.
    Does NOT fail or raise errors (non-breaking design).
    """
    try:
        file_path = Path(file_path)
        if not file_path.exists():
            logger.warning(f"Reference mapping file does not exist: {file_path}")
            return
            
        ext = file_path.suffix.lower()
        engine = create_engine(db_url)
        
        # Read the raw binary content of the file
        original_bytes = None
        try:
            original_bytes = file_path.read_bytes()
        except Exception as err:
            logger.warning(f"Could not read bytes of {file_path.name}: {err}")
            
        # Parse dataframes
        sheets_data: Dict[str, pd.DataFrame] = {}
        if ext == ".csv":
            try:
                sheets_data["default"] = pd.read_csv(file_path, keep_default_na=False)
            except Exception:
                sheets_data["default"] = pd.read_csv(file_path, keep_default_na=False, encoding="latin1")
        else:
            sheets_data = pd.read_excel(file_path, sheet_name=None, keep_default_na=False)
            
        file_stem = Path(file_path).stem.lower()
        for sheet_name, df in sheets_data.items():
            if df.empty or len(df.columns) < 2:
                logger.info(f"Skipping empty or invalid sheet/file {sheet_name} in {file_path.name}")
                continue
                
            try:
                src_col, lbl_col, mappings = parse_sheet_mappings(df, file_stem=file_stem)
                if not mappings:
                    logger.info(f"No valid mappings extracted from sheet {sheet_name} in {file_path.name}")
                    continue
                    
                # Determine clean mapping type from filename and source column
                clean_stem = re.sub(r'[^a-z0-9_]', '_', file_stem).strip('_')
                mtype_parts = [clean_stem]
                if sheet_name != "default" and ext != ".csv":
                    clean_sheet = re.sub(r'[^a-z0-9_]', '_', sheet_name.lower()).strip('_')
                    mtype_parts.append(clean_sheet)
                clean_src = re.sub(r'[^a-z0-9_]', '_', src_col.lower()).strip('_')
                mtype_parts.append(clean_src)
                mapping_type = "_".join(mtype_parts)
                if not mapping_type:
                    mapping_type = "generic_mapping"
                    
                filename = file_path.name
                if sheet_name != "default" and ext != ".csv":
                    filename = f"{file_path.name}#{sheet_name}"
                    
                import json
                mappings_json = json.dumps(mappings)
                
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO dataset_reference_mappings (
                                survey_schema, dataset_schema, filename, mapping_type, source_column, label_column, mappings, original_file
                            ) VALUES (
                                :survey, :dataset, :fname, :mtype, :scol, :lcol, :mappings, :file_bytes
                            ) ON CONFLICT (dataset_schema, filename, mapping_type) DO UPDATE SET
                                mappings = EXCLUDED.mappings,
                                source_column = EXCLUDED.source_column,
                                label_column = EXCLUDED.label_column,
                                original_file = EXCLUDED.original_file,
                                created_at = CURRENT_TIMESTAMP
                        """),
                        {
                            "survey": survey_schema,
                            "dataset": dataset_schema,
                            "fname": filename,
                            "mtype": mapping_type,
                            "scol": src_col,
                            "lcol": lbl_col,
                            "mappings": mappings_json,
                            "file_bytes": original_bytes
                        }
                    )
                logger.info(f"Successfully stored reference mapping: {filename} ({mapping_type})")
            except Exception as sheet_err:
                logger.warning(f"Error parsing sheet {sheet_name} in {file_path.name}: {sheet_err}")
                
    except Exception as e:
        logger.warning(f"Failed to store reference mapping for {file_path.name}: {e}")

def check_subject_match(subject: str, text: str) -> bool:
    """
    Checks if a text matches a core reference mapping subject (case and separator insensitive, synonyms-aware).
    """
    if not text:
        return False
    text_clean = re.sub(r'[^a-z0-9]', ' ', str(text).lower())
    words = text_clean.split()
    
    subject_synonyms = {
        "state": ["state", "st", "stnew", "hta", "region"],
        "district": ["district", "dist", "dt", "dta", "dc"],
        "village": ["village", "vil", "vlg"],
        "town": ["town", "twn"],
        "nic": ["nic", "industry", "ind", "niccode"],
        "nco": ["nco", "occupation", "occ", "ncocode"],
        "industry": ["industry", "ind"],
        "occupation": ["occupation", "occ"],
        "education": ["education", "edu", "lit"],
        "religion": ["religion", "rel"],
        "social": ["social", "soc", "caste", "group", "category"],
        "sector": ["sector", "sec"],
        "enterprise": ["enterprise", "ent"],
        "bstrm": ["bstrm", "bstream", "htrm", "strm", "sstrm"]
    }
    
    synonyms = subject_synonyms.get(subject, [subject])
    
    for syn in synonyms:
        if syn in words:
            return True
            
    joined_text = "".join(words)
    for syn in synonyms:
        if syn in joined_text:
            # Avoid false positives like "status" for "st"
            if syn == "st" and joined_text != "st" and not joined_text.startswith("stcode") and not joined_text.startswith("stcd") and not joined_text.startswith("stnew"):
                continue
            if syn == "dt" and joined_text != "dt" and not joined_text.startswith("dtcode") and not joined_text.startswith("dtcd"):
                continue
            if syn == "dc" and joined_text != "dc" and not joined_text.startswith("dccode") and not joined_text.startswith("dccd"):
                continue
            return True
            
    return False

def get_core_subject(name: str) -> str:
    """
    Extracts the core reference mapping subject from a variable/mapping type name.
    """
    name_clean = re.sub(r'[^a-z0-9]', ' ', str(name).lower())
    words = name_clean.split()
    joined = "".join(words)
    
    subjects = ["state", "district", "village", "town", "nic", "nco", "industry", "occupation", "education", "religion", "social", "sector", "enterprise", "bstrm"]
    for s in subjects:
        subject_synonyms = {
            "state": ["state", "st", "stnew", "hta"],
            "district": ["district", "dist", "dt", "dta", "dc"],
            "village": ["village", "vil"],
            "town": ["town", "twn"],
            "nic": ["nic"],
            "nco": ["nco"],
            "social": ["social", "caste", "group"],
            "bstrm": ["bstrm", "bstream", "htrm", "strm", "sstrm"]
        }
        syns = subject_synonyms.get(s, [s])
        for syn in syns:
            if syn in words or (len(syn) > 2 and syn in joined):
                return s
    return joined

def is_fuzzy_match(col_name: str, ddi_var: Any, map_source_col: str, map_type: str) -> bool:
    """
    Fuzzy matches a table column to a stored reference mapping using name normalizations,
    subject synonyms, and DDI metadata (variable labels, question text, concept/universe).
    """
    col_lower = col_name.lower()
    map_src_lower = map_source_col.lower()
    map_type_lower = map_type.lower()
    
    def norm(s):
        return re.sub(r'[^a-z0-9]', '', str(s).lower())
    
    col_norm = norm(col_name)
    src_norm = norm(map_source_col)
    type_norm = norm(map_type)
    
    def strip_suffixes(s):
        for suffix in ['code', 'cd', 'id', 'key', 'value', 'name', 'desc', 'label']:
            if s.endswith(suffix) and len(s) > len(suffix):
                s = s[:-len(suffix)]
        return s
        
    col_stripped = strip_suffixes(col_norm)
    src_stripped = strip_suffixes(src_norm)
    type_stripped = strip_suffixes(type_norm)
    
    # Check normalized string matches
    if col_stripped == src_stripped or col_stripped == type_stripped:
        return True
    if col_norm == src_norm or col_norm == type_norm:
        return True
        
    # Check core subjects (e.g. state, district, nic) matching
    subject = get_core_subject(map_type) or get_core_subject(map_source_col)
    if subject:
        if check_subject_match(subject, col_name):
            return True
        if ddi_var:
            if hasattr(ddi_var, "label") and ddi_var.label:
                if check_subject_match(subject, ddi_var.label):
                    return True
            if hasattr(ddi_var, "question") and ddi_var.question:
                if check_subject_match(subject, ddi_var.question):
                    return True
            if hasattr(ddi_var, "concept") and ddi_var.concept:
                if check_subject_match(subject, ddi_var.concept):
                    return True
            if hasattr(ddi_var, "universe") and ddi_var.universe:
                if check_subject_match(subject, ddi_var.universe):
                    return True
                    
    return False

def auto_link_mappings(db_url: str, dataset_schema: str, created_tables: List[str], ddi_variables: List[Any] = None):
    """
    Compares the columns of ingested dataset tables with stored reference mapping types
    using fuzzy matching and DDI metadata, and automatically registers links in dataset_column_mappings.
    """
    try:
        engine = create_engine(db_url)
        
        # 1. Fetch all reference mappings for this dataset schema
        with engine.connect() as conn:
            ref_mappings = conn.execute(
                text("""
                    SELECT id, source_column, mapping_type
                    FROM dataset_reference_mappings
                    WHERE dataset_schema = :ds_schema
                """),
                {"ds_schema": dataset_schema}
            ).fetchall()
            
        if not ref_mappings:
            logger.info(f"No reference mappings stored for dataset schema {dataset_schema}")
            return
            
        # Build lookup for DDI variables by name (case-insensitive)
        ddi_var_lookup = {}
        if ddi_variables:
            for v in ddi_variables:
                ddi_var_lookup[str(v.name).lower()] = v
                
        # 2. Inspect each table's columns and link them
        for table in created_tables:
            with engine.connect() as conn:
                cols = conn.execute(
                    text("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = :schema AND table_name = :table
                    """),
                    {"schema": dataset_schema, "table": table}
                ).fetchall()
                
            column_names = [str(c[0]) for c in cols]
            
            for column_name in column_names:
                ddi_var = ddi_var_lookup.get(column_name.lower())
                
                for r_map in ref_mappings:
                    map_id = r_map[0]
                    src_col = str(r_map[1])
                    mtype = str(r_map[2])
                    
                    if is_fuzzy_match(column_name, ddi_var, src_col, mtype):
                        try:
                            with engine.begin() as conn:
                                conn.execute(
                                    text("""
                                        INSERT INTO dataset_column_mappings (
                                            dataset_schema, table_name, column_name, mapping_id
                                        ) VALUES (
                                            :ds_schema, :table, :col, :map_id
                                        ) ON CONFLICT (dataset_schema, table_name, column_name, mapping_id) DO NOTHING
                                    """),
                                    {
                                        "ds_schema": dataset_schema,
                                        "table": table,
                                        "col": column_name,
                                        "map_id": map_id
                                    }
                                )
                            logger.info(f"Auto-linked column {table}.{column_name} to reference mapping id {map_id} ({mtype})")
                        except Exception as link_err:
                            logger.warning(f"Could not link column {table}.{column_name} to mapping {map_id}: {link_err}")
                            
    except Exception as e:
        logger.warning(f"Error in auto_link_mappings for schema {dataset_schema}: {e}")
