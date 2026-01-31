import os
import shutil
import zipfile
import tempfile
import logging
import pandas as pd
import pyreadstat
import warnings
from pathlib import Path
from typing import Optional, Dict, List, Any
from sqlalchemy import create_engine, text
from datetime import datetime
import hashlib

from utils.ddi_parser import parse_ddi_xml, DDIVariable
from utils.table_naming import get_safe_table_name
from utils.job_manager import update_job

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _hash_file_sha1(file_path: str, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha1()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

def _normalize_col_name(name: Any) -> str:
    return str(name).strip().lower()

def _is_blank_cell(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    s = str(v).strip()
    if s == "":
        return True
    return False

def _count_leading_blank_csv_rows(file_path: Path, max_lines: int = 2000) -> int:
    count = 0
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for _ in range(max_lines):
                line = f.readline()
                if line == "":
                    break
                stripped = line.strip()
                if stripped == "":
                    count += 1
                    continue
                # Treat lines like ",,," as blank
                stripped2 = stripped.replace(",", "").replace("\t", "").replace(";", "")
                if stripped2.strip() == "":
                    count += 1
                    continue
                break
    except Exception:
        return 0
    return count

def _count_leading_blank_excel_rows(file_path: Path, max_rows: int = 200) -> int:
    try:
        preview = pd.read_excel(file_path, header=None, nrows=max_rows)
    except Exception:
        return 0

    for i in range(len(preview)):
        row = preview.iloc[i].tolist()
        if all(_is_blank_cell(v) for v in row):
            continue
        return i
    return len(preview)

def _maybe_format_month_year_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    def _to_datetime_no_warn(values: Any) -> Any:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"Could not infer format, so each element will be parsed individually.*",
                category=UserWarning,
            )
            return pd.to_datetime(values, errors="coerce")

    def _format_month_year(dt_series: pd.Series) -> pd.Series:
        months = {
            1: "jan",
            2: "feb",
            3: "mar",
            4: "apr",
            5: "may",
            6: "jun",
            7: "jul",
            8: "aug",
            9: "sep",
            10: "oct",
            11: "nov",
            12: "dec",
        }
        year = dt_series.dt.year.astype("Int64")
        month = dt_series.dt.month.astype("Int64")
        month_str = month.map(months).astype("string")
        out = month_str + "-" + year.astype("string")
        out = out.where(dt_series.notna(), other=pd.NA)
        return out

    for col in df.columns:
        series = df[col]
        cleaned = series.astype("string").str.strip()
        cleaned = cleaned.mask(cleaned == "", other=pd.NA)
        non_null = cleaned.dropna()
        if non_null.empty:
            continue

        sample = non_null.head(200)

        if pd.api.types.is_datetime64_any_dtype(sample):
            dt = sample
        else:
            dt = _to_datetime_no_warn(sample)

        parsed = dt.dropna()
        if parsed.empty:
            continue

        day_mode = int(parsed.dt.day.mode().iloc[0]) if hasattr(parsed, "dt") else 0
        day_is_constant = (parsed.dt.day == day_mode).mean() if hasattr(parsed, "dt") else 0
        time_is_midnight = (
            ((parsed.dt.hour == 0) & (parsed.dt.minute == 0) & (parsed.dt.second == 0)).mean()
            if hasattr(parsed, "dt")
            else 0
        )
        parsed_ratio = len(parsed) / max(len(sample), 1)
        month_variety = int(parsed.dt.month.nunique()) if hasattr(parsed, "dt") else 0
        year_variety = int(parsed.dt.year.nunique()) if hasattr(parsed, "dt") else 0

        if (
            parsed_ratio >= 0.7
            and day_is_constant >= 0.95
            and time_is_midnight >= 0.9
            and (month_variety >= 2 or year_variety >= 2)
        ):
            full_dt = _to_datetime_no_warn(cleaned)
            df[col] = _format_month_year(full_dt)

    return df

async def process_dataset_zip(zip_path: str, db_url: str, schema: str = "public", current_user_email: str = "system", job_id: str = None):
    """
    Main entry point for processing a dataset ZIP file.
    """
    logger.info(f"Processing ZIP: {zip_path}")
    if job_id:
        update_job(job_id, status="processing", progress=2, message="Reading ZIP and checking for duplicates...")

    zip_sha1 = _hash_file_sha1(zip_path)
    dataset_id = f"{schema}_{zip_sha1[:12]}"
    try:
        with zipfile.ZipFile(zip_path, "r"):
            pass
    except zipfile.BadZipFile:
        if job_id:
            update_job(job_id, status="failed", progress=100, message="Invalid ZIP file", error="Invalid ZIP file")
        raise ValueError("Invalid ZIP file")

    if job_id:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                names = [n for n in zf.namelist() if n and not n.endswith('/')]
            visible = [Path(n).name for n in names if Path(n).suffix.lower() in ['.xml', '.txt', '.csv', '.sav', '.xlsx']]
            file_status_list = [{"name": n, "status": "pending", "rows": 0} for n in sorted(set(visible))]
            update_job(job_id, progress=5, message="ZIP opened. Preparing extraction...", files=file_status_list)
        except Exception:
            update_job(job_id, progress=5, message="ZIP opened. Preparing extraction...")
    
    with tempfile.TemporaryDirectory() as extract_dir:
        extract_path = Path(extract_dir)
        
        # 1. Extract ZIP
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(extract_path)
        except zipfile.BadZipFile:
            if job_id:
                update_job(job_id, status="failed", progress=100, message="Invalid ZIP file", error="Invalid ZIP file")
            raise ValueError("Invalid ZIP file")

        if job_id:
            update_job(job_id, progress=8, message="ZIP extracted. Scanning contents...")

        return await process_directory(extract_path, db_url, schema, job_id, dataset_id)

async def process_directory(directory: Path, db_url: str, schema: str = "public", job_id: str = None, dataset_id: Optional[str] = None):
    """
    Processes a directory containing dataset files (DDI + Data).
    """
    # 2. Identify Files
    files = [f for f in directory.rglob("*") if f.is_file()]
    ddi_file = next((f for f in files if f.suffix.lower() == '.xml'), None)
    data_files = [f for f in files if f.suffix.lower() in ['.txt', '.csv', '.sav', '.xlsx']]
    
    if not data_files:
        if job_id:
            update_job(job_id, status="failed", progress=100, message="No data files (.txt, .csv, .sav, .xlsx) found", error="No data files (.txt, .csv, .sav, .xlsx) found")
        raise ValueError("No data files (.txt, .csv, .sav, .xlsx) found")

    # Initialize file status in job
    if job_id:
        file_status_list = [{"name": f.name, "status": "pending", "rows": 0} for f in data_files]
        update_job(job_id, progress=10, message="Identified files", files=file_status_list)

    # 3. Parse DDI (if present)
    ddi_metadata = None
    if ddi_file:
        if job_id:
            update_job(job_id, progress=12, message=f"Parsing DDI: {ddi_file.name}")
        logger.info(f"Found DDI XML: {ddi_file.name}")
        ddi_metadata = parse_ddi_xml(str(ddi_file))
    else:
        logger.warning("No DDI XML found. Metadata will be limited.")
    
    if job_id:
        update_job(job_id, progress=20, message="Starting file processing...")

    # 4. Process Each Data File
    uploaded_tables = []
    if dataset_id is None:
        dataset_id = f"{schema}_{int(datetime.now().timestamp())}"

    # Connect to DB for metadata storage
    engine = create_engine(db_url)

    def _table_exists(conn, schema_name: str, table_name: str) -> bool:
        row = conn.execute(
            text(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :schema
                  AND table_name = :tname
                LIMIT 1
                """
            ),
            {"schema": schema_name, "tname": table_name},
        ).fetchone()
        return row is not None
    
    # Store dataset metadata
    with engine.connect() as conn:
        # Create/Update dataset record
        conn.execute(text("""
            INSERT INTO datasets (dataset_id, title, source)
            VALUES (:id, :title, :source)
            ON CONFLICT (dataset_id) DO NOTHING
        """), {"id": dataset_id, "title": ddi_metadata.get('title', 'Unknown') if ddi_metadata else 'Unknown', "source": str(directory)})
        
        # Store DDI variables if available
        if ddi_metadata:
            _store_variables(conn, dataset_id, ddi_metadata['variables'])
            conn.commit()

    total_files = len(data_files)
    for idx, data_file in enumerate(data_files):
        rel = str(data_file.relative_to(directory)).replace("\\", "/")
        table_name = get_safe_table_name(data_file.stem, db_url, salt=rel)
        full_table_name = f"{schema}.{table_name}"
        
        logger.info(f"Processing file: {data_file.name} -> Table: {full_table_name}")
        
        # Update job status for current file
        if job_id:
            # Update specific file status to 'processing'
            # We need to fetch current files list, update it, and save back
            # Or assume we can rebuild it or find it.
            # Ideally update_job handles full replace, so we keep a local copy
            for f in file_status_list:
                if f["name"] == data_file.name:
                    f["status"] = "processing"
            update_job(job_id, message=f"Processing {data_file.name}...", files=file_status_list)

        try:
            with engine.connect() as conn:
                already_uploaded = _table_exists(conn, schema, table_name)

            if already_uploaded:
                logger.info(f"Skipping {data_file.name}: {full_table_name} already exists")
                if job_id:
                    for f in file_status_list:
                        if f["name"] == data_file.name:
                            f["status"] = "skipped"
                            f["rows"] = 0
                            f["error"] = "Already uploaded"
                    current_progress = 20 + int(80 * (idx + 1) / total_files)
                    update_job(job_id, progress=current_progress, files=file_status_list, message=f"Skipped {data_file.name} (already uploaded)")
                continue

            df = _load_data_file(data_file, ddi_metadata)
            
            if df is not None:
                # Validate against DDI if available
                if ddi_metadata:
                    ddi_name_map = {_normalize_col_name(v.name): v.name for v in ddi_metadata['variables']}
                    rename_map: Dict[str, str] = {}
                    used_targets = set()
                    for col in list(df.columns):
                        norm = _normalize_col_name(col)
                        target = ddi_name_map.get(norm)
                        if target and col != target and target not in used_targets and target not in df.columns:
                            rename_map[col] = target
                            used_targets.add(target)
                    if rename_map:
                        df = df.rename(columns=rename_map)

                    # For CSV/XLSX/SAV, ensure columns match DDI to some extent
                    if data_file.suffix.lower() in ['.csv', '.xlsx', '.sav']:
                        ddi_var_names = set(v.name for v in ddi_metadata['variables'])
                        df_columns = set(df.columns)
                        
                        # Check for missing variables
                        # It's possible the dataset is a subset, so we don't reject if data is subset of DDI.
                        # But if data has columns NOT in DDI, or DDI has columns NOT in data?
                        # "Reject files that contradict DDI structure"
                        
                        # We will log warnings for mismatches
                        missing_in_data = ddi_var_names - df_columns
                        extra_in_data = df_columns - ddi_var_names
                        
                        if len(missing_in_data) > 0:
                            logger.warning(f"File {data_file.name} is missing {len(missing_in_data)} variables defined in DDI.")
                        
                        if len(extra_in_data) > 0:
                            logger.warning(f"File {data_file.name} has {len(extra_in_data)} columns not in DDI.")

                    df = _enforce_ddi_types(df, ddi_metadata['variables'])

                # Upload to DB
                if job_id:
                     for f in file_status_list:
                        if f["name"] == data_file.name:
                            f["status"] = "loading_db"
                     update_job(job_id, files=file_status_list)

                df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
                
                row_count = len(df)
                logger.info(f"Uploaded {row_count} rows to {full_table_name}")
                
                # Log file
                with engine.connect() as conn:
                    existing_file = conn.execute(
                        text(
                            """
                            SELECT 1
                            FROM dataset_files
                            WHERE dataset_id = :did
                              AND filename = :fname
                            LIMIT 1
                            """
                        ),
                        {"did": dataset_id, "fname": data_file.name},
                    ).fetchone()
                    if not existing_file:
                        conn.execute(
                            text(
                                """
                                INSERT INTO dataset_files (dataset_id, filename, file_type)
                                VALUES (:did, :fname, :ftype)
                                """
                            ),
                            {"did": dataset_id, "fname": data_file.name, "ftype": data_file.suffix},
                        )
                    
                    # Link variables to table/column (update mapping)
                    # This assumes column names match DDI variable names
                    if ddi_metadata:
                            for col in df.columns:
                                conn.execute(text("""
                                    UPDATE variables
                                    SET table_name = :tname, column_name = :cname
                                    WHERE dataset_id = :did AND lower(variable_id) = lower(:vid)
                                """), {
                                    "tname": table_name,
                                    "cname": col,
                                    "did": dataset_id,
                                    "vid": f"{dataset_id}_{col}"
                                })
                    conn.commit()

                uploaded_tables.append(full_table_name)
                logger.info(f"Successfully uploaded {full_table_name}")

                if job_id:
                    for f in file_status_list:
                        if f["name"] == data_file.name:
                            f["status"] = "completed"
                            f["rows"] = row_count
                    # Calculate overall progress
                    # 20% + (80% * (idx + 1) / total_files)
                    current_progress = 20 + int(80 * (idx + 1) / total_files)
                    update_job(job_id, progress=current_progress, files=file_status_list)
            
        except Exception as e:
            logger.error(f"Failed to process {data_file.name}: {e}")
            if job_id:
                for f in file_status_list:
                    if f["name"] == data_file.name:
                        f["status"] = "failed"
                        f["error"] = str(e)
                update_job(job_id, files=file_status_list, error=f"File {data_file.name} failed: {str(e)}")
            continue

    if job_id:
        update_job(job_id, status="completed", progress=100, message="Upload complete!")

    return uploaded_tables

async def ingest_directory(conn, root_dir: Path, db_url: str, schema: str):
    """
    Adapter for NADA routes to use the new pipeline.
    Ignores 'conn' as we use SQLAlchemy engine created from db_url.
    """
    return await process_directory(root_dir, db_url, schema)

def _store_variables(conn, dataset_id: str, variables: List[DDIVariable]):
    """Stores variable metadata into the database."""
    for var in variables:
        vid = f"{dataset_id}_{var.name}" # Unique ID
        
        # Insert Variable
        conn.execute(text("""
            INSERT INTO variables (variable_id, dataset_id, label, data_type, start_pos, width, decimals, universe, question_text, concept)
            VALUES (:vid, :did, :label, :dtype, :start, :width, :dec, :univ, :q, :concept)
            ON CONFLICT (variable_id) DO UPDATE SET
                label = EXCLUDED.label,
                data_type = EXCLUDED.data_type,
                concept = EXCLUDED.concept
        """), {
            "vid": vid,
            "did": dataset_id,
            "label": var.label,
            "dtype": var.data_type,
            "start": var.start_pos,
            "width": var.width,
            "dec": var.decimals,
            "univ": var.universe,
            "q": var.question,
            "concept": var.concept
        })
        
        # Insert Categories
        for cat in var.categories:
            conn.execute(text("""
                INSERT INTO variable_categories (variable_id, category_code, category_label, frequency)
                VALUES (:vid, :code, :label, :freq)
            """), {
                "vid": vid, 
                "code": cat['code'], 
                "label": cat['label'],
                "freq": cat['frequency']
            })
            
        # Insert Missing Values
        for mv in var.missing_values:
            conn.execute(text("""
                INSERT INTO variable_missing_values (variable_id, missing_value)
                VALUES (:vid, :val)
            """), {"vid": vid, "val": mv})

def _load_data_file(file_path: Path, ddi_metadata: Optional[Dict]) -> Optional[pd.DataFrame]:
    """Loads data file into DataFrame using appropriate method."""
    ext = file_path.suffix.lower()
    
    if ext == '.txt':
        if not ddi_metadata:
            raise ValueError(f"Cannot parse fixed-width file {file_path.name} without DDI metadata")
        return _parse_fixed_width(file_path, ddi_metadata['variables'])
    
    elif ext == '.sav':
        df, meta = pyreadstat.read_sav(str(file_path))
        return df
    
    elif ext == '.csv':
        # Read as string to preserve formatting (e.g. leading zeros)
        skip = _count_leading_blank_csv_rows(file_path)
        df = pd.read_csv(file_path, low_memory=False, dtype=str, skiprows=skip, skip_blank_lines=True)
        df = _maybe_format_month_year_columns(df)
        return df
        
    elif ext == '.xlsx':
        # Read as string to preserve formatting
        skip = _count_leading_blank_excel_rows(file_path)
        df = pd.read_excel(file_path, dtype=str, skiprows=skip)
        df = _maybe_format_month_year_columns(df)
        return df
        
    return None

def _parse_fixed_width(file_path: Path, variables: List[DDIVariable]) -> pd.DataFrame:
    """Parses fixed-width text file using DDI variable positions."""
    colspecs = []
    names = []
    
    # Filter variables that have start_pos and width
    valid_vars = [v for v in variables if v.start_pos is not None and v.width is not None]
    
    # Sort by start position to be safe
    valid_vars.sort(key=lambda v: v.start_pos)
    
    for var in valid_vars:
        # DDI is 1-based usually, python is 0-based
        start = var.start_pos - 1
        end = start + var.width
        colspecs.append((start, end))
        names.append(var.name)
        
    if not colspecs:
        raise ValueError("No valid variable positions found in DDI for fixed-width parsing")
        
    # Read file
    # Use 'dtype=str' initially to avoid pandas inferring wrong types, we will cast later
    df = pd.read_fwf(file_path, colspecs=colspecs, names=names, dtype=str)
    return df

def _enforce_ddi_types(df: pd.DataFrame, variables: List[DDIVariable]) -> pd.DataFrame:
    """Casts DataFrame columns to types specified in DDI."""
    for var in variables:
        if var.name in df.columns:
            if var.data_type == 'numeric':
                df[var.name] = pd.to_numeric(df[var.name], errors='coerce')
    return df
