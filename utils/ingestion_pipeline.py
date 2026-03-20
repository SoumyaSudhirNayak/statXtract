import os
import shutil
import zipfile
import tempfile
import subprocess
import json
import asyncio
import time
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
from utils.job_manager import (
    update_job,
    JOB_STATUS_QUEUED,
    JOB_STATUS_PROCESSING,
    JOB_STATUS_CONVERTING,
    JOB_STATUS_PARSING_DDI,
    JOB_STATUS_INGESTING,
    JOB_STATUS_COMPLETED,
    JOB_STATUS_FAILED,
)
from utils.db_utils import (
    ensure_dataset_schema_tables,
    make_dataset_schema_name,
    to_snake_case_identifier,
    schema_exists,
    ensure_metadata_tables,
)
 
# Configure logging with custom format for terminal visibility
log_format = "%(asctime)s [statXtract] %(levelname)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger(__name__)


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def get_file_checksum(file_path: str) -> str:
    """Calculate SHA256 checksum of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def log_terminal(msg: str, level: str = "info"):
    """Enhanced terminal logging with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_msg = f"[{timestamp}] {msg}"
    if level == "info":
        print(f"\033[94mℹ️ {formatted_msg}\033[0m")
        logger.info(msg)
    elif level == "success":
        print(f"\033[92m✅ {formatted_msg}\033[0m")
        logger.info(msg)
    elif level == "warning":
        print(f"\033[93m⚠️ {formatted_msg}\033[0m")
        logger.warning(msg)
    elif level == "error":
        print(f"\033[91m❌ {formatted_msg}\033[0m")
        logger.error(msg)


def _dataset_root(upload_root: Path, schema: str, dataset: str) -> Path:
    return (upload_root / schema / dataset).resolve()


def _apply_ddi_labels(df: pd.DataFrame, ddi_vars: Dict[str, DDIVariable]) -> pd.DataFrame:
    """Applies category labels from DDI to DataFrame values."""
    # Create a mapping of normalized column names to original column names in df
    norm_to_orig = {str(c).strip().lower(): c for c in df.columns}

    for var_name, var in ddi_vars.items():
        norm_var_name = str(var_name).strip().lower()
        if norm_var_name in norm_to_orig:
            col = norm_to_orig[norm_var_name]
            if var.categories:
                # Create mapping: {code: label}
                mapping = {}
                for cat in var.categories:
                    code = cat.get("code")
                    label = cat.get("label")
                    if code is not None and label:
                        # Store multiple versions of the code for better matching
                        c_str = str(code).strip()
                        mapping[c_str] = label
                        try:
                            # If it's "01", mapping["1.0"] and mapping[1.0] should also work
                            mapping[str(float(code))] = label
                            mapping[float(code)] = label
                        except (ValueError, TypeError):
                            pass
                        try:
                            # mapping[1] should also work
                            mapping[int(float(code))] = label
                        except (ValueError, TypeError):
                            pass

                if mapping:
                    s = df[col]
                    # Direct mapping
                    mapped = s.map(mapping)
                    
                    # Try mapping after string normalization for remaining NaNs
                    still_na = mapped.isna() & s.notna()
                    if still_na.any():
                        # Try matching stripped strings
                        mapped.update(s[still_na].astype(str).str.strip().map(mapping))
                        
                        # Try matching stripped strings without leading zeros (e.g. "01" -> "1")
                        still_na = mapped.isna() & s.notna()
                        if still_na.any():
                            def _strip_leading_zeros(v):
                                try:
                                    return str(int(float(v)))
                                except:
                                    return str(v).strip()
                            mapped.update(s[still_na].apply(_strip_leading_zeros).map(mapping))
                    
                    # Only apply if we actually mapped something
                    if mapped.notna().any():
                        df[col] = mapped.fillna(s)
    return df


def _profile_series(series: pd.Series) -> dict[str, Any]:
    s = series
    if s is None:
        return {"unique_count": 0}
    try:
        unique_count = int(s.nunique(dropna=True))
    except Exception:
        unique_count = None

    if pd.api.types.is_numeric_dtype(s):
        clean = pd.to_numeric(s, errors="coerce")
        return {
            "mean": float(clean.mean()) if clean.notna().any() else None,
            "min": float(clean.min()) if clean.notna().any() else None,
            "max": float(clean.max()) if clean.notna().any() else None,
            "stddev": float(clean.std()) if clean.notna().any() else None,
            "unique_count": unique_count,
        }

    return {"unique_count": unique_count}


def _infer_and_convert_column(series: pd.Series, ddi_hint: str | None) -> tuple[pd.Series, str]:
    hint = (ddi_hint or "").strip().lower()
    if hint in {"numeric", "integer", "float", "double"}:
        numeric = pd.to_numeric(series, errors="coerce")
        ratio = float(numeric.notna().mean()) if len(series) else 0.0
        if ratio >= 0.9:
            as_int = numeric.dropna()
            if not as_int.empty and (as_int % 1 == 0).all():
                return numeric.astype("Int64"), "INTEGER"
            return numeric.astype(float), "FLOAT"

    numeric = pd.to_numeric(series, errors="coerce")
    ratio = float(numeric.notna().mean()) if len(series) else 0.0
    if ratio >= 0.9:
        as_int = numeric.dropna()
        if not as_int.empty and (as_int % 1 == 0).all():
            return numeric.astype("Int64"), "INTEGER"
        return numeric.astype(float), "FLOAT"

    return series.astype(str).where(series.notna(), None), "TEXT"


async def ingest_upload_file(
    input_path: str,
    db_url: str,
    schema: str,
    year: str,
    dataset_display_name: str,
    dataset_db_name: str,
    job_id: str,
) -> list[str]:
    upload_root = Path(os.getenv("UPLOAD_DIR") or "uploads").resolve()
    dataset_db = to_snake_case_identifier(dataset_db_name or dataset_display_name) or "dataset"
    dataset_root = _dataset_root(upload_root, schema, dataset_db)
    raw_dir = dataset_root / "raw_files"
    processed_dir = dataset_root / "processed"
    _ensure_dir(raw_dir)
    _ensure_dir(processed_dir)

    dataset_schema = make_dataset_schema_name(schema, dataset_db)
    ensure_dataset_schema_tables(dataset_schema, db_url)

    src = Path(input_path).resolve()
    raw_dest = raw_dir / src.name
    try:
        shutil.copy2(src, raw_dest)
    except Exception:
        raw_dest = src

    ext = src.suffix.lower()

    ddi_path: Path | None = None
    processed_files: list[Path] = []

    update_job(job_id, status=JOB_STATUS_PROCESSING, current_state=JOB_STATUS_PROCESSING, message="Preparing files...")
    log_terminal(f"Starting ingestion for dataset: {dataset_display_name} (Job: {job_id})")

    # Load manifest for duplicate detection
    manifest_path = processed_dir / "manifest.json"
    manifest = {}
    if manifest_path.exists():
        try:
            with open(manifest_path, "r") as f:
                manifest = json.load(f)
        except Exception:
            manifest = {}

    if ext == ".nesstar":
        log_terminal(f"Metadata detected: {src.name} (Nesstar Study)", "success")
        update_job(job_id, status=JOB_STATUS_CONVERTING, current_state=JOB_STATUS_CONVERTING, message="Converting Nesstar to SPSS...")
        
        from utils.nesstar_orchestrator import run_nesstar_conversion_pipeline
        # Use fixed root-level export_nesstar path — this is where Nesstar Explorer
        # actually saves exported files regardless of the dataset subdirectory.
        export_nesstar_dir = upload_root / "export_nesstar"
        _ensure_dir(export_nesstar_dir)
        
        # Run conversion orchestration: AutoIt (Data) -> Python Watcher -> AutoIt (DDI)
        run_info = await run_nesstar_conversion_pipeline(job_id, str(raw_dest), str(export_nesstar_dir), schema)
        
        # NOTE: 'returncode' can be 0 (falsy!) so we must check explicitly with is not None
        rc = run_info.get("returncode")
        if rc is None or int(rc) != 0:
            err = run_info.get('error') or 'Unknown Error'
            log_terminal(f"Nesstar conversion failed: {err}", "error")
            raise ValueError(f"Nesstar conversion failed: {err}")

        # ── Redirect exported files into the SAME pipeline as ZIP upload ──
        # Treat export_nesstar/ exactly like extract_dir from a ZIP:
        #   1) Find DDI XML → copy to ddi_path
        #   2) Find data files → copy to processed_dir
        #   3) Fall through to the unified ingestion block (shared with ZIP)

        log_terminal("Nesstar export complete. Feeding exported files into standard ingestion pipeline...")
        update_job(job_id, status=JOB_STATUS_PARSING_DDI, current_state=JOB_STATUS_PARSING_DDI,
                   message="Scanning exported files for DDI and datasets...")

        # Scan for DDI XML in exported folder
        nesstar_ddi_candidates = [
            p for p in export_nesstar_dir.rglob("*")
            if p.is_file() and p.suffix.lower() in {".xml", ".nsdstat"}
        ]
        nesstar_ddi_candidates.sort(key=lambda p: 0 if p.suffix.lower() == ".nsdstat" else 1)
        if nesstar_ddi_candidates:
            log_terminal(f"DDI metadata found in export: {nesstar_ddi_candidates[0].name}", "success")
            fixed_ddi = dataset_root / "ddi.xml"
            shutil.copy2(nesstar_ddi_candidates[0], fixed_ddi)
            ddi_path = fixed_ddi

        # Scan for data files and copy them to processed_dir (same as ZIP extraction)
        for p in export_nesstar_dir.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() in {".csv", ".xlsx", ".txt", ".sav", ".por"}:
                checksum = get_file_checksum(str(p))
                if p.name in manifest and manifest[p.name]["checksum"] == checksum:
                    log_terminal(f"Skipping duplicate file: {p.name} (already uploaded)", "warning")
                    update_job(job_id, processed_file={"name": p.name, "status": "duplicate", "message": "Already uploaded"})
                    continue

                dest = processed_dir / p.name
                shutil.copy2(p, dest)
                processed_files.append(dest)
                manifest[p.name] = {
                    "checksum": checksum,
                    "size": p.stat().st_size,
                    "timestamp": datetime.now().isoformat()
                }

        # Cleanup Nesstar export folder CONTENTS (keep the folder itself for next upload)
        try:
            for item in os.listdir(str(export_nesstar_dir)):
                item_path = os.path.join(str(export_nesstar_dir), item)
                try:
                    if os.path.isfile(item_path) or os.path.islink(item_path):
                        os.unlink(item_path)
                    elif os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                except Exception as e:
                    log_terminal(f"Cleanup warning (file {item}): {e}", "warning")
            log_terminal("Cleaned up export_nesstar folder contents.")
        except Exception as e:
            log_terminal(f"Cleanup warning (export dir): {e}", "warning")

        try:
            os.remove(str(raw_dest))
            log_terminal(f"Cleaned up source .Nesstar file: {raw_dest.name}")
        except Exception as e:
            log_terminal(f"Cleanup warning (source file): {e}", "warning")

        # ── Falls through to the unified ingestion block below (line 336+) ──
        # processed_files and ddi_path are now populated exactly like a ZIP extraction.
        # The rest of the pipeline (DDI parsing, type inference, table creation,
        # metadata storage, variable profiling) runs identically to ZIP upload.


    elif ext == ".zip":
        log_terminal(f"Processing ZIP archive: {src.name}")
        extract_dir = raw_dir / "extracted"
        _ensure_dir(extract_dir)
        with zipfile.ZipFile(raw_dest, "r") as zf:
            zf.extractall(extract_dir)

        ddi_candidates = [p for p in extract_dir.rglob("*") if p.is_file() and p.suffix.lower() in {".xml", ".nsdstat"}]
        ddi_candidates.sort(key=lambda p: 0 if p.suffix.lower() == ".nsdstat" else 1)
        if ddi_candidates:
            log_terminal(f"Metadata found in ZIP: {ddi_candidates[0].name}", "success")
            fixed_ddi = dataset_root / "ddi.xml"
            shutil.copy2(ddi_candidates[0], fixed_ddi)
            ddi_path = fixed_ddi

        for p in extract_dir.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() in {".csv", ".xlsx", ".txt", ".sav", ".por"}:
                # Duplicate detection
                checksum = get_file_checksum(str(p))
                if p.name in manifest and manifest[p.name]["checksum"] == checksum:
                    log_terminal(f"Skipping duplicate file: {p.name} (already uploaded)", "warning")
                    update_job(job_id, processed_file={"name": p.name, "status": "duplicate", "message": "Already uploaded"})
                    continue
                
                dest = processed_dir / p.name
                shutil.copy2(p, dest)
                processed_files.append(dest)
                manifest[p.name] = {
                    "checksum": checksum,
                    "size": p.stat().st_size,
                    "timestamp": datetime.now().isoformat()
                }

    else:
        # Single file upload
        checksum = get_file_checksum(str(raw_dest))
        if src.name in manifest and manifest[src.name]["checksum"] == checksum:
            log_terminal(f"Skipping duplicate file: {src.name} (already uploaded)", "warning")
            # We still need to mark job as completed if it was a single file
            update_job(job_id, status=JOB_STATUS_COMPLETED, progress=100, message="File already exists, skipped.", processed_file={"name": src.name, "status": "duplicate", "message": "Already uploaded"})
            return []

        dest = processed_dir / src.name
        shutil.copy2(raw_dest, dest)
        processed_files.append(dest)
        manifest[src.name] = {
            "checksum": checksum,
            "size": src.stat().st_size,
            "timestamp": datetime.now().isoformat()
        }

    # Save manifest
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    processed_files = [p for p in processed_files if p.exists() and p.stat().st_size > 0]
    if not processed_files:
        if any(m for m in manifest):
             log_terminal("All files were duplicates and skipped", "warning")
             update_job(job_id, status=JOB_STATUS_COMPLETED, progress=100, message="All files skipped (duplicates)")
             return []
        raise ValueError("No data files found to ingest")

    update_job(job_id, status=JOB_STATUS_PARSING_DDI, current_state=JOB_STATUS_PARSING_DDI, message="Parsing DDI metadata...")
    ddi_meta: dict[str, Any] | None = None
    ddi_vars_by_name: dict[str, DDIVariable] = {}
    if ddi_path and ddi_path.exists() and ddi_path.stat().st_size > 0:
        ddi_meta = parse_ddi_xml(str(ddi_path))
        for v in ddi_meta.get("variables") or []:
            ddi_vars_by_name[str(v.name)] = v

    update_job(job_id, status=JOB_STATUS_INGESTING, current_state=JOB_STATUS_INGESTING, message="Ingesting into PostgreSQL...")
    log_terminal(f"Starting database ingestion for {len(processed_files)} files")
    engine = create_engine(db_url)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO dataset_registry (survey_schema, dataset_schema, dataset_display_name)
                VALUES (:survey, :ds_schema, :ds_name)
                ON CONFLICT (dataset_schema) DO UPDATE SET dataset_display_name = EXCLUDED.dataset_display_name
                """
            ),
            {"survey": schema, "ds_schema": dataset_schema, "ds_name": dataset_display_name},
        )

    if ddi_meta:
        keywords = ddi_meta.get("keywords")
        if isinstance(keywords, list):
            keywords = ", ".join([str(x) for x in keywords if x is not None and str(x).strip()])
        coverage = ddi_meta.get("coverage") or {}
        file_desc = ddi_meta.get("file_description") or {}
        try:
            case_count = int(file_desc.get("case_count")) if file_desc.get("case_count") is not None else None
        except Exception:
            case_count = None
        try:
            var_count = int(file_desc.get("variable_count")) if file_desc.get("variable_count") is not None else None
        except Exception:
            var_count = None

        with engine.begin() as conn:
            conn.execute(
                text(f'DELETE FROM "{dataset_schema}".dataset_metadata'),
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO "{dataset_schema}".dataset_metadata
                        (survey_schema, dataset_display_name, survey_id, title, abstract, keywords,
                         geographic_coverage, industrial_coverage, product_coverage,
                         weighting, frequency, methodology,
                         collection_mode, time_method, procedures,
                         producer, ddi_id, file_case_count, file_variable_count)
                    VALUES
                        (:survey_schema, :dataset_display_name, :survey_id, :title, :abstract, :keywords,
                         :geo, :ind, :prod,
                         :weighting, :frequency, :methodology,
                         :mode, :time_method, :procedures,
                         :producer, :ddi_id, :cases, :vars)
                    """
                ),
                {
                    "survey_schema": schema,
                    "dataset_display_name": dataset_display_name,
                    "survey_id": ddi_meta.get("survey_id"),
                    "title": ddi_meta.get("title"),
                    "abstract": ddi_meta.get("abstract"),
                    "keywords": keywords,
                    "geo": coverage.get("geographic_coverage"),
                    "ind": coverage.get("industrial_coverage"),
                    "prod": coverage.get("product_coverage"),
                    "weighting": ddi_meta.get("weighting"),
                    "frequency": ddi_meta.get("frequency"),
                    "methodology": ddi_meta.get("methodology"),
                    "mode": ddi_meta.get("collection_mode"),
                    "time_method": ddi_meta.get("time_method"),
                    "procedures": ddi_meta.get("procedures"),
                    "producer": ddi_meta.get("producer"),
                    "ddi_id": ddi_meta.get("ddi_id"),
                    "cases": case_count,
                    "vars": var_count,
                },
            )

    created_tables: list[str] = []
    for i, data_file in enumerate(processed_files):
        try:
            level_display = data_file.stem
            level_db = to_snake_case_identifier(level_display) or "level"
            table_name = get_safe_table_name(level_db, db_url)

            # Update progress (starts at 50% after file prep)
            prog = 50 + int((i / len(processed_files)) * 40)
            update_job(job_id, progress=prog, message=f"Ingesting {data_file.name}...")

            df = _load_data_file(data_file, ddi_meta)
            if df is None or df.empty:
                log_terminal(f"Skipping empty or invalid file: {data_file.name}", "warning")
                update_job(job_id, processed_file={"name": data_file.name, "status": "failed", "message": "Empty or invalid"})
                continue

            # Apply category labels from DDI if available
            if ddi_vars_by_name:
                df = _apply_ddi_labels(df, ddi_vars_by_name)

            final_types: dict[str, str] = {}
            for col in list(df.columns):
                hint = None
                if col in ddi_vars_by_name:
                    hint = ddi_vars_by_name[col].data_type
                converted, ftype = _infer_and_convert_column(df[col], hint)
                df[col] = converted
                final_types[col] = ftype

            with engine.begin() as conn:
                df.to_sql(table_name, conn, schema=dataset_schema, if_exists="replace", index=False)
                log_terminal(f"File upload successful: {data_file.name} -> {dataset_schema}.{table_name} ({len(df)} rows)", "success")
                update_job(job_id, processed_file={"name": data_file.name, "status": "success", "message": f"Ingested {len(df)} rows"})

                conn.execute(text(f'DELETE FROM "{dataset_schema}".variables WHERE table_name = :t'), {"t": table_name})
                conn.execute(text(f'DELETE FROM "{dataset_schema}".variable_statistics WHERE table_name = :t'), {"t": table_name})
                conn.execute(text(f'DELETE FROM "{dataset_schema}".variable_categories WHERE table_name = :t'), {"t": table_name})

                for col in df.columns:
                    dv = ddi_vars_by_name.get(col)
                    ddi_type = dv.data_type if dv else None
                    width = dv.width if dv else None
                    interval = getattr(dv, "interval", None) if dv else None

                    stats = _profile_series(df[col])
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO "{dataset_schema}".variables
                                (table_name, variable_name, label, ddi_type, width, interval, valid_count, invalid_count, final_type)
                            VALUES
                                (:tname, :vname, :label, :ddi_type, :width, :interval, :valid, :invalid, :final_type)
                            """
                        ),
                        {
                            "tname": table_name,
                            "vname": str(col),
                            "label": getattr(dv, "label", None) if dv else None,
                            "ddi_type": ddi_type,
                            "width": width,
                            "interval": interval,
                            "valid": int(df[col].notna().sum()),
                            "invalid": int(df[col].isna().sum()),
                            "final_type": final_types.get(col),
                        },
                    )

                    conn.execute(
                        text(
                            f"""
                            INSERT INTO "{dataset_schema}".variable_statistics
                                (table_name, variable_name, mean, min, max, stddev, unique_count)
                            VALUES
                                (:tname, :vname, :mean, :min, :max, :stddev, :uniq)
                            ON CONFLICT (table_name, variable_name)
                            DO UPDATE SET mean = EXCLUDED.mean,
                                          min = EXCLUDED.min,
                                          max = EXCLUDED.max,
                                          stddev = EXCLUDED.stddev,
                                          unique_count = EXCLUDED.unique_count
                            """
                        ),
                        {
                            "tname": table_name,
                            "vname": str(col),
                            "mean": stats.get("mean"),
                            "min": stats.get("min"),
                            "max": stats.get("max"),
                            "stddev": stats.get("stddev"),
                            "uniq": stats.get("unique_count"),
                        },
                    )

                if ddi_meta:
                    for dv in ddi_meta.get("variables") or []:
                        for cat in dv.categories or []:
                            conn.execute(
                                text(
                                    f"""
                                    INSERT INTO "{dataset_schema}".variable_categories
                                        (table_name, variable_name, value, label, frequency)
                                    VALUES
                                        (:tname, :vname, :val, :label, :freq)
                                    """
                                ),
                                {
                                    "tname": table_name,
                                    "vname": dv.name,
                                    "val": str(cat.get("code")) if cat.get("code") is not None else None,
                                    "label": cat.get("label"),
                                    "freq": cat.get("frequency"),
                                },
                            )

            created_tables.append(table_name)
        except Exception as e:
            log_terminal(f"Failed to ingest file {data_file.name}: {e}", "error")
            update_job(job_id, processed_file={"name": data_file.name, "status": "failed", "message": str(e)})
            continue

    if not created_tables:
        raise ValueError("No tables were ingested")

    update_job(job_id, status=JOB_STATUS_COMPLETED, current_state=JOB_STATUS_COMPLETED, progress=100, message="Upload completed successfully")
    return created_tables

def _clean_windows_path_candidate(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    if "," in s:
        left, right = s.rsplit(",", 1)
        if right.strip().lstrip("-").isdigit():
            s = left.strip()
            if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
                s = s[1:-1].strip()
    return s


def _extract_exe_from_windows_command(command: str) -> str:
    s = (command or "").strip()
    if not s:
        return ""
    if s.startswith('"'):
        end = s.find('"', 1)
        if end > 1:
            return s[1:end]
        return ""
    head = s.split(" ", 1)[0].strip()
    return head


def discover_nesstar_converter_exe() -> str:
    env = (os.getenv("NESSTAR_CONVERTER_EXE") or "").strip()
    if env and os.path.exists(env):
        return env

    if os.name != "nt":
        return ""

    try:
        import winreg  # type: ignore
    except Exception:
        return ""

    exe_names = [
        "Nesstar Explorer.exe",
        "NesstarExplorer.exe",
        "Nesstar Publisher.exe",
        "NesstarPublisher.exe",
    ]

    candidates: List[str] = []

    for root_env in (os.environ.get("ProgramFiles"), os.environ.get("ProgramFiles(x86)"), os.environ.get("ProgramW6432")):
        if not root_env:
            continue
        for folder in ("Nesstar Explorer", "NesstarExplorer", "Nesstar Publisher", "NesstarPublisher", "Nesstar"):
            for exe_name in exe_names:
                candidates.append(os.path.join(root_env, folder, exe_name))

    def _reg_query_str(key, name: str) -> str:
        try:
            v, _t = winreg.QueryValueEx(key, name)
            return str(v)
        except Exception:
            return ""

    def _try_add_path(s: str):
        p = _clean_windows_path_candidate(s)
        if p:
            candidates.append(p)

    for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
        for exe_name in exe_names:
            subkey = r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\{}".format(exe_name)
            try:
                with winreg.OpenKey(hive, subkey) as k:
                    _try_add_path(_reg_query_str(k, ""))
                    _try_add_path(_reg_query_str(k, "Path"))
            except Exception:
                pass

    for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
        for base in (
            r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall",
            r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall",
        ):
            try:
                with winreg.OpenKey(hive, base) as uninstall:
                    i = 0
                    while True:
                        try:
                            sub = winreg.EnumKey(uninstall, i)
                        except OSError:
                            break
                        i += 1
                        try:
                            with winreg.OpenKey(uninstall, sub) as appkey:
                                name = (_reg_query_str(appkey, "DisplayName") or "").strip()
                                if "nesstar" not in name.lower():
                                    continue
                                install_loc = (_reg_query_str(appkey, "InstallLocation") or "").strip()
                                display_icon = (_reg_query_str(appkey, "DisplayIcon") or "").strip()
                                if display_icon:
                                    _try_add_path(_extract_exe_from_windows_command(display_icon))
                                    _try_add_path(display_icon)
                                if install_loc:
                                    for exe_name in exe_names:
                                        candidates.append(os.path.join(install_loc, exe_name))
                        except Exception:
                            continue
            except Exception:
                pass

    for exe_name in exe_names:
        for hkcr_path in (
            rf"Applications\{exe_name}\shell\open\command",
            rf"Applications\{exe_name}\shell\Open\command",
        ):
            try:
                with winreg.OpenKey(winreg.HKEY_CLASSES_ROOT, hkcr_path) as k:
                    cmd = _reg_query_str(k, "")
                    _try_add_path(_extract_exe_from_windows_command(cmd))
            except Exception:
                pass

    seen = set()
    for c in candidates:
        p = _clean_windows_path_candidate(c)
        if not p:
            continue
        lp = p.lower()
        if lp in seen:
            continue
        seen.add(lp)
        if os.path.exists(p) and os.path.isfile(p):
            return p

    return ""


def _hash_file_sha1(file_path: str, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha1()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()

_NESSTAR_STAGE_PROGRESS: Dict[str, int] = {
    "QUEUED": 0,
    "CONVERTING_WITH_NESSTAR": 15,
    "EXPORTING_METADATA": 35,
    "EXPORT_DIALOG_OPENED": 40,
    "EXPORTING_ALL_DATASETS": 45,
    "SAVING_DATASETS": 55,
    "VALIDATING_EXPORTED_FILES": 75,
    "EXPORT_COMPLETE": 80,
    "CONVERSION_COMPLETE": 80,
    "INGESTING": 85,
    "COMPLETED": 100,
    "FAILED": 100,
}

_NESSTAR_STAGE_MESSAGE: Dict[str, str] = {
    "QUEUED": "Queued for conversion",
    "CONVERTING_WITH_NESSTAR": "Opening Nesstar study…",
    "EXPORT_DIALOG_OPENED": "Export dialog opened — waiting for Save confirmation…",
    "SAVING_DATASETS": "Saving exported dataset to workspace…",
    "EXPORTING_METADATA": "Exporting metadata (DDI)…",
    "EXPORTING_ALL_DATASETS": "Exporting ALL datasets (Shift+Ctrl+E)…",
    "VALIDATING_EXPORTED_FILES": "Export confirmed — validating file…",
    "EXPORT_COMPLETE": "Export complete — preparing ingestion…",
    "CONVERSION_COMPLETE": "Conversion complete — preparing ingestion…",
    "INGESTING": "Loading into PostgreSQL…",
    "COMPLETED": "Dataset ready for querying",
}

def _set_nesstar_stage(job_id: str, stage: str, message: Optional[str] = None):
    stg = (stage or "").strip().upper()
    pct = _NESSTAR_STAGE_PROGRESS.get(stg, None)
    msg = message or _NESSTAR_STAGE_MESSAGE.get(stg) or stg
    
    # Determine high-level state based on Nesstar internal stage
    new_state = "processing"
    if stg == "QUEUED":
        new_state = JOB_STATUS_QUEUED
    elif stg == "CONVERTING_WITH_NESSTAR":
        new_state = JOB_STATUS_CONVERTING
    elif stg in ("EXPORTING_METADATA", "EXPORT_DIALOG_OPENED", "EXPORTING_ALL_DATASETS", "SAVING_DATASETS", "VALIDATING_EXPORTED_FILES", "EXPORT_COMPLETE", "CONVERSION_COMPLETE"):
        new_state = JOB_STATUS_EXPORTING
    elif stg == "INGESTING":
        new_state = JOB_STATUS_INGESTING
    elif stg == "COMPLETED":
        new_state = JOB_STATUS_COMPLETED
    elif stg == "FAILED":
        new_state = JOB_STATUS_FAILED
        
    update_kwargs = {
        "status": new_state,
        "current_state": new_state,
        "message": msg
    }
    if pct is not None:
        update_kwargs["progress"] = pct
    
    if new_state == JOB_STATUS_FAILED:
         # Ensure progress is 100 on fail? Or keep it?
         # User requirement: "Do not continue polling once a terminal state is reached."
         update_kwargs["progress"] = 100
         
    update_job(job_id, **update_kwargs)

def _run_nesstar_converter_streaming(job_id: str, input_path: str, output_dir: str, timeout_sec: int, schema: str) -> Dict[str, Any]:
    if not schema or not schema.strip():
        raise ValueError("Schema must be provided for Nesstar conversion")
        
    exe = (os.getenv("NESSTAR_CONVERTER_EXE") or "").strip()
    if exe and not os.path.exists(exe):
        exe = ""
    if not exe:
        exe = discover_nesstar_converter_exe()
        if exe:
            os.environ.setdefault("NESSTAR_CONVERTER_EXE", exe)
    this_file = Path(__file__).resolve()
    project_root = this_file.parent.parent
    default_script = str((project_root / "utils" / "nesstar_convert.ps1").resolve())
    script = (os.getenv("NESSTAR_CONVERTER_SCRIPT") or default_script).strip()
    if not exe:
        raise ValueError("NESSTAR_CONVERTER_EXE not configured")
    if not script:
        raise ValueError("NESSTAR_CONVERTER_SCRIPT not configured")
    if not os.path.exists(exe):
        raise ValueError(f"NESSTAR_CONVERTER_EXE does not exist: {exe}")
    if not os.path.exists(script):
        raise ValueError(f"NESSTAR_CONVERTER_SCRIPT does not exist: {script}")

    system_root = os.environ.get("SystemRoot") or r"C:\Windows"
    pwsh_candidates = [
        os.path.join(system_root, "System32", "WindowsPowerShell", "v1.0", "powershell.exe"),
        shutil.which("powershell.exe"),
        shutil.which("powershell"),
    ]
    pwsh = next((p for p in pwsh_candidates if p and os.path.exists(p)), None)
    if not pwsh:
        raise ValueError("PowerShell not found on PATH (expected powershell.exe)")

    ver = subprocess.run(
        [pwsh, "-NoProfile", "-Command", "(Get-Variable -Name PSVersionTable -ValueOnly).PSVersion.ToString()"],
        capture_output=True,
        text=True,
    )
    if ver.returncode != 0:
        raise ValueError(f"PowerShell preflight failed: {ver.stderr.strip() or ver.stdout.strip()}")
    update_job(job_id, log=f"PowerShell path: {pwsh}")
    update_job(job_id, log=f"PowerShell version: {(ver.stdout or '').strip()}")

    step_timeout = int(os.getenv("NESSTAR_STEP_TIMEOUT_SEC") or "900")
    autoit_exe = (os.getenv("NESSTAR_AUTOIT_EXE") or "").strip()
    autoit_script = (os.getenv("NESSTAR_AUTOIT_SCRIPT") or str((project_root / "utils" / "nesstar_export.au3").resolve())).strip()

    cmd = [
        pwsh,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        script,
        "-NesstarExe",
        exe,
        "-InputStudy",
        input_path,
        "-OutputDir",
        output_dir,
        "-StepTimeoutSec",
        str(step_timeout),
        "-JobId",
        job_id,
        "-Schema",
        schema,
    ]
    if autoit_exe:
        cmd.extend(["-AutoItExe", autoit_exe])
    if autoit_script:
        cmd.extend(["-AutoItScript", autoit_script])
    update_job(job_id, log=f"Running command: {json.dumps(cmd)}")

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=output_dir,
        creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )

    stdout_lines: List[str] = []
    stderr_lines: List[str] = []

    def _handle_line(line: str):
        line2 = (line or "").rstrip("\r\n")
        if not line2:
            return
        stdout_lines.append(line2)
        update_job(job_id, log=line2)
        parts = line2.split()
        if "STAGE" in parts:
            i = parts.index("STAGE")
            if i + 1 < len(parts):
                _set_nesstar_stage(job_id, parts[i + 1])

    start = datetime.now()
    try:
        if p.stdout:
            while True:
                if (datetime.now() - start).total_seconds() > timeout_sec:
                    raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout_sec)

                line = p.stdout.readline()
                if line:
                    _handle_line(line)
                    continue
                if p.poll() is not None:
                    break
                awaitable_sleep = os.getenv("NESSTAR_STREAM_SLEEP_MS")
                try:
                    ms = int(awaitable_sleep) if awaitable_sleep else 100
                except Exception:
                    ms = 100
                time.sleep(ms / 1000.0)

        out_rest = p.stdout.read() if p.stdout else ""
        if out_rest:
            for ln in out_rest.splitlines():
                _handle_line(ln)

        err_rest = p.stderr.read() if p.stderr else ""
        if err_rest:
            stderr_lines.extend(err_rest.splitlines())
            for ln in err_rest.splitlines()[-200:]:
                update_job(job_id, log=ln)

        rc = p.returncode if p.returncode is not None else p.wait(timeout=5)
        return {"returncode": rc, "stdout": "\n".join(stdout_lines), "stderr": "\n".join(stderr_lines), "pid": p.pid}
    except subprocess.TimeoutExpired:
        try:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], check=False, capture_output=True, text=True)
        except Exception:
            pass
        update_job(job_id, log="Conversion timed out")
        return {"returncode": 124, "stdout": "\n".join(stdout_lines), "stderr": "Conversion timed out", "pid": p.pid}

def _pick_exported_files(output_dir: Path) -> Dict[str, Optional[Path]]:
    savs = list(output_dir.rglob("*.sav"))
    pors = list(output_dir.rglob("*.por"))
    csvs = list(output_dir.rglob("*.csv"))
    xmls = list(output_dir.rglob("*.xml"))

    data_candidates = savs or pors or csvs
    data = None
    if data_candidates:
        data = max(data_candidates, key=lambda p: p.stat().st_size if p.exists() else 0)

    ddi = None
    ddi_candidates = [p for p in xmls if "ddi" in p.name.lower() or "nsd" in p.name.lower()]
    if ddi_candidates:
        ddi = max(ddi_candidates, key=lambda p: p.stat().st_size if p.exists() else 0)
    elif xmls:
        ddi = max(xmls, key=lambda p: p.stat().st_size if p.exists() else 0)

    return {"data": data, "ddi": ddi}

def _count_csv_columns(file_path: Path, max_lines: int = 2000) -> Optional[int]:
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            for _ in range(max_lines):
                line = f.readline()
                if line == "":
                    break
                stripped = line.strip()
                if stripped == "":
                    continue
                stripped2 = stripped.replace(",", "").replace("\t", "").replace(";", "")
                if stripped2 == "":
                    continue
                delimiter = "," if line.count(",") >= line.count(";") else ";"
                return len(line.rstrip("\r\n").split(delimiter))
    except Exception:
        return None
    return None

def _validate_converted_outputs(data_path: Path, ddi_path: Optional[Path]) -> Dict[str, Any]:
    if not data_path or not data_path.exists() or data_path.stat().st_size <= 2048:
        raise ValueError("Converted data file missing or empty")

    data_cols = None
    data_suffix = (data_path.suffix or "").lower()
    if data_suffix == ".csv":
        data_cols = _count_csv_columns(data_path)
    else:
        try:
            _, meta = pyreadstat.read_sav(str(data_path), metadataonly=True)
            data_cols = len(getattr(meta, "column_names", []) or [])
        except Exception:
            try:
                _, meta = pyreadstat.read_por(str(data_path), metadataonly=True)
                data_cols = len(getattr(meta, "column_names", []) or [])
            except Exception:
                data_cols = None

    if not ddi_path or not ddi_path.exists() or ddi_path.stat().st_size <= 0:
        return {"ddi_variables": None, "data_columns": data_cols}

    ddi_meta = parse_ddi_xml(str(ddi_path))
    ddi_vars = ddi_meta.get("variables") or []
    if len(ddi_vars) == 0:
        raise ValueError("Converted DDI metadata contains no variables")

    if data_cols is not None:
        ddi_count = len(ddi_vars)
        mismatch = abs(data_cols - ddi_count)
        allowed = max(5, int(0.1 * max(ddi_count, data_cols)))
        if mismatch > allowed:
            raise ValueError(f"Variable count mismatch (DDI={ddi_count}, DATA={data_cols})")

    return {"ddi_variables": len(ddi_vars), "data_columns": data_cols}

async def convert_and_ingest_nesstar_binary_study(
    input_path: str,
    db_url: str,
    schema: str,
    job_id: str,
    dataset_id: Optional[str] = None,
) -> List[str]:
    try:
        from utils.job_manager import get_job
        job = get_job(job_id)
        job_schema = (job or {}).get("schema")
        if job_schema and str(job_schema).strip():
            schema = str(job_schema).strip()
    except Exception:
        pass

    if not schema or not schema.strip():
        raise ValueError("Schema must be provided for Nesstar conversion")
    
    if not schema_exists(schema, db_url):
        raise ValueError(f"Target schema '{schema}' does not exist in the database")

    ensure_metadata_tables(schema, db_url)
        
    update_job(job_id, log=f"Starting Nesstar conversion with schema: {schema}")

    input_abs = str(Path(input_path).resolve())
    this_file = Path(__file__).resolve()
    project_root = this_file.parent.parent
    out_root = Path(os.getenv("NESSTAR_CONVERSION_OUTPUT_DIR") or str((project_root / "uploads").resolve())).resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    out_dir = (out_root / schema / job_id).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    timeout_sec = int(os.getenv("NESSTAR_CONVERSION_TIMEOUT_SEC") or "3600")
    max_attempts = int(os.getenv("NESSTAR_MAX_ATTEMPTS") or "1")
    retry_initial_delay_sec = float(os.getenv("NESSTAR_RETRY_INITIAL_DELAY_SEC") or "2")
    retry_backoff_factor = float(os.getenv("NESSTAR_RETRY_BACKOFF_FACTOR") or "2")

    update_job(job_id, job_type="nesstar_conversion", input_file_path=input_abs)
    _set_nesstar_stage(job_id, "QUEUED")

    last_err = None
    run_info = None
    for attempt in range(1, max_attempts + 1):
        try:
            from utils.job_manager import get_job, JOB_STATUS_COMPLETED
            current = get_job(job_id) or {}
            if str(current.get("status") or "").upper() == JOB_STATUS_COMPLETED:
                return []
        except Exception:
            pass

        if attempt > 1:
            try:
                shutil.rmtree(out_dir, ignore_errors=True)
            except Exception:
                pass
            out_dir.mkdir(parents=True, exist_ok=True)

        update_job(job_id, log=f"Nesstar conversion attempt {attempt}/{max_attempts}")
        _set_nesstar_stage(job_id, "CONVERTING_WITH_NESSTAR")
        run_info = await asyncio.to_thread(_run_nesstar_converter_streaming, job_id, input_abs, str(out_dir), timeout_sec, schema)

        if int(run_info.get("returncode") or 1) != 0:
            picked_check = _pick_exported_files(out_dir)
            data_check = picked_check.get("data")
            if data_check and data_check.exists():
                try:
                    if int(data_check.stat().st_size or 0) > 1024:
                        update_job(
                            job_id,
                            log=f"Nesstar conversion returned code {int(run_info.get('returncode') or 1)} but output files were detected. Proceeding to ingestion.",
                        )
                        run_info["returncode"] = 0
                        _set_nesstar_stage(job_id, "CONVERSION_COMPLETE", message="Exported files detected — proceeding to ingestion…")
                        break
                except Exception:
                    pass

            rc = int(run_info.get("returncode") or 1)
            diag = "\n".join([str(run_info.get("stderr") or ""), str(run_info.get("stdout") or "")]).lower()
            if "autoit export failed" in diag and "(exit=11)" in diag:
                last_err = "Waiting for Nesstar file-open dialog…"
            elif "autoit export failed" in diag and "(exit=20)" in diag:
                last_err = "Export failed — Save dialog not completed"
            elif "autoit export failed" in diag and "(exit=21)" in diag:
                last_err = "Dataset file missing"
            elif "autoit export failed" in diag and "(exit=22)" in diag:
                last_err = "Metadata missing"
            elif "nesstar export did not generate dataset" in diag:
                last_err = "Dataset file missing"
            elif "nesstar export did not generate metadata" in diag:
                last_err = "Metadata missing"
            elif "conversion timed out" in diag or rc == 124:
                last_err = "Conversion timed out"
            else:
                last_err = f"Nesstar conversion failed (code={rc})"
            if attempt < max_attempts:
                try:
                    from utils.job_manager import get_job, JOB_STATUS_COMPLETED
                    current = get_job(job_id) or {}
                    if str(current.get("status") or "").upper() == JOB_STATUS_COMPLETED:
                        return []
                except Exception:
                    pass
                delay = retry_initial_delay_sec * (retry_backoff_factor ** (attempt - 1))
                msg = f"Retrying conversion ({attempt + 1}/{max_attempts}) in {int(delay)}s — {last_err}"
                _set_nesstar_stage(job_id, "QUEUED", message=msg)
                await asyncio.sleep(delay)
            continue

        picked = _pick_exported_files(out_dir)
        data_path = picked.get("data")
        ddi_path = picked.get("ddi")

        try:
            meta_check = await asyncio.to_thread(_validate_converted_outputs, data_path, ddi_path)
            out_paths: Dict[str, Any] = {"data": str(data_path), "output_dir": str(out_dir), "meta": meta_check}
            if ddi_path:
                out_paths["ddi"] = str(ddi_path)
            update_job(
                job_id,
                status="processing",
                output_paths=out_paths,
            )
            break
        except Exception as e:
            last_err = str(e)
            if data_path and data_path.exists():
                out_paths: Dict[str, Any] = {
                    "data": str(data_path),
                    "output_dir": str(out_dir),
                    "meta": {"warning": last_err},
                }
                if ddi_path:
                    out_paths["ddi"] = str(ddi_path)
                update_job(job_id, log=f"Proceeding with ingestion despite metadata validation error: {last_err}")
                update_job(job_id, status="processing", output_paths=out_paths)
                _set_nesstar_stage(job_id, "CONVERSION_COMPLETE", message="Outputs detected — skipping validation and proceeding to ingestion…")
                break
            if attempt < max_attempts:
                try:
                    from utils.job_manager import get_job, JOB_STATUS_COMPLETED
                    current = get_job(job_id) or {}
                    if str(current.get("status") or "").upper() == JOB_STATUS_COMPLETED:
                        return []
                except Exception:
                    pass
                delay = retry_initial_delay_sec * (retry_backoff_factor ** (attempt - 1))
                msg = f"Retrying conversion ({attempt + 1}/{max_attempts}) in {int(delay)}s — {last_err}"
                _set_nesstar_stage(job_id, "QUEUED", message=msg)
                await asyncio.sleep(delay)
            continue

    if not run_info or int(run_info.get("returncode") or 1) != 0:
        msg = last_err or "Nesstar conversion failed"
        _set_nesstar_stage(job_id, "FAILED", message=msg)
        update_job(job_id, error=msg)
        raise ValueError(msg)

    picked = _pick_exported_files(out_dir)
    data_path = picked.get("data")
    ddi_path = picked.get("ddi")
    if not data_path:
        msg = last_err or "Converted outputs not found"
        _set_nesstar_stage(job_id, "FAILED", message=msg)
        update_job(job_id, error=msg)
        raise ValueError(msg)

    _set_nesstar_stage(job_id, "INGESTING")
    try:
        tables = await process_directory(out_dir, db_url, schema, job_id, dataset_id, require_metadata=False, base_progress=85)
    except Exception as e:
        _set_nesstar_stage(job_id, "FAILED", message="Ingestion failed")
        update_job(job_id, error=str(e))
        raise
    _set_nesstar_stage(job_id, "COMPLETED")
    return tables

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

async def process_dataset_zip(zip_path: str, db_url: str, schema: str = None, current_user_email: str = "system", job_id: str = None):
    """
    Main entry point for processing an uploaded dataset package (.zip or .nesstar).
    """
    if not schema or not schema.strip():
        raise ValueError("Schema must be provided for ingestion")

    if not schema_exists(schema, db_url):
        raise ValueError(f"Target schema '{schema}' does not exist in the database")
    
    ensure_metadata_tables(schema, db_url)
        
    if job_id:
        update_job(job_id, log=f"Starting ingestion with schema: {schema}")
        
    package_ext = Path(zip_path).suffix.lower()
    logger.info(f"Processing package: {zip_path}")

    # FIX: Handle standalone files directly to bypass Nesstar/Zip logic
    if package_ext in [".sav", ".por", ".csv", ".xlsx", ".txt"]:
        if job_id:
            update_job(job_id, status="processing", progress=5, message=f"Processing standalone file: {os.path.basename(zip_path)}")
        file_sha1 = _hash_file_sha1(zip_path)
        dataset_id = f"{schema}_{file_sha1[:12]}"
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dest_file = temp_path / os.path.basename(zip_path)
            shutil.copy2(zip_path, dest_file)
            return await process_directory(temp_path, db_url, schema, job_id, dataset_id=dataset_id, require_metadata=False)

    if job_id:
        if package_ext == ".nesstar":
            update_job(job_id, status="processing", progress=2, message="Detected Nesstar Study upload. Reading package...")
        else:
            update_job(job_id, status="processing", progress=2, message="Reading ZIP and checking for duplicates...")

    zip_sha1 = _hash_file_sha1(zip_path)
    dataset_id = f"{schema}_{zip_sha1[:12]}"
    try:
        with zipfile.ZipFile(zip_path, "r"):
            pass
    except zipfile.BadZipFile:
        if job_id:
            if package_ext == ".nesstar":
                update_job(
                    job_id,
                    status="failed",
                    progress=100,
                    message="Invalid .nesstar file (expected a zipped study package)",
                    error="Invalid .nesstar file (expected a zipped study package)",
                )
            else:
                update_job(job_id, status="failed", progress=100, message="Invalid ZIP file", error="Invalid ZIP file")
        if package_ext == ".nesstar":
            raise ValueError("Invalid .nesstar file (expected a zipped study package)")
        raise ValueError("Invalid ZIP file")

    if job_id:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                names = [n for n in zf.namelist() if n and not n.endswith('/')]
            visible = [
                Path(n).name
                for n in names
                if Path(n).suffix.lower() in [".xml", ".nsdstat", ".nesstar", ".txt", ".csv", ".sav", ".por", ".xlsx"]
            ]
            file_status_list = [{"name": n, "status": "pending", "rows": 0} for n in sorted(set(visible))]
            update_job(job_id, progress=5, message="Package opened. Preparing extraction...", files=file_status_list)
        except Exception:
            update_job(job_id, progress=5, message="Package opened. Preparing extraction...")
    
    with tempfile.TemporaryDirectory() as extract_dir:
        extract_path = Path(extract_dir)
        
        # 1. Extract ZIP
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(extract_path)
        except zipfile.BadZipFile:
            if job_id:
                if package_ext == ".nesstar":
                    update_job(
                        job_id,
                        status="failed",
                        progress=100,
                        message="Invalid .nesstar file (expected a zipped study package)",
                        error="Invalid .nesstar file (expected a zipped study package)",
                    )
                else:
                    update_job(job_id, status="failed", progress=100, message="Invalid ZIP file", error="Invalid ZIP file")
            if package_ext == ".nesstar":
                raise ValueError("Invalid .nesstar file (expected a zipped study package)")
            raise ValueError("Invalid ZIP file")

        if job_id:
            update_job(job_id, progress=8, message="Package extracted. Scanning contents...")

        require_metadata = package_ext == ".nesstar"
        return await process_directory(
            extract_path,
            db_url,
            schema,
            job_id,
            dataset_id,
            require_metadata=require_metadata,
        )

async def process_directory(
    directory: Path,
    db_url: str,
    schema: str = None,
    job_id: str = None,
    dataset_id: Optional[str] = None,
    require_metadata: bool = False,
    base_progress: int = 0,
):
    """
    Processes a directory containing dataset files (DDI + Data).
    """
    if not schema or not schema.strip():
        raise ValueError("Schema must be provided for directory processing")

    if not schema_exists(schema, db_url):
        raise ValueError(f"Target schema '{schema}' does not exist in the database")

    if job_id:
        update_job(job_id, log=f"Starting directory processing with authoritative schema: {schema}")

    ensure_metadata_tables(schema, db_url)

    def _scaled_progress(raw: int) -> int:
        r = int(raw)
        r = max(0, min(r, 100))
        if base_progress <= 0:
            return r
        b = max(0, min(int(base_progress), 100))
        return max(b, min(100, b + int((100 - b) * (r / 100.0))))

    def _file_progress(file_index: int, total: int) -> int:
        denom = max(int(total), 1)
        i = max(0, min(int(file_index), denom))
        if base_progress > 0:
            b = max(0, min(int(base_progress), 100))
            return max(b, min(100, b + int((100 - b) * (i / denom))))
        return 20 + int(80 * (i / denom))

    # 2. Identify Files
    files = [f for f in directory.rglob("*") if f.is_file()]
    nesstar_studies = [f for f in files if f.suffix.lower() == ".nesstar"]
    ddi_candidates = [f for f in files if f.suffix.lower() in [".nsdstat", ".xml"]]
    ddi_candidates.sort(key=lambda p: 0 if p.suffix.lower() == ".nsdstat" else 1)
    has_nesstar_metadata = any(f.suffix.lower() == ".nsdstat" for f in ddi_candidates)
    data_files = [f for f in files if f.suffix.lower() in [".txt", ".csv", ".sav", ".por", ".xlsx"]]

    if nesstar_studies and (not ddi_candidates) and (not data_files):
        if len(nesstar_studies) > 1:
            if job_id:
                update_job(
                    job_id,
                    status="failed",
                    progress=100,
                    message="Multiple .nesstar studies found in ZIP; upload a single study",
                    error="Multiple .nesstar studies found in ZIP; upload a single study",
                )
            raise ValueError("Multiple .nesstar studies found in ZIP; upload a single study")

        nested = nesstar_studies[0]
        if job_id:
            update_job(job_id, progress=_scaled_progress(10), message=f"Found Nesstar Study: {nested.name}. Extracting...")

        try:
            with zipfile.ZipFile(nested, "r") as zf:
                with tempfile.TemporaryDirectory() as nested_dir:
                    nested_path = Path(nested_dir)
                    zf.extractall(nested_path)
                    return await process_directory(
                        nested_path,
                        db_url,
                        schema,
                        job_id,
                        dataset_id,
                        require_metadata=True,
                        base_progress=base_progress,
                    )
        except zipfile.BadZipFile:
            if job_id:
                update_job(
                    job_id,
                    status="failed",
                    progress=100,
                    message="Invalid .nesstar file (expected a zipped study package)",
                    error="Invalid .nesstar file (expected a zipped study package)",
                )
            raise ValueError("Invalid .nesstar file (expected a zipped study package)")
    
    if not data_files:
        if job_id:
            update_job(
                job_id,
                status="failed",
                progress=100,
                message="No data files (.txt, .csv, .sav, .por, .xlsx) found",
                error="No data files (.txt, .csv, .sav, .por, .xlsx) found",
            )
        raise ValueError("No data files (.txt, .csv, .sav, .por, .xlsx) found")

    # Initialize file status in job
    if job_id:
        file_status_list = [{"name": f.name, "status": "pending", "rows": 0} for f in data_files]
        update_job(job_id, progress=_scaled_progress(10), message="Identified files", files=file_status_list)

    # 3. Parse DDI (if present)
    ddi_metadata = None
    ddi_file = None
    for cand in ddi_candidates:
        try:
            if job_id:
                update_job(job_id, progress=_scaled_progress(12), message=f"Parsing metadata: {cand.name}")
            ddi_metadata = parse_ddi_xml(str(cand))
            ddi_file = cand
            break
        except Exception as e:
            logger.warning(f"Failed to parse metadata file {cand.name}: {e}")
            continue

    if require_metadata and not ddi_file:
        if job_id:
            update_job(
                job_id,
                status="failed",
                progress=100,
                message="No metadata found in Nesstar Study package",
                error="No metadata found in Nesstar Study package",
            )
        raise ValueError("No metadata found in Nesstar Study package")

    if require_metadata and (not ddi_metadata or not ddi_metadata.get("variables")):
        if job_id:
            update_job(
                job_id,
                status="failed",
                progress=100,
                message="Metadata parsed but contains no variables",
                error="Metadata parsed but contains no variables",
            )
        raise ValueError("Metadata parsed but contains no variables")

    if has_nesstar_metadata and (not ddi_file or ddi_file.suffix.lower() != ".nsdstat"):
        if job_id:
            update_job(
                job_id,
                status="failed",
                progress=100,
                message="Nesstar metadata (.nsdstat) found but could not be parsed",
                error="Nesstar metadata (.nsdstat) found but could not be parsed",
            )
        raise ValueError("Nesstar metadata (.nsdstat) found but could not be parsed")

    if has_nesstar_metadata and (not ddi_metadata or not ddi_metadata.get("variables")):
        if job_id:
            update_job(
                job_id,
                status="failed",
                progress=100,
                message="Nesstar metadata parsed but contains no variables",
                error="Nesstar metadata parsed but contains no variables",
            )
        raise ValueError("Nesstar metadata parsed but contains no variables")

    if ddi_file and ddi_file.suffix.lower() == ".nsdstat":
        logger.info(f"Detected Nesstar metadata: {ddi_file.name}")
        if job_id:
            update_job(job_id, progress=_scaled_progress(14), message="Detected Nesstar dataset metadata")
    elif ddi_file:
        logger.info(f"Found DDI XML: {ddi_file.name}")
    else:
        logger.warning("No DDI/Nesstar metadata found. Metadata will be limited.")
    
    if job_id:
        update_job(job_id, progress=_scaled_progress(20), message="Starting file processing...")

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
    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                INSERT INTO "{schema}".datasets (dataset_id, title, source)
                VALUES (:id, :title, :source)
                ON CONFLICT (dataset_id) DO NOTHING
                """
            ),
            {
                "id": dataset_id,
                "title": ddi_metadata.get("title", "Unknown") if ddi_metadata else "Unknown",
                "source": str(directory),
            },
        )

        if ddi_metadata:
            _store_variables(conn, dataset_id, ddi_metadata["variables"], schema)

    total_files = len(data_files)
    for idx, data_file in enumerate(data_files):
        rel = str(data_file.relative_to(directory)).replace("\\", "/")
        table_name = get_safe_table_name(data_file.stem, db_url, dataset_id=dataset_id, salt=rel)
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
                    update_job(
                        job_id,
                        progress=current_progress,
                        files=file_status_list,
                        message=f"Skipped {data_file.name} (already uploaded)",
                    )
                continue

            df = _load_data_file(data_file, ddi_metadata)

            if df is not None:
                if ddi_metadata:
                    ddi_name_map = {_normalize_col_name(v.name): v.name for v in ddi_metadata["variables"]}
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

                    if data_file.suffix.lower() in [".csv", ".xlsx", ".sav"]:
                        ddi_var_names = set(v.name for v in ddi_metadata["variables"])
                        df_columns = set(df.columns)

                        missing_in_data = ddi_var_names - df_columns
                        extra_in_data = df_columns - ddi_var_names

                        if len(missing_in_data) > 0:
                            logger.warning(
                                f"File {data_file.name} is missing {len(missing_in_data)} variables defined in DDI."
                            )

                        if len(extra_in_data) > 0:
                            logger.warning(f"File {data_file.name} has {len(extra_in_data)} columns not in DDI.")

                    df = _enforce_ddi_types(df, ddi_metadata["variables"])

                    # Apply category labels from DDI if available
                    ddi_vars_by_name = {v.name: v for v in ddi_metadata["variables"]}
                    df = _apply_ddi_labels(df, ddi_vars_by_name)

                    if has_nesstar_metadata:
                        ddi_norm = set(ddi_name_map.keys())
                        df_norm = set(_normalize_col_name(c) for c in df.columns)
                        matched = len(ddi_norm.intersection(df_norm))
                        denom = max(len(ddi_norm), 1)
                        match_ratio = matched / denom
                        if match_ratio < 0.5:
                            raise ValueError(
                                f"Loaded data columns do not match Nesstar metadata (match_ratio={match_ratio:.2f})"
                            )

                if job_id:
                    for f in file_status_list:
                        if f["name"] == data_file.name:
                            f["status"] = "loading_db"
                    update_job(job_id, files=file_status_list)

                with engine.begin() as conn:
                    df.to_sql(table_name, conn, schema=schema, if_exists="replace", index=False)

                    existing_file = conn.execute(
                        text(
                            f"""
                            SELECT 1
                            FROM "{schema}".dataset_files
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
                                f"""
                                INSERT INTO "{schema}".dataset_files (dataset_id, filename, file_type)
                                VALUES (:did, :fname, :ftype)
                                """
                            ),
                            {"did": dataset_id, "fname": data_file.name, "ftype": data_file.suffix},
                        )

                    if ddi_metadata:
                        for col in df.columns:
                            conn.execute(
                                text(
                                    f"""
                                    UPDATE "{schema}".variables
                                    SET table_name = :tname, column_name = :cname
                                    WHERE dataset_id = :did AND lower(variable_id) = lower(:vid)
                                    """
                                ),
                                {
                                    "tname": table_name,
                                    "cname": col,
                                    "did": dataset_id,
                                    "vid": f"{dataset_id}_{col}",
                                },
                            )

                row_count = len(df)
                logger.info(f"Uploaded {row_count} rows to {full_table_name}")

                uploaded_tables.append(full_table_name)
                logger.info(f"Successfully uploaded {full_table_name}")

                if job_id:
                    for f in file_status_list:
                        if f["name"] == data_file.name:
                            f["status"] = "completed"
                            f["rows"] = row_count
                    current_progress = _file_progress(idx + 1, total_files)
                    update_job(job_id, progress=current_progress, files=file_status_list)
                    update_job(
                        job_id,
                        status=JOB_STATUS_COMPLETED,
                        current_state=JOB_STATUS_COMPLETED,
                        progress=100,
                        message="Upload completed successfully",
                    )

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
        update_job(job_id, status=JOB_STATUS_COMPLETED, progress=100, message="Upload completed successfully", current_state=JOB_STATUS_COMPLETED)

    return uploaded_tables

async def ingest_directory(conn, root_dir: Path, db_url: str, schema: str):
    """
    Adapter for NADA routes to use the new pipeline.
    Ignores 'conn' as we use SQLAlchemy engine created from db_url.
    """
    return await process_directory(root_dir, db_url, schema)

def _store_variables(conn, dataset_id: str, variables: List[DDIVariable], schema: str):
    """Stores variable metadata into the database.
    
    Uses the schema created by ensure_dataset_schema_tables:
      variables(table_name, variable_name, label, ddi_type, width, ...)
      variable_categories(table_name, variable_name, value, label, frequency)
      variable_missing_values(table_name, variable_name, missing_value)
    """
    # Use dataset_id as the logical table_name group for DDI variables
    table_name = dataset_id

    for var in variables:
        var_name = (var.name or "").strip()
        if not var_name:
            continue

        # ── Insert / upsert variable ──────────────────────────────────────
        conn.execute(text(f"""
            INSERT INTO "{schema}".variables
                (table_name, variable_name, label, ddi_type, width, interval,
                 valid_count, invalid_count, final_type)
            VALUES
                (:tbl, :vname, :label, :dtype, :width, :interval,
                 :valid_count, :invalid_count, :final_type)
            ON CONFLICT (table_name, variable_name) DO UPDATE SET
                label        = EXCLUDED.label,
                ddi_type     = EXCLUDED.ddi_type,
                width        = EXCLUDED.width,
                final_type   = EXCLUDED.final_type
        """), {
            "tbl":          table_name,
            "vname":        var_name,
            "label":        var.label or "",
            "dtype":        var.data_type or "",
            "width":        var.width,
            "interval":     "",
            "valid_count":  None,
            "invalid_count": None,
            "final_type":   var.data_type or "",
        })

        # ── Categories ────────────────────────────────────────────────────
        conn.execute(
            text(f'DELETE FROM "{schema}".variable_categories WHERE table_name = :tbl AND variable_name = :vname'),
            {"tbl": table_name, "vname": var_name},
        )
        for cat in (var.categories or []):
            conn.execute(text(f"""
                INSERT INTO "{schema}".variable_categories
                    (table_name, variable_name, value, label, frequency)
                VALUES (:tbl, :vname, :code, :label, :freq)
            """), {
                "tbl":   table_name,
                "vname": var_name,
                "code":  cat.get('code', ''),
                "label": cat.get('label', ''),
                "freq":  cat.get('frequency'),
            })

        # ── Missing values ────────────────────────────────────────────────
        conn.execute(
            text(f'DELETE FROM "{schema}".variable_missing_values WHERE variable_id = :vid'),
            {"vid": f"{table_name}_{var_name}"},
        )
        for mv in (var.missing_values or []):
            conn.execute(text(f"""
                INSERT INTO "{schema}".variable_missing_values (variable_id, missing_value)
                VALUES (:vid, :val)
            """), {"vid": f"{table_name}_{var_name}", "val": mv})


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

    elif ext == ".por":
        df, meta = pyreadstat.read_por(str(file_path))
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
