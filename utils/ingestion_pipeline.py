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
from utils.job_manager import update_job

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    "INGESTING": "Loading into PostgreSQL…",
    "COMPLETED": "Dataset ready for querying",
}

def _set_nesstar_stage(job_id: str, stage: str, message: Optional[str] = None):
    stg = (stage or "").strip().upper()
    pct = _NESSTAR_STAGE_PROGRESS.get(stg, None)
    msg = message or _NESSTAR_STAGE_MESSAGE.get(stg) or stg
    if stg == "COMPLETED":
        update_job(job_id, status="completed", current_state=stg, progress=100, message=msg)
        return
    if stg == "FAILED":
        update_job(job_id, status="failed", current_state=stg, progress=100, message=msg)
        return
    if pct is None:
        update_job(job_id, status="processing", current_state=stg, message=msg)
        return
    update_job(job_id, status="processing", current_state=stg, progress=pct, message=msg)

def _run_nesstar_converter_streaming(job_id: str, input_path: str, output_dir: str, timeout_sec: int) -> Dict[str, Any]:
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
        _set_nesstar_stage(job_id, "FAILED", message="Conversion timed out")
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
    input_abs = str(Path(input_path).resolve())
    this_file = Path(__file__).resolve()
    project_root = this_file.parent.parent
    out_root = Path(os.getenv("NESSTAR_CONVERSION_OUTPUT_DIR") or str((project_root / "uploads").resolve())).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    timeout_sec = int(os.getenv("NESSTAR_CONVERSION_TIMEOUT_SEC") or "3600")
    max_attempts = int(os.getenv("NESSTAR_MAX_ATTEMPTS") or "1")

    update_job(job_id, job_type="nesstar_conversion", input_file_path=input_abs)
    _set_nesstar_stage(job_id, "QUEUED")

    last_err = None
    run_info = None
    for attempt in range(1, max_attempts + 1):
        _set_nesstar_stage(job_id, "CONVERTING_WITH_NESSTAR")
        run_info = await asyncio.to_thread(_run_nesstar_converter_streaming, job_id, input_abs, str(out_root), timeout_sec)

        if int(run_info.get("returncode") or 1) != 0:
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
            else:
                last_err = f"Nesstar conversion failed (code={rc})"
            continue

        picked = _pick_exported_files(out_root)
        data_path = picked.get("data")
        ddi_path = picked.get("ddi")

        try:
            meta_check = await asyncio.to_thread(_validate_converted_outputs, data_path, ddi_path)
            out_paths: Dict[str, Any] = {"data": str(data_path), "output_dir": str(out_root), "meta": meta_check}
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
            continue

    if not run_info or int(run_info.get("returncode") or 1) != 0:
        msg = last_err or "Nesstar conversion failed"
        _set_nesstar_stage(job_id, "FAILED", message=msg)
        update_job(job_id, error=msg)
        raise ValueError(msg)

    picked = _pick_exported_files(out_root)
    data_path = picked.get("data")
    ddi_path = picked.get("ddi")
    if not data_path:
        msg = last_err or "Converted outputs not found"
        _set_nesstar_stage(job_id, "FAILED", message=msg)
        update_job(job_id, error=msg)
        raise ValueError(msg)

    _set_nesstar_stage(job_id, "INGESTING")
    try:
        tables = await process_directory(out_root, db_url, schema, job_id, dataset_id, require_metadata=False)
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

async def process_dataset_zip(zip_path: str, db_url: str, schema: str = "public", current_user_email: str = "system", job_id: str = None):
    """
    Main entry point for processing an uploaded dataset package (.zip or .nesstar).
    """
    package_ext = Path(zip_path).suffix.lower()
    logger.info(f"Processing package: {zip_path}")

    # FIX: Handle standalone files directly to bypass Nesstar/Zip logic
    if package_ext in [".sav", ".por", ".csv", ".xlsx", ".txt"]:
        if job_id:
            update_job(job_id, status="processing", progress=5, message=f"Processing standalone file: {os.path.basename(zip_path)}")
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            dest_file = temp_path / os.path.basename(zip_path)
            shutil.copy2(zip_path, dest_file)
            return await process_directory(temp_path, db_url, schema, job_id, require_metadata=False)

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
    schema: str = "public",
    job_id: str = None,
    dataset_id: Optional[str] = None,
    require_metadata: bool = False,
):
    """
    Processes a directory containing dataset files (DDI + Data).
    """
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
            update_job(job_id, progress=10, message=f"Found Nesstar Study: {nested.name}. Extracting...")

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
        update_job(job_id, progress=10, message="Identified files", files=file_status_list)

    # 3. Parse DDI (if present)
    ddi_metadata = None
    ddi_file = None
    for cand in ddi_candidates:
        try:
            if job_id:
                update_job(job_id, progress=12, message=f"Parsing metadata: {cand.name}")
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
            update_job(job_id, progress=14, message="Detected Nesstar dataset metadata")
    elif ddi_file:
        logger.info(f"Found DDI XML: {ddi_file.name}")
    else:
        logger.warning("No DDI/Nesstar metadata found. Metadata will be limited.")
    
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
