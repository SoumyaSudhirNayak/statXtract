import os
import uuid
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, List
from pydantic import BaseModel

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, Request

from auth.local.dependencies import get_current_active_user_with_role
from utils.ingestion_pipeline import ingest_directory, ingest_upload_file
from utils.db_utils import to_snake_case_identifier
from utils.redundancy_detector import DuplicateDatasetException
from utils.nada_client import (
    extract_files_list,
    guess_file_name,
    guess_file_no,
    nada_download_file,
    nada_fileslist,
    nada_listdatasets,
)


router = APIRouter(prefix="/admin/nada", tags=["NADA"], include_in_schema=False)

_jobs: dict[str, dict[str, Any]] = {}


def _sanitize_filename(name: str) -> str:
    keep = []
    for ch in name:
        if ch.isalnum() or ch in {".", "_", "-", " "}:
            keep.append(ch)
        else:
            keep.append("_")
    return "".join(keep).strip().replace(" ", "_") or "file"


def _get_ingest_root() -> Path:
    root = os.getenv("NADA_INGEST_DIR", "nada_ingestion")
    p = Path(root)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _resolve_api_key(x_api_key: str | None) -> str:
    if x_api_key and x_api_key.strip():
        return x_api_key.strip()
    for k in ("NADA_API_KEY", "API_KEY"):
        v = os.getenv(k)
        if v and v.strip():
            return v.strip()
    raise HTTPException(status_code=400, detail="Missing NADA API key")


def _upstream_error_detail(prefix: str, upstream: httpx.Response) -> str:
    detail = f"{prefix}: HTTP {upstream.status_code}"
    try:
        body = upstream.json()
        if isinstance(body, dict):
            msg = body.get("message") or body.get("error") or body.get("detail")
            if msg:
                return f"{detail} ({msg})"
    except Exception:
        pass
    text = (upstream.text or "").strip()
    if text:
        return f"{detail} ({text[:250]})"
    return detail


def _raise_for_upstream_http_error(prefix: str, e: httpx.HTTPStatusError) -> None:
    upstream = e.response
    if upstream is None:
        raise HTTPException(status_code=502, detail=f"{prefix}: {e}")
    detail = _upstream_error_detail(prefix, upstream)
    if upstream.status_code in {401, 403, 404}:
        raise HTTPException(status_code=upstream.status_code, detail=detail)
    raise HTTPException(status_code=502, detail=detail)


@router.get("/datasets")
async def list_datasets(
    limit: int = Query(15, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    return await nada_listdatasets(limit=limit, offset=offset)


@router.get("/datasets/{dataset_id}/fileslist")
async def get_dataset_fileslist(
    dataset_id: str,
    x_api_key: str | None = Header(None, alias="X-API-KEY"),
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    api_key = _resolve_api_key(x_api_key)
    try:
        return await nada_fileslist(dataset_id, api_key=api_key)
    except httpx.HTTPStatusError as e:
        _raise_for_upstream_http_error("NADA fileslist failed", e)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"NADA fileslist failed: {e}")


@router.post("/datasets/{dataset_id}/download")
async def download_dataset_files(
    request: Request,
    background_tasks: BackgroundTasks,
    dataset_id: str,
    x_api_key: str | None = Header(None, alias="X-API-KEY"),
    file_nos: Optional[str] = Query(
        None,
        description="Comma-separated file numbers to download. If omitted, downloads all.",
    ),
    schema: str = Query(..., description="Target schema for ingestion"),
    ingest: bool = Query(True),
    current_user=Depends(get_current_active_user_with_role(["1"])),
) -> dict[str, Any]:
    api_key = _resolve_api_key(x_api_key)
    try:
        files_payload = await nada_fileslist(dataset_id, api_key=api_key)
    except httpx.HTTPStatusError as e:
        _raise_for_upstream_http_error("NADA fileslist failed", e)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"NADA fileslist failed: {e}")

    items = extract_files_list(files_payload)
    if not items:
        return {
            "dataset_id": dataset_id,
            "downloaded": [],
            "errors": [],
            "note": "No downloadable files found in fileslist response.",
            "fileslist": files_payload,
        }

    requested: set[str] | None = None
    if file_nos:
        requested = {x.strip() for x in file_nos.split(",") if x.strip()}

    ingest_dir = _get_ingest_root() / _sanitize_filename(dataset_id)
    ingest_dir.mkdir(parents=True, exist_ok=True)

    downloaded: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for idx, item in enumerate(items, start=1):
        file_no = guess_file_no(item)
        if not file_no:
            errors.append({"index": idx, "error": "Missing file_no", "item": item})
            continue
        if requested is not None and file_no not in requested:
            continue

        raw_name = guess_file_name(item, fallback=f"{dataset_id}_{file_no}")
        filename = _sanitize_filename(raw_name)
        dest_path = ingest_dir / filename

        try:
            saved = await nada_download_file(
                dataset_id,
                file_no,
                api_key=api_key,
                dest_path=dest_path,
            )
            downloaded.append(
                {
                    "file_no": file_no,
                    "filename": filename,
                    "path": str(saved),
                }
            )
        except Exception as e:
            errors.append(
                {
                    "file_no": file_no,
                    "filename": filename,
                    "error": str(e),
                }
            )

    if not ingest:
        return {
            "dataset_id": dataset_id,
            "ingest_dir": str(ingest_dir),
            "downloaded": downloaded,
            "errors": errors,
            "downloaded_count": len(downloaded),
            "error_count": len(errors),
        }

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "job_id": job_id,
        "dataset_id": dataset_id,
        "schema": schema,
        "status": "queued",
        "created_at": datetime.utcnow().isoformat(),
        "runner_name": "ingest_dir",
        "runner_args": {
            "dataset_id": dataset_id,
            "schema": schema,
            "ingest_dir": str(ingest_dir),
            "downloaded": downloaded,
            "download_errors": errors,
        }
    }
    background_tasks.add_task(
        _run_ingest_dir_job,
        job_id=job_id,
        request=request,
        dataset_id=dataset_id,
        schema=schema,
        ingest_dir=ingest_dir,
        downloaded=downloaded,
        download_errors=errors,
    )
    return _jobs[job_id]


async def _run_ingest_dir_job(
    *,
    job_id: str,
    request: Request,
    dataset_id: str,
    schema: str,
    ingest_dir: Path,
    downloaded: list[dict[str, Any]],
    download_errors: list[dict[str, Any]],
) -> None:
    _jobs[job_id]["status"] = "running"
    _jobs[job_id]["started_at"] = datetime.utcnow().isoformat()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["error"] = "DATABASE_URL not set"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        return

    try:
        ingest_report = []
        for file_info in downloaded:
            filepath = file_info["path"]
            import re
            year_match = re.search(r'\b(19\d\d|20\d\d)(?:-\d\d)?\b', dataset_id)
            year = year_match.group(0) if year_match else str(datetime.utcnow().year)

            tables = await ingest_upload_file(
                input_path=filepath,
                db_url=db_url,
                schema=schema,
                year=year,
                dataset_display_name=dataset_id,
                dataset_db_name=to_snake_case_identifier(dataset_id),
                job_id=job_id,
            )
            if tables:
                ingest_report.extend(tables)

        _jobs[job_id]["status"] = "completed"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        
        from utils.job_manager import get_job
        g_job = get_job(job_id)
        if g_job:
            _jobs[job_id]["duplicate_info"] = g_job.get("duplicate_info")
            _jobs[job_id]["validation_status"] = g_job.get("validation_status", "duplicate_not_found")

        _jobs[job_id]["result"] = {
            "dataset_id": dataset_id,
            "schema": schema,
            "ingest_dir": str(ingest_dir),
            "downloaded": downloaded,
            "download_errors": download_errors,
            "ingest": ingest_report,
        }
    except DuplicateDatasetException as de:
        _jobs[job_id]["status"] = "duplicate"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _jobs[job_id]["error"] = de.message
        _jobs[job_id]["duplicate_info"] = de.duplicate_info
    except Exception as e:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _jobs[job_id]["error"] = str(e)
    finally:
        import shutil
        if _jobs[job_id].get("status") == "duplicate":
            print(f"Bypassing cleanup for NADA duplicate job: {job_id}")
        else:
            if 'ingest_dir' in locals() and ingest_dir and ingest_dir.exists():
                shutil.rmtree(ingest_dir, ignore_errors=True)


async def _run_ingest_job(
    *,
    job_id: str,
    request: Request,
    dataset_id: str,
    schema: str,
    api_key: str,
    file_nos: str | None,
) -> None:
    _jobs[job_id]["status"] = "running"
    _jobs[job_id]["started_at"] = datetime.utcnow().isoformat()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["error"] = "DATABASE_URL not set"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        return

    ingest_dir = _get_ingest_root() / _sanitize_filename(dataset_id)
    try:
        files_payload = await nada_fileslist(dataset_id, api_key=api_key)
        items = extract_files_list(files_payload)
        requested: set[str] | None = None
        if file_nos:
            requested = {x.strip() for x in file_nos.split(",") if x.strip()}

        ingest_dir.mkdir(parents=True, exist_ok=True)

        downloaded: list[dict[str, Any]] = []
        download_errors: list[dict[str, Any]] = []

        from utils.batch_validation import is_layout_file

        for idx, item in enumerate(items, start=1):
            file_no = guess_file_no(item)
            if not file_no:
                download_errors.append({"index": idx, "error": "Missing file_no", "item": item})
                continue

            raw_name = guess_file_name(item, fallback=f"{dataset_id}_{file_no}")
            filename = _sanitize_filename(raw_name)
            ext = Path(filename).suffix.lower()

            is_layout = is_layout_file(Path(filename))

            if requested is not None and file_no not in requested and not is_layout:
                continue

            if ext and ext not in {".zip", ".xml", ".csv", ".txt", ".sav", ".por", ".xlsx", ".json", ".pdf", ".xls"} and not is_layout:
                continue

            dest_path = ingest_dir / filename
            try:
                saved = await nada_download_file(
                    dataset_id,
                    file_no,
                    api_key=api_key,
                    dest_path=dest_path,
                )
                downloaded.append({"file_no": file_no, "filename": filename, "path": str(saved)})
            except Exception as e:
                download_errors.append({"file_no": file_no, "filename": filename, "error": str(e)})

        ingest_report = []
        for file_info in downloaded:
            filepath = file_info["path"]
            import re
            year_match = re.search(r'\b(19\d\d|20\d\d)(?:-\d\d)?\b', dataset_id)
            year = year_match.group(0) if year_match else str(datetime.utcnow().year)

            tables = await ingest_upload_file(
                input_path=filepath,
                db_url=db_url,
                schema=schema,
                year=year,
                dataset_display_name=dataset_id,
                dataset_db_name=to_snake_case_identifier(dataset_id),
                job_id=job_id,
            )
            if tables:
                ingest_report.extend(tables)

        _jobs[job_id]["status"] = "completed"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        
        from utils.job_manager import get_job
        g_job = get_job(job_id)
        if g_job:
            _jobs[job_id]["duplicate_info"] = g_job.get("duplicate_info")
            _jobs[job_id]["validation_status"] = g_job.get("validation_status", "duplicate_not_found")

        _jobs[job_id]["result"] = {
            "dataset_id": dataset_id,
            "schema": schema,
            "ingest_dir": str(ingest_dir),
            "downloaded": downloaded,
            "download_errors": download_errors,
            "ingest": ingest_report,
        }
    except DuplicateDatasetException as de:
        _jobs[job_id]["status"] = "duplicate"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _jobs[job_id]["error"] = de.message
        _jobs[job_id]["duplicate_info"] = de.duplicate_info
    except Exception as e:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _jobs[job_id]["error"] = str(e)
    finally:
        import shutil
        if _jobs[job_id].get("status") == "duplicate":
            print(f"Bypassing cleanup for NADA duplicate job: {job_id}")
        else:
            if 'ingest_dir' in locals() and ingest_dir and ingest_dir.exists():
                shutil.rmtree(ingest_dir, ignore_errors=True)


@router.post("/datasets/{dataset_id}/ingest")
async def ingest_dataset(
    request: Request,
    background_tasks: BackgroundTasks,
    dataset_id: str,
    schema: str = Query(..., description="Target schema for ingestion"),
    file_nos: Optional[str] = Query(None),
    x_api_key: str | None = Header(None, alias="X-API-KEY"),
    current_user=Depends(get_current_active_user_with_role(["1"])),
) -> dict[str, Any]:
    api_key = _resolve_api_key(x_api_key)
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "job_id": job_id,
        "dataset_id": dataset_id,
        "schema": schema,
        "status": "queued",
        "created_at": datetime.utcnow().isoformat(),
        "runner_name": "ingest_job",
        "runner_args": {
            "dataset_id": dataset_id,
            "schema": schema,
            "api_key": api_key,
            "file_nos": file_nos,
        }
    }
    background_tasks.add_task(
        _run_ingest_job,
        job_id=job_id,
        request=request,
        dataset_id=dataset_id,
        schema=schema,
        api_key=api_key,
        file_nos=file_nos,
    )
    return _jobs[job_id]


@router.get("/jobs/{job_id}")
async def get_job_status(
    job_id: str,
    current_user=Depends(get_current_active_user_with_role(["1"])),
) -> dict[str, Any]:
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    # Return duplicate_info if present
    return job


class IngestPreparedRequest(BaseModel):
    target_schema: str
    year: str
    dataset_display_name: str
    selected_files: List[str]


def _format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"


@router.post("/datasets/{dataset_id}/prepare")
async def prepare_dataset(
    dataset_id: str,
    x_api_key: str | None = Header(None, alias="X-API-KEY"),
    file_nos: Optional[str] = Query(None),
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    api_key = _resolve_api_key(x_api_key)
    try:
        files_payload = await nada_fileslist(dataset_id, api_key=api_key)
    except httpx.HTTPStatusError as e:
        _raise_for_upstream_http_error("NADA fileslist failed", e)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"NADA fileslist failed: {e}")

    items = extract_files_list(files_payload)
    if not items:
        return {
            "prepare_id": "",
            "files": [],
            "note": "No files found in NADA dataset fileslist."
        }

    requested: set[str] | None = None
    if file_nos:
        requested = {x.strip() for x in file_nos.split(",") if x.strip()}

    prepare_id = str(uuid.uuid4())
    prepare_dir = _get_ingest_root() / "prepare" / prepare_id
    download_dir = prepare_dir / "download"
    extracted_dir = prepare_dir / "extracted"
    
    download_dir.mkdir(parents=True, exist_ok=True)
    extracted_dir.mkdir(parents=True, exist_ok=True)

    downloaded_count = 0
    from utils.batch_validation import is_layout_file

    for idx, item in enumerate(items, start=1):
        file_no = guess_file_no(item)
        if not file_no:
            continue

        raw_name = guess_file_name(item, fallback=f"{dataset_id}_{file_no}")
        filename = _sanitize_filename(raw_name)
        is_layout = is_layout_file(Path(filename))

        if requested is not None and file_no not in requested and not is_layout:
            continue

        dest_path = download_dir / filename

        try:
            saved = await nada_download_file(
                dataset_id,
                file_no,
                api_key=api_key,
                dest_path=dest_path,
            )
            downloaded_count += 1
            
            # Extract ZIP or copy file
            if saved.suffix.lower() == ".zip":
                with zipfile.ZipFile(saved, "r") as zf:
                    zf.extractall(extracted_dir)
            else:
                shutil.copy2(saved, extracted_dir / filename)
        except Exception as e:
            shutil.rmtree(prepare_dir, ignore_errors=True)
            raise HTTPException(status_code=500, detail=f"Failed downloading/extracting file {filename}: {e}")

    if downloaded_count == 0:
        shutil.rmtree(prepare_dir, ignore_errors=True)
        raise HTTPException(status_code=400, detail="No matching files downloaded from NADA dataset.")

    # Build inventory
    inventory = []
    for p in extracted_dir.rglob("*"):
        if p.is_file():
            rel_path = p.relative_to(extracted_dir).as_posix()
            ext = p.suffix.upper().replace(".", "") or "FILE"
            size_bytes = p.stat().st_size
            size_formatted = _format_size(size_bytes)
            inventory.append({
                "relative_path": rel_path,
                "filename": p.name,
                "type": ext,
                "size_bytes": size_bytes,
                "size_formatted": size_formatted
            })

    # Clean up download directory (we extracted its contents to extracted_dir)
    shutil.rmtree(download_dir, ignore_errors=True)

    return {
        "prepare_id": prepare_id,
        "files": inventory
    }


@router.post("/prepare/{prepare_id}/ingest")
async def ingest_prepared_dataset(
    prepare_id: str,
    payload: IngestPreparedRequest,
    background_tasks: BackgroundTasks,
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    prepare_dir = _get_ingest_root() / "prepare" / prepare_id
    if not prepare_dir.exists():
        raise HTTPException(status_code=404, detail="Prepared session not found or expired.")
    
    if not payload.selected_files:
        raise HTTPException(status_code=400, detail="No files selected for ingestion.")

    job_id = str(uuid.uuid4())
    _jobs[job_id] = {
        "job_id": job_id,
        "dataset_id": payload.dataset_display_name,
        "schema": payload.target_schema,
        "status": "queued",
        "created_at": datetime.utcnow().isoformat(),
        "runner_name": "prepared",
        "runner_args": {
            "prepare_id": prepare_id,
            "schema": payload.target_schema,
            "year": payload.year,
            "dataset_display_name": payload.dataset_display_name,
            "selected_files": payload.selected_files,
        }
    }
    background_tasks.add_task(
        _run_prepared_ingest_job,
        job_id=job_id,
        prepare_id=prepare_id,
        schema=payload.target_schema,
        year=payload.year,
        dataset_display_name=payload.dataset_display_name,
        selected_files=payload.selected_files,
    )
    return _jobs[job_id]


async def _run_prepared_ingest_job(
    job_id: str,
    prepare_id: str,
    schema: str,
    year: str,
    dataset_display_name: str,
    selected_files: list[str],
) -> None:
    _jobs[job_id]["status"] = "running"
    _jobs[job_id]["started_at"] = datetime.utcnow().isoformat()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["error"] = "DATABASE_URL not set"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        return

    prepare_dir = _get_ingest_root() / "prepare" / prepare_id
    extracted_dir = prepare_dir / "extracted"
    ingest_temp_dir = _get_ingest_root() / "ingest" / job_id
    ingest_files_dir = ingest_temp_dir / "files"
    
    raw_zip_dest = None
    dataset_raw_extracted_dir = None

    try:
        ingest_files_dir.mkdir(parents=True, exist_ok=True)

        # 1. Copy selected files preserving relative paths
        for rel_path in selected_files:
            src_file = extracted_dir / rel_path
            if not src_file.exists():
                raise ValueError(f"Selected file not found in extracted dataset: {rel_path}")
            
            dest_file = ingest_files_dir / rel_path
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dest_file)

        # Automatically copy layout files if present in the extracted dir
        from utils.batch_validation import is_layout_file
        for p in extracted_dir.rglob("*"):
            if p.is_file() and is_layout_file(p):
                rel_path = p.relative_to(extracted_dir)
                dest_file = ingest_files_dir / rel_path
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(p, dest_file)

        # 2. Package selected files into a temporary ZIP file
        temp_zip_path = ingest_temp_dir / f"{job_id}_ingest.zip"
        with zipfile.ZipFile(temp_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(ingest_files_dir):
                for file in files:
                    file_path = Path(root) / file
                    rel_zip_path = file_path.relative_to(ingest_files_dir)
                    zf.write(file_path, rel_zip_path)

        # 3. Call the existing ingestion workflow
        dataset_db_name = to_snake_case_identifier(dataset_display_name)
        tables = await ingest_upload_file(
            input_path=str(temp_zip_path),
            db_url=db_url,
            schema=schema,
            year=year,
            dataset_display_name=dataset_display_name,
            dataset_db_name=dataset_db_name,
            job_id=job_id,
        )

        _jobs[job_id]["status"] = "completed"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        
        from utils.job_manager import get_job
        g_job = get_job(job_id)
        if g_job:
            _jobs[job_id]["duplicate_info"] = g_job.get("duplicate_info")
            _jobs[job_id]["validation_status"] = g_job.get("validation_status", "duplicate_not_found")

        _jobs[job_id]["result"] = {
            "dataset_id": dataset_display_name,
            "schema": schema,
            "ingest": tables or [],
        }

        # Resolve paths to clean up inside raw_files
        upload_root = Path(os.getenv("UPLOAD_DIR") or "uploads").resolve()
        dataset_db = dataset_db_name or "dataset"
        dataset_root = upload_root / schema / dataset_db
        raw_dir = dataset_root / "raw_files"
        raw_zip_dest = raw_dir / temp_zip_path.name
        dataset_raw_extracted_dir = raw_dir / "extracted"

    except DuplicateDatasetException as de:
        _jobs[job_id]["status"] = "duplicate"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _jobs[job_id]["error"] = de.message
        _jobs[job_id]["duplicate_info"] = de.duplicate_info
    except Exception as e:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _jobs[job_id]["error"] = str(e)
    finally:
        # Clean up temporary folders unless duplicate state pauses it
        if _jobs[job_id].get("status") == "duplicate":
            shutil.rmtree(prepare_dir, ignore_errors=True)
            # Keep ingest_temp_dir and temp_zip_path!
        else:
            shutil.rmtree(prepare_dir, ignore_errors=True)
            shutil.rmtree(ingest_temp_dir, ignore_errors=True)
            
            # Clean up copied zip in raw_dir and extracted files inside raw_dir
            if raw_zip_dest and raw_zip_dest.exists():
                try:
                    os.remove(raw_zip_dest)
                except Exception:
                    pass
            if dataset_raw_extracted_dir and dataset_raw_extracted_dir.exists():
                shutil.rmtree(dataset_raw_extracted_dir, ignore_errors=True)


@router.post("/jobs/{job_id}/force")
async def force_nada_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    request: Request,
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job.get("status") != "duplicate":
        raise HTTPException(status_code=400, detail="Job is not in duplicate status")
        
    # Set force_import in the global job store for ingest_upload_file to read
    from utils.job_manager import get_job, jobs as global_jobs
    job_mgr_job = get_job(job_id)
    if not job_mgr_job:
        global_jobs[job_id] = {
            "status": "QUEUED",
            "force_import": True
        }
    else:
        job_mgr_job["force_import"] = True
        
    job["status"] = "queued"
    job["error"] = None
    
    runner_name = job.get("runner_name")
    runner_args = job.get("runner_args", {})
    kwargs = dict(runner_args)
    if "ingest_dir" in kwargs:
        kwargs["ingest_dir"] = Path(kwargs["ingest_dir"])
        
    if runner_name == "prepared":
        background_tasks.add_task(
            _run_prepared_ingest_job,
            job_id=job_id,
            **kwargs
        )
    elif runner_name == "ingest_job":
        background_tasks.add_task(
            _run_ingest_job,
            job_id=job_id,
            request=request,
            **kwargs
        )
    elif runner_name == "ingest_dir":
        background_tasks.add_task(
            _run_ingest_dir_job,
            job_id=job_id,
            request=request,
            **kwargs
        )
        
    return {"status": "queued", "message": "Force import started."}


@router.post("/jobs/{job_id}/cancel")
async def cancel_nada_job(
    job_id: str,
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    job["status"] = "failed"
    job["error"] = "Import cancelled by admin"
    
    # Clean up files if they exist
    ingest_temp_dir = _get_ingest_root() / "ingest" / job_id
    if ingest_temp_dir.exists():
        shutil.rmtree(ingest_temp_dir, ignore_errors=True)
        
    runner_args = job.get("runner_args", {})
    prepare_id = runner_args.get("prepare_id")
    if prepare_id:
        prepare_dir = _get_ingest_root() / "prepare" / prepare_id
        if prepare_dir.exists():
            shutil.rmtree(prepare_dir, ignore_errors=True)
            
    ingest_dir = runner_args.get("ingest_dir")
    if ingest_dir and os.path.exists(str(ingest_dir)):
        shutil.rmtree(str(ingest_dir), ignore_errors=True)
        
    return {"status": "cancelled", "message": "NADA Import cancelled."}
