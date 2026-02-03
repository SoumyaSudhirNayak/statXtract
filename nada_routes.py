import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, Request

from auth.local.dependencies import get_current_active_user_with_role
from utils.ingestion_pipeline import ingest_directory
from utils.nada_client import (
    extract_files_list,
    guess_file_name,
    guess_file_no,
    nada_download_file,
    nada_fileslist,
    nada_listdatasets,
)


router = APIRouter(prefix="/admin/nada", tags=["NADA"])

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

    ingest_dir = _get_ingest_root() / dataset_id
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
        pool = request.app.state.db
        async with pool.acquire() as conn:
            ingest_report = await ingest_directory(
                conn=conn,
                root_dir=ingest_dir,
                db_url=db_url,
                schema=schema,
            )

        _jobs[job_id]["status"] = "completed"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _jobs[job_id]["result"] = {
            "dataset_id": dataset_id,
            "schema": schema,
            "ingest_dir": str(ingest_dir),
            "downloaded": downloaded,
            "download_errors": download_errors,
            "ingest": ingest_report,
        }
    except Exception as e:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _jobs[job_id]["error"] = str(e)


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

    try:
        files_payload = await nada_fileslist(dataset_id, api_key=api_key)
        items = extract_files_list(files_payload)
        requested: set[str] | None = None
        if file_nos:
            requested = {x.strip() for x in file_nos.split(",") if x.strip()}

        ingest_dir = _get_ingest_root() / dataset_id
        ingest_dir.mkdir(parents=True, exist_ok=True)

        downloaded: list[dict[str, Any]] = []
        download_errors: list[dict[str, Any]] = []

        for idx, item in enumerate(items, start=1):
            file_no = guess_file_no(item)
            if not file_no:
                download_errors.append({"index": idx, "error": "Missing file_no", "item": item})
                continue
            if requested is not None and file_no not in requested:
                continue

            raw_name = guess_file_name(item, fallback=f"{dataset_id}_{file_no}")
            filename = _sanitize_filename(raw_name)
            ext = Path(filename).suffix.lower()
            if ext and ext not in {".zip", ".xml", ".csv", ".txt", ".sav", ".xlsx"}:
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

        pool = request.app.state.db
        async with pool.acquire() as conn:
            ingest_report = await ingest_directory(
                conn=conn,
                root_dir=ingest_dir,
                db_url=db_url,
                schema=schema,
            )

        _jobs[job_id]["status"] = "completed"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _jobs[job_id]["result"] = {
            "dataset_id": dataset_id,
            "schema": schema,
            "ingest_dir": str(ingest_dir),
            "downloaded": downloaded,
            "download_errors": download_errors,
            "ingest": ingest_report,
        }
    except Exception as e:
        _jobs[job_id]["status"] = "failed"
        _jobs[job_id]["ended_at"] = datetime.utcnow().isoformat()
        _jobs[job_id]["error"] = str(e)


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
    return job
