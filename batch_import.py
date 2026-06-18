import os
import uuid
import zipfile
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse

from auth.local.dependencies import get_current_active_user_with_role
from utils.job_manager import create_job, get_job, update_job
from utils.batch_validation import scan_batch_archive
from utils.batch_ingestion_service import run_batch_import_job

router = APIRouter(tags=["Batch Import"], include_in_schema=False)

UPLOAD_DIR = "uploads"
BATCH_TEMP_DIR = os.path.join(UPLOAD_DIR, "batch_temp")
os.makedirs(BATCH_TEMP_DIR, exist_ok=True)

@router.get("/admin/batch-import", response_class=HTMLResponse)
async def get_batch_import_page(
    request: Request,
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    """Render the Batch Import UI Page."""
    from main import templates
    return templates.TemplateResponse(
        request=request,
        name="batch_import_ui.html",
        context={
            "request": request,
            "username": current_user.username,
            "role": current_user.role,
        }
    )

@router.post("/api/admin/batch-import/upload")
async def api_batch_import_upload(
    request: Request,
    file: UploadFile = File(...),
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    """
    Accepts ZIP archive, extracts it, scans folders, 
    performs Step 3 validation, and returns structured preview.
    """
    if not file.filename.lower().endswith(".zip"):
        return JSONResponse({"error": "Only ZIP archive files are supported for batch import"}, status_code=400)

    # Create a unique temp folder for this upload
    import_id = str(uuid.uuid4())
    temp_extract_dir = os.path.join(BATCH_TEMP_DIR, import_id)
    os.makedirs(temp_extract_dir, exist_ok=True)

    # Save uploaded ZIP temporary
    temp_zip_path = os.path.join(temp_extract_dir, f"{import_id}.zip")
    try:
        with open(temp_zip_path, "wb") as buffer:
            buffer.write(await file.read())

        # Extract files
        with zipfile.ZipFile(temp_zip_path, "r") as zf:
            # Prevent Zip Slip Vulnerability by verifying paths
            for member in zf.infolist():
                # Resolve destination path safely
                target_path = Path(temp_extract_dir) / member.filename
                if not target_path.resolve().is_relative_to(Path(temp_extract_dir).resolve()):
                    raise HTTPException(status_code=400, detail="Vulnerable ZIP file structure detected.")
            zf.extractall(temp_extract_dir)

        # Remove temporary zip file
        os.remove(temp_zip_path)

        # Scan and validate subdirectories
        surveys = scan_batch_archive(temp_extract_dir)

        # Post-process surveys to add duplicate checks
        from sqlalchemy import create_engine
        from utils.redundancy_detector import check_dataset_duplicate, compute_dataset_fingerprint_and_pk
        from utils.ingestion_pipeline import _load_data_file
        
        db_url = os.getenv("DATABASE_URL")
        engine = create_engine(db_url) if db_url else None
        
        for s in surveys:
            s["duplicate_statuses"] = {}
            if not engine:
                continue
                
            subdir_path = Path(temp_extract_dir) / s["relative_dir"]
            for df_rel in s["dataset_files"]:
                file_path = subdir_path / df_rel
                try:
                    df = _load_data_file(file_path, None)
                    if df is not None and not df.empty:
                        fingerprint, candidate_key = compute_dataset_fingerprint_and_pk(df)
                        
                        # 1. Fingerprint check
                        res = check_dataset_duplicate(
                            engine,
                            survey_name="",
                            year="",
                            dataset_name="",
                            block_name=file_path.stem,
                            fingerprint=fingerprint,
                            candidate_key=candidate_key
                        )
                        
                        # 2. Guessed identity check
                        import re
                        year_match = re.search(r'\b(19\d\d|20\d\d)(?:-\d\d)?\b', s["folder_name"])
                        year = year_match.group(0) if year_match else str(datetime.now().year)
                        
                        res_with_identity = check_dataset_duplicate(
                            engine,
                            survey_name=s["folder_name"],
                            year=year,
                            dataset_name=s["folder_name"],
                            block_name=file_path.stem,
                            fingerprint=fingerprint,
                            candidate_key=candidate_key
                        )
                        
                        if res["status"] != "new":
                            s["duplicate_statuses"][df_rel] = {
                                "status": res["status"],
                                "reason": res["reason"],
                                "existing": res["existing"]
                            }
                        elif res_with_identity["status"] != "new":
                            s["duplicate_statuses"][df_rel] = {
                                "status": res_with_identity["status"],
                                "reason": res_with_identity["reason"],
                                "existing": res_with_identity["existing"]
                            }
                        else:
                            s["duplicate_statuses"][df_rel] = {
                                "status": "new",
                                "reason": None,
                                "existing": None
                            }
                except Exception as ex:
                    print(f"Error checking duplicate for {df_rel}: {ex}")

        return {
            "batch_id": import_id,
            "temp_dir": temp_extract_dir,
            "surveys": surveys
        }

    except Exception as e:
        # Clean up on error
        if os.path.exists(temp_extract_dir):
            shutil.rmtree(temp_extract_dir)
        return JSONResponse({"error": f"Failed to extract and parse ZIP archive: {e}"}, status_code=500)

@router.post("/api/admin/batch-import/execute")
async def api_batch_import_execute(
    request: Request,
    payload: Dict[str, Any],
    background_tasks: BackgroundTasks,
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    """
    Executes the batch import.
    Launches run_batch_import_job as background task.
    """
    batch_id = payload.get("batch_id")
    temp_dir = payload.get("temp_dir")
    mappings = payload.get("mappings")

    if not batch_id or not temp_dir or not mappings:
        raise HTTPException(status_code=400, detail="Missing required parameters: batch_id, temp_dir, or mappings")

    if not os.path.exists(temp_dir):
        raise HTTPException(status_code=400, detail="Temporary folder has expired or does not exist")

    # Create ingestion job
    job_id = create_job(
        filename="Batch_Import_Archive.zip",
        schema="batch_import",
        schema_display_name="Batch Import",
        year=datetime.now().strftime("%Y"),
        dataset_display_name="Batch Ingestion Group"
    )
    
    # Update job type
    job = get_job(job_id)
    if job:
        job["job_type"] = "batch_import"

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise HTTPException(status_code=500, detail="DATABASE_URL environment variable is not configured")

    # Spawn background task
    background_tasks.add_task(run_batch_import_job, job_id, temp_dir, mappings, db_url)

    return {
        "job_id": job_id,
        "status": "queued",
        "progress_url": f"/api/admin/batch-import/status/{job_id}"
    }

@router.get("/api/admin/batch-import/status/{job_id}")
async def api_batch_import_status(
    job_id: str,
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    """Get the live status/progress of a batch import job."""
    job = get_job(job_id)
    if not job or job.get("job_type") != "batch_import":
        raise HTTPException(status_code=404, detail="Batch import job not found")
    return job

@router.get("/api/admin/batch-import/report/{job_id}")
async def api_batch_import_report(
    job_id: str,
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    """Download the batch import completion summary report."""
    job = get_job(job_id)
    if not job or job.get("job_type") != "batch_import":
        raise HTTPException(status_code=404, detail="Batch import job not found")
    
    batch_info = job.get("batch_info", {})
    return batch_info

@router.get("/api/admin/batch-import/schemas")
async def api_batch_import_schemas(
    request: Request,
    current_user=Depends(get_current_active_user_with_role(["1"])),
):
    """List target schemas for mapping dropdown options."""
    pool = request.app.state.db
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT display_name, db_name
            FROM schema_registry
            ORDER BY display_name;
            """
        )
        exclude_schemas = {"public", "nss", "mss"}
        return [{"schema": r["db_name"], "name": r["display_name"]} for r in rows if r["db_name"].lower() not in exclude_schemas]
