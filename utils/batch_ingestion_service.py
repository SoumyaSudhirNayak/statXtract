import os
import shutil
import traceback
import zipfile
import asyncio
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

from utils.job_manager import update_job, get_job, JOB_STATUS_PROCESSING, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED
from utils.batch_validation import scan_batch_archive
from utils.batch_mapping import resolve_target_mappings
from utils.ingestion_pipeline import ingest_upload_file, log_terminal

def package_survey_zip(isolated_survey_dir: Path, target_zip_path: Path):
    """
    Creates a flat ZIP archive containing all files from isolated_survey_dir.
    This ensures that DDI, layout, and renamed datasets are packaged in a flat,
    clean structure expected by the existing upload ingestion pipeline.
    """
    with zipfile.ZipFile(target_zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in isolated_survey_dir.rglob("*"):
            if f.is_file():
                # Store flat inside the zip (no nested folder hierarchies)
                zf.write(f, arcname=f.name)

async def run_batch_import_job(job_id: str, temp_dir: str, mappings: Dict[str, Any], db_url: str):
    """
    Background task that executes the batch import job.
    Processes each survey folder completely independently in isolated contexts,
    and packages them to call the EXISTING upload ingestion workflow.
    """
    try:
        update_job(job_id, status=JOB_STATUS_PROCESSING, current_state=JOB_STATUS_PROCESSING, message="Starting batch import...")

        # 1. Scan the archive to get all available survey folders and validation details
        surveys = scan_batch_archive(temp_dir)
        
        # 2. Resolve final target mappings (target schemas, custom table names, skipped files/folders)
        resolved_surveys = resolve_target_mappings(surveys, mappings)
        
        total_surveys = len(resolved_surveys)
        if total_surveys == 0:
            update_job(
                job_id,
                status=JOB_STATUS_COMPLETED,
                current_state=JOB_STATUS_COMPLETED,
                message="Completed: No surveys selected/mapped for import.",
                progress=100
            )
            return

        # Prepare batch_info for live progress tracking
        batch_info = {
            "surveys": {},
            "total_surveys": total_surveys,
            "surveys_imported": 0,
            "tables_created": 0,
            "records_imported": 0,
            "success_count": 0,
            "failure_count": 0,
            "warnings": [],
            "completed_at": None
        }

        # Initialize the batch_info status for each survey
        for s in resolved_surveys:
            folder = s["folder_name"]
            # Read mapped Dataset Display Name from overrides mapping
            folder_mapping = mappings.get(folder, {})
            custom_display_name = folder_mapping.get("display_name", "").strip()
            dataset_display = custom_display_name if custom_display_name else folder

            batch_info["surveys"][folder] = {
                "status": "pending",
                "progress": 0,
                "current_file": None,
                "current_table": None,
                "success_count": 0,
                "failure_count": 0,
                "error": None,
                "schema": s["target_schema"],
                "dataset_name": dataset_display,
                "processing_time_sec": 0,
                "tables_created": 0,
                "metadata_status": {"ddi_found": False, "layout_found": False},
                "tables_info": [],
                "files": {
                    **{f["relative_path"]: {"status": "pending", "table": f["target_table"]} for f in s["files"]},
                    **{doc: {"status": "pending", "table": "Documentation"} for doc in s.get("documentation_files", [])}
                }
            }

        update_job(job_id, message="Batch initialized", progress=5)
        
        # Save batch_info into the job object
        job = get_job(job_id)
        if job:
            job["batch_info"] = batch_info

        temp_path = Path(temp_dir)
        isolated_root = temp_path / "isolated"
        isolated_root.mkdir(parents=True, exist_ok=True)

        for s_idx, s in enumerate(resolved_surveys):
            folder = s["folder_name"]
            target_schema = s["target_schema"]
            
            # Start timer for processing time
            folder_start_time = datetime.now()

            # Set current folder in job context
            job = get_job(job_id)
            if job:
                job["current_folder"] = folder

            # Update batch_info/job status for current survey
            batch_info["surveys"][folder]["status"] = "processing"
            batch_info["surveys"][folder]["progress"] = 10
            
            # Log milestones
            update_job(job_id, log=f"[{datetime.now().strftime('%H:%M:%S')}] Processing folder {folder}")
            update_job(job_id, log=f"[{datetime.now().strftime('%H:%M:%S')}] Extracting files")
            if len(s["ddi_files"]) > 0:
                update_job(job_id, log=f"[{datetime.now().strftime('%H:%M:%S')}] DDI detected")

            update_job(job_id, message=f"Importing survey folder: {folder}...")

            # Calculate overall progress percentage
            overall_progress = int((s_idx / total_surveys) * 100)
            update_job(job_id, progress=overall_progress)

            # Create survey-specific isolated extraction/ingestion context
            isolated_survey_dir = isolated_root / folder
            isolated_survey_dir.mkdir(parents=True, exist_ok=True)

            sub_zip_path = isolated_root / f"{folder}_package.zip"

            try:
                # Resolve source directory path
                source_subdir = temp_path / s["relative_dir"]

                # Copy DDI file if present
                for ddi in s["ddi_files"]:
                    src_ddi = source_subdir / ddi
                    dest_ddi = isolated_survey_dir / Path(ddi).name
                    dest_ddi.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_ddi, dest_ddi)

                # Copy Layout file if present
                for layout in s["layout_files"]:
                    src_layout = source_subdir / layout
                    dest_layout = isolated_survey_dir / Path(layout).name
                    dest_layout.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_layout, dest_layout)

                # Copy dataset files renaming to target table name
                for f in s["files"]:
                    rel_path = f["relative_path"]
                    target_table = f["target_table"]
                    
                    src_file = source_subdir / rel_path
                    orig_suffix = Path(rel_path).suffix
                    dest_file = isolated_survey_dir / f"{target_table}{orig_suffix}"
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    
                    shutil.copy2(src_file, dest_file)

                # Copy documentation files directly (preserving original filenames)
                for doc in s.get("documentation_files", []):
                    src_doc = source_subdir / doc
                    dest_doc = isolated_survey_dir / Path(doc).name
                    dest_doc.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_doc, dest_doc)

                # Package files inside isolated folder into a temporary zip
                package_survey_zip(isolated_survey_dir, sub_zip_path)

                from utils.db_utils import to_snake_case_identifier
                from utils.job_manager import create_job
                
                # Use Dataset Display Name mapping overrides
                folder_mapping = mappings.get(folder, {})
                custom_display_name = folder_mapping.get("display_name", "").strip()
                dataset_display = custom_display_name if custom_display_name else folder
                dataset_db = to_snake_case_identifier(dataset_display) or "dataset"
                year = datetime.now().strftime("%Y")

                # Create a sub-job ID for this folder ingestion
                sub_job_id = create_job(
                    filename=f"{folder}_package.zip",
                    schema=target_schema,
                    schema_display_name=target_schema,
                    year=year,
                    dataset_display_name=dataset_display,
                    dataset_db_name=dataset_db
                )

                batch_info["surveys"][folder]["progress"] = 30
                update_job(job_id, log=f"[{datetime.now().strftime('%H:%M:%S')}] Starting ingestion")

                # Call the EXISTING upload ingestion workflow as an asyncio Task to poll it in real-time
                ingest_task = asyncio.create_task(
                    ingest_upload_file(
                        input_path=str(sub_zip_path),
                        db_url=db_url,
                        schema=target_schema,
                        year=year,
                        dataset_display_name=dataset_display,
                        dataset_db_name=dataset_db,
                        job_id=sub_job_id
                    )
                )

                last_message = None
                last_log_idx = 0
                last_proc_file_idx = 0

                while not ingest_task.done():
                    await asyncio.sleep(0.5)
                    
                    sub_job = get_job(sub_job_id)
                    if sub_job:
                        # 1. Update sub-progress for this folder
                        sub_progress = sub_job.get("progress") or 0
                        batch_info["surveys"][folder]["progress"] = sub_progress
                        
                        # Smoothly update overall progress: (s_idx / total_surveys * 100) + (sub_progress / total_surveys)
                        overall_progress_mid = int((s_idx / total_surveys) * 100 + (sub_progress / total_surveys))
                        update_job(job_id, progress=overall_progress_mid)

                        # 2. Check for message change
                        current_msg = sub_job.get("message")
                        if current_msg and current_msg != last_message:
                            update_job(job_id, log=f"[{datetime.now().strftime('%H:%M:%S')}] {current_msg}")
                            last_message = current_msg

                        # 3. Check for sub-job logs
                        sub_logs = sub_job.get("logs") or []
                        if len(sub_logs) > last_log_idx:
                            for log_msg in sub_logs[last_log_idx:]:
                                update_job(job_id, log=f"[{datetime.now().strftime('%H:%M:%S')}] {log_msg}")
                            last_log_idx = len(sub_logs)

                        # 4. Check for processed files status updates
                        proc_files = sub_job.get("processed_files") or []
                        if len(proc_files) > last_proc_file_idx:
                            for pf in proc_files[last_proc_file_idx:]:
                                stat = pf.get("status", "unknown").upper()
                                msg_detail = pf.get("message", "")
                                update_job(job_id, log=f"[{datetime.now().strftime('%H:%M:%S')}] Table created: {pf.get('name')} - {stat} ({msg_detail})")
                            last_proc_file_idx = len(proc_files)

                # Await the task to capture exceptions
                await ingest_task

                # After completion, verify sub-job status
                sub_job = get_job(sub_job_id) or {}
                if sub_job.get("status") == JOB_STATUS_FAILED:
                    raise ValueError(sub_job.get("message") or "Sub-job ingestion failed")

                # Connect to database to fetch row and variable counts
                import asyncpg
                from utils.db_utils import make_dataset_schema_name

                var_counts = {}
                row_counts = {}
                
                try:
                    conn = await asyncpg.connect(db_url)
                    try:
                        dataset_schema = make_dataset_schema_name(target_schema, dataset_db)
                        
                        # Variable counts
                        try:
                            var_rows = await conn.fetch(
                                f'SELECT table_name, COUNT(*) as count FROM "{dataset_schema}".variables GROUP BY table_name'
                            )
                            var_counts = {r["table_name"]: r["count"] for r in var_rows}
                        except Exception as ve:
                            log_terminal(f"Could not query variables table: {ve}", "warning")

                        # Row counts
                        for f in s["files"]:
                            t_name = f["target_table"]
                            try:
                                r_count = await conn.fetchval(f'SELECT COUNT(*) FROM "{dataset_schema}"."{t_name}"')
                                row_counts[t_name] = r_count
                            except Exception as re:
                                log_terminal(f"Could not query row count for table {t_name}: {re}", "warning")
                                row_counts[t_name] = 0
                    finally:
                        await conn.close()
                except Exception as db_err:
                    log_terminal(f"Database statistics fetch failed: {db_err}", "warning")

                # Calculate elapsed time in seconds
                elapsed_seconds = int((datetime.now() - folder_start_time).total_seconds())

                # Update status on success
                batch_info["surveys"][folder]["status"] = "success"
                batch_info["surveys"][folder]["progress"] = 100
                batch_info["surveys"][folder]["processing_time_sec"] = elapsed_seconds
                batch_info["surveys"][folder]["tables_created"] = len(s["files"])
                batch_info["surveys"][folder]["success_count"] = len(s["files"])
                batch_info["surveys"][folder]["metadata_status"] = {
                    "ddi_found": len(s["ddi_files"]) > 0,
                    "layout_found": len(s["layout_files"]) > 0
                }
                batch_info["surveys"][folder]["tables_info"] = [
                    {
                        "table_name": f["target_table"],
                        "rows": row_counts.get(f["target_table"], 0),
                        "variables": var_counts.get(f["target_table"], 0)
                    }
                    for f in s["files"]
                ]
                
                # Log completion milestone
                update_job(job_id, log=f"[{datetime.now().strftime('%H:%M:%S')}] Import completed")

                # Mark files as success
                for f in s["files"]:
                    rel_path = f["relative_path"]
                    batch_info["surveys"][folder]["files"][rel_path]["status"] = "success"
                for doc in s.get("documentation_files", []):
                    batch_info["surveys"][folder]["files"][doc]["status"] = "success"

                batch_info["surveys_imported"] += 1
                batch_info["success_count"] += 1
                batch_info["tables_created"] += len(s["files"])

                # Update progress after completing the current folder iteration
                overall_progress_post = int(((s_idx + 1) / total_surveys) * 100)
                update_job(job_id, progress=overall_progress_post)

            except Exception as e:
                # Capture traceback
                tb = traceback.format_exc()
                log_terminal(f"❌ Batch survey {folder} failed: {e}\n{tb}", "error")
                
                # Calculate elapsed time in seconds for failed folder
                elapsed_seconds = int((datetime.now() - folder_start_time).total_seconds())

                batch_info["surveys"][folder]["status"] = "failed"
                batch_info["surveys"][folder]["progress"] = 100
                batch_info["surveys"][folder]["processing_time_sec"] = elapsed_seconds
                batch_info["surveys"][folder]["tables_created"] = 0
                batch_info["surveys"][folder]["error"] = str(e)
                batch_info["surveys"][folder]["failure_count"] = len(s["files"])
                
                # Log completion with failed status
                update_job(job_id, log=f"[{datetime.now().strftime('%H:%M:%S')}] Import failed: {e}")

                # Mark files as failed
                for f in s["files"]:
                    rel_path = f["relative_path"]
                    batch_info["surveys"][folder]["files"][rel_path]["status"] = "failed"
                    batch_info["surveys"][folder]["files"][rel_path]["error"] = str(e)
                for doc in s.get("documentation_files", []):
                    batch_info["surveys"][folder]["files"][doc]["status"] = "failed"
                    batch_info["surveys"][folder]["files"][doc]["error"] = str(e)

                batch_info["failure_count"] += 1
                batch_info["warnings"].append(f"Survey {folder} failed: {e}")

                # Update progress after completing folder iteration with error
                overall_progress_post = int(((s_idx + 1) / total_surveys) * 100)
                update_job(job_id, progress=overall_progress_post)
            finally:
                # Clean up nested folder and zip for this sub-job
                if os.path.exists(sub_zip_path):
                    try:
                        os.remove(sub_zip_path)
                    except Exception:
                        pass

        # Finalize job status
        batch_info["completed_at"] = datetime.now().isoformat()
        
        # Check overall success or partial success
        if batch_info["failure_count"] == total_surveys:
            overall_status = JOB_STATUS_FAILED
            msg = "Batch import failed for all survey groups."
        elif batch_info["failure_count"] > 0:
            overall_status = JOB_STATUS_COMPLETED
            msg = f"Batch import partially completed. {batch_info['success_count']} success, {batch_info['failure_count']} failed."
        else:
            overall_status = JOB_STATUS_COMPLETED
            msg = f"Batch import completed successfully. {batch_info['success_count']} surveys imported."

        update_job(job_id, status=overall_status, current_state=overall_status, progress=100, message=msg)

    except Exception as e:
        tb = traceback.format_exc()
        log_terminal(f"❌ Global Batch import exception: {e}\n{tb}", "error")
        update_job(job_id, status=JOB_STATUS_FAILED, current_state=JOB_STATUS_FAILED, message=str(e), error=str(e))
    finally:
        # Clean up temp directory
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                log_terminal(f"🧹 Deleted batch temp extraction folder: {temp_dir}", "success")
            except Exception as e:
                log_terminal(f"⚠️ Failed to delete batch temp folder {temp_dir}: {e}", "warning")
