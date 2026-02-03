
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

# Job Status Constants
JOB_STATUS_INITIALIZED = "INITIALIZED"
JOB_STATUS_QUEUED = "QUEUED"
JOB_STATUS_CONVERTING = "CONVERTING"
JOB_STATUS_EXPORTING = "EXPORTING"
JOB_STATUS_INGESTING = "INGESTING"
JOB_STATUS_COMPLETED = "COMPLETED"
JOB_STATUS_FAILED = "FAILED"

# In-memory job store
jobs: Dict[str, Dict[str, Any]] = {}

def create_job(filename: Optional[str] = None, schema: Optional[str] = None) -> str:
    job_id = str(uuid.uuid4())
    # Ensure schema is not None/empty; if it is, this should likely be caught earlier, 
    # but we can default to 'public' ONLY if absolutely necessary. 
    # However, per requirements, we want to fail fast if schema is invalid.
    # But create_job might be called with validated schema.
    
    jobs[job_id] = {
        "status": JOB_STATUS_INITIALIZED,
        "current_state": JOB_STATUS_INITIALIZED,
        "progress": 0,
        "message": "Job initialized",
        "filename": filename,
        "schema": schema,
        "job_type": "upload",
        "input_file_path": None,
        "output_paths": {},
        "logs": [],
        "log_messages": [],
        "files": [],
        "errors": [],
        "transitions": [{
            "from": None,
            "to": JOB_STATUS_INITIALIZED,
            "timestamp": datetime.now().isoformat(),
            "reason": "Job created"
        }],
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    return job_id

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    return jobs.get(job_id)

def update_job(
    job_id: str,
    status: str = None,
    current_state: str = None,
    progress: int = None,
    message: str = None,
    files: list = None,
    error: str = None,
    filename: str = None,
    schema: str = None,
    job_type: str = None,
    input_file_path: str = None,
    output_paths: dict = None,
    log: str = None,
):
    job = jobs.get(job_id)
    if not job:
        return

    if status is not None:
        s = str(status).strip()
        status = s.upper() if s else None
    if current_state is not None:
        s = str(current_state).strip()
        current_state = s.upper() if s else None

    # Valid lifecycle states in order
    LIFECYCLE_ORDER = [
        JOB_STATUS_INITIALIZED,
        JOB_STATUS_QUEUED,
        JOB_STATUS_CONVERTING,
        JOB_STATUS_EXPORTING,
        JOB_STATUS_INGESTING,
        JOB_STATUS_COMPLETED
    ]
    
    TERMINAL_STATES = {JOB_STATUS_COMPLETED, JOB_STATUS_FAILED}
    
    old_status = job.get("status")
    old_state = job.get("current_state")
    old_progress = job.get("progress")
    
    # If job is already in a terminal state, DO NOT allow changing status/state 
    # unless it is to add logs or if it's strictly just updating message/progress (though progress should be 100)
    is_terminal = old_status in TERMINAL_STATES
    
    if is_terminal:
        # Block status/state changes if they try to move out of terminal
        if status and status not in TERMINAL_STATES:
            # If trying to reset a terminal job, ignore status change
            status = None 
        if current_state and current_state not in TERMINAL_STATES:
            current_state = None
        if progress is not None:
            try:
                p = int(progress)
            except Exception:
                p = None
            if p is None or p != 100:
                progress = None

    # Resolve new status/state
    new_status = status if status else old_status
    new_state = current_state if current_state else old_state

    # Sync logic: If current_state is provided and is a lifecycle state, it dictates status
    if current_state and current_state in set(LIFECYCLE_ORDER) | {JOB_STATUS_FAILED}:
        new_status = current_state
    
    # If status is provided, it might imply current_state
    if status and not current_state:
        new_state = status

    # Normalize
    if new_status: job["status"] = new_status
    if new_state: job["current_state"] = new_state

    # Record transition if status changed
    if new_status != old_status:
        job["transitions"].append({
            "from": old_status,
            "to": new_status,
            "timestamp": datetime.now().isoformat(),
            "reason": message or "Status updated"
        })

    if progress is not None:
        job["progress"] = progress
    if message:
        job["message"] = message
    if filename is not None:
        job["filename"] = filename
    if schema is not None:
        job["schema"] = schema
    if job_type:
        job["job_type"] = job_type
    if input_file_path is not None:
        job["input_file_path"] = input_file_path
    if output_paths is not None:
        job["output_paths"] = output_paths
    if files is not None:
        job["files"] = files
    if error:
        job["errors"].append(error)
        # Auto-fail? 
        # Ideally, explicit failure is better, but if error is passed, we often want to fail.
        # However, sometimes we just log a non-fatal error.
        # So we leave status change to the caller or explicit status arg.
    if log:
        job["logs"].append(log)
        if "log_messages" in job:
            job["log_messages"].append(log)
        
    job["updated_at"] = datetime.now().isoformat()

def list_jobs() -> Dict[str, Dict[str, Any]]:
    return jobs
