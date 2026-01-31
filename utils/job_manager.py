
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

# In-memory job store
# Structure:
# {
#   "job_id": {
#       "status": "pending" | "processing" | "completed" | "failed",
#       "progress": 0-100,
#       "message": "Starting...",
#       "files": [
#           {"name": "data.txt", "status": "pending", "rows": 0, "error": None}
#       ],
#       "errors": [],
#       "created_at": timestamp,
#       "updated_at": timestamp
#   }
# }
jobs: Dict[str, Dict[str, Any]] = {}

def create_job(filename: Optional[str] = None) -> str:
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "pending",
        "progress": 0,
        "message": "Job created",
        "filename": filename,
        "files": [],
        "errors": [],
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    return job_id

def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    return jobs.get(job_id)

def update_job(job_id: str, status: str = None, progress: int = None, message: str = None, files: list = None, error: str = None, filename: str = None):
    job = jobs.get(job_id)
    if not job:
        return
    
    if status:
        job["status"] = status
    if progress is not None:
        job["progress"] = progress
    if message:
        job["message"] = message
    if filename is not None:
        job["filename"] = filename
    if files is not None:
        job["files"] = files
    if error:
        job["errors"].append(error)
        
    job["updated_at"] = datetime.now().isoformat()
