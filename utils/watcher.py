
import asyncio
import logging
import os
import re
from typing import Dict, Any, List

from utils.job_manager import list_jobs, update_job, JOB_STATUS_COMPLETED, JOB_STATUS_FAILED

logger = logging.getLogger(__name__)

class IngestionWatcher:
    """
    Watches ingestion jobs and ensures they are correctly marked as completed.
    Parses logs for success messages to handle cases where the main process might have missed the update.
    """

    def __init__(self, poll_interval_sec: float = 1.0):
        self.poll_interval_sec = max(poll_interval_sec, 0.2)
        self.running = False
        self._task = None

    async def start(self):
        """Starts the watcher loop in the background."""
        if self.running:
            return
        self.running = True
        logger.info(f"IngestionWatcher started (interval={self.poll_interval_sec}s)")
        while self.running:
            try:
                self.tick()
            except Exception as e:
                logger.error(f"IngestionWatcher error: {e}")
            await asyncio.sleep(self.poll_interval_sec)

    def stop(self):
        """Stops the watcher loop."""
        self.running = False

    def tick(self) -> int:
        """
        Single iteration of the watcher.
        Returns the number of jobs finalized in this tick.
        """
        finalized_count = 0
        jobs = list_jobs()
        
        # Iterate over a copy of items to avoid modification issues if any
        for job_id, job in list(jobs.items()):
            # Check against authoritative terminal states
            status = str(job.get("status") or "").upper()
            if status in (JOB_STATUS_COMPLETED, JOB_STATUS_FAILED):
                continue

            should_complete = False
            msg = "Ingestion detected as complete by watcher"

            # Check 1: Check if current_state is COMPLETED but status is not
            current_state = str(job.get("current_state") or "").upper()
            if current_state == JOB_STATUS_COMPLETED:
                should_complete = True
                msg = "Ingestion complete (state sync)"

            # Check 2: Check logs for success message
            # The user specifically mentioned: "successfully deleted and process complete"
            # We also check for "Job {job_id} completed successfully" which main.py logs
            # And "successfully uploaded" which ingestion_pipeline logs
            logs = job.get("logs") or []
            log_text = "\n".join(str(x) for x in logs).lower()
            
            success_markers = [
                "successfully deleted and process complete",
                "job completed successfully",
                "upload complete!",
                "nesstar_real_export_success",
                "conversion completed",
                "export complete",
                "export phase completed",
            ]
            
            if any(marker in log_text for marker in success_markers):
                # Double check we aren't in a failed state in logs
                if "failed:" not in log_text or "successfully uploaded" in log_text:
                     should_complete = True
                     msg = "Ingestion complete (log detection)"

            if should_complete:
                logger.info(f"Watcher finalizing job {job_id}: {msg}")
                update_job(
                    job_id,
                    status=JOB_STATUS_COMPLETED,
                    progress=100,
                    message=msg,
                    current_state=JOB_STATUS_COMPLETED
                )
                finalized_count += 1

        return finalized_count
