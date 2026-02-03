
import unittest
import time
from utils.watcher import IngestionWatcher
from utils.job_manager import create_job, update_job, get_job, jobs

class TestIngestionWatcher(unittest.TestCase):
    def setUp(self):
        # Clear jobs before each test
        jobs.clear()
        self.watcher = IngestionWatcher(poll_interval_sec=0.1)

    def test_finalizes_job_with_completed_state(self):
        job_id = create_job(filename="test.zip")
        # Simulate job stuck in processing but with COMPLETED state
        update_job(job_id, status="processing", progress=99)
        jobs[job_id]["current_state"] = "COMPLETED"
        
        finalized = self.watcher.tick()
        
        self.assertEqual(finalized, 1)
        job = get_job(job_id)
        self.assertEqual(job["status"], "COMPLETED")
        self.assertEqual(job["progress"], 100)
        self.assertIn("state sync", job["message"])

    def test_finalizes_job_with_log_marker(self):
        job_id = create_job(filename="test.zip")
        # Simulate job processing but with success log
        update_job(job_id, status="processing", progress=50)
        update_job(job_id, log="Some processing...")
        update_job(job_id, log="successfully deleted and process complete")
        
        finalized = self.watcher.tick()
        
        self.assertEqual(finalized, 1)
        job = get_job(job_id)
        self.assertEqual(job["status"], "COMPLETED")
        self.assertEqual(job["progress"], 100)
        self.assertIn("log detection", job["message"])

    def test_ignores_already_completed_jobs(self):
        job_id = create_job(filename="test.zip")
        update_job(job_id, status="COMPLETED", progress=100)
        
        finalized = self.watcher.tick()
        
        self.assertEqual(finalized, 0)

    def test_ignores_failed_jobs(self):
        job_id = create_job(filename="test.zip")
        update_job(job_id, status="FAILED", error="Some error")
        
        finalized = self.watcher.tick()
        
        self.assertEqual(finalized, 0)

    def test_does_not_finalize_incomplete_jobs(self):
        job_id = create_job(filename="test.zip")
        update_job(job_id, status="processing", progress=50)
        update_job(job_id, log="Still working...")
        
        finalized = self.watcher.tick()
        
        self.assertEqual(finalized, 0)
        job = get_job(job_id)
        self.assertEqual(job["status"], "PROCESSING")

if __name__ == "__main__":
    unittest.main()
