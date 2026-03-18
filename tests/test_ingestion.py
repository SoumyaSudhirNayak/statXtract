
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import os
import tempfile
import shutil
from pathlib import Path
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import run_ingestion_job, _wait_for_stable_file, _schema_exists
from utils.job_manager import create_job, get_job, update_job
from utils.watcher import IngestionWatcher

@pytest.mark.asyncio
async def test_standalone_sav_bypasses_nesstar():
    """Test that a standalone .sav file is processed directly without Nesstar conversion."""
    with patch("main.ingest_upload_file", new_callable=AsyncMock) as mock_ingest, \
         patch("utils.ingestion_pipeline.update_job") as mock_update_job:
        
        # Create a dummy .sav file
        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as tmp:
            tmp.write(b"dummy content")
            tmp_path = tmp.name
            
        try:
            job_id = create_job(
                filename=os.path.basename(tmp_path),
                schema="public",
                schema_display_name="Public",
                year="2023",
                dataset_display_name="Test Dataset",
                dataset_db_name="test_dataset",
            )
            db_url = "postgresql://user:pass@localhost/db"

            await run_ingestion_job(job_id, tmp_path, db_url)

            assert mock_ingest.called

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


@pytest.mark.asyncio
async def test_run_ingestion_job_uses_job_schema_over_argument():
    with patch("main.ingest_upload_file", new_callable=AsyncMock) as mock_ingest, \
         patch("utils.ingestion_pipeline.update_job"):

        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as tmp:
            tmp.write(b"dummy content")
            tmp_path = tmp.name

        try:
            job_id = create_job(
                filename="x.sav",
                schema="annual_survey_of_industries",
                schema_display_name="Annual Survey of Industries",
                year="2023",
                dataset_display_name="ASI 2023",
                dataset_db_name="asi_2023",
            )
            db_url = "postgresql://user:pass@localhost/db"

            await run_ingestion_job(job_id, tmp_path, db_url)

            assert mock_ingest.called
            kwargs = mock_ingest.call_args.kwargs
            assert kwargs["schema"] == "annual_survey_of_industries"
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

@pytest.mark.asyncio
async def test_zip_with_sav_works():
    """Test that a zip containing .sav is processed via extraction."""
    with patch("main.ingest_upload_file", new_callable=AsyncMock) as mock_ingest, \
         patch("utils.ingestion_pipeline.update_job") as mock_update_job:
        
        # Create a dummy zip file
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
            import zipfile
            with zipfile.ZipFile(tmp_zip, "w") as zf:
                zf.writestr("data.sav", b"dummy content")
            tmp_zip_path = tmp_zip.name

        try:
            job_id = create_job(
                filename=os.path.basename(tmp_zip_path),
                schema="public",
                schema_display_name="Public",
                year="2023",
                dataset_display_name="Zip Dataset",
                dataset_db_name="zip_dataset",
            )
            db_url = "postgresql://user:pass@localhost/db"

            await run_ingestion_job(job_id, tmp_zip_path, db_url)

            assert mock_ingest.called
            
        finally:
            if os.path.exists(tmp_zip_path):
                os.remove(tmp_zip_path)

@pytest.mark.asyncio
async def test_nesstar_large_file_trigger():
    """Test that a .nesstar file is handled via the unified ingestion entrypoint."""
    with patch("main.ingest_upload_file", new_callable=AsyncMock) as mock_ingest, \
         patch("utils.ingestion_pipeline.update_job") as mock_update_job:
        
        # Create a dummy .nesstar file
        with tempfile.NamedTemporaryFile(suffix=".nesstar", delete=False) as tmp:
            tmp.write(b"dummy content")
            tmp_path = tmp.name
            
        try:
            job_id = create_job(
                filename=os.path.basename(tmp_path),
                schema="periodic_labour_force_survey",
                schema_display_name="Periodic Labour Force Survey",
                year="2023",
                dataset_display_name="PLFS 2023",
                dataset_db_name="plfs_2023",
            )
            db_url = "postgresql://user:pass@localhost/db"

            await run_ingestion_job(job_id, tmp_path, db_url)

            assert mock_ingest.called
            
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


@pytest.mark.asyncio
async def test_wait_for_stable_file_detects_ready_file():
    with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as tmp:
        tmp.write(b"A" * 2048)
        tmp_path = tmp.name

    try:
        ready = await _wait_for_stable_file(
            tmp_path,
            min_bytes=1024,
            stable_checks=1,
            interval_sec=0.01,
            timeout_sec=1,
        )
        assert ready is True
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def test_completion_watcher_closes_stuck_job_at_45_percent():
    job_id = create_job(filename="stuck.zip", schema="public")
    update_job(job_id, status="processing", progress=45, message="Exporting ALL datasets")
    update_job(job_id, log="successfully deleted and process complete")

    watcher = IngestionWatcher()
    n = watcher.tick()
    assert n == 1

    job = get_job(job_id)
    assert job is not None
    assert job.get("status") == "COMPLETED"
    assert job.get("progress") == 100
    assert job.get("current_state") == "COMPLETED"


@pytest.mark.asyncio
async def test_schema_exists_uses_information_schema():
    conn = AsyncMock()
    conn.fetchval = AsyncMock(return_value=1)
    ok = await _schema_exists(conn, "public")
    assert ok is True
    assert conn.fetchval.called


@pytest.mark.asyncio
async def test_schema_exists_empty_returns_false():
    conn = AsyncMock()
    conn.fetchval = AsyncMock(return_value=1)
    ok = await _schema_exists(conn, "")
    assert ok is False
    assert not conn.fetchval.called
