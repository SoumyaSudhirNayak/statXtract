
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
    # We must patch 'main.convert_and_ingest_nesstar_binary_study' because 'main' imports it.
    # We patch 'utils.ingestion_pipeline.process_directory' because 'process_dataset_zip' (which is real) calls it.
    
    with patch("utils.ingestion_pipeline.process_directory", new_callable=AsyncMock) as mock_process_dir, \
         patch("utils.ingestion_pipeline.schema_exists", return_value=True), \
         patch("utils.ingestion_pipeline.ensure_metadata_tables"), \
         patch("main.convert_and_ingest_nesstar_binary_study", new_callable=AsyncMock) as mock_nesstar, \
         patch("utils.ingestion_pipeline.update_job") as mock_update_job:
        
        # Create a dummy .sav file
        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as tmp:
            tmp.write(b"dummy content")
            tmp_path = tmp.name
            
        try:
            job_id = create_job(filename=os.path.basename(tmp_path), schema="public")
            db_url = "postgresql://user:pass@localhost/db"
            schema = "public"
            
            await run_ingestion_job(job_id, tmp_path, db_url, schema)
            
            # Assert process_directory was called
            assert mock_process_dir.called, "process_directory should be called for .sav files"
            
            # Assert Nesstar conversion was NOT called
            assert not mock_nesstar.called, "Nesstar conversion should NOT be called for .sav files"
            
            # Verify arguments to process_directory
            # It should be called with a temporary directory containing the file
            args, kwargs = mock_process_dir.call_args
            temp_path_arg = args[0]
            assert isinstance(temp_path_arg, Path)
            
            # Check if file was copied to temp dir
            copied_file = temp_path_arg / os.path.basename(tmp_path)
            # We can't check file existence here because the context manager (TemporaryDirectory) exits after await returns.
            # But we can check that the path argument was different from original path
            assert str(temp_path_arg) != os.path.dirname(tmp_path)
            
            # Verify require_metadata is False
            assert kwargs.get("require_metadata") is False

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


@pytest.mark.asyncio
async def test_run_ingestion_job_uses_job_schema_over_argument():
    with patch("utils.ingestion_pipeline.process_directory", new_callable=AsyncMock) as mock_process_dir, \
         patch("utils.ingestion_pipeline.schema_exists", return_value=True), \
         patch("utils.ingestion_pipeline.ensure_metadata_tables"), \
         patch("main.convert_and_ingest_nesstar_binary_study", new_callable=AsyncMock) as mock_nesstar, \
         patch("utils.ingestion_pipeline.update_job"):

        with tempfile.NamedTemporaryFile(suffix=".sav", delete=False) as tmp:
            tmp.write(b"dummy content")
            tmp_path = tmp.name

        try:
            job_id = create_job(filename="x.sav", schema="asi")
            db_url = "postgresql://user:pass@localhost/db"

            await run_ingestion_job(job_id, tmp_path, db_url, schema="public")

            assert mock_process_dir.called
            assert not mock_nesstar.called
            args, _ = mock_process_dir.call_args
            assert args[2] == "asi"
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

@pytest.mark.asyncio
async def test_zip_with_sav_works():
    """Test that a zip containing .sav is processed via extraction."""
    with patch("utils.ingestion_pipeline.process_directory", new_callable=AsyncMock) as mock_process_dir, \
         patch("utils.ingestion_pipeline.schema_exists", return_value=True), \
         patch("utils.ingestion_pipeline.ensure_metadata_tables"), \
         patch("main.convert_and_ingest_nesstar_binary_study", new_callable=AsyncMock) as mock_nesstar, \
         patch("utils.ingestion_pipeline.update_job") as mock_update_job:
        
        # Create a dummy zip file
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_zip:
            import zipfile
            with zipfile.ZipFile(tmp_zip, "w") as zf:
                zf.writestr("data.sav", b"dummy content")
            tmp_zip_path = tmp_zip.name

        try:
            job_id = create_job(filename=os.path.basename(tmp_zip_path), schema="public")
            db_url = "postgresql://user:pass@localhost/db"
            schema = "public"
            
            await run_ingestion_job(job_id, tmp_zip_path, db_url, schema)
            
            # Assert process_directory was called
            assert mock_process_dir.called
            # Assert Nesstar conversion was NOT called
            assert not mock_nesstar.called
            
        finally:
            if os.path.exists(tmp_zip_path):
                os.remove(tmp_zip_path)

@pytest.mark.asyncio
async def test_nesstar_large_file_trigger():
    """Test that a large .nesstar file triggers Nesstar conversion."""
    # Mock os.path.getsize to return a large size
    # Mock os.getenv to return a small threshold
    
    # We need to wrap os.path.getsize to only mock for our file
    original_getsize = os.path.getsize
    
    def side_effect_getsize(path):
        if str(path).endswith(".nesstar"):
            return 1024 * 1024 * 200 # 200MB
        return original_getsize(path)

    with patch("utils.ingestion_pipeline.process_directory", new_callable=AsyncMock) as mock_process_dir, \
         patch("main.convert_and_ingest_nesstar_binary_study", new_callable=AsyncMock) as mock_nesstar, \
         patch("utils.ingestion_pipeline.update_job") as mock_update_job, \
         patch("os.path.getsize", side_effect=side_effect_getsize), \
         patch("os.getenv", side_effect=lambda k, d=None: "1048576" if k == "NESSTAR_BINARY_THRESHOLD_BYTES" else (d or "")):
        
        # Create a dummy .nesstar file
        with tempfile.NamedTemporaryFile(suffix=".nesstar", delete=False) as tmp:
            tmp.write(b"dummy content")
            tmp_path = tmp.name
            
        try:
            job_id = "test_job_nesstar"
            db_url = "postgresql://user:pass@localhost/db"
            schema = "public"
            
            await run_ingestion_job(job_id, tmp_path, db_url, schema)
            
            # Assert Nesstar conversion WAS called
            assert mock_nesstar.called
            # Assert process_directory was NOT called
            assert not mock_process_dir.called
            
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
