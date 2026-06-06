
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


def test_extract_positions_from_layout():
    """Test that variable positions and widths are correctly extracted from a layout Excel file."""
    from utils.ingestion_pipeline import extract_positions_from_layout, DDIVariable
    import pandas as pd
    
    # 1. Create a dummy layout Excel structure with separate sheets and a stacked sheet
    layout_data = {
        "Level 1": [
            ["Title row - MOSPI layout info", "", ""],
            ["Variable Name", "Byte Position", "Length"],
            ["FSU Serial No.", "1-5", 5],
            ["Schedule", "6-8", 3],
            ["Sector", "9-9", 1],
        ],
        "Stacked": [
            ["Level/Section A", "", ""],
            ["Variable Name", "Byte Position", "Length"],
            ["age", "1-5", 5],
            ["Level/Section B", "", ""],
            ["Item", "Position", "Length"],
            ["status", "6 to 8", 3],
        ]
    }
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        layout_file = temp_path / "Data_Layout.xlsx"
        
        # Write layout spreadsheet
        with pd.ExcelWriter(layout_file, engine='openpyxl') as writer:
            for sheet, data in layout_data.items():
                pd.DataFrame(data).to_excel(writer, sheet_name=sheet, index=False, header=False)
                
        # DDI variables mock
        ddi_vars = []
        v1 = DDIVariable()
        v1.name = "fsu"
        v1.label = "FSU Serial No."
        ddi_vars.append(v1)
        
        v2 = DDIVariable()
        v2.name = "sch"
        v2.label = "Schedule"
        ddi_vars.append(v2)
        
        v3 = DDIVariable()
        v3.name = "sec"
        v3.label = "Sector"
        ddi_vars.append(v3)
        
        # Test Case 1: Separate sheet
        vars_parsed = extract_positions_from_layout(layout_file, "Level01.txt", ddi_vars)
        assert len(vars_parsed) == 3
        assert vars_parsed[0].name == "fsu"
        assert vars_parsed[0].start_pos == 1
        assert vars_parsed[0].width == 5
        
        # Test Case 2: Stacked letter sections
        vars_parsed_a = extract_positions_from_layout(layout_file, "Level_A.txt", ddi_vars)
        assert len(vars_parsed_a) == 1
        assert vars_parsed_a[0].name == "age"
        assert vars_parsed_a[0].start_pos == 1
        assert vars_parsed_a[0].width == 5
        
        vars_parsed_b = extract_positions_from_layout(layout_file, "Level_B.txt", ddi_vars)
        assert len(vars_parsed_b) == 1
        assert vars_parsed_b[0].name == "status"
        assert vars_parsed_b[0].start_pos == 6
        assert vars_parsed_b[0].width == 3


