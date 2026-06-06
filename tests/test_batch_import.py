import os
import sys
# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tempfile
import zipfile
import shutil
from pathlib import Path
import pytest
import pandas as pd

from utils.batch_validation import scan_batch_archive
from utils.batch_mapping import resolve_target_mappings, sanitize_table_name

def test_batch_validation_and_mapping():
    """Test that batch validation scans directories correctly and resolves mappings."""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # 1. Create a simulated survey package ZIP structure
        survey_a = temp_path / "Survey_A"
        survey_a.mkdir()
        
        (survey_a / "ddi.xml").write_text("<codeBook></codeBook>")
        (survey_a / "layout.xlsx").write_bytes(b"dummy_excel")
        (survey_a / "MHH.txt").write_text("12345")
        
        survey_b = temp_path / "Survey_B"
        survey_b.mkdir()
        (survey_b / "MPER.sav").write_bytes(b"dummy_sav")
        
        # Scan archive
        surveys = scan_batch_archive(temp_dir)
        
        assert len(surveys) == 2
        
        # Verify Survey A
        sa = next(s for s in surveys if s["folder_name"] == "Survey_A")
        assert sa["dataset_count"] == 1
        assert "MHH.txt" in sa["dataset_files"]
        assert sa["validation"]["ddi_found"] is True
        assert sa["validation"]["layout_found"] is True
        
        # Verify Survey B
        sb = next(s for s in surveys if s["folder_name"] == "Survey_B")
        assert sb["dataset_count"] == 1
        assert "MPER.sav" in sb["dataset_files"]
        assert sb["validation"]["ddi_found"] is False
        assert sb["validation"]["layout_found"] is False
        
        # 2. Test resolve_target_mappings
        custom_mappings = {
            "Survey_A": {
                "schema": "plfs",
                "files": {
                    "MHH.txt": {
                        "table_name": "custom_mhh_table",
                        "skip": False
                    }
                }
            },
            "Survey_B": {
                "skip": True
            }
        }
        
        # Boolean variables for mapping config format (Python uses True/False)
        custom_mappings["Survey_A"]["files"]["MHH.txt"]["skip"] = False
        
        resolved = resolve_target_mappings(surveys, custom_mappings)
        
        # Survey B should be skipped
        assert len(resolved) == 1
        
        r_sa = resolved[0]
        assert r_sa["folder_name"] == "Survey_A"
        assert r_sa["target_schema"] == "plfs"
        assert len(r_sa["files"]) == 1
        assert r_sa["files"][0]["relative_path"] == "MHH.txt"
        assert r_sa["files"][0]["target_table"] == "custom_mhh_table"
        
        # Test table naming sanitization
        assert sanitize_table_name("My Table! Name-123") == "my_table_name_123"

def test_wrapped_zip_structure():
    """Test that scan_batch_archive steps past a single-folder wrapper inside the ZIP."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create Main_Zip wrapper directory
        main_zip = temp_path / "Main_Zip"
        main_zip.mkdir()
        
        # Create nested survey folders under wrapper
        survey_a = main_zip / "Survey_A"
        survey_a.mkdir()
        (survey_a / "MHH.txt").write_text("12345")
        
        survey_b = main_zip / "Survey_B"
        survey_b.mkdir()
        (survey_b / "MPER.sav").write_bytes(b"dummy")
        
        surveys = scan_batch_archive(temp_dir)
        
        # Bypassed "Main_Zip" and correctly returned Survey_A and Survey_B
        assert len(surveys) == 2
        folder_names = [s["folder_name"] for s in surveys]
        assert "Survey_A" in folder_names
        assert "Survey_B" in folder_names
        
        # Check relative pathing
        sa = next(s for s in surveys if s["folder_name"] == "Survey_A")
        assert sa["relative_dir"] == "Main_Zip/Survey_A"

