import os
import sys
from pathlib import Path
import tempfile
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.ingestion_pipeline import _load_data_file

def test_load_dta_data_file():
    """Test that Stata (.dta) files are loaded correctly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        dta_file = temp_path / "test.dta"
        
        # Create a simple dataframe and write to stata
        df_src = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
        df_src.to_stata(str(dta_file), write_index=False)
        
        df_loaded = _load_data_file(dta_file, None)
        assert df_loaded is not None
        assert list(df_loaded.columns) == ['a', 'b']
        assert df_loaded.shape == (2, 2)

def test_load_xpt_data_file():
    """Test that SAS Transport (.xpt) files are loaded correctly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        xpt_file = temp_path / "test.xpt"
        
        # Create a simple dataframe and write to SAS XPORT format using pyreadstat
        df_src = pd.DataFrame({'a': [1.0, 2.0], 'b': ['x', 'y']})
        import pyreadstat
        pyreadstat.write_xport(df_src, str(xpt_file))
        
        df_loaded = _load_data_file(xpt_file, None)
        assert df_loaded is not None
        # Columns might be read in uppercase or lowercase depending on the SAS format
        cols = [c.lower() for c in df_loaded.columns]
        assert 'a' in cols
        assert 'b' in cols

def test_load_flat_json_data_file():
    """Test that flat JSON files are loaded correctly."""
    import json
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        json_file = temp_path / "test_flat.json"
        
        data = [
            {"state": "AP", "district": "Krishna", "population": 1000},
            {"state": "AP", "district": "Guntur", "population": 2000}
        ]
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f)
            
        df_loaded = _load_data_file(json_file, None)
        assert df_loaded is not None
        assert list(df_loaded.columns) == ["state", "district", "population"]
        assert df_loaded.iloc[0]["state"] == "AP"
        assert df_loaded.iloc[0]["population"] == "1000"
        assert df_loaded.iloc[1]["district"] == "Guntur"

def test_load_nested_json_data_file():
    """Test that nested JSON files are flattened correctly according to specification rules."""
    import json
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        json_file = temp_path / "test_nested.json"
        
        data = {
            "household": {
                "id": 1001,
                "state": "AP"
            },
            "members": [
                {
                    "name": "A",
                    "age": 25
                }
            ]
        }
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(data, f)
            
        df_loaded = _load_data_file(json_file, None)
        assert df_loaded is not None
        expected_columns = {"household.id", "household.state", "members.0.name", "members.0.age"}
        assert set(df_loaded.columns) == expected_columns
        assert df_loaded.iloc[0]["household.id"] == "1001"
        assert df_loaded.iloc[0]["household.state"] == "AP"
        assert df_loaded.iloc[0]["members.0.name"] == "A"
        assert df_loaded.iloc[0]["members.0.age"] == "25"

def test_load_malformed_json_data_file():
    """Test that malformed JSON files raise the specific ValueError required by the task."""
    import pytest
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        json_file = temp_path / "test_malformed.json"
        
        with open(json_file, "w", encoding="utf-8") as f:
            f.write("{invalid json content}")
            
        with pytest.raises(ValueError) as excinfo:
            _load_data_file(json_file, None)
        assert "Invalid JSON format detected." in str(excinfo.value)

@pytest.mark.asyncio
async def test_ingest_upload_layout_pdf_mock():
    """Test that ingestion pipeline handles a layout PDF correctly and parses it as a metadata source."""
    from unittest.mock import MagicMock, patch
    from pathlib import Path
    
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    
    # Mock return values for DB checks
    mock_conn.fetchval.return_value = False
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        pdf_file = temp_path / "survey_layout.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 mock pdf content")
        
        with patch("utils.ingestion_pipeline.create_engine", return_value=mock_engine), \
             patch("utils.ingestion_pipeline.update_job") as mock_update, \
             patch("utils.ingestion_pipeline.ensure_dataset_schema_tables"), \
             patch("utils.ingestion_pipeline.get_file_checksum", side_effect=lambda *args: __import__("uuid").uuid4().hex), \
             patch("utils.layout_parser.detect_and_parse_layout") as mock_parse:
            
            mock_parse.return_value = [
                {
                    "block_name": "BLOCK-A",
                    "field_name": "1",
                    "variable_name": "YR",
                    "description": "'24' for ASI 2023-24",
                    "data_type": "Character",
                    "width": "2",
                    "reference": "-",
                    "position": "1",
                    "code_values": None
                }
            ]
            
            from utils.ingestion_pipeline import ingest_upload_file
            await ingest_upload_file(
                input_path=str(pdf_file),
                db_url="postgresql://localhost/mock",
                schema="mock_survey",
                year="2024",
                dataset_display_name="Mock Layout",
                dataset_db_name="mock_layout",
                job_id="job_mock"
            )
            
            # Verify layout parser was called
            assert mock_parse.call_count >= 1

@pytest.mark.asyncio
async def test_ingest_upload_layout_zip_csv_mock():
    """Test that ingestion pipeline handles a ZIP containing CSV and layout PDF correctly."""
    from unittest.mock import MagicMock, patch
    from pathlib import Path
    import zipfile
    
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    
    # Mock return values for DB checks
    mock_conn.fetchval.return_value = False
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        zip_file = temp_path / "dataset.zip"
        
        # Create a mock zip containing data.csv and layout.pdf
        with zipfile.ZipFile(zip_file, "w") as zf:
            zf.writestr("data.csv", "a,b\n1,2\n3,4")
            zf.writestr("layout.pdf", "%PDF-1.4 mock pdf content")
        
        with patch("utils.ingestion_pipeline.create_engine", return_value=mock_engine), \
             patch("utils.ingestion_pipeline.update_job") as mock_update, \
             patch("utils.ingestion_pipeline.ensure_dataset_schema_tables"), \
             patch("utils.ingestion_pipeline.get_file_checksum", side_effect=lambda *args: __import__("uuid").uuid4().hex), \
             patch("utils.layout_parser.detect_and_parse_layout") as mock_parse, \
             patch("utils.ingestion_pipeline._load_data_file") as mock_load:
            
            mock_load.return_value = pd.DataFrame({"a": [1, 3], "b": [2, 4]})
            mock_parse.return_value = [
                {
                    "block_name": "BLOCK-A",
                    "field_name": "1",
                    "variable_name": "a",
                    "description": "Variable A description",
                    "data_type": "Character",
                    "width": "2",
                    "reference": "-",
                    "position": "1",
                    "code_values": None
                }
            ]
            
            from utils.ingestion_pipeline import ingest_upload_file
            await ingest_upload_file(
                input_path=str(zip_file),
                db_url="postgresql://localhost/mock",
                schema="mock_survey",
                year="2024",
                dataset_display_name="Mock ZIP CSV",
                dataset_db_name="mock_zip_csv",
                job_id="job_mock"
            )
            
            # Verify layout parser was called
            assert mock_parse.call_count >= 1
