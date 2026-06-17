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
