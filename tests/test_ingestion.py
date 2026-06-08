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
