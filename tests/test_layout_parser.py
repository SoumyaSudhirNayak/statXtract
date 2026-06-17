import os
import sys
import tempfile
import pandas as pd
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.layout_parser import parse_code_values_string, parse_excel_layout, detect_and_parse_layout

def test_parse_code_values_string():
    """Test parsing code values from layout string representations."""
    # Test dictionary-like syntax
    s1 = "1: Andhra Pradesh\n2: Telengana\n3: Karnataka"
    res1 = parse_code_values_string(s1)
    assert isinstance(res1, dict)
    assert res1["1"] == "Andhra Pradesh"
    assert res1["2"] == "Telengana"
    assert res1["3"] == "Karnataka"

    # Test hyphen-separated syntax
    s2 = "01-Rural, 02-Urban"
    res2 = parse_code_values_string(s2)
    assert isinstance(res2, dict)
    assert res2["01"] == "Rural"
    assert res2["02"] == "Urban"

    # Test fallback syntax (not structured code values)
    s3 = "Continuous numeric scale"
    res3 = parse_code_values_string(s3)
    assert res3 == "Continuous numeric scale"

def test_parse_excel_layout():
    """Test parsing layout tables from mock layout Excel files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        excel_file = temp_path / "survey_layout_file.xlsx"
        
        # Create a mock layout sheet
        data = {
            0: [None, None, "Block 1", None, None, None],
            1: [None, "Field Name", "Description", "Type", "Width", "Code Values"],
            2: [1.0, "state_code", "State Code Identifier", "Numeric", 2.0, "1: AP, 2: KA"],
            3: [2.0, "dist_code", "District Code Identifier", "Numeric", 3.0, "10: Guntur, 20: Bangalore"]
        }
        df = pd.DataFrame.from_dict(data, orient='index')
        
        with pd.ExcelWriter(str(excel_file)) as writer:
            df.to_excel(writer, sheet_name="Block 1", header=False, index=False)
            
        rows = parse_excel_layout(excel_file)
        assert len(rows) == 2
        assert rows[0]["block_name"] == "Block 1"
        assert rows[0]["variable_name"] == "state_code"
        assert rows[0]["description"] == "State Code Identifier"
        assert rows[0]["data_type"] == "Numeric"
        assert rows[0]["width"] in ("2", "2.0")
        assert isinstance(rows[0]["code_values"], dict)
        assert rows[0]["code_values"]["1"] == "AP"

        assert rows[1]["variable_name"] == "dist_code"
        assert rows[1]["code_values"]["10"] == "Guntur"

def test_detect_and_parse_layout():
    """Test automatic layout detection based on filename."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # File without layout keyword in name should be skipped
        f1 = temp_path / "data_file.xlsx"
        df = pd.DataFrame({"col1": [1, 2]})
        df.to_excel(str(f1), index=False)
        
        res1 = detect_and_parse_layout(f1)
        assert res1 == []

        # File with layout keyword should be processed
        f2 = temp_path / "variable_layout.xlsx"
        # Create a valid layout structure
        data = {
            0: ["Field Name", "Description", "Type", "Width", "Code Values"],
            1: ["state_code", "State Code Identifier", "Numeric", 2, "1: AP, 2: KA"]
        }
        df_layout = pd.DataFrame.from_dict(data, orient='index')
        df_layout.to_excel(str(f2), header=False, index=False)
        
        res2 = detect_and_parse_layout(f2)
        assert len(res2) == 1
        assert res2[0]["variable_name"] == "state_code"

def test_parse_pdf_layout_mock():
    """Test parsing layout tables from mock PDF layout data with block detection."""
    from unittest.mock import MagicMock, patch
    
    mock_page = MagicMock()
    mock_page.extract_words.return_value = [
        {"text": "BLOCK-A", "top": 50, "bottom": 60, "x0": 10, "x1": 50},
        {"text": "Variable", "top": 100, "bottom": 110, "x0": 10, "x1": 50},
        {"text": "Description", "top": 100, "bottom": 110, "x0": 60, "x1": 150},
        {"text": "Type", "top": 100, "bottom": 110, "x0": 160, "x1": 200},
        {"text": "Width", "top": 100, "bottom": 110, "x0": 210, "x1": 250},
    ]
    
    mock_table = MagicMock()
    mock_table.bbox = (10, 95, 300, 250)
    mock_table.extract.return_value = [
        ["Variable", "Description", "Type", "Width"],
        ["YR", "'24' for ASI 2023-24", "Character", "2"],
        ["BLK", "Block code", "Character", "1"],
    ]
    mock_page.find_tables.return_value = [mock_table]
    
    mock_pdf = MagicMock()
    mock_pdf.pages = [mock_page]
    
    with patch("pdfplumber.open") as mock_open:
        mock_open.return_value.__enter__.return_value = mock_pdf
        
        from utils.layout_parser import parse_pdf_layout
        rows = parse_pdf_layout(Path("dummy.pdf"))
        
        assert len(rows) == 2
        assert rows[0]["block_name"] == "BLOCK-A"
        assert rows[0]["variable_name"] == "YR"
        assert rows[0]["description"] == "'24' for ASI 2023-24"
        assert rows[0]["data_type"] == "Character"
        assert rows[0]["width"] == "2"
        
        assert rows[1]["block_name"] == "BLOCK-A"
        assert rows[1]["variable_name"] == "BLK"
