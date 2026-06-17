import os
import re
import json
import pandas as pd
import pdfplumber
from pathlib import Path
from typing import List, Dict, Any, Tuple

def parse_code_values_string(s: str) -> Any:
    if not s or s.strip() == "":
        return None
    items = re.split(r'[,;\n\r]+', s)
    codes_dict = {}
    for item in items:
        m = re.match(r'^\s*([0-9a-zA-Z_]+)\s*[:=\-\.]\s*(.+)$', item.strip())
        if m:
            key = m.group(1).strip()
            val = m.group(2).strip()
            codes_dict[key] = val
    if codes_dict:
        return codes_dict
    return s

def clean_cell_val(cell) -> str:
    if cell is None or pd.isna(cell):
        return ""
    val = str(cell).strip()
    if val.endswith(".0"):
        try:
            return str(int(float(val)))
        except ValueError:
            pass
    return val

def _process_table_cells(table: List[List[Any]], current_block: str) -> Tuple[List[Dict[str, Any]], str]:
    rows = []
    if not table or len(table) < 2:
        return rows, current_block
        
    header_row = None
    col_mapping = {}
    
    for r_idx in range(min(3, len(table))):
        row = table[r_idx]
        cleaned_row = [clean_cell_val(cell).lower() for cell in row]
        
        has_var = False
        row_map = {}
        for col_idx, val in enumerate(cleaned_row):
            if not val:
                continue
            if "descr" in val or "label" in val:
                row_map["description"] = col_idx
            elif "type" in val or "data_type" in val:
                row_map["type"] = col_idx
            elif any(k in val for k in ["width", "len", "size"]):
                row_map["width"] = col_idx
            elif any(k in val for k in ["pos", "byte", "start"]):
                row_map["position"] = col_idx
            elif any(k in val for k in ["ref", "sec", "block", "ref_no", "sch"]):
                row_map["reference"] = col_idx
            elif any(k in val for k in ["s.no", "sno", "sr", "serial", "srl"]):
                row_map["field_name"] = col_idx
            elif any(k in val for k in ["val", "meaning", "code"]):
                if "name" not in val:
                    row_map["code_values"] = col_idx
            elif any(k in val for k in ["var", "field", "item", "name", "variable"]):
                if not has_var:
                    row_map["variable_name"] = col_idx
                    has_var = True
                    
        if "variable_name" in row_map and len(row_map) >= 2:
            header_row = r_idx
            col_mapping = row_map
            break
            
    if not col_mapping:
        if len(table[0]) >= 4:
            col_mapping = {
                "field_name": 0,
                "variable_name": 1,
                "description": 2 if len(table[0]) == 4 else (3 if len(table[0]) >= 6 else 2),
                "type": 3 if len(table[0]) == 4 else (4 if len(table[0]) >= 6 else 3),
                "width": len(table[0]) - 1
            }
            header_row = 0
            
    if not col_mapping or header_row is None:
        return rows, current_block
        
    for r_idx in range(header_row + 1, len(table)):
        row = table[r_idx]
        if not row or all(c is None or clean_cell_val(c) == "" for c in row):
            continue
            
        orig_row_str = " ".join([clean_cell_val(c) for c in row if c is not None]).strip()
        non_empty_count = len([c for c in row if clean_cell_val(c) != ""])
        if "block" in orig_row_str.lower() and len(orig_row_str) < 30 and non_empty_count <= 2:
            current_block = orig_row_str
            continue
            
        v_name = clean_cell_val(row[col_mapping["variable_name"]]) if "variable_name" in col_mapping and col_mapping["variable_name"] < len(row) else ""
        if not v_name or v_name.lower() in ["variable", "field name", "field", "name", "variable name"]:
            continue
            
        f_name = clean_cell_val(row[col_mapping["field_name"]]) if "field_name" in col_mapping and col_mapping["field_name"] < len(row) else ""
        desc = clean_cell_val(row[col_mapping["description"]]) if "description" in col_mapping and col_mapping["description"] < len(row) else ""
        d_type = clean_cell_val(row[col_mapping["type"]]) if "type" in col_mapping and col_mapping["type"] < len(row) else ""
        w = clean_cell_val(row[col_mapping["width"]]) if "width" in col_mapping and col_mapping["width"] < len(row) else ""
        ref = clean_cell_val(row[col_mapping["reference"]]) if "reference" in col_mapping and col_mapping["reference"] < len(row) else ""
        pos = clean_cell_val(row[col_mapping["position"]]) if "position" in col_mapping and col_mapping["position"] < len(row) else ""
        
        codes = None
        if "code_values" in col_mapping and col_mapping["code_values"] < len(row) and row[col_mapping["code_values"]] is not None:
            codes_str = clean_cell_val(row[col_mapping["code_values"]])
            if codes_str:
                codes = parse_code_values_string(codes_str)
                
        rows.append({
            "block_name": current_block,
            "field_name": f_name,
            "variable_name": v_name,
            "description": desc,
            "data_type": d_type,
            "width": w,
            "reference": ref,
            "position": pos,
            "code_values": codes
        })
        
    return rows, current_block

def extract_lines_with_positions(page) -> List[Dict[str, Any]]:
    try:
        words = page.extract_words()
    except Exception:
        return []
    if not words:
        return []
    
    # Sort words by top, then x0
    words.sort(key=lambda w: (w["top"], w["x0"]))
    
    lines = []
    current_line_words = []
    current_top = None
    
    for w in words:
        if current_top is None:
            current_top = w["top"]
            current_line_words.append(w)
        elif abs(w["top"] - current_top) < 4:  # tolerance for same line
            current_line_words.append(w)
        else:
            current_line_words.sort(key=lambda w: w["x0"])
            line_text = " ".join([w["text"] for w in current_line_words])
            lines.append({
                "text": line_text,
                "top": current_top,
                "bottom": max(w["bottom"] for w in current_line_words)
            })
            current_top = w["top"]
            current_line_words = [w]
            
    if current_line_words:
        current_line_words.sort(key=lambda w: w["x0"])
        line_text = " ".join([w["text"] for w in current_line_words])
        lines.append({
            "text": line_text,
            "top": current_top,
            "bottom": max(w["bottom"] for w in current_line_words)
        })
        
    return lines

def parse_pdf_layout(file_path: Path) -> List[Dict[str, Any]]:
    extracted_rows = []
    current_block = "BLOCK 1"
    
    # Try pdfplumber first (preferred)
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                # Extract lines to find block headings
                lines = extract_lines_with_positions(page)
                
                BLOCK_RE = re.compile(r'(?i)\b(?:Block|Section|Record Type|Level|Lvl)[\s\-]*([0-9a-zA-Z]+)\b')
                block_headings = []
                for line in lines:
                    match = BLOCK_RE.search(line["text"])
                    if match:
                        matched_str = match.group(0).strip()
                        if len(line["text"]) < 40:
                            block_name = line["text"].strip()
                        else:
                            block_name = matched_str
                        block_headings.append({
                            "name": block_name,
                            "top": line["top"]
                        })
                
                # Find tables
                tables = page.find_tables()
                if not tables:
                    # Try text strategy for borderless tables
                    tables = page.find_tables({
                        "vertical_strategy": "text",
                        "horizontal_strategy": "text",
                        "snap_y_tolerance": 5,
                        "intersection_x_tolerance": 5,
                    })
                
                tables_with_pos = []
                for t in tables:
                    cells = t.extract()
                    if cells:
                        tables_with_pos.append({
                            "cells": cells,
                            "top": t.bbox[1]
                        })
                tables_with_pos.sort(key=lambda x: x["top"])
                
                for table_data in tables_with_pos:
                    table_cells = table_data["cells"]
                    table_top = table_data["top"]
                    
                    # Find closest preceding block heading
                    applicable_block = current_block
                    closest_dist = float('inf')
                    for bh in block_headings:
                        if bh["top"] < table_top:
                            dist = table_top - bh["top"]
                            if dist < closest_dist:
                                closest_dist = dist
                                applicable_block = bh["name"]
                                
                    current_block = applicable_block
                    
                    rows, current_block = _process_table_cells(table_cells, current_block)
                    extracted_rows.extend(rows)
    except Exception as e:
        print(f"Error parsing PDF layout with pdfplumber: {e}")
        
    # Fallback to Camelot
    if not extracted_rows:
        try:
            import camelot
            tables = camelot.read_pdf(str(file_path), pages='all')
            for table in tables:
                cells = table.df.values.tolist()
                rows, current_block = _process_table_cells(cells, current_block)
                extracted_rows.extend(rows)
        except Exception:
            pass
            
    # Fallback to Tabula
    if not extracted_rows:
        try:
            import tabula
            dfs = tabula.read_pdf(str(file_path), pages='all', multiple_tables=True)
            for df in dfs:
                if df.empty:
                    continue
                cells = df.values.tolist()
                rows, current_block = _process_table_cells(cells, current_block)
                extracted_rows.extend(rows)
        except Exception:
            pass
            
    return extracted_rows

def parse_excel_layout(file_path: Path) -> List[Dict[str, Any]]:
    extracted_rows = []
    xls = None
    try:
        xls = pd.ExcelFile(file_path)
        for sheet_name in xls.sheet_names:
            try:
                df = xls.parse(sheet_name, header=None)
                if df.empty:
                    continue
                
                current_block = sheet_name
                cells = df.values.tolist()
                rows, current_block = _process_table_cells(cells, current_block)
                extracted_rows.extend(rows)
            except Exception as sheet_err:
                print(f"Error parsing sheet {sheet_name}: {sheet_err}")
    except Exception as e:
        print(f"Error opening Excel layout: {e}")
    finally:
        if xls is not None:
            try:
                xls.close()
            except Exception:
                pass
    return extracted_rows

def detect_and_parse_layout(file_path: Path) -> List[Dict[str, Any]]:
    suffix = file_path.suffix.lower()
    name_lower = file_path.name.lower()
    
    layout_keywords = ["layout", "lyt", "lay_out", "position", "var_list", "variable", "structure", "block", "record", "field"]
    is_layout = any(k in name_lower for k in layout_keywords)
    
    if not is_layout:
        return []
        
    if suffix == ".pdf":
        return parse_pdf_layout(file_path)
    elif suffix in [".xlsx", ".xls"]:
        return parse_excel_layout(file_path)
        
    return []
