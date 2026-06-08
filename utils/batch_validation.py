import os
import re
from pathlib import Path
from typing import Dict, Any, List

def is_layout_file(p: Path) -> bool:
    """Helper to check if a file looks like a MOSPI layout file."""
    name = p.name.lower()
    return p.suffix.lower() in [".xlsx", ".xls"] and any(
        k in name for k in ["layout", "position", "var_list", "variable"]
    )

def is_ddi_file(p: Path) -> bool:
    """Helper to check if a file is a DDI XML metadata file."""
    return p.suffix.lower() in [".xml", ".nsdstat"]

def is_documentation_file(p: Path) -> bool:
    """Helper to check if a file is a documentation PDF or DOCX."""
    return p.suffix.lower() in [".pdf", ".docx"]

def is_dataset_file(p: Path) -> bool:
    """Helper to check if a file is an ingestible dataset."""
    return p.suffix.lower() in [".txt", ".csv", ".sav", ".por", ".xlsx", ".dta", ".xpt"]

def scan_batch_archive(temp_dir: str) -> List[Dict[str, Any]]:
    """
    Scans the extracted batch temp directory.
    Identifies top-level directories as survey folders, gathers files,
    detects metadata and layout, and performs structure validation.
    """
    temp_path = Path(temp_dir)
    surveys = []

    # Find direct subdirectories, traversing nested single-folder roots (e.g. Main_Zip/)
    current_root = temp_path
    while True:
        subdirs = [d for d in current_root.iterdir() if d.is_dir() and not d.name.startswith("__") and d.name != ".venv"]
        
        # If there is exactly one subdirectory, and it contains no dataset files directly in it,
        # we step into it to bypass the outer ZIP container/wrapper folder.
        if len(subdirs) == 1:
            single_sub = subdirs[0]
            direct_files = [f for f in single_sub.iterdir() if f.is_file() and is_dataset_file(f)]
            if not direct_files:
                current_root = single_sub
                continue
        break

    # If there are no subdirectories in the resolved root, treat it as a single group
    if not subdirs:
        subdirs = [current_root]

    for subdir in subdirs:
        folder_name = subdir.name if subdir != temp_path else "Root"
        
        # Recursively list all files in this survey folder
        all_files = [f for f in subdir.rglob("*") if f.is_file()]
        
        ddi_files = []
        layout_files = []
        dataset_files = []
        documentation_files = []
        unsupported_files = []
        total_size = 0
        file_tree = []

        for f in all_files:
            rel_path = str(f.relative_to(subdir)).replace("\\", "/")
            total_size += f.stat().st_size
            file_tree.append(rel_path)

            if is_ddi_file(f):
                ddi_files.append(rel_path)
            elif is_layout_file(f):
                layout_files.append(rel_path)
            elif is_dataset_file(f):
                dataset_files.append(rel_path)
            elif is_documentation_file(f):
                documentation_files.append(rel_path)
            else:
                unsupported_files.append(rel_path)

        # Basic validations
        has_datasets = len(dataset_files) > 0
        has_ddi = len(ddi_files) > 0
        has_layout = len(layout_files) > 0

        # Build table naming previews
        from utils.table_naming import get_safe_table_name
        table_naming_previews = []
        for df in dataset_files:
            file_stem = Path(df).stem
            # Clean and get safe DB table name
            safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_stem)
            safe_name = re.sub(r'_+', '_', safe_name).strip('_').lower()
            table_naming_previews.append({
                "file": df,
                "table_name": safe_name or "dataset_table"
            })

        validation = {
            "datasets_found": has_datasets,
            "ddi_found": has_ddi,
            "layout_found": has_layout,
            "missing_ddi": not has_ddi,
            "missing_layout": not has_layout and any(df.endswith(".txt") for df in dataset_files),
            "unsupported_files": unsupported_files
        }

        surveys.append({
            "folder_name": folder_name,
            "relative_dir": str(subdir.relative_to(temp_path)).replace("\\", "/"),
            "dataset_count": len(dataset_files),
            "total_size_bytes": total_size,
            "files": file_tree,
            "ddi_files": ddi_files,
            "layout_files": layout_files,
            "dataset_files": dataset_files,
            "documentation_files": documentation_files,
            "validation": validation,
            "table_naming_previews": table_naming_previews
        })

    return surveys
