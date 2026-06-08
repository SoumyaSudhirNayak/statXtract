import re
from typing import Dict, Any, List

def sanitize_table_name(name: str) -> str:
    """Sanitize the proposed table name to be Postgres safe."""
    s = str(name).strip()
    s = re.sub(r'[^a-zA-Z0-9_]', '_', s)
    s = re.sub(r'_+', '_', s)
    return s.strip('_').lower()

def resolve_target_mappings(surveys: List[Dict[str, Any]], custom_mappings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Combines scanned survey metadata with admin mapping custom overrides.
    Returns resolved configurations ready for ingestion.
    """
    resolved = []
    
    for survey in surveys:
        folder = survey["folder_name"]
        override = custom_mappings.get(folder, {})
        
        # Check if entire folder is skipped
        if override.get("skip", False):
            continue
            
        target_schema = override.get("schema", "public").strip().lower()
        
        resolved_files = []
        for dfile in survey["dataset_files"]:
            file_override = override.get("files", {}).get(dfile, {})
            
            # Check if specific file is skipped
            if file_override.get("skip", False):
                continue
                
            custom_table_name = file_override.get("table_name", "").strip()
            if not custom_table_name:
                # Use default safe preview table name
                preview = next((p["table_name"] for p in survey["table_naming_previews"] if p["file"] == dfile), "dataset_table")
                custom_table_name = preview
                
            resolved_files.append({
                "relative_path": dfile,
                "target_table": sanitize_table_name(custom_table_name)
            })
            
        resolved_docs = []
        if "documentation_files" in survey:
            for doc_file in survey["documentation_files"]:
                doc_override = override.get("documentation_files", {}).get(doc_file, {})
                if doc_override.get("skip", False):
                    continue
                resolved_docs.append(doc_file)
                
        if not resolved_files and not resolved_docs:
            continue
            
        resolved.append({
            "folder_name": folder,
            "relative_dir": survey["relative_dir"],
            "target_schema": target_schema,
            "ddi_files": survey["ddi_files"],
            "layout_files": survey["layout_files"],
            "files": resolved_files,
            "documentation_files": resolved_docs
        })
        
    return resolved
