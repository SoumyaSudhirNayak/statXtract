import shutil
import os

def delete_directory(path: str):
    """
    Delete a directory (e.g., uploads/extracted ZIP) after processing.
    """
    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"✅ Deleted: {path}")
    else:
        print(f"⚠️ Directory not found: {path}")
