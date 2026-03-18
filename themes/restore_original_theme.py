import os
import shutil

TEMPLATES_DIR = "templates"
BACKUP_DIR = "templates/backups_original"

FILES_TO_RESTORE = [
    "admin_dashboard.html",
    "index.html",
    "login.html",
    "register.html",
    "query_ui.html",
    "usage.html",
    "datasets.html",
    "schemas.html",
    "upload_progress.html"
]

def restore_theme():
    if not os.path.exists(BACKUP_DIR):
        print(f"Error: Backup directory {BACKUP_DIR} not found. Cannot restore.")
        return

    for filename in FILES_TO_RESTORE:
        backup_path = os.path.join(BACKUP_DIR, filename)
        file_path = os.path.join(TEMPLATES_DIR, filename)
        
        if os.path.exists(backup_path):
            shutil.copy2(backup_path, file_path)
            print(f"Restored {filename} from backup")
        else:
            print(f"Skipping {filename} (no backup found)")

if __name__ == "__main__":
    restore_theme()
    print("\\nTheme restored successfully! Refresh your browser to see the changes.")
