import shutil
import os

dir_path = "scratch_nada"
if os.path.exists(dir_path):
    shutil.rmtree(dir_path, ignore_errors=True)
    print("Cleaned up scratch_nada directory")
