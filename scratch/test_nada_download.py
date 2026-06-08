import asyncio
import os
import sys
import zipfile
from pathlib import Path
from dotenv import load_dotenv

sys.path.append('e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway')
load_dotenv('e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway\\.env')

from utils.nada_client import nada_download_file

async def main():
    api_key = os.getenv("API_KEY")
    dataset_id = "DDI-IND-NSO-ASI-2020-21"
    file_no = "QVNJX0RBVEFfMjAyMF8yMV9DU1Yuemlw" # ASI_DATA_2020_21_CSV.zip
    
    dest_dir = Path("scratch_nada")
    dest_dir.mkdir(exist_ok=True)
    dest_path = dest_dir / "ASI_DATA_2020_21_CSV.zip"
    
    print(f"Downloading {file_no} for dataset {dataset_id}...")
    try:
        saved = await nada_download_file(
            dataset_id=dataset_id,
            file_no=file_no,
            api_key=api_key,
            dest_path=dest_path
        )
        print("Download completed! Saved to:", saved)
        print("File size:", saved.stat().st_size)
        
        # Verify ZIP
        print("Checking ZIP content:")
        with zipfile.ZipFile(saved, "r") as zf:
            namelist = zf.namelist()
            print(f"Found {len(namelist)} items in zip:")
            for name in namelist[:20]:
                print(" -", name)
                
    except Exception as e:
        print("Error during download/inspect:", e)

if __name__ == "__main__":
    asyncio.run(main())
