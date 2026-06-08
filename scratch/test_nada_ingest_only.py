import asyncio
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

sys.path.append('e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway')
load_dotenv('e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway\\.env')

from utils.ingestion_pipeline import ingest_upload_file
from utils.job_manager import create_job

async def main():
    db_url = os.getenv("DATABASE_URL")
    filepath = "scratch_nada/ASI_DATA_2020_21_CSV.zip"
    
    # Create a dummy job
    job_id = create_job(
        filename="ASI_DATA_2020_21_CSV.zip",
        schema="nss",
        schema_display_name="National Sample Survey",
        year="2020",
        dataset_display_name="ASI_DATA_2020_21_CSV"
    )
    
    print("Created job ID:", job_id)
    print("Calling ingest_upload_file...")
    try:
        tables = await ingest_upload_file(
            input_path=filepath,
            db_url=db_url,
            schema="nss",
            year="2020",
            dataset_display_name="ASI_DATA_2020_21_CSV",
            dataset_db_name="asi_data_2020_21_csv",
            job_id=job_id
        )
        print("Success! Created tables:", tables)
    except Exception as e:
        print("Ingestion failed with exception:")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
