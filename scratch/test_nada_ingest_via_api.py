import asyncio
import os
import sys
import httpx
import json
from dotenv import load_dotenv

sys.path.append('e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway')
load_dotenv('e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway\\.env')

async def main():
    base_url = "http://localhost:8000"
    
    # 1. Login as Admin
    print("Logging in as admin...")
    async with httpx.AsyncClient() as client:
        res = await client.post(
            f"{base_url}/auth/token",
            data={
                "username": "sudheernayak122006@gmail.com",
                "password": "password123"
            }
        )
        if res.status_code != 200:
            print("Login failed. Status:", res.status_code, "Body:", res.text)
            return
            
        token = res.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}
        print("Login successful! Token acquired.")
        
        # 2. Trigger Direct Ingest
        idno = "DDI-IND-NSO-ASI-2020-21"
        file_nos = "QVNJX0RBVEFfMjAyMF8yMV9DU1Yuemlw" # ASI_DATA_2020_21_CSV.zip
        schema = "nss"
        
        url = f"{base_url}/admin/nada/datasets/{idno}/ingest"
        print(f"\nTriggering Direct Ingest: POST {url}")
        res = await client.post(
            url,
            params={"schema": schema, "file_nos": file_nos},
            headers=headers
        )
        print("Status:", res.status_code)
        if res.status_code != 200:
            print("Failed to trigger ingest:", res.text)
            return
            
        job = res.json()
        job_id = job.get("job_id")
        print("Job started! Job ID:", job_id)
        
        # 3. Poll status
        status_url = f"{base_url}/admin/nada/jobs/{job_id}"
        print(f"\nPolling status: GET {status_url}")
        for _ in range(30):
            await asyncio.sleep(2)
            res = await client.get(status_url, headers=headers)
            if res.status_code == 200:
                job_status = res.json()
                status = job_status.get("status")
                print(f"Status: {status} ...")
                if status in ("completed", "failed"):
                    print(json.dumps(job_status, indent=2))
                    break
            else:
                print("Failed to get job status:", res.text)
                break

if __name__ == "__main__":
    asyncio.run(main())
