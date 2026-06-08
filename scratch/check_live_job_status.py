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
    job_id = "6aacfe1e-ef6d-4cc8-b0fa-a225ebebfeb7"
    
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
        
        # 2. Get status
        status_url = f"{base_url}/admin/nada/jobs/{job_id}"
        print(f"\nChecking status: GET {status_url}")
        res = await client.get(status_url, headers=headers)
        print("Status code:", res.status_code)
        if res.status_code == 200:
            print(json.dumps(res.json(), indent=2))
        else:
            print("Response:", res.text)

if __name__ == "__main__":
    asyncio.run(main())
