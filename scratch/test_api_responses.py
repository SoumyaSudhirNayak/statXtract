import asyncio
import os
from fastapi.testclient import TestClient
from main import app
from auth.local.dependencies import get_current_user

class MockUser:
    def __init__(self, username="tiger@gmail.com", role="3"):
        self.username = username
        self.role = role

def test_routes():
    app.dependency_overrides[get_current_user] = lambda: MockUser(username="tiger@gmail.com", role="3")
    
    with TestClient(app) as client:
        # Test POST /api/sql/execute as Guest
        print("\n--- POST /api/sql/execute as guest ---")
        payload = {
            "survey": "annual_survey_of_industries",
            "dataset": "annual_survey_of_industries__asi_2023_24",
            "table": "blka202324",
            "sql": "SELECT a1, a2 FROM blka202324 LIMIT 10"
        }
        r = client.post("/api/sql/execute", json=payload)
        print("Status:", r.status_code)
        print("Response:", r.json())

if __name__ == "__main__":
    test_routes()
