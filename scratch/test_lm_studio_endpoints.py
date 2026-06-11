import httpx
import sys

# Reconfigure stdout to use UTF-8 to prevent charmap print errors on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

async def run_diagnostics():
    url = "http://127.0.0.1:8001"
    print("=== Checking /health ===")
    try:
        resp = httpx.get(f"{url}/health")
        print("Status:", resp.status_code)
        print("Response:", resp.json())
    except Exception as e:
        print("Error on /health:", e)

    print("\n=== Checking /test-model ===")
    try:
        resp = httpx.get(f"{url}/test-model", timeout=30.0)
        print("Status:", resp.status_code)
        print("Response:", resp.json())
    except Exception as e:
        print("Error on /test-model:", e)

    print("\n=== Checking /summarize ===")
    try:
        payload = {
            "dataset_key": "test_diagnostics_dataset",
            "text": "The Index of Industrial Production (IIP) is a key economic indicator that measures the short-term changes in the volume of production of a basket of industrial products during a given period. It is compiled and published monthly by the National Statistical Office (NSO). The coverage includes Mining, Manufacturing, and Electricity sectors."
        }
        resp = httpx.post(f"{url}/summarize", json=payload, timeout=60.0)
        print("Status:", resp.status_code)
        print("Response:", resp.json())
    except Exception as e:
        print("Error on /summarize:", e)

import asyncio
asyncio.run(run_diagnostics())
