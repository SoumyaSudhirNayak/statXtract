import asyncio
from main import app
import json

def main():
    openapi = app.openapi()
    print("Paths in OpenAPI:")
    for path in openapi.get("paths", {}):
        print(path)
        
    print("\n--- Schema for /surveys/{survey}/datasets/{dataset}/tables ---")
    print(json.dumps(openapi.get("paths", {}).get("/surveys/{survey}/datasets/{dataset}/tables", {}), indent=2))

if __name__ == "__main__":
    main()
