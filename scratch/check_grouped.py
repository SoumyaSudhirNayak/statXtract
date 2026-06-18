import asyncio
import os
import asyncpg
from main import _group_schemas_by_survey

async def main():
    env_path = r"e:\STATATHON 2025 LOCAL\Statathon_API_Gateway\.env"
    db_url = None
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith("DATABASE_URL="):
                    db_url = line.strip().split("DATABASE_URL=")[1].strip("'\"")
                    break
                    
    conn = await asyncpg.connect(db_url)
    try:
        grouped = await _group_schemas_by_survey(conn)
        print("GROUPED KEYS:", list(grouped.keys()))
        for k, v in grouped.items():
            print(f"Key: {k}, survey: {v.get('survey')}, datasets: {[d['schema'] for d in v.get('datasets', [])]}")
    finally:
        await conn.close()

asyncio.run(main())
