import asyncio
import os
import asyncpg

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
        rows = await conn.fetch("SELECT * FROM dataset_configs")
        print(f"Total dataset configs: {len(rows)}")
        for r in rows:
            print(dict(r))
    finally:
        await conn.close()

asyncio.run(main())
