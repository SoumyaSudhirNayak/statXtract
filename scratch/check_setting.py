import asyncio
import asyncpg
import os

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
        val = await conn.fetchval("SELECT value FROM system_settings WHERE key = 'enable_sensitive_columns'")
        print("enable_sensitive_columns value:", val)
    finally:
        await conn.close()

asyncio.run(main())
