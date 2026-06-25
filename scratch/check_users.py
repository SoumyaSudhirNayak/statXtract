import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv('e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway\\.env')

async def main():
    db_url = os.getenv("DATABASE_URL")
    conn = await asyncpg.connect(db_url)
    users = await conn.fetch("SELECT email, role_id, status, is_blocked, is_verified FROM users")
    for u in users:
        print(f"Email: {u['email']}, Role: {u['role_id']}, Status: {u['status']}, Blocked: {u.get('is_blocked')}, Verified: {u.get('is_verified')}")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
