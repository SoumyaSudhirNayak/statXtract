import asyncio
import asyncpg
import json
import os

from dotenv import load_dotenv
load_dotenv('.env')

async def test():
    conn = await asyncpg.connect(os.getenv('DATABASE_URL'))
    
    # 2. Look at actual data in the table WITHOUT DISTINCT to see what is really there
    rows = await conn.fetch('SELECT * FROM "employment_and_unemployment__test_3"."block_3_household_characteristics" LIMIT 2')
    for r in rows:
        print(dict(r))
    
    await conn.close()
    
asyncio.run(test())
