import asyncio
import os
import sys
import asyncpg
from dotenv import load_dotenv

sys.path.append('e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway')
load_dotenv('e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway\\.env')

async def main():
    db_url = os.getenv("DATABASE_URL")
    conn = await asyncpg.connect(db_url)
    try:
        for sch in ('nss__asi_data_2020_21_csv', 'nss__ddi_ind_nso_asi_2020_21'):
            rows = await conn.fetch(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{sch}'
                ORDER BY table_name
            """)
            print(f"\nTables in schema '{sch}':")
            for r in rows:
                table_name = r["table_name"]
                count = await conn.fetchval(f'SELECT COUNT(*) FROM "{sch}"."{table_name}"')
                print(f" - {table_name}: {count} rows")
            
        print("\nChecking job status from DB:")
        # Let's check if there are any jobs or dataset registries
        registry = await conn.fetch("SELECT * FROM dataset_registry")
        print("Registry rows:")
        for reg in registry:
            print(dict(reg))
            
    finally:
        await conn.close()

if __name__ == '__main__':
    asyncio.run(main())
