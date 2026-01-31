# create_admin.py - Run this once to create your admin account
import asyncio
import asyncpg
import bcrypt
from dotenv import load_dotenv
import os

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

async def create_fixed_admin():
    """Create a single fixed admin account"""
    
    # Admin credentials - CHANGE THESE!
    ADMIN_USERNAME = "admin"
    ADMIN_EMAIL = ""  # Change this
    ADMIN_PASSWORD = ""      # Change this to a strong password
    
    try:
        conn = await asyncpg.connect(DB_URL)
        
        # Check if admin already exists
        existing_admin = await conn.fetchrow(
            "SELECT * FROM users WHERE email = $1 OR role_id = 1", 
            ADMIN_EMAIL
        )
        
        if existing_admin:
            print("❌ Admin account already exists!")
            print(f"   Email: {existing_admin['email']}")
            print("   No changes made.")
            return
        
        # Get admin role ID
        admin_role = await conn.fetchrow("SELECT id FROM roles WHERE name = 'admin'")
        if not admin_role:
            print("❌ Admin role not found in database!")
            print("   Please ensure your roles table has an 'admin' role.")
            return
        
        # Hash password
        hashed_password = bcrypt.hashpw(ADMIN_PASSWORD.encode(), bcrypt.gensalt()).decode()
        
        # Create admin user
        await conn.execute(
            """
            INSERT INTO users (username, email, hashed_password, role_id) 
            VALUES ($1, $2, $3, $4)
            """,
            ADMIN_USERNAME,
            ADMIN_EMAIL,
            hashed_password,
            admin_role['id']
        )
        
        print("✅ Fixed admin account created successfully!")
        print(f"   Username: {ADMIN_USERNAME}")
        print(f"   Email: {ADMIN_EMAIL}")
        print(f"   Password: {ADMIN_PASSWORD}")
        print("   ")
        print("🔒 IMPORTANT SECURITY NOTES:")
        print("   1. Change the admin password after first login")
        print("   2. Delete this script or change the credentials in it")
        print("   3. Admin registration is now locked for security")
        
    except Exception as e:
        print(f"❌ Error creating admin: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(create_fixed_admin())
