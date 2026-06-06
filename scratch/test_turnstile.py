import asyncio
import os
import httpx
import asyncpg
import bcrypt
import time

ENV_PATH = 'e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway\\.env'

def set_env_turnstile(enabled_str: str):
    with open(ENV_PATH, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # We want to replace TURNSTILE_ENABLED=...
    lines = content.splitlines()
    replaced = False
    for i, line in enumerate(lines):
        if line.strip().startswith("TURNSTILE_ENABLED="):
            lines[i] = f"TURNSTILE_ENABLED={enabled_str}"
            replaced = True
            break
            
    if not replaced:
        lines.append(f"TURNSTILE_ENABLED={enabled_str}")
        
    with open(ENV_PATH, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines) + "\n")

async def test_turnstile_flow():
    db_url = "postgresql://postgres:santoshkanakswapna151424@localhost:5432/STATXTRACT?sslmode=disable"
    conn = await asyncpg.connect(db_url)
    
    # Clean up test accounts
    await conn.execute("DELETE FROM users WHERE email IN ('testuser_turnstile@gmail.com', 'testadmin_turnstile@gmail.com')")
    
    # Create test user
    user_pw_hash = bcrypt.hashpw(b"password123", bcrypt.gensalt()).decode()
    await conn.execute(
        """
        INSERT INTO users (username, email, hashed_password, role_id, status, is_verified, is_blocked) 
        VALUES ($1, $2, $3, 2, 'active', TRUE, FALSE)
        """,
        "testuser_turnstile", "testuser_turnstile@gmail.com", user_pw_hash
    )
    
    # Create test admin
    admin_pw_hash = bcrypt.hashpw(b"admin123", bcrypt.gensalt()).decode()
    await conn.execute(
        """
        INSERT INTO users (username, email, hashed_password, role_id, status, is_verified, is_blocked) 
        VALUES ($1, $2, $3, 1, 'active', TRUE, FALSE)
        """,
        "testadmin_turnstile", "testadmin_turnstile@gmail.com", admin_pw_hash
    )
        
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    try:
        print("\n--- TEST 1: Turnstile Disabled ---")
        set_env_turnstile("false")
        await asyncio.sleep(2.0) # Wait for reload
        
        res = await client.post("/auth/login", data={"username": "testuser_turnstile@gmail.com", "password": "password123"})
        print(f"Status: {res.status_code}, Location: {res.headers.get('location')}")
        assert res.status_code == 302
        assert "/user/dashboard" in res.headers.get("location", "")
        print("User login succeeded without Turnstile as expected.")
        
        print("\n--- TEST 2: Turnstile Enabled, Validation Fails (Missing / Invalid Token) ---")
        set_env_turnstile("true")
        await asyncio.sleep(2.0) # Wait for reload
        
        res = await client.post("/auth/login", data={"username": "testuser_turnstile@gmail.com", "password": "password123"})
        print(f"Status: {res.status_code}, Location: {res.headers.get('location')}")
        assert res.status_code == 302
        assert "error=CAPTCHA" in res.headers.get("location", "")
        print("User login correctly blocked and redirected with Turnstile verification error.")
        
        print("\n--- TEST 3: Turnstile Enabled, Admin Bypass ---")
        # Admin should bypass Turnstile completely
        res = await client.post("/auth/login", data={"username": "testadmin_turnstile@gmail.com", "password": "admin123"})
        print(f"Status: {res.status_code}, Location: {res.headers.get('location')}")
        assert res.status_code == 302
        assert "/admin/dashboard" in res.headers.get("location", "")
        print("Admin login successfully bypassed Turnstile.")
        
    finally:
        # Restore env to false
        set_env_turnstile("false")
        
        # Clean up test accounts
        await conn.execute("DELETE FROM users WHERE email IN ('testuser_turnstile@gmail.com', 'testadmin_turnstile@gmail.com')")
            
    await conn.close()
    await client.aclose()
    print("\nALL TURNSTILE VERIFICATION TESTS PASSED!")

if __name__ == '__main__':
    asyncio.run(test_turnstile_flow())
