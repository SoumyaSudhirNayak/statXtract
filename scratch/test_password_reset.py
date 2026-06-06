import asyncio
import os
import httpx
import asyncpg
import bcrypt
from dotenv import load_dotenv

load_dotenv('e:\\STATATHON 2025 LOCAL\\Statathon_API_Gateway\\.env')

async def test_flow():
    db_url = os.getenv("DATABASE_URL")
    conn = await asyncpg.connect(db_url)
    
    # Get initial user state
    user = await conn.fetchrow("SELECT * FROM users WHERE email = 'soumyasudhirn@gmail.com'")
    if not user:
        print("Test user soumyasudhirn@gmail.com not found. Creating one...")
        role_row = await conn.fetchrow("SELECT id FROM roles WHERE name = 'user'")
        role_id = role_row["id"] if role_row else 2
        hashed_pw = bcrypt.hashpw(b"password123", bcrypt.gensalt()).decode()
        await conn.execute(
            "INSERT INTO users (username, email, hashed_password, role_id, status) VALUES ($1, $2, $3, $4, 'active')",
            "soumyasudhirn", "soumyasudhirn@gmail.com", hashed_pw, role_id
        )
        user = await conn.fetchrow("SELECT * FROM users WHERE email = 'soumyasudhirn@gmail.com'")

    orig_hash = user["hashed_password"]
    orig_status = user["status"]
    orig_role = user["role_id"]
    orig_blocked = user.get("is_blocked")
    orig_attempts = user.get("failed_login_attempts")
    orig_locked_until = user.get("account_locked_until")
    orig_plan = user.get("plan")
    
    client = httpx.AsyncClient(base_url="http://localhost:8000")
    
    print("\n--- TEST 1: GET /forgot-password ---")
    res = await client.get("/forgot-password")
    assert res.status_code == 200
    assert "Reset Password" in res.text
    print("GET /forgot-password page returned 200 OK.")
    
    print("\n--- TEST 2: POST /forgot-password (Non-existent user) ---")
    res = await client.post("/forgot-password", json={"email": "nonexistent_999@gmail.com", "password": "newpassword123"})
    assert res.status_code == 404
    assert res.json()["detail"] == "User not found."
    print("POST non-existent user correctly returned 404 User not found.")
    
    print("\n--- TEST 3: POST /forgot-password (Successful Reset) ---")
    res = await client.post("/forgot-password", json={"email": "soumyasudhirn@gmail.com", "password": "newpassword123"})
    assert res.status_code == 200
    assert res.json()["message"] == "Password reset successfully."
    print("POST password reset returned 200 OK.")
    
    # Check DB updates
    user_after = await conn.fetchrow("SELECT * FROM users WHERE email = 'soumyasudhirn@gmail.com'")
    new_hash = user_after["hashed_password"]
    
    assert new_hash != orig_hash, "Hashed password should be updated"
    assert user_after["status"] == orig_status, "Status should not change"
    assert user_after["role_id"] == orig_role, "Role should not change"
    assert user_after.get("is_blocked") == orig_blocked, "is_blocked should not change"
    assert user_after.get("failed_login_attempts") == orig_attempts, "failed_login_attempts should not change"
    assert user_after.get("account_locked_until") == orig_locked_until, "account_locked_until should not change"
    assert user_after.get("plan") == orig_plan, "plan should not change"
    
    # Verify new password works with bcrypt verification
    assert bcrypt.checkpw("newpassword123".encode(), new_hash.encode()), "Bcrypt verification of new password failed"
    print("Database invariants and hashing verified.")
    
    print("\n--- TEST 4: Login Verification with New Password ---")
    res = await client.post("/auth/token", data={"username": "soumyasudhirn@gmail.com", "password": "newpassword123"})
    assert res.status_code == 200
    assert "access_token" in res.json()
    print("Successfully logged in with new password.")
    
    # Restore original password
    await conn.execute("UPDATE users SET hashed_password = $1 WHERE email = 'soumyasudhirn@gmail.com'", orig_hash)
    print("Restored original password hash.")
    
    await conn.close()
    await client.aclose()
    print("\nALL PASSWORD RESET TESTS PASSED!")

if __name__ == '__main__':
    asyncio.run(test_flow())
