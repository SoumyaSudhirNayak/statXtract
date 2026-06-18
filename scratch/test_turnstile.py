import asyncio
import os
import httpx
import asyncpg
import bcrypt

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
        print("\n--- TEST 1: User Login Fails Without CAPTCHA ---")
        res = await client.post("/auth/login", data={"username": "testuser_turnstile@gmail.com", "password": "password123"})
        print(f"Status: {res.status_code}, Location: {res.headers.get('location')}")
        assert res.status_code == 302
        assert "error=CAPTCHA" in res.headers.get("location", "")
        print("User login correctly blocked without CAPTCHA.")
        
        print("\n--- TEST 2: User Login Succeeds With Correct CAPTCHA ---")
        # Fetch CAPTCHA to get the question and establish the session cookie
        res_cap = await client.get("/auth/captcha")
        cap_data = res_cap.json()
        question = cap_data["question"]
        parts = question.split()
        num1 = int(parts[0])
        op = parts[1]
        num2 = int(parts[2])
        correct_ans = num1 + num2 if op == "+" else num1 - num2
        print(f"Solved question '{question}' -> Answer: {correct_ans}")
        
        res = await client.post(
            "/auth/login",
            data={
                "username": "testuser_turnstile@gmail.com",
                "password": "password123",
                "captcha_answer": str(correct_ans)
            }
        )
        print(f"Status: {res.status_code}, Location: {res.headers.get('location')}")
        assert res.status_code == 302
        assert "/user/dashboard" in res.headers.get("location", "")
        print("User login succeeded with correct CAPTCHA answer.")
        
        print("\n--- TEST 3: Admin Login Bypasses CAPTCHA Completely ---")
        # Admin should bypass CAPTCHA completely without submitting captcha_answer
        res = await client.post("/auth/login", data={"username": "testadmin_turnstile@gmail.com", "password": "admin123"})
        print(f"Status: {res.status_code}, Location: {res.headers.get('location')}")
        assert res.status_code == 302
        assert "/admin/dashboard" in res.headers.get("location", "")
        print("Admin login successfully bypassed CAPTCHA.")
        
    finally:
        # Clean up test accounts
        await conn.execute("DELETE FROM users WHERE email IN ('testuser_turnstile@gmail.com', 'testadmin_turnstile@gmail.com')")
            
    await conn.close()
    await client.aclose()
    print("\nALL TURNSTILE VERIFICATION TESTS PASSED!")

if __name__ == '__main__':
    asyncio.run(test_turnstile_flow())
