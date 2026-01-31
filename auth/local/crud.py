import bcrypt
from asyncpg import Connection
from fastapi import HTTPException

async def get_user_by_email(conn: Connection, email: str):
    return await conn.fetchrow("SELECT * FROM users WHERE email = $1", email)

async def register_user(conn: Connection, email: str, password: str, role: str = "user"):
    user = await get_user_by_email(conn, email)
    if user:
        raise HTTPException(status_code=400, detail="User already exists")

    # Get role_id from roles table
    role_row = await conn.fetchrow("SELECT id FROM roles WHERE name = $1", role)
    if not role_row:
        raise HTTPException(status_code=400, detail=f"Role '{role}' does not exist")
    
    role_id = role_row["id"]

    # Hash password
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    # Insert user with role_id
    await conn.execute(
        "INSERT INTO users (email, hashed_password, role_id) VALUES ($1, $2, $3)",
        email, hashed, role_id
    )

    return {"message": "User registered successfully"}
