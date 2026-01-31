from fastapi import APIRouter, Request, HTTPException, Depends, Form
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import RedirectResponse, HTMLResponse
from starlette.status import HTTP_302_FOUND
from auth.local.schemas import UserCreate, UserLogin, Token
from auth.local.utils import hash_password, verify_password, create_access_token
from auth.local.crud import get_user_by_email
import bcrypt

router = APIRouter(prefix="/auth", tags=["Auth"])

# ✅ Register via JSON (used by API or JS client)
@router.post("/register")
async def register_user(user: UserCreate, request: Request):
    async with request.app.state.db.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM users WHERE username = $1 OR email = $2",
            user.username, user.email
        )
        if exists:
            raise HTTPException(status_code=400, detail="Username or Email already exists")

        role_exists = await conn.fetchval("SELECT 1 FROM roles WHERE id = $1", user.role_id)
        if not role_exists:
            raise HTTPException(status_code=400, detail="Invalid role_id")

        hashed = hash_password(user.password)

        await conn.execute("""
            INSERT INTO users (username, email, hashed_password, role_id)
            VALUES ($1, $2, $3, $4)
        """, user.username, user.email, hashed, user.role_id)

    return {"message": "User registered successfully"}


# ✅ Login for Swagger or API use — returns JWT token
@router.post("/token")
async def login_for_access_token(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends()
):
    async with request.app.state.db.acquire() as conn:
        user = await get_user_by_email(conn, form_data.username)
        if not user or not bcrypt.checkpw(form_data.password.encode(), user["hashed_password"].encode()):
            raise HTTPException(status_code=400, detail="Invalid email or password")

        token = create_access_token({
            "sub": user["email"],
            "role": str(user["role_id"])
        })
        return {"access_token": token, "token_type": "bearer"}


# ✅ HTML form-based login (with redirect and cookie)
@router.post("/login", response_class=HTMLResponse)
async def login_form(
    request: Request,
    form_data: OAuth2PasswordRequestForm = Depends()
):
    async with request.app.state.db.acquire() as conn:
        user = await get_user_by_email(conn, form_data.username)
        if not user or not bcrypt.checkpw(form_data.password.encode(), user["hashed_password"].encode()):
            return RedirectResponse("/login?error=invalid", status_code=HTTP_302_FOUND)

        token = create_access_token({
            "sub": user["email"],
            "role": str(user["role_id"])
        })

        # 👇 Redirect based on role
        role_id = str(user["role_id"])
        target = "/admin/dashboard" if role_id == "1" else "/query"

        response = RedirectResponse(url=target, status_code=HTTP_302_FOUND)
        response.set_cookie("access_token", token, httponly=True)
        return response
