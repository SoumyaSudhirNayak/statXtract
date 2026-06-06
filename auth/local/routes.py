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
    import os
    from dotenv import load_dotenv
    load_dotenv(override=True)
    turnstile_enabled = os.getenv("TURNSTILE_ENABLED", "false").lower() == "true"
    if turnstile_enabled:
        async with request.app.state.db.acquire() as conn:
            user = await get_user_by_email(conn, form_data.username)
        
        referer = request.headers.get("referer", "") or ""
        is_admin_login = "/login" in referer and "/user/login" not in referer
        if user and str(user.get("role_id")) == "1":
            is_admin_login = True
            
        if not is_admin_login:
            form = await request.form()
            token = form.get("cf-turnstile-response")
            ip = request.client.host if request.client else None
            secret = os.getenv("TURNSTILE_SECRET_KEY")
            
            import httpx
            verified = False
            if token and secret:
                try:
                    async with httpx.AsyncClient() as client:
                        resp = await client.post(
                            "https://challenges.cloudflare.com/turnstile/v0/siteverify",
                            data={
                                "secret": secret,
                                "response": token,
                                "remoteip": ip
                            },
                            timeout=5.0
                        )
                        if resp.status_code == 200:
                            res_json = resp.json()
                            verified = res_json.get("success", False)
                except Exception as e:
                    print(f"Turnstile error: {e}")
                    
            if not verified:
                return RedirectResponse(
                    "/user/login?error=CAPTCHA verification failed. Please try again.",
                    status_code=302
                )

    async with request.app.state.db.acquire() as conn:
        user = await get_user_by_email(conn, form_data.username)
        
        referer = request.headers.get("referer", "") or ""
        is_admin_login = "/login" in referer and "/user/login" not in referer
        if user and str(user.get("role_id")) == "1":
            is_admin_login = True
        error_redirect_base = "/login" if is_admin_login else "/user/login"


        if not user or not bcrypt.checkpw(form_data.password.encode(), user["hashed_password"].encode()):
            return RedirectResponse(f"{error_redirect_base}?error=invalid", status_code=HTTP_302_FOUND)

        # Check if blocked
        if user.get("is_blocked"):
            return RedirectResponse(f"{error_redirect_base}?error=blocked", status_code=HTTP_302_FOUND)

        # Check if verified (only for non-admin users)
        role_id = str(user["role_id"])
        if role_id != "1" and not user.get("is_verified"):
            return RedirectResponse(f"{error_redirect_base}?error=unverified", status_code=HTTP_302_FOUND)

        token = create_access_token({
            "sub": user["email"],
            "role": str(user["role_id"])
        })

        # 👇 Redirect based on role
        target = "/admin/dashboard" if role_id == "1" else "/user/dashboard"

        response = RedirectResponse(url=target, status_code=HTTP_302_FOUND)
        response.set_cookie("access_token", token, httponly=True)
        return response

