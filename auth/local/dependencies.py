from fastapi import Depends, HTTPException, Request
from jose import jwt, JWTError
from fastapi.security.utils import get_authorization_scheme_param
from auth.local.schemas import TokenData
import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

# Secret + Algorithm
SECRET_KEY = os.getenv("SECRET_KEY", "fallback_secret")
ALGORITHM = os.getenv("ALGORITHM", "HS256")


# ✅ Reads token from Authorization header or Cookie
# ✅ Reads token from Authorization header or Cookie
async def get_current_user(request: Request) -> TokenData:
    token = None

    # 1️⃣ Try to read from Authorization header
    auth = request.headers.get("Authorization")
    if auth:
        scheme, param = get_authorization_scheme_param(auth)
        if scheme.lower() == "bearer":
            token = param
            if token and token.startswith("Bearer "):
                token = token[7:]
            elif token and token.startswith("bearer "):
                token = token[7:]

    # If the token is a developer/API token (starts with stx_live_)
    if token and token.startswith("stx_live_"):
        pool = getattr(request.app.state, "db", None)
        if pool is not None:
            async with pool.acquire() as conn:
                token_row = await conn.fetchrow(
                    """
                    SELECT t.active AS token_active, t.plan AS token_plan, u.id AS user_id, u.email, u.is_blocked, u.status
                    FROM api_tokens t
                    JOIN users u ON t.user_id = u.id
                    WHERE t.token = $1
                    LIMIT 1
                    """,
                    token
                )
                if not token_row:
                    raise HTTPException(status_code=401, detail="Invalid API token.")
                if not token_row["token_active"]:
                    raise HTTPException(status_code=401, detail="API token has been revoked or deactivated.")
                if token_row["is_blocked"]:
                    raise HTTPException(status_code=403, detail="User account is blocked.")
                if token_row["status"] != "active":
                    raise HTTPException(status_code=403, detail="User account is suspended.")
                if token_row["token_plan"] not in ("pro", "enterprise", "admin"):
                    raise HTTPException(status_code=403, detail="API access requires a Pro or Enterprise plan.")
                
                role_id = await conn.fetchval("SELECT role_id FROM users WHERE id = $1 LIMIT 1", token_row["user_id"])
                role_str = str(role_id) if role_id is not None else "3"
                return TokenData(username=token_row["email"], role=role_str)

    # Detect if this is a developer programmatic/API request (Swagger/Postman/etc)
    path = request.url.path
    is_developer_api = (
        (path == "/query" and request.method == "POST") or 
        path.startswith("/datasets/")
    )
    referer = request.headers.get("referer", "")
    is_swagger = "/docs" in referer or "/redoc" in referer
    is_programmatic = not referer

    if is_developer_api and (is_swagger or is_programmatic):
        # Programmatic/developer access strictly requires a developer token (which wasn't supplied above)
        raise HTTPException(
            status_code=401,
            detail="Authentication credentials were not provided. Please authorize with a valid stx_live API token."
        )

    # 2️⃣ Fallback to access_token cookie
    cookie_token = request.cookies.get("access_token")

    if not cookie_token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    # 3️⃣ Decode JWT token
    try:
        payload = jwt.decode(cookie_token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("sub")
        role = payload.get("role")
        if email is None or role is None:
            raise HTTPException(status_code=401, detail="Invalid token payload")
        try:
            pool = getattr(request.app.state, "db", None)
            if pool is not None:
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(
                        """
                        SELECT COALESCE(is_blocked, FALSE) as is_blocked, token_valid_after
                        FROM users
                        WHERE email = $1
                        LIMIT 1
                        """,
                        email,
                    )
                    if row:
                        if row["is_blocked"]:
                            raise HTTPException(status_code=403, detail="User blocked")
                        
                        token_valid_after = row["token_valid_after"]
                        if token_valid_after:
                            iat = payload.get("iat")
                            if iat:
                                import calendar
                                valid_after_ts = calendar.timegm(token_valid_after.utctimetuple())
                                if iat < valid_after_ts:
                                    raise HTTPException(status_code=401, detail="Session revoked/expired")
        except HTTPException:
            raise
        except Exception as e:
            print(f"Error checking user validity: {e}")
            pass
        return TokenData(username=email, role=role)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


# ✅ Role-based access control
def get_current_active_user_with_role(roles: list[str]):
    def role_checker(user: TokenData = Depends(get_current_user)):
        if str(user.role) not in roles:  # Always compare as strings
            raise HTTPException(status_code=403, detail="Forbidden: role mismatch")
        return user
    return role_checker
