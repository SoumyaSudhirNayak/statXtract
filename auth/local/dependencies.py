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
async def get_current_user(request: Request) -> TokenData:
    token = None

    # 1️⃣ Try to read from Authorization header
    auth = request.headers.get("Authorization")
    if auth:
        scheme, param = get_authorization_scheme_param(auth)
        if scheme.lower() == "bearer":
            token = param

    # 2️⃣ Fallback to access_token cookie
    if not token:
        token = request.cookies.get("access_token")

    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    # 3️⃣ Decode JWT token
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("sub")
        role = payload.get("role")
        if email is None or role is None:
            raise HTTPException(status_code=401, detail="Invalid token payload")
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
