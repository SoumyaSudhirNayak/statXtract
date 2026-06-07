import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock
from fastapi import HTTPException

# Add current directory to path
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from auth.local.dependencies import get_current_user
from auth.local.utils import create_access_token
from auth.local.schemas import TokenData

class MockRequest:
    def __init__(self, headers=None, cookies=None, path="/schemas", method="GET"):
        self.headers = headers or {}
        self.cookies = cookies or {}
        self.url = MagicMock()
        self.url.path = path
        self.method = method
        self.app = MagicMock()
        self.app.state = MagicMock()
        self.app.state.db = AsyncMock()

async def run_tests():
    print("Running authentication enforcement tests...\n")

    # 1. No Authorization header and no cookie
    req1 = MockRequest(path="/schemas", headers={}, cookies={})
    try:
        await get_current_user(req1)
        print("[FAIL] Test 1 Failed: Allowed access with no token/cookie.")
    except HTTPException as e:
        if e.status_code == 401 and e.detail == "Not authenticated":
            print("[OK] Test 1 Passed: No token -> 401 Not authenticated.")
        else:
            print(f"[FAIL] Test 1 Failed: Got HTTP {e.status_code} ({e.detail}).")

    # 2. Invalid JWT token
    req2 = MockRequest(path="/schemas", headers={"Authorization": "Bearer invalid_jwt_token"})
    try:
        await get_current_user(req2)
        print("[FAIL] Test 2 Failed: Allowed access with invalid JWT.")
    except HTTPException as e:
        if e.status_code == 401 and "Invalid token" in str(e.detail):
            print("[OK] Test 2 Passed: Invalid JWT -> 401 Invalid token.")
        else:
            print(f"[FAIL] Test 2 Failed: Got HTTP {e.status_code} ({e.detail}).")

    # 3. Invalid API token
    req3 = MockRequest(path="/schemas", headers={"Authorization": "Bearer stx_live_invalid"})
    # Mock database to return None for invalid token
    req3.app.state.db.acquire = MagicMock()
    conn_mock = AsyncMock()
    conn_mock.fetchrow = AsyncMock(return_value=None)
    
    # Set up context manager mock for acquire
    ctx_mock = MagicMock()
    ctx_mock.__aenter__ = AsyncMock(return_value=conn_mock)
    ctx_mock.__aexit__ = AsyncMock(return_value=None)
    req3.app.state.db.acquire.return_value = ctx_mock

    try:
        await get_current_user(req3)
        print("[FAIL] Test 3 Failed: Allowed access with invalid API token.")
    except HTTPException as e:
        if e.status_code == 401 and "Invalid API token" in str(e.detail):
            print("[OK] Test 3 Passed: Invalid API token -> 401 Invalid API token.")
        else:
            print(f"[FAIL] Test 3 Failed: Got HTTP {e.status_code} ({e.detail}).")

    # 4. Valid JWT token
    valid_jwt = create_access_token(data={"sub": "test@user.com", "role": "3"})
    req4 = MockRequest(path="/schemas", headers={"Authorization": f"Bearer {valid_jwt}"})
    # Mock DB query for user validity
    req4.app.state.db.acquire = MagicMock()
    conn_mock4 = AsyncMock()
    conn_mock4.fetchrow = AsyncMock(return_value={"is_blocked": False, "token_valid_after": None})
    ctx_mock4 = MagicMock()
    ctx_mock4.__aenter__ = AsyncMock(return_value=conn_mock4)
    ctx_mock4.__aexit__ = AsyncMock(return_value=None)
    req4.app.state.db.acquire.return_value = ctx_mock4

    try:
        res = await get_current_user(req4)
        if isinstance(res, TokenData) and res.username == "test@user.com" and res.role == "3":
            print("[OK] Test 4 Passed: Valid JWT -> Success.")
        else:
            print(f"[FAIL] Test 4 Failed: Expected TokenData, got {res}.")
    except Exception as e:
        print(f"[FAIL] Test 4 Failed: Got exception {e}.")

    # 5. Valid stx_live token
    req5 = MockRequest(path="/schemas", headers={"Authorization": "Bearer stx_live_valid_token"})
    # Mock DB query for valid token row
    req5.app.state.db.acquire = MagicMock()
    conn_mock5 = AsyncMock()
    conn_mock5.fetchrow = AsyncMock(return_value={
        "token_active": True,
        "token_plan": "pro",
        "user_id": 42,
        "email": "dev@stx.in",
        "is_blocked": False,
        "status": "active"
    })
    conn_mock5.fetchval = AsyncMock(return_value=3)
    ctx_mock5 = MagicMock()
    ctx_mock5.__aenter__ = AsyncMock(return_value=conn_mock5)
    ctx_mock5.__aexit__ = AsyncMock(return_value=None)
    req5.app.state.db.acquire.return_value = ctx_mock5

    try:
        res = await get_current_user(req5)
        if isinstance(res, TokenData) and res.username == "dev@stx.in" and res.role == "3":
            print("[OK] Test 5 Passed: Valid stx_live token -> Success.")
        else:
            print(f"[FAIL] Test 5 Failed: Expected TokenData, got {res}.")
    except Exception as e:
        print(f"[FAIL] Test 5 Failed: Got exception {e}.")

    # 6. Fallback cookie only allowed on non-API routes
    valid_jwt = create_access_token(data={"sub": "test@user.com", "role": "3"})
    req6 = MockRequest(path="/user/dashboard", headers={}, cookies={"access_token": valid_jwt})
    req6.app.state.db.acquire = MagicMock()
    conn_mock6 = AsyncMock()
    conn_mock6.fetchrow = AsyncMock(return_value={"is_blocked": False, "token_valid_after": None})
    ctx_mock6 = MagicMock()
    ctx_mock6.__aenter__ = AsyncMock(return_value=conn_mock6)
    ctx_mock6.__aexit__ = AsyncMock(return_value=None)
    req6.app.state.db.acquire.return_value = ctx_mock6

    try:
        res = await get_current_user(req6)
        if isinstance(res, TokenData) and res.username == "test@user.com" and res.role == "3":
            print("[OK] Test 6 Passed: Fallback cookie on UI route -> Success.")
        else:
            print(f"[FAIL] Test 6 Failed: Expected TokenData, got {res}.")
    except Exception as e:
        print(f"[FAIL] Test 6 Failed: Got exception {e}.")

    # 7. Fallback cookie blocked on API routes
    req7 = MockRequest(path="/schemas", headers={}, cookies={"access_token": valid_jwt})
    try:
        await get_current_user(req7)
        print("[FAIL] Test 7 Failed: Allowed cookie fallback on API route.")
    except HTTPException as e:
        if e.status_code == 401 and e.detail == "Not authenticated":
            print("[OK] Test 7 Passed: Cookie fallback blocked on API route -> 401 Not authenticated.")
        else:
            print(f"[FAIL] Test 7 Failed: Got HTTP {e.status_code} ({e.detail}).")

    # 8. Fallback cookie allowed on non-developer API routes (e.g. /api/user/token)
    req8 = MockRequest(path="/api/user/token", headers={}, cookies={"access_token": valid_jwt})
    req8.app.state.db.acquire = MagicMock()
    conn_mock8 = AsyncMock()
    conn_mock8.fetchrow = AsyncMock(return_value={"is_blocked": False, "token_valid_after": None})
    ctx_mock8 = MagicMock()
    ctx_mock8.__aenter__ = AsyncMock(return_value=conn_mock8)
    ctx_mock8.__aexit__ = AsyncMock(return_value=None)
    req8.app.state.db.acquire.return_value = ctx_mock8

    try:
        res = await get_current_user(req8)
        if isinstance(res, TokenData) and res.username == "test@user.com" and res.role == "3":
            print("[OK] Test 8 Passed: Fallback cookie allowed on /api/user/token -> Success.")
        else:
            print(f"[FAIL] Test 8 Failed: Expected TokenData, got {res}.")
    except Exception as e:
        print(f"[FAIL] Test 8 Failed: Got exception {e}.")

if __name__ == "__main__":
    asyncio.run(run_tests())
