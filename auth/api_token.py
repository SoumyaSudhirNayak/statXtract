import secrets
from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from auth.local.schemas import TokenData

security_bearer = HTTPBearer(auto_error=False)

def generate_api_token() -> str:
    """Generate a cryptographically secure API token starting with stx_live_"""
    return f"stx_live_{secrets.token_hex(16)}"

async def sync_user_api_token(conn, email: str):
    """
    Synchronizes the API token for a user.
    - If user plan is pro or enterprise (and user is active & not blocked),
      generate a token if it doesn't exist, or make sure it is active.
    - Otherwise (plan is free or user is suspended/blocked), deactivate the token.
    """
    user = await conn.fetchrow(
        """
        SELECT id, plan, COALESCE(is_blocked, FALSE) as is_blocked, COALESCE(status, 'active') as status
        FROM users
        WHERE email = $1
        LIMIT 1
        """,
        email
    )
    if not user:
        return

    user_id = user["id"]
    plan = (user["plan"] or "free").strip().lower()
    is_blocked = user["is_blocked"]
    status = (user["status"] or "active").strip().lower()

    # User gets API token only if plan is pro or enterprise, and user is active and not blocked
    has_api_access = (plan in ("pro", "enterprise")) and (not is_blocked) and (status == "active")

    if has_api_access:
        # Check if they already have an API token
        token_row = await conn.fetchrow(
            "SELECT id, active FROM api_tokens WHERE user_id = $1 LIMIT 1",
            user_id
        )
        if not token_row:
            # Generate and insert new token
            token = generate_api_token()
            await conn.execute(
                """
                INSERT INTO api_tokens (token, user_id, plan, active, created_at)
                VALUES ($1, $2, $3, TRUE, CURRENT_TIMESTAMP)
                """,
                token, user_id, plan
            )
        else:
            # Make sure it's active and has correct plan
            if not token_row["active"]:
                await conn.execute(
                    "UPDATE api_tokens SET active = TRUE, plan = $1 WHERE user_id = $2",
                    plan, user_id
                )
            else:
                # Update plan in case it changed
                await conn.execute(
                    "UPDATE api_tokens SET plan = $1 WHERE user_id = $2",
                    plan, user_id
                )
    else:
        # Downgraded to free, or blocked/suspended -> deactivate token
        await conn.execute(
            "UPDATE api_tokens SET active = FALSE WHERE user_id = $1",
            user_id
        )

async def require_api_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security_bearer)
) -> TokenData:
    """
    Dependency to validate the Bearer API token.
    Checks:
    - Token format and existence
    - Token active status
    - User not blocked and status is active
    - Plan allows API access (Pro or Enterprise)
    """
    if not credentials:
        raise HTTPException(
            status_code=401,
            detail="Missing API token. Please provide 'Authorization: Bearer stx_live_...'"
        )

    token = credentials.credentials
    if not token.startswith("stx_live_"):
        raise HTTPException(
            status_code=401,
            detail="Invalid API token format. Developer API tokens must start with 'stx_live_'"
        )

    pool = request.app.state.db
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
            raise HTTPException(
                status_code=401,
                detail="Invalid API token."
            )

        if not token_row["token_active"]:
            raise HTTPException(
                status_code=401,
                detail="API token has been revoked or deactivated."
            )

        if token_row["is_blocked"]:
            raise HTTPException(
                status_code=403,
                detail="User account is blocked."
            )

        if token_row["status"] != "active":
            raise HTTPException(
                status_code=403,
                detail=f"User account status is '{token_row['status']}'. API access suspended."
            )

        plan = token_row["token_plan"]
        if plan not in ("pro", "enterprise", "admin"):
            raise HTTPException(
                status_code=403,
                detail="API access requires a Pro or Enterprise plan."
            )

        # Retrieve role_id of the user
        role_id = await conn.fetchval(
            "SELECT role_id FROM users WHERE id = $1 LIMIT 1",
            token_row["user_id"]
        )
        role_str = str(role_id) if role_id is not None else "3"

        return TokenData(username=token_row["email"], role=role_str)
