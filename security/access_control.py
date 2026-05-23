# security/access_control.py
from fastapi import HTTPException
from .warning_manager import verify_user_status, issue_governance_warning
from .plan_enforcer import get_and_enforce_plan_limits, is_export_format_allowed
from .usage_tracker import check_daily_usage, check_download_limits
from .suspicious_detector import detect_suspicious_activity, check_behavioral_patterns

def _normalize_role(role_value) -> str:
    raw = str(role_value or "").strip().lower()
    return {"1": "admin", "2": "analyst", "3": "user"}.get(raw, raw or "user")

async def check_user_access(
    conn,
    user,
    action_type: str,
    schema_name: str = None,
    table_name: str = None,
    rows_requested: int = 0,
    export_type: str = None,
    filters: str = None,
    limit: int = 100
):
    """
    Central Access Control & Security Enforcement Engine.
    Controls: User status, role checks, plan limits, daily usage tracker, API/download access,
    suspicious activity checks, and privacy enforcement.
    
    Full runtime check order:
    1. verified?
    2. blocked?
    3. current assigned plan?
    4. testing mode?
    5. queries remaining?
    6. row limits?
    7. export permission?
    8. sensitive variable access?
    9. suppression thresholds?
    10. suspicious activity score?
    """
    user_email = str(getattr(user, "username", "") or "")
    user_role = str(getattr(user, "role", "3"))
    role_name = _normalize_role(user_role)

    # 1. Enforce Status Checks (Blocking, Freezing, and Document Verification status)
    status_info = await verify_user_status(conn, user_email, action_type, user_role)

    # Admins bypass remaining limits and validations (but still screen for malicious SQLi inputs)
    if role_name == "admin":
        if action_type in ("query", "download"):
            await detect_suspicious_activity(conn, user_email, filters=filters, limit=limit)
        return {
            "allowed": True,
            "role": role_name,
            "plan": "admin",
            "limits": {}
        }

    # 2. Check Action and Role Level Access Controls
    if action_type == "dashboard":
        # Only admin and analyst have access to dashboards
        if role_name not in ("admin", "analyst"):
            raise HTTPException(
                status_code=403,
                detail="Access Denied: You do not have permissions to view administration dashboards."
            )

    elif action_type in ("query", "download", "export"):
        # Enforce Suspicious Activity Detection (SQL Injection detection)
        await detect_suspicious_activity(conn, user_email, filters=filters, limit=limit)

        # Run behavioral pattern check (non-blocking, just logs)
        try:
            behavioral = await check_behavioral_patterns(conn, user_email)
            if behavioral.get("total_risk", 0) > 80:
                # Issue governance warning for high-risk behavior
                await issue_governance_warning(
                    conn, user_email,
                    violation_type="behavioral_abuse",
                    message=f"High-risk behavior detected: {', '.join(p['type'] for p in behavioral['patterns'])}"
                )
        except Exception:
            pass

        # Enforce Plan and Custom Limits
        plan_limits = await get_and_enforce_plan_limits(conn, user_email, user_role)

        # Enforce Request Rate Limiting System (excluding admin)
        if role_name != "admin":
            plan_name = plan_limits.get("plan", "free")
            requests_limit = plan_limits.get("rate_limit", 5)

            recent_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM usage_logs
                WHERE user_email = $1 AND queried_at >= NOW() - INTERVAL '1 minute'
                """,
                user_email
            ) or 0

            if recent_count >= requests_limit:
                # Increment user's suspicious score / activity logs
                try:
                    await conn.execute(
                        """
                        INSERT INTO suspicious_activity_logs (user_email, activity_type, risk_score, detail, created_at)
                        VALUES ($1, 'rate_limit_exceeded', 20, $2, CURRENT_TIMESTAMP)
                        """,
                        user_email,
                        f"Rate limit of {requests_limit} req/min exceeded (Current: {recent_count})"
                    )
                    await conn.execute(
                        "UPDATE users SET suspicious_score = COALESCE(suspicious_score, 0) + 10 WHERE email = $1",
                        user_email
                    )
                    await issue_governance_warning(
                        conn, user_email,
                        violation_type="rate_limit_exceeded",
                        message=f"Rate limit exceeded: {recent_count} requests in 1 minute. Limit is {requests_limit}/min."
                    )
                except Exception as e:
                    print(f"Error logging rate limit violation: {e}")

                raise HTTPException(
                    status_code=429,
                    detail=f"Rate Limit Exceeded: You have exceeded the limit of {requests_limit} requests per minute for your {plan_name.upper()} plan. Please slow down."
                )

        # Enforce daily quota checks (Daily queries and rows sum)
        await check_daily_usage(conn, user_email, plan_limits, rows_requested)

        # Enforce download limits / format restrictions
        if action_type in ("download", "export"):
            await check_download_limits(conn, user_email, plan_limits)

        # Enforce export format restrictions
        if action_type == "export" and export_type:
            if not is_export_format_allowed(plan_limits, export_type):
                raise HTTPException(
                    status_code=403,
                    detail=f"Access Denied: Export format '{export_type}' is not available on your current plan. Please upgrade."
                )

        return {
            "allowed": True,
            "role": role_name,
            "plan": plan_limits.get("plan", "free"),
            "limits": plan_limits
        }

    return {
        "allowed": True,
        "role": role_name,
        "status": status_info.get("status")
    }
