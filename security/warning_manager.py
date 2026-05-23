# security/warning_manager.py
from fastapi import HTTPException
from datetime import datetime, timedelta
import json


async def check_and_auto_unfreeze(conn, user_email: str):
    """
    Checks if the user's status is 'frozen' and the freeze duration has expired.
    If so, unfreezes the user automatically.
    """
    row = await conn.fetchrow(
        "SELECT status, freeze_until FROM users WHERE email = $1 LIMIT 1",
        user_email
    )
    if row and str(row["status"]).strip().lower() == "frozen":
        freeze_until = row["freeze_until"]
        if freeze_until and freeze_until < datetime.utcnow():
            try:
                await conn.execute(
                    """
                    UPDATE users SET status = 'active', freeze_until = NULL, warning_count = 0
                    WHERE email = $1
                    """,
                    user_email
                )
                await conn.execute(
                    """
                    INSERT INTO governance_logs (user_email, event_type, detail, created_at)
                    VALUES ($1, 'auto_unfreeze', 'Account automatically unfrozen after freeze period expired', CURRENT_TIMESTAMP)
                    """,
                    user_email
                )
                return True
            except Exception:
                pass
    return False


async def verify_user_status(conn, user_email: str, action_type: str, user_role: str):
    """
    Enforces user verification status, blocks, warnings, and freezing logic.
    - Admins (role 'admin' or '1') bypass status/verification checks.
    - Blocked users are immediately forbidden with reason.
    - Non-verified users are forbidden from querying/downloading data.
    - Frozen users are forbidden from performing actions.
    - Warning state is logged or passed along.
    """
    # Admins bypass verification and status checks
    if user_role in ("admin", "1"):
        # Update last_active for admin
        try:
            await conn.execute(
                "UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE email = $1",
                user_email
            )
        except Exception:
            pass
        return {"status": "active", "is_verified": True}

    # Automatically check and unfreeze if expired
    await check_and_auto_unfreeze(conn, user_email)

    row = await conn.fetchrow(
        """
        SELECT 
            COALESCE(is_blocked, FALSE) AS is_blocked,
            COALESCE(is_verified, FALSE) AS is_verified,
            COALESCE(status, 'active') AS status,
            blocked_reason,
            freeze_until,
            COALESCE(warning_count, 0) AS warning_count
        FROM users
        WHERE email = $1
        LIMIT 1
        """,
        user_email
    )

    if not row:
        raise HTTPException(status_code=404, detail="User not found in system")

    # Update last_active
    try:
        await conn.execute(
            "UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE email = $1",
            user_email
        )
    except Exception:
        pass

    # 1. Check if user is blocked
    if row["is_blocked"]:
        reason = row["blocked_reason"] or "No reason provided"
        raise HTTPException(
            status_code=403, 
            detail=f"Access Denied: Your account has been blocked. Reason: {reason}"
        )

    # 2. Check if user is frozen
    user_status = row["status"].strip().lower()
    if user_status == "frozen":
        raise HTTPException(
            status_code=403,
            detail="Access Denied: Your account is currently frozen. Please contact support."
        )

    # 3. Check if user is verified (only block query and download actions)
    if action_type in ("query", "download") and not row["is_verified"]:
        raise HTTPException(
            status_code=403,
            detail="Access Denied: Your account is pending verification. Please upload the required documents in your dashboard and wait for admin approval."
        )

    return {
        "status": user_status,
        "is_verified": row["is_verified"],
        "warning_count": row["warning_count"],
    }


async def issue_governance_warning(conn, user_email: str, violation_type: str, message: str):
    """
    Issues a governance warning and applies escalation:
      1st violation → warning
      2nd violation → temporary restriction (reduced limits)
      3rd violation → account freeze for 24 hours
    """
    # Get current warning count and plan
    row = await conn.fetchrow(
        "SELECT COALESCE(warning_count, 0) AS warning_count, COALESCE(plan, 'free') AS plan FROM users WHERE email = $1",
        user_email
    )
    if not row:
        return

    current_warnings = row["warning_count"]
    new_count = current_warnings + 1
    severity = min(new_count, 3)

    # Store the warning
    try:
        await conn.execute(
            """
            INSERT INTO governance_warnings (user_email, violation_type, severity, message, created_at)
            VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
            """,
            user_email, violation_type, severity, message
        )
    except Exception as e:
        print(f"⚠️ warning_manager: Failed to store warning: {e}")

    # Apply escalation
    is_rate_limit = (violation_type == "rate_limit_exceeded")
    is_cell_suppression = (violation_type == "cell_suppression_violation")

    if is_cell_suppression and new_count >= 3:
        # Block account permanently for cell suppression violations (admin must manually unblock)
        try:
            await conn.execute(
                """
                UPDATE users 
                SET warning_count = $1, 
                    is_blocked = TRUE, 
                    blocked_reason = 'Repeated cell suppression violations (3 warnings). Admin must manually unblock.', 
                    status = 'blocked'
                WHERE email = $2
                """,
                new_count, user_email
            )
            await conn.execute(
                """
                INSERT INTO governance_logs (user_email, event_type, detail, metadata, created_at)
                VALUES ($1, 'account_blocked', $2, $3, CURRENT_TIMESTAMP)
                """,
                user_email,
                f"Account permanently blocked due to {new_count} cell suppression violations.",
                json.dumps({"violations": new_count, "last_violation": violation_type})
            )
        except Exception as e:
            print(f"⚠️ warning_manager: Failed to block account: {e}")
    elif is_rate_limit:
        # Rate limit freeze: free- 5mins, pro- 2 min, enterprise-30 secs.
        plan = row["plan"].strip().lower()
        if "free" in plan:
            duration = timedelta(minutes=5)
            duration_str = "5 minutes"
        elif "pro" in plan:
            duration = timedelta(minutes=2)
            duration_str = "2 minutes"
        else:
            duration = timedelta(seconds=30)
            duration_str = "30 seconds"

        freeze_until = datetime.utcnow() + duration
        try:
            await conn.execute(
                """
                UPDATE users SET warning_count = $1, status = 'frozen', freeze_until = $2
                WHERE email = $3
                """,
                new_count, freeze_until, user_email
            )
            await conn.execute(
                """
                INSERT INTO governance_logs (user_email, event_type, detail, metadata, created_at)
                VALUES ($1, 'account_frozen', $2, $3, CURRENT_TIMESTAMP)
                """,
                user_email,
                f"Account frozen for {duration_str} due to rate limit violation. Unfreezes at {freeze_until.isoformat()}",
                json.dumps({"freeze_until": freeze_until.isoformat(), "violations": new_count, "last_violation": violation_type, "rate_limit_freeze": True})
            )
        except Exception as e:
            print(f"⚠️ warning_manager: Failed to freeze account on rate limit: {e}")
    elif new_count >= 3:
        # Freeze account for 24 hours
        freeze_until = datetime.utcnow() + timedelta(hours=24)
        try:
            await conn.execute(
                """
                UPDATE users SET warning_count = $1, status = 'frozen', freeze_until = $2
                WHERE email = $3
                """,
                new_count, freeze_until, user_email
            )
            await conn.execute(
                """
                INSERT INTO governance_logs (user_email, event_type, detail, metadata, created_at)
                VALUES ($1, 'account_frozen', $2, $3, CURRENT_TIMESTAMP)
                """,
                user_email,
                f"Account frozen due to {new_count} governance violations. Unfreezes at {freeze_until.isoformat()}",
                json.dumps({"freeze_until": freeze_until.isoformat(), "violations": new_count, "last_violation": violation_type})
            )
        except Exception as e:
            print(f"⚠️ warning_manager: Failed to freeze account: {e}")
    elif new_count == 2:
        # Temporary restriction — update warning count
        try:
            await conn.execute(
                "UPDATE users SET warning_count = $1 WHERE email = $2",
                new_count, user_email
            )
            await conn.execute(
                """
                INSERT INTO governance_logs (user_email, event_type, detail, metadata, created_at)
                VALUES ($1, 'temporary_restriction', $2, $3, CURRENT_TIMESTAMP)
                """,
                user_email,
                f"Temporary restrictions applied after {new_count} governance violations.",
                json.dumps({"violations": new_count, "last_violation": violation_type})
            )
        except Exception as e:
            print(f"⚠️ warning_manager: Failed to apply restriction: {e}")
    else:
        # Just a warning
        try:
            await conn.execute(
                "UPDATE users SET warning_count = $1 WHERE email = $2",
                new_count, user_email
            )
            await conn.execute(
                """
                INSERT INTO governance_logs (user_email, event_type, detail, metadata, created_at)
                VALUES ($1, 'warning_issued', $2, $3, CURRENT_TIMESTAMP)
                """,
                user_email,
                f"Governance warning issued: {violation_type}",
                json.dumps({"violations": new_count, "violation_type": violation_type, "message": message})
            )
        except Exception as e:
            print(f"⚠️ warning_manager: Failed to log warning event: {e}")

    return {"warning_count": new_count, "severity": severity}


async def check_and_escalate_suspicious_score(conn, user_email: str):
    """
    Checks the user's suspicious_score and automatically issues warnings/escalations.
    Score thresholds:
      >= 90: issues a severe warning (triggers freeze if warning_count goes to 3)
      >= 60: issues a warning / restriction (triggers restriction if warning_count goes to 2)
      >= 30: issues a first warning
    """
    row = await conn.fetchrow(
        "SELECT COALESCE(suspicious_score, 0) AS suspicious_score, COALESCE(warning_count, 0) AS warning_count FROM users WHERE email = $1",
        user_email
    )
    if not row:
        return
    
    score = row["suspicious_score"]
    warns = row["warning_count"]
    
    if score >= 90 and warns < 3:
        await issue_governance_warning(
            conn, user_email,
            violation_type="severe_abuse",
            message=f"Critical risk score reached: {score}. Account frozen for 24 hours."
        )
    elif score >= 60 and warns < 2:
        await issue_governance_warning(
            conn, user_email,
            violation_type="moderate_abuse",
            message=f"High risk score reached: {score}. Monthly limits restricted (halved)."
        )
    elif score >= 30 and warns < 1:
        await issue_governance_warning(
            conn, user_email,
            violation_type="minor_abuse",
            message=f"Risk score reached: {score}. First warning issued. Please review usage guidelines."
        )


async def get_user_warnings(conn, user_email: str):
    """Get all active (unresolved) warnings for a user."""
    try:
        rows = await conn.fetch(
            """
            SELECT id, violation_type, severity, message, created_at
            FROM governance_warnings
            WHERE user_email = $1 AND resolved = FALSE
            ORDER BY created_at DESC
            LIMIT 20
            """,
            user_email
        )
        return [dict(r) for r in rows]
    except Exception:
        return []


async def get_user_governance_notices(conn, user_email: str):
    """Get recent governance notices/events for a user."""
    try:
        rows = await conn.fetch(
            """
            SELECT event_type, detail, created_at
            FROM governance_logs
            WHERE user_email = $1
            ORDER BY created_at DESC
            LIMIT 10
            """,
            user_email
        )
        return [dict(r) for r in rows]
    except Exception:
        return []
