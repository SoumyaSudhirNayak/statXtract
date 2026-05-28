# security/suspicious_detector.py
import re
import json
from fastapi import HTTPException
from .usage_tracker import get_qualified_table

# SQL Injection signature patterns
SQLI_PATTERNS = [
    re.compile(r"(--|/\*|\*/|;)", re.IGNORECASE),                                             # comments, stacked queries
    re.compile(r"\b(UNION\s+ALL|UNION\s+SELECT|SELECT\s+.*\s+FROM)\b", re.IGNORECASE),         # unions
    re.compile(r"\b(DROP\s+TABLE|DELETE\s+FROM|UPDATE\s+.*SET|INSERT\s+INTO)\b", re.IGNORECASE),  # DDL/DML statements
    re.compile(r"\bOR\s+\d+\s*=\s*\d+\b", re.IGNORECASE),                                     # tautologies like OR 1=1
    re.compile(r"\bOR\s+'[^']+'\s*=\s*'[^']+'\b", re.IGNORECASE),                               # tautologies like OR 'a'='a'
    re.compile(r"\b(EXEC|EXECUTE|DECLARE|CAST|CONVERT)\b", re.IGNORECASE)                      # dynamic code execution
]


async def detect_suspicious_activity(conn, user_email: str, query_sql: str = None, filters: str = None, limit: int = 100):
    """
    Checks parameters for suspicious activity, SQL Injection attempts, or query abuse.
    If suspicious behavior is detected, logs the attempt, blocks the request, 
    and raises an HTTPException.
    """
    suspicious = False
    offending_part = ""
    activity_type = ""
    risk_score = 0

    # 1. Inspect filters for SQL Injection
    if filters:
        for pattern in SQLI_PATTERNS:
            if pattern.search(filters):
                suspicious = True
                offending_part = f"Filter criteria: '{filters}'"
                activity_type = "sql_injection_attempt"
                risk_score = 90
                break

    # 2. Inspect raw SQL queries if run
    if query_sql:
        for pattern in SQLI_PATTERNS:
            if pattern.search(query_sql):
                suspicious = True
                offending_part = f"SQL statement: '{query_sql}'"
                activity_type = "sql_injection_attempt"
                risk_score = 95
                break

    # 3. Detect abuse: extremely high limits for normal users
    if limit > 50000:
        suspicious = True
        offending_part = f"Requested limit abnormally high: {limit}"
        activity_type = "excessive_row_request"
        risk_score = 60

    if suspicious:
        # Log to system console and potentially flag in database
        print(f"⚠️ SECURITY ALERT: Suspicious activity detected from {user_email}. Details: {offending_part}")
        
        # Log to suspicious_activity_logs
        try:
            susp_table = await get_qualified_table(conn, "suspicious_activity_logs")
            await conn.execute(
                f"""
                INSERT INTO {susp_table} (user_email, activity_type, risk_score, detail, created_at)
                VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                """,
                user_email,
                activity_type,
                risk_score,
                offending_part[:500]
            )
        except Exception:
            pass

        # Also log to query_logs for backward compat
        try:
            qlogs_table = await get_qualified_table(conn, "query_logs")
            await conn.execute(
                f"""
                INSERT INTO {qlogs_table} (user_email, dataset_name, table_name, filters, rows_returned, query_time_ms)
                VALUES ($1, 'SECURITY_ALERT', 'MALICIOUS_ATTEMPT', $2, 0, 0)
                """,
                user_email,
                offending_part[:500]
            )
        except Exception:
            pass

        # Update user's suspicious score
        try:
            users_table = await get_qualified_table(conn, "users")
            await conn.execute(
                f"UPDATE {users_table} SET suspicious_score = COALESCE(suspicious_score, 0) + $1 WHERE email = $2",
                risk_score,
                user_email
            )
        except Exception:
            pass

        # Log governance event
        try:
            govlogs_table = await get_qualified_table(conn, "governance_logs")
            await conn.execute(
                f"""
                INSERT INTO {govlogs_table} (user_email, event_type, detail, metadata, created_at)
                VALUES ($1, 'suspicious_activity', $2, $3, CURRENT_TIMESTAMP)
                """,
                user_email,
                offending_part[:500],
                json.dumps({"activity_type": activity_type, "risk_score": risk_score})
            )
        except Exception:
            pass

        # Call issue_governance_warning to handle escalation and plan-based freeze
        try:
            from security.warning_manager import issue_governance_warning
            await issue_governance_warning(
                conn, user_email, 
                violation_type="suspicious_query_pattern", 
                message=f"Suspicious query pattern detected: {offending_part[:200]}"
            )
        except Exception as e:
            print(f"⚠️ suspicious_detector: Failed to issue governance warning: {e}")

        raise HTTPException(
            status_code=400,
            detail=f"Security Policy Block: Suspicious or malicious request parameters detected. Threat logged."
        )

    return {"suspicious": False}


async def check_behavioral_patterns(conn, user_email: str):
    """
    Checks for behavioral patterns indicating abuse:
    - Rapid repeated queries (>30 queries in last 5 minutes)
    - Huge row extraction (>100K rows in last hour)
    - Repeated blocked queries (>5 blocked in last hour)
    - Excessive exports (>10 downloads in last hour)
    
    Returns dict with detected patterns and risk score.
    """
    patterns = []
    total_risk = 0

    try:
        usage_table = await get_qualified_table(conn, "usage_logs")
        qlogs_table = await get_qualified_table(conn, "query_logs")
        download_table = await get_qualified_table(conn, "download_logs")
        susp_table = await get_qualified_table(conn, "suspicious_activity_logs")
        users_table = await get_qualified_table(conn, "users")

        # 1. Rapid queries: >30 in last 5 minutes
        rapid_count = await conn.fetchval(
            f"""
            SELECT COUNT(*) FROM {usage_table}
            WHERE user_email = $1 AND queried_at >= NOW() - INTERVAL '5 minutes'
            """,
            user_email
        )
        if rapid_count and rapid_count > 30:
            patterns.append({
                "type": "rapid_queries",
                "detail": f"{rapid_count} queries in 5 minutes",
                "risk_score": 40
            })
            total_risk += 40

        # 2. Huge row extraction: >100K rows in last hour
        hourly_rows = await conn.fetchval(
            f"""
            SELECT COALESCE(SUM(rows_returned), 0) FROM {usage_table}
            WHERE user_email = $1 AND queried_at >= NOW() - INTERVAL '1 hour'
            """,
            user_email
        )
        if hourly_rows and hourly_rows > 100000:
            patterns.append({
                "type": "excessive_row_extraction",
                "detail": f"{hourly_rows} rows in last hour",
                "risk_score": 50
            })
            total_risk += 50

        # 3. Repeated blocked queries: check query_logs for SECURITY_ALERT entries
        blocked_count = await conn.fetchval(
            f"""
            SELECT COUNT(*) FROM {qlogs_table}
            WHERE user_email = $1
              AND dataset_name = 'SECURITY_ALERT'
              AND created_at >= NOW() - INTERVAL '1 hour'
            """,
            user_email
        )
        if blocked_count and blocked_count > 5:
            patterns.append({
                "type": "repeated_blocked_queries",
                "detail": f"{blocked_count} blocked queries in last hour",
                "risk_score": 70
            })
            total_risk += 70

        # 4. Excessive exports
        export_count = await conn.fetchval(
            f"""
            SELECT COUNT(*) FROM {download_table}
            WHERE user_email = $1 AND created_at >= NOW() - INTERVAL '1 hour'
            """,
            user_email
        )
        if export_count and export_count > 10:
            patterns.append({
                "type": "excessive_exports",
                "detail": f"{export_count} downloads in last hour",
                "risk_score": 45
            })
            total_risk += 45

        # Log detected patterns
        if patterns:
            for p in patterns:
                try:
                    await conn.execute(
                        f"""
                        INSERT INTO {susp_table} (user_email, activity_type, risk_score, detail, created_at)
                        VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
                        """,
                        user_email,
                        p["type"],
                        p["risk_score"],
                        p["detail"]
                    )
                except Exception:
                    pass

            # Update user's suspicious score
            try:
                await conn.execute(
                    f"UPDATE {users_table} SET suspicious_score = LEAST(COALESCE(suspicious_score, 0) + $1, 999) WHERE email = $2",
                    min(total_risk, 100),
                    user_email
                )
            except Exception:
                pass

    except Exception as e:
        print(f"⚠️ suspicious_detector.check_behavioral_patterns error: {e}")

    return {"patterns": patterns, "total_risk": total_risk}
