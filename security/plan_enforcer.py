# security/plan_enforcer.py
from datetime import datetime
from fastapi import HTTPException

# Hardcoded FALLBACK defaults — only used if system_settings table has no overrides
PLAN_LIMITS = {
    "free": {
        "max_queries_per_month": 100,
        "max_rows_per_month": 5000,
        "max_downloads_per_month": 10,
        "max_ai_queries_per_month": 3,
        "downloads_allowed": True,
        "api_access": False,
        "advanced_analytics": False,
        "export_formats": ["csv"],
        "rate_limit": 5,
        # Daily keys for backward compatibility
        "max_queries_per_day": 100,
        "max_rows_per_day": 5000,
        "max_downloads_per_day": 10,
    },
    "pro": {
        "max_queries_per_month": 1000,
        "max_rows_per_month": 100000,
        "max_downloads_per_month": 100,
        "max_ai_queries_per_month": 50,
        "downloads_allowed": True,
        "api_access": True,
        "advanced_analytics": True,
        "export_formats": ["csv", "pdf", "json"],
        "rate_limit": 20,
        # Daily keys for backward compatibility
        "max_queries_per_day": 1000,
        "max_rows_per_day": 100000,
        "max_downloads_per_day": 100,
    },
    "enterprise": {
        "max_queries_per_month": 999999,
        "max_rows_per_month": 999999999,
        "max_downloads_per_month": 999999,
        "max_ai_queries_per_month": 999999,
        "downloads_allowed": True,
        "api_access": True,
        "advanced_analytics": True,
        "export_formats": ["csv", "excel", "pdf", "json", "api"],
        "rate_limit": 100,
        # Daily keys for backward compatibility
        "max_queries_per_day": 999999,
        "max_rows_per_day": 999999999,
        "max_downloads_per_day": 999999,
    },
    "admin": {
        "max_queries_per_month": 9999999,
        "max_rows_per_month": 999999999,
        "max_downloads_per_month": 999999,
        "max_ai_queries_per_month": 999999,
        "downloads_allowed": True,
        "api_access": True,
        "advanced_analytics": True,
        "export_formats": ["csv", "excel", "pdf", "json", "api"],
        "rate_limit": 1000,
        # Daily keys for backward compatibility
        "max_queries_per_day": 9999999,
        "max_rows_per_day": 999999999,
        "max_downloads_per_day": 999999,
    }
}

# ── Mapping from system_settings keys → plan limit fields ──
# These keys are what the admin sets in the System Settings UI.
_SETTINGS_KEY_MAP = {
    "free": {
        "max_queries_per_month": "plans.free.max_queries_per_month",
        "max_rows_per_month":    "plans.free.max_rows_per_month",
        "max_downloads_per_month": "plans.free.max_downloads_per_month",
        "max_ai_queries_per_month": "plans.free.max_ai_queries_per_month",
        "downloads_allowed":   "plans.free.downloads_allowed",
        "api_access":          "plans.free.api_access",
        "advanced_analytics":  "plans.free.advanced_analytics",
        "export_formats":      "plans.free.export_formats",
        "rate_limit":          "plans.free.rate_limit",
    },
    "pro": {
        "max_queries_per_month": "plans.pro.max_queries_per_month",
        "max_rows_per_month":    "plans.pro.max_rows_per_month",
        "max_downloads_per_month": "plans.pro.max_downloads_per_month",
        "max_ai_queries_per_month": "plans.pro.max_ai_queries_per_month",
        "downloads_allowed":   "plans.pro.downloads_allowed",
        "api_access":          "plans.pro.api_access",
        "advanced_analytics":  "plans.pro.advanced_analytics",
        "export_formats":      "plans.pro.export_formats",
        "rate_limit":          "plans.pro.rate_limit",
    },
    "enterprise": {
        "max_queries_per_month": "plans.enterprise.max_queries_per_month",
        "max_rows_per_month":    "plans.enterprise.max_rows_per_month",
        "max_downloads_per_month": "plans.enterprise.max_downloads_per_month",
        "max_ai_queries_per_month": "plans.enterprise.max_ai_queries_per_month",
        "downloads_allowed":   "plans.enterprise.downloads_allowed",
        "api_access":          "plans.enterprise.api_access",
        "advanced_analytics":  "plans.enterprise.advanced_analytics",
        "export_formats":      "plans.enterprise.export_formats",
        "rate_limit":          "plans.enterprise.rate_limit",
    },
}


async def _get_setting(conn, key: str, default=None):
    """Fetch a single value from system_settings table."""
    try:
        val = await conn.fetchval(
            "SELECT value FROM system_settings WHERE key = $1 LIMIT 1", key
        )
        return val if val is not None else default
    except Exception:
        return default


async def _load_dynamic_plan_limits(conn, plan: str) -> dict:
    """
    Load plan limits from system_settings table.
    Falls back to hardcoded PLAN_LIMITS if no DB overrides exist.
    """
    base = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"]).copy()

    # Normal mode: read per-plan overrides from system_settings
    key_map = _SETTINGS_KEY_MAP.get(plan)
    if key_map:
        db_queries = await _get_setting(conn, key_map["max_queries_per_month"], None)
        db_rows    = await _get_setting(conn, key_map["max_rows_per_month"], None)
        db_dl      = await _get_setting(conn, key_map["downloads_allowed"], None)
        db_dl_max  = await _get_setting(conn, key_map["max_downloads_per_month"], None)
        db_ai_max  = await _get_setting(conn, key_map["max_ai_queries_per_month"], None)
        db_api     = await _get_setting(conn, key_map.get("api_access", ""), None)
        db_analytics = await _get_setting(conn, key_map.get("advanced_analytics", ""), None)
        db_formats = await _get_setting(conn, key_map.get("export_formats", ""), None)
        db_rate_limit = await _get_setting(conn, key_map.get("rate_limit", ""), None)

        if db_queries is not None:
            base["max_queries_per_month"] = int(db_queries)
            base["max_queries_per_day"] = int(db_queries)
        if db_rows is not None:
            base["max_rows_per_month"] = int(db_rows)
            base["max_rows_per_day"] = int(db_rows)
        if db_dl is not None:
            base["downloads_allowed"] = str(db_dl).strip().lower() in ("true", "1", "yes")
        if db_dl_max is not None:
            base["max_downloads_per_month"] = int(db_dl_max)
            base["max_downloads_per_day"] = int(db_dl_max)
        if db_ai_max is not None:
            base["max_ai_queries_per_month"] = int(db_ai_max)
        if db_api is not None:
            base["api_access"] = str(db_api).strip().lower() in ("true", "1", "yes")
        if db_analytics is not None:
            base["advanced_analytics"] = str(db_analytics).strip().lower() in ("true", "1", "yes")
        if db_formats is not None:
            base["export_formats"] = [f.strip().lower() for f in str(db_formats).split(",") if f.strip()]
        if db_rate_limit is not None:
            base["rate_limit"] = int(db_rate_limit)

    return base


async def get_and_enforce_plan_limits(conn, user_email: str, user_role: str):
    """
    Retrieves and applies the user's subscription plan, validating expiry dates.
    Returns plan details including max monthly queries, max monthly rows, and permissions.
    """
    if user_role in ("admin", "1"):
        return PLAN_LIMITS["admin"]

    row = await conn.fetchrow(
        """
        SELECT 
            COALESCE(plan, 'free') AS plan,
            plan_expiry,
            max_queries_per_day AS max_queries_override,
            max_rows_per_day AS max_rows_override,
            COALESCE(warning_count, 0) AS warning_count
        FROM users
        WHERE email = $1
        LIMIT 1
        """,
        user_email
    )

    if not row:
        return await _load_dynamic_plan_limits(conn, "free")

    plan = str(row["plan"]).strip().lower()
    plan_expiry = row["plan_expiry"]

    # Check for plan expiry (DB stores naive local timestamps)
    if plan_expiry and plan_expiry < datetime.now():
        plan = "free"

    # Fetch limits dynamically from system_settings (with hardcoded fallback)
    limits = await _load_dynamic_plan_limits(conn, plan)
    limits["plan"] = plan

    # Override with database-specific user overrides if defined and different from database defaults
    if row["max_queries_override"] is not None and int(row["max_queries_override"]) != 1000:
        limits["max_queries_per_month"] = int(row["max_queries_override"])
        limits["max_queries_per_day"] = int(row["max_queries_override"])
    if row["max_rows_override"] is not None and int(row["max_rows_override"]) != 100000:
        limits["max_rows_per_month"] = int(row["max_rows_override"])
        limits["max_rows_per_day"] = int(row["max_rows_override"])

    # Sanity boundaries: user overrides cannot exceed plan ceiling from system_settings
    plan_ceiling = await _load_dynamic_plan_limits(conn, plan)
    if plan in ("free", "pro", "enterprise"):
        limits["max_queries_per_month"] = min(limits["max_queries_per_month"], plan_ceiling["max_queries_per_month"])
        limits["max_queries_per_day"] = limits["max_queries_per_month"]
        limits["max_rows_per_month"] = min(limits["max_rows_per_month"], plan_ceiling["max_rows_per_month"])
        limits["max_rows_per_day"] = limits["max_rows_per_month"]

    limits["downloads_allowed"] = limits.get("downloads_allowed", False) or len(limits.get("export_formats", [])) > 0

    # Enforce Warning Count = 2 Temporary Restriction (halve monthly limits)
    if int(row["warning_count"]) == 2:
        limits["max_queries_per_month"] = max(1, limits["max_queries_per_month"] // 2)
        limits["max_queries_per_day"] = limits["max_queries_per_month"]
        limits["max_rows_per_month"] = max(1, limits["max_rows_per_month"] // 2)
        limits["max_rows_per_day"] = limits["max_rows_per_month"]

    return limits


def get_plan_export_formats(limits: dict) -> list:
    """Get allowed export formats from plan limits."""
    return limits.get("export_formats", ["csv"])


def is_export_format_allowed(limits: dict, export_format: str) -> bool:
    """Check if a specific export format is allowed by the user's plan."""
    allowed = get_plan_export_formats(limits)
    fmt = export_format.lower()
    if fmt == "excel" or fmt == "xlsx":
        return "excel" in allowed or "xlsx" in allowed
    return fmt in allowed
