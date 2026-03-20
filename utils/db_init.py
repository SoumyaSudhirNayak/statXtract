import os
import asyncpg


def _to_pg_schema_name(display_name: str) -> str:
    s = (display_name or "").strip().lower()
    s = s.replace("&", " and ")
    out = []
    prev_us = False
    for ch in s:
        is_ok = ("a" <= ch <= "z") or ("0" <= ch <= "9")
        if is_ok:
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    name = "".join(out).strip("_")
    if not name:
        return "survey"
    if name[0].isdigit():
        name = f"s_{name}"
    return name


SURVEY_SCHEMA_DISPLAY_NAMES = [
    "Annual Survey of Industries",
    "Index of Industrial Production",
    "Household Consumption Expenditure",
    "Economic Census",
    "Enterprises Surveys",
    "Periodic Labour Force Survey",
    "Employment and Unemployment",
    "Land and Livestock Holding Surveys",
    "Other Surveys",
]


async def ensure_core_tables(conn: asyncpg.Connection) -> None:
    # Fix schema_registry if it's in a bad state
    bad_schema_reg = await conn.fetchval("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'schema_registry' AND column_name NOT IN ('id', 'display_name', 'db_name')
        )
    """)
    if bad_schema_reg:
        await conn.execute("DROP TABLE IF EXISTS schema_registry CASCADE")

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_registry (
            id SERIAL PRIMARY KEY,
            display_name TEXT NOT NULL,
            db_name TEXT UNIQUE NOT NULL
        );
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS roles (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50) UNIQUE NOT NULL
        );
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username TEXT,
            email VARCHAR(255) UNIQUE NOT NULL,
            hashed_password TEXT NOT NULL,
            role_id INTEGER REFERENCES roles(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    await conn.execute(
        """
        INSERT INTO roles (name) VALUES ('admin'), ('analyst'), ('user')
        ON CONFLICT (name) DO NOTHING;
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS usage_logs (
            id SERIAL PRIMARY KEY,
            user_email TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            schema_name TEXT,
            table_name TEXT,
            rows_returned INTEGER,
            bytes_sent BIGINT,
            queried_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_configs (
            id SERIAL PRIMARY KEY,
            schema_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            show_table_to_users BOOLEAN DEFAULT TRUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (schema_name, table_name)
        );
        """
    )

    try:
        await conn.execute(
            "ALTER TABLE dataset_configs ADD COLUMN IF NOT EXISTS show_table_to_users BOOLEAN DEFAULT TRUE"
        )
    except Exception:
        pass

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS variable_configs (
            id SERIAL PRIMARY KEY,
            schema_name TEXT NOT NULL,
            variable_name TEXT NOT NULL,
            label TEXT,
            include_in_api BOOLEAN DEFAULT TRUE,
            filterable BOOLEAN DEFAULT FALSE,
            display_mode TEXT DEFAULT 'label',
            is_sensitive BOOLEAN DEFAULT FALSE,
            min_rows INTEGER DEFAULT 5,
            table_name TEXT DEFAULT '*',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (schema_name, table_name, variable_name)
        );
        """
    )

    for col_def in [
        ("display_mode", "TEXT DEFAULT 'label'"),
        ("is_sensitive", "BOOLEAN DEFAULT FALSE"),
        ("min_rows", "INTEGER DEFAULT 5"),
        ("table_name", "TEXT DEFAULT '*'"),
    ]:
        try:
            await conn.execute(
                f"ALTER TABLE variable_configs ADD COLUMN IF NOT EXISTS {col_def[0]} {col_def[1]}"
            )
        except Exception:
            pass

    try:
        await conn.execute("ALTER TABLE variable_configs DROP CONSTRAINT IF EXISTS variable_configs_schema_name_variable_name_key")
    except Exception:
        pass
    try:
        await conn.execute("ALTER TABLE variable_configs DROP CONSTRAINT IF EXISTS varconfigs_unique")
    except Exception:
        pass
    try:
        await conn.execute("ALTER TABLE variable_configs ADD CONSTRAINT varconfigs_unique UNIQUE (schema_name, table_name, variable_name)")
    except Exception:
        pass

    # Fix dataset_registry if it's in a bad state from previous versions
    # Check for old column names which indicates old schema
    bad_table = await conn.fetchval("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'dataset_registry' 
            AND column_name IN ('display_name', 'db_name')
        )
    """)
    if bad_table:
        await conn.execute("DROP TABLE IF EXISTS dataset_registry CASCADE")

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_registry (
            id SERIAL PRIMARY KEY,
            survey_schema TEXT NOT NULL,
            dataset_schema TEXT UNIQUE NOT NULL,
            dataset_display_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    keep_schemas = set()
    for display in SURVEY_SCHEMA_DISPLAY_NAMES:
        db_name = _to_pg_schema_name(display)
        keep_schemas.add(db_name)
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{db_name}"')
        await conn.execute(
            """
            INSERT INTO schema_registry (display_name, db_name)
            VALUES ($1, $2)
            ON CONFLICT (db_name) DO UPDATE SET display_name = EXCLUDED.display_name
            """,
            display,
            db_name,
        )

    drop_extra = (os.getenv("DROP_UNREGISTERED_SCHEMAS_ON_STARTUP") or "0").strip().lower() in {"1", "true", "yes", "on"}
    if drop_extra:
        rows = await conn.fetch(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'public')
            """
        )
        for r in rows:
            schema_name = r["schema_name"]
            if schema_name in keep_schemas:
                continue
            if any(schema_name.startswith(f"{s}__") for s in keep_schemas):
                continue
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
