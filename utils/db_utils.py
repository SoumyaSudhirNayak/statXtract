import hashlib
import re
from sqlalchemy import create_engine, inspect, text

MAX_PG_IDENTIFIER_LEN = 63


def to_snake_case_identifier(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    s = s.replace("&", " and ")
    s = re.sub(r"[^0-9A-Za-z]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    s = s.lower()
    if not s:
        return ""
    if s[0].isdigit():
        s = f"s_{s}"
    if len(s) > MAX_PG_IDENTIFIER_LEN:
        suffix = hashlib.sha1(s.encode("utf-8")).hexdigest()[:6]
        keep = MAX_PG_IDENTIFIER_LEN - (1 + len(suffix))
        s = f"{s[:keep].rstrip('_')}_{suffix}"
    return s

def table_exists(table_name: str, db_url: str, schema: str) -> bool:
    """
    Check if a table exists in a specific schema of the PostgreSQL database.
    """
    engine = create_engine(db_url)
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names(schema=schema)
    return table_name.lower() in [t.lower() for t in existing_tables]

def schema_exists(schema_name: str, db_url: str) -> bool:
    """
    Check if a schema exists in the PostgreSQL database.
    """
    engine = create_engine(db_url)
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema"),
            {"schema": schema_name}
        ).fetchone()
    return result is not None

def ensure_metadata_tables(schema_name: str, db_url: str) -> None:
    schema = (schema_name or "").strip()
    if not schema:
        raise ValueError("schema_name is required")

    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".datasets (
                    dataset_id TEXT PRIMARY KEY,
                    title TEXT,
                    source TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".dataset_files (
                    id SERIAL PRIMARY KEY,
                    dataset_id TEXT REFERENCES "{schema}".datasets(dataset_id) ON DELETE CASCADE,
                    filename TEXT,
                    file_type TEXT,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".variables (
                    variable_id TEXT PRIMARY KEY,
                    dataset_id TEXT REFERENCES "{schema}".datasets(dataset_id) ON DELETE CASCADE,
                    table_name TEXT,
                    column_name TEXT,
                    label TEXT,
                    data_type TEXT,
                    start_pos INTEGER,
                    width INTEGER,
                    decimals INTEGER,
                    concept TEXT,
                    universe TEXT,
                    question_text TEXT
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".variable_categories (
                    id SERIAL PRIMARY KEY,
                    variable_id TEXT REFERENCES "{schema}".variables(variable_id) ON DELETE CASCADE,
                    category_code TEXT,
                    category_label TEXT,
                    frequency INTEGER
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".variable_missing_values (
                    id SERIAL PRIMARY KEY,
                    variable_id TEXT REFERENCES "{schema}".variables(variable_id) ON DELETE CASCADE,
                    missing_value TEXT
                );
                """
            )
        )


def ensure_survey_metadata_tables(schema_name: str, db_url: str) -> None:
    schema = (schema_name or "").strip()
    if not schema:
        raise ValueError("schema_name is required")

    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".dataset_registry (
                    id SERIAL PRIMARY KEY,
                    year TEXT NOT NULL,
                    dataset_display_name TEXT NOT NULL,
                    dataset_db_name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (year, dataset_db_name)
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".dataset_tables (
                    id SERIAL PRIMARY KEY,
                    year TEXT NOT NULL,
                    dataset_db_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    level_display_name TEXT,
                    level_db_name TEXT,
                    row_count BIGINT,
                    column_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (year, dataset_db_name, table_name)
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".dataset_metadata (
                    id SERIAL PRIMARY KEY,
                    year TEXT NOT NULL,
                    dataset_db_name TEXT NOT NULL,
                    title TEXT,
                    abstract TEXT,
                    keywords JSONB,
                    coverage JSONB,
                    file_description JSONB,
                    variable_count INTEGER,
                    case_count BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (year, dataset_db_name)
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".variable_dictionary (
                    id SERIAL PRIMARY KEY,
                    year TEXT NOT NULL,
                    dataset_db_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    variable_name TEXT NOT NULL,
                    label TEXT,
                    ddi_type TEXT,
                    width INTEGER,
                    interval TEXT,
                    valid_count BIGINT,
                    invalid_count BIGINT,
                    final_type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (year, dataset_db_name, table_name, variable_name)
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".variable_categories (
                    id SERIAL PRIMARY KEY,
                    year TEXT NOT NULL,
                    dataset_db_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    variable_name TEXT NOT NULL,
                    value TEXT,
                    label TEXT,
                    frequency BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".variable_statistics (
                    id SERIAL PRIMARY KEY,
                    year TEXT NOT NULL,
                    dataset_db_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    variable_name TEXT NOT NULL,
                    mean DOUBLE PRECISION,
                    min DOUBLE PRECISION,
                    max DOUBLE PRECISION,
                    stddev DOUBLE PRECISION,
                    unique_count BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (year, dataset_db_name, table_name, variable_name)
                );
                """
            )
        )


def make_dataset_schema_name(survey_schema: str, dataset_name: str) -> str:
    survey = (survey_schema or "").strip()
    if not survey:
        raise ValueError("survey_schema is required")

    ds = to_snake_case_identifier(dataset_name)
    if not ds:
        ds = "dataset"

    base = f"{survey}__{ds}"
    if len(base) <= MAX_PG_IDENTIFIER_LEN:
        return base

    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:6]
    keep = MAX_PG_IDENTIFIER_LEN - (1 + len(h))
    return f"{base[:keep].rstrip('_')}_{h}"[:MAX_PG_IDENTIFIER_LEN]


def ensure_dataset_schema_tables(dataset_schema: str, db_url: str) -> None:
    schema = (dataset_schema or "").strip()
    if not schema:
        raise ValueError("dataset_schema is required")

    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".dataset_metadata (
                    id SERIAL PRIMARY KEY,
                    survey_schema TEXT,
                    dataset_display_name TEXT,
                    survey_id TEXT,
                    title TEXT,
                    abstract TEXT,
                    keywords TEXT,
                    geographic_coverage TEXT,
                    industrial_coverage TEXT,
                    product_coverage TEXT,
                    weighting TEXT,
                    frequency TEXT,
                    methodology TEXT,
                    collection_mode TEXT,
                    time_method TEXT,
                    procedures TEXT,
                    producer TEXT,
                    ddi_id TEXT,
                    file_case_count BIGINT,
                    file_variable_count BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".variables (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    variable_name TEXT NOT NULL,
                    label TEXT,
                    ddi_type TEXT,
                    width INTEGER,
                    interval TEXT,
                    valid_count BIGINT,
                    invalid_count BIGINT,
                    final_type TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (table_name, variable_name)
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".variable_categories (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    variable_name TEXT NOT NULL,
                    value TEXT,
                    label TEXT,
                    frequency BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS "{schema}".variable_statistics (
                    id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    variable_name TEXT NOT NULL,
                    mean DOUBLE PRECISION,
                    min DOUBLE PRECISION,
                    max DOUBLE PRECISION,
                    stddev DOUBLE PRECISION,
                    unique_count BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (table_name, variable_name)
                );
                """
            )
        )
