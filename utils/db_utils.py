from sqlalchemy import create_engine, inspect, text

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
