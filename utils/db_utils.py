from sqlalchemy import create_engine, inspect

def table_exists(table_name: str, db_url: str, schema: str = "public") -> bool:
    """
    Check if a table exists in a specific schema of the PostgreSQL database.
    """
    engine = create_engine(db_url)
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names(schema=schema)
    return table_name.lower() in [t.lower() for t in existing_tables]
