import pandas as pd
from sqlalchemy import create_engine

def load_csv_to_postgres(csv_path, table_name, db_url, schema):
    df = pd.read_csv(csv_path, low_memory=False)  # prevents DtypeWarning

    if "?sslmode=" not in db_url:
        db_url += "?sslmode=disable"  # ensure SSL is disabled for local testing

    engine = create_engine(db_url)
    with engine.connect() as conn:
        print("✅ Connected to database")

    df.to_sql(table_name, engine, schema=schema, if_exists='fail', index=False)  # respect schema
    print(f"✅ Data uploaded to table: {schema}.{table_name}")
    return f"{schema}.{table_name}"
