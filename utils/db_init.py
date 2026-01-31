import asyncpg


async def ensure_core_tables(conn: asyncpg.Connection) -> None:
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
        CREATE TABLE IF NOT EXISTS variable_configs (
            id SERIAL PRIMARY KEY,
            schema_name TEXT NOT NULL,
            variable_name TEXT NOT NULL,
            label TEXT,
            include_in_api BOOLEAN DEFAULT TRUE,
            filterable BOOLEAN DEFAULT FALSE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (schema_name, variable_name)
        );
        """
    )

    # ================= NEW METADATA TABLES =================

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS datasets (
            dataset_id TEXT PRIMARY KEY,
            title TEXT,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dataset_files (
            id SERIAL PRIMARY KEY,
            dataset_id TEXT REFERENCES datasets(dataset_id) ON DELETE CASCADE,
            filename TEXT,
            file_type TEXT,
            uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS variables (
            variable_id TEXT PRIMARY KEY, -- dataset_id + variable_name
            dataset_id TEXT REFERENCES datasets(dataset_id) ON DELETE CASCADE,
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

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS variable_categories (
            id SERIAL PRIMARY KEY,
            variable_id TEXT REFERENCES variables(variable_id) ON DELETE CASCADE,
            category_code TEXT,
            category_label TEXT,
            frequency INTEGER
        );
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS variable_missing_values (
            id SERIAL PRIMARY KEY,
            variable_id TEXT REFERENCES variables(variable_id) ON DELETE CASCADE,
            missing_value TEXT
        );
        """
    )

