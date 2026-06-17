from ai_query.sql_generator import generate_sql_from_plan

def test_sql_generator_record_lookup():
    plan = {
        "query_type": "record_lookup",
        "target_columns": ["*"],
        "filters": [
            {"column": "Sector", "operator": "=", "value": "Urban"},
            {"column": "Religion", "operator": "=", "value": "Hinduism"}
        ],
        "limit": 25
    }
    
    sql = generate_sql_from_plan(
        schema="employment_and_unemployment__test",
        table="block_3_household_characteristics",
        plan=plan,
        allowed_cols=["Sector", "Religion", "State"],
        raw_cols_with_labels={"Sector"}
    )
    
    assert "SELECT" in sql
    assert '"Sector_label" = \'Urban\'' in sql  # Maps label column!
    assert '"Religion" = \'Hinduism\'' in sql  # Normal column
    assert "LIMIT 25" in sql

def test_sql_generator_distinct_values():
    plan = {
        "query_type": "distinct_values",
        "target_columns": ["Religion"]
    }
    
    sql = generate_sql_from_plan(
        schema="employment_and_unemployment__test",
        table="block_3_household_characteristics",
        plan=plan,
        allowed_cols=["Sector", "Religion"],
        raw_cols_with_labels=set()
    )
    
    assert sql == 'SELECT DISTINCT "Religion" FROM "employment_and_unemployment__test"."block_3_household_characteristics"  LIMIT 200'

def test_sql_generator_group_by_aggregation():
    plan = {
        "query_type": "group_by",
        "group_by": ["State"],
        "aggregations": [
            {"function": "AVG", "column": "Income"}
        ]
    }
    
    sql = generate_sql_from_plan(
        schema="public",
        table="block_3_household_characteristics",
        plan=plan,
        allowed_cols=["State", "Income"],
        raw_cols_with_labels={"State"}
    )
    
    assert 'SELECT "State_label" AS "State", AVG("Income") AS "avg_income"' in sql
    assert 'GROUP BY "State_label"' in sql

def test_sql_generator_ranking():
    plan = {
        "query_type": "ranking",
        "group_by": ["District"],
        "aggregations": [
            {"function": "AVG", "column": "Income"}
        ],
        "sorting": [
            {"column": "AVG(Income)", "direction": "DESC"}
        ],
        "limit": 10
    }
    
    sql = generate_sql_from_plan(
        schema="public",
        table="test",
        plan=plan,
        allowed_cols=["District", "Income"],
        raw_cols_with_labels=set()
    )
    
    assert 'SELECT "District", AVG("Income") AS "avg_income" FROM "public"."test"' in sql
    assert 'GROUP BY "District"' in sql
    assert 'ORDER BY AVG("Income") DESC' in sql
    assert 'LIMIT 10' in sql

def test_sql_generator_operators():
    plan = {
        "query_type": "record_lookup",
        "filters": [
            {"column": "Income", "operator": ">", "value": 10000},
            {"column": "State", "operator": "IN", "value": "(Delhi, Karnataka)"},
            {"column": "Age", "operator": "BETWEEN", "value": "20 AND 30"},
            {"column": "Name", "operator": "LIKE", "value": "John"}
        ]
    }
    
    sql = generate_sql_from_plan(
        schema="public",
        table="test",
        plan=plan,
        allowed_cols=["Income", "State", "Age", "Name"],
        raw_cols_with_labels=set()
    )
    
    assert '"Income" > 10000' in sql
    assert '"State" IN (\'Delhi\', \'Karnataka\')' in sql
    assert '"Age" BETWEEN 20 AND 30' in sql
    assert '"Name"::text ILIKE \'%John%\'' in sql

def test_sql_generator_new_operators_and_logic():
    plan = {
        "query_type": "record_lookup",
        "filters": [
            {"column": "Religion", "operator": "NOT IN", "value": "Hinduism, Islam", "logic": "AND"},
            {"column": "State", "operator": "IS NOT NULL", "value": None, "logic": "AND"},
            {"column": "Income", "operator": "IS NULL", "value": None, "logic": "OR"}
        ]
    }
    
    sql = generate_sql_from_plan(
        schema="public",
        table="test",
        plan=plan,
        allowed_cols=["Religion", "State", "Income"],
        raw_cols_with_labels=set()
    )
    
    assert '"Religion" NOT IN (\'Hinduism\', \'Islam\')' in sql
    assert '"State" IS NOT NULL' in sql
    assert 'OR "Income" IS NULL' in sql

def test_sql_generator_type_aware_numeric():
    plan = {
        "query_type": "record_lookup",
        "filters": [
            {"column": "HH_Size", "operator": "=", "value": 4}
        ]
    }
    col_types = {
        "hh_size": "bigint"
    }
    sql = generate_sql_from_plan(
        schema="public",
        table="test",
        plan=plan,
        allowed_cols=["HH_Size"],
        raw_cols_with_labels=set(),
        col_types=col_types
    )
    assert '"HH_Size" = 4' in sql

def test_sql_generator_type_aware_text_quoted():
    plan = {
        "query_type": "record_lookup",
        "filters": [
            {"column": "Sector", "operator": "=", "value": "Urban"}
        ]
    }
    col_types = {
        "sector": "text"
    }
    sql = generate_sql_from_plan(
        schema="public",
        table="test",
        plan=plan,
        allowed_cols=["Sector"],
        raw_cols_with_labels=set(),
        col_types=col_types
    )
    assert '"Sector" = \'Urban\'' in sql

def test_sql_generator_type_aware_text_cast():
    plan = {
        "query_type": "record_lookup",
        "filters": [
            {"column": "Land_Owned", "operator": "=", "value": "20.0"}
        ]
    }
    col_types = {
        "land_owned": "text"
    }
    sql = generate_sql_from_plan(
        schema="public",
        table="test",
        plan=plan,
        allowed_cols=["Land_Owned"],
        raw_cols_with_labels=set(),
        col_types=col_types
    )
    assert 'CAST(NULLIF("Land_Owned", \'-\') AS NUMERIC) = 20.0' in sql

def test_sql_generator_type_aware_text_inequality_cast():
    plan = {
        "query_type": "record_lookup",
        "filters": [
            {"column": "Land_Owned", "operator": ">", "value": 20}
        ]
    }
    col_types = {
        "land_owned": "text"
    }
    sql = generate_sql_from_plan(
        schema="public",
        table="test",
        plan=plan,
        allowed_cols=["Land_Owned"],
        raw_cols_with_labels=set(),
        col_types=col_types
    )
    assert 'CAST(NULLIF("Land_Owned", \'-\') AS NUMERIC) > 20' in sql
