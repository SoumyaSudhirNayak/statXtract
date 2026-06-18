import pytest
from fastapi.testclient import TestClient
from main import app, SQLExecuteRequest
from auth.local.dependencies import get_current_user
from unittest.mock import AsyncMock, patch, MagicMock

client = TestClient(app)

@pytest.fixture
def mock_db():
    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value=True) # schema/dataset exists checks
    mock_conn.fetch = AsyncMock(return_value=[
        {"table_name": "citizens", "column_name": "name"},
        {"table_name": "citizens", "column_name": "age"}
    ])
    
    mock_pool = MagicMock()
    mock_acquire = MagicMock()
    mock_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_acquire.__aexit__ = AsyncMock(return_value=None)
    mock_pool.acquire = MagicMock(return_value=mock_acquire)
    
    app.state.db = mock_pool
    return mock_conn

@pytest.fixture
def mock_current_user():
    user = MagicMock()
    user.username = "test_user@statxtract.in"
    user.role = "2" # Analyst
    return user

@pytest.fixture(autouse=True)
def setup_dependencies(mock_current_user):
    app.dependency_overrides[get_current_user] = lambda: mock_current_user
    yield
    app.dependency_overrides.clear()

@patch("main.apply_admin_rules")
@patch("main._group_schemas_by_survey")
@patch("main.apply_config")
def test_list_survey_dataset_tables_public(mock_apply_config, mock_group_schemas, mock_apply_rules, mock_db):
    mock_apply_rules.return_value = None
    mock_apply_config.return_value = (["name", "age"], None)
    
    # Mock group schemas
    mock_group_schemas.return_value = {
        "ASI": {
            "survey": "ASI",
            "display_name": "Annual Survey of Industries",
            "datasets": [{"schema": "asi_2020", "year": "2020"}]
        }
    }
    
    # Mock table names rows returning from query
    mock_db.fetch.return_value = [{"table_name": "citizens"}]
    
    response = client.get("/surveys/ASI/datasets/asi_2020/tables")
    
    assert response.status_code == 200
    data = response.json()
    assert data["survey"] == "ASI"
    assert data["dataset"] == "asi_2020"
    assert "citizens" in data["tables"]

@patch("main._group_schemas_by_survey")
@patch("main.query_table")
def test_query_survey_dataset_public_params(mock_query_table, mock_group_schemas, mock_db):
    mock_query_table.return_value = {"results": [{"name": "John", "age": 30}]}
    mock_group_schemas.return_value = {
        "ASI": {
            "survey": "ASI",
            "display_name": "Annual Survey of Industries",
            "datasets": [{"schema": "asi_2020", "year": "2020"}]
        }
    }
    
    response = client.get(
        "/surveys/ASI/datasets/asi_2020/query?table=citizens&columns=name,age&limit=10"
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    mock_query_table.assert_called_once()

@patch("main._group_schemas_by_survey")
@patch("main.execute_sql_query")
def test_query_survey_dataset_public_sql(mock_execute_sql, mock_group_schemas, mock_db):
    mock_execute_sql.return_value = [{"name": "John", "age": 30}]
    mock_group_schemas.return_value = {
        "ASI": {
            "survey": "ASI",
            "display_name": "Annual Survey of Industries",
            "datasets": [{"schema": "asi_2020", "year": "2020"}]
        }
    }
    
    response = client.get(
        "/surveys/ASI/datasets/asi_2020/query?sql=SELECT * FROM citizens LIMIT 10"
    )
    
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert data[0]["name"] == "John"
    
    # Verify SQLExecuteRequest was constructed properly and routed
    args, kwargs = mock_execute_sql.call_args
    payload = args[1]
    assert isinstance(payload, SQLExecuteRequest)
    assert payload.survey == "ASI"
    assert payload.dataset == "asi_2020"
    assert payload.table == "citizens"
    assert payload.sql == "SELECT * FROM citizens LIMIT 10"
