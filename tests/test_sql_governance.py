import pytest
import re
from fastapi import HTTPException
from unittest.mock import AsyncMock, MagicMock
from main import execute_sql_query, SQLExecuteRequest
from auth.local.schemas import TokenData

class MockRequest:
    def __init__(self, db_mock):
        self.app = MagicMock()
        self.app.state = MagicMock()
        self.app.state.db = db_mock

@pytest.mark.asyncio
async def test_sql_empty_query():
    req = MockRequest(MagicMock())
    payload = SQLExecuteRequest(survey="ASI", dataset="asi_2020", table="citizens", sql="")
    current_user = TokenData(username="test@guest.com", role="3")
    
    with pytest.raises(HTTPException) as exc_info:
        await execute_sql_query(req, payload, current_user)
    assert exc_info.value.status_code == 400
    assert "cannot be empty" in exc_info.value.detail

@pytest.mark.asyncio
async def test_sql_write_blocked():
    req = MockRequest(MagicMock())
    payload = SQLExecuteRequest(survey="ASI", dataset="asi_2020", table="citizens", sql="SELECT * FROM citizens; DROP TABLE citizens;")
    current_user = TokenData(username="test@guest.com", role="3")
    
    with pytest.raises(HTTPException) as exc_info:
        await execute_sql_query(req, payload, current_user)
    assert exc_info.value.status_code == 400
    assert "Write operations" in exc_info.value.detail

@pytest.mark.asyncio
async def test_sql_read_only_allowed():
    # Setup mocks for db
    db_mock = MagicMock()
    conn_mock = MagicMock()
    conn_mock.fetch = AsyncMock()
    conn_mock.fetchval = AsyncMock()
    conn_mock.execute = AsyncMock()
    
    # Mock connection pool acquire context manager
    ctx_mock = MagicMock()
    ctx_mock.__aenter__ = AsyncMock(return_value=conn_mock)
    ctx_mock.__aexit__ = AsyncMock(return_value=None)
    db_mock.acquire.return_value = ctx_mock
    
    # Mock conn.transaction context manager
    tx_mock = MagicMock()
    tx_mock.__aenter__ = AsyncMock(return_value=None)
    tx_mock.__aexit__ = AsyncMock(return_value=None)
    conn_mock.transaction.return_value = tx_mock
    
    # Table existence and column queries mocks
    conn_mock.fetch.side_effect = [
        [{"table_name": "citizens"}], # Table check
        [{"column_name": "name"}, {"column_name": "age"}, {"column_name": "salary"}], # Actual columns
        [("Seq Scan on citizens",)] # EXPLAIN plan
    ]
    conn_mock.fetchval.side_effect = [
        0, # Rate limit count check
    ]
    
    req = MockRequest(db_mock)
    payload = SQLExecuteRequest(survey="ASI", dataset="asi_2020", table="citizens", sql="SELECT name, age FROM citizens;")
    current_user = TokenData(username="test@guest.com", role="3")
    
    # Mock dependency check_user_access and apply_config
    import main
    original_check_access = main.check_user_access
    original_apply_config = main.apply_config
    original_get_configs = main.get_variable_configs
    
    main.check_user_access = AsyncMock(return_value={"allowed": True})
    main.apply_config = AsyncMock(return_value=(["name", "age", "salary"], None))
    main.get_variable_configs = AsyncMock(return_value={
        "salary": {"is_sensitive": True, "include_in_api": True}
    })
    
    # Execute query (mocking fetch to return data)
    conn_mock.fetch = AsyncMock(side_effect=[
        [{"table_name": "citizens"}], # Table check
        [{"column_name": "name"}, {"column_name": "age"}, {"column_name": "salary"}], # Columns
        [("Seq Scan on citizens",)], # EXPLAIN
        [{"name": "John", "age": 30, "salary": 5000}, {"name": "Jane", "age": 25, "salary": 6000}, 
         {"name": "Bob", "age": 40, "salary": 7000}, {"name": "Alice", "age": 35, "salary": 8000},
         {"name": "Charlie", "age": 45, "salary": 9000}] # Executed rows (5 rows -> passes cell suppression)
    ])
    
    try:
        res = await execute_sql_query(req, payload, current_user)
        # Ensure salary column is filtered out (Step 1)
        assert len(res) == 5
        assert "salary" not in res[0]
        assert "name" in res[0]
        assert "age" in res[0]
    finally:
        main.check_user_access = original_check_access
        main.apply_config = original_apply_config
        main.get_variable_configs = original_get_configs
