import pytest
from fastapi import HTTPException
from unittest.mock import AsyncMock, MagicMock, patch
from main import execute_sql_query, SQLExecuteRequest
from auth.local.schemas import TokenData

class MockRequest:
    def __init__(self, db_mock):
        self.app = MagicMock()
        self.app.state = MagicMock()
        self.app.state.db = db_mock

@pytest.fixture
def db_mocks():
    db_mock = MagicMock()
    conn_mock = MagicMock()
    conn_mock.fetch = AsyncMock()
    conn_mock.fetchval = AsyncMock()
    conn_mock.execute = AsyncMock()
    
    # Mock pool acquire context manager
    ctx_mock = MagicMock()
    ctx_mock.__aenter__ = AsyncMock(return_value=conn_mock)
    ctx_mock.__aexit__ = AsyncMock(return_value=None)
    db_mock.acquire.return_value = ctx_mock
    
    # Mock transaction context manager
    tx_mock = MagicMock()
    tx_mock.__aenter__ = AsyncMock(return_value=None)
    tx_mock.__aexit__ = AsyncMock(return_value=None)
    conn_mock.transaction.return_value = tx_mock
    
    return db_mock, conn_mock

@pytest.fixture(autouse=True)
def mock_dependencies():
    # Setup global mocks for helpers used in execute_sql_query
    with patch("main.check_user_access", return_value={"allowed": True}), \
         patch("main.apply_config", return_value=(["name", "age", "state"], None)), \
         patch("main.get_variable_configs", return_value={}):
        yield

@pytest.mark.asyncio
async def test_sql_suppression_non_aggregated_under_threshold(db_mocks):
    db_mock, conn_mock = db_mocks
    
    # 1. Mock DB returns:
    # - Table existence check: returns table name
    # - Columns: returns name, age, state
    # - Explain cost: returns plan row
    # - Result execution: returns 3 rows (< 5 threshold)
    conn_mock.fetch.side_effect = [
        [{"table_name": "citizens"}],
        [{"column_name": "name"}, {"column_name": "age"}, {"column_name": "state"}],
        [("Seq Scan on citizens",)],
        [
            {"name": "John", "age": 30, "state": "KA"},
            {"name": "Jane", "age": 25, "state": "AP"},
            {"name": "Bob", "age": 40, "state": "TS"}
        ]
    ]
    conn_mock.fetchval.return_value = 0 # Rate limit check
    
    req = MockRequest(db_mock)
    payload = SQLExecuteRequest(survey="ASI", dataset="asi_2020", table="citizens", sql="SELECT * FROM citizens WHERE age > 30;")
    current_user = TokenData(username="test@guest.com", role="3") # Non-admin
    
    # Non-aggregated query returning 3 rows should raise HTTPException 403
    with pytest.raises(HTTPException) as exc_info:
        await execute_sql_query(req, payload, current_user)
    assert exc_info.value.status_code == 403
    assert "Result suppressed" in exc_info.value.detail

@pytest.mark.asyncio
async def test_sql_suppression_count_query_passes(db_mocks):
    db_mock, conn_mock = db_mocks
    
    # COUNT query returns 1 row, but should NOT raise HTTPException since it is classified as is_agg
    conn_mock.fetch.side_effect = [
        [{"table_name": "citizens"}],
        [{"column_name": "name"}, {"column_name": "age"}, {"column_name": "state"}],
        [("Seq Scan on citizens",)],
        [{"count": 62641}] # 1 row returned
    ]
    conn_mock.fetchval.return_value = 0
    
    req = MockRequest(db_mock)
    payload = SQLExecuteRequest(survey="ASI", dataset="asi_2020", table="citizens", sql="SELECT COUNT(*) FROM citizens;")
    current_user = TokenData(username="test@guest.com", role="3")
    
    res = await execute_sql_query(req, payload, current_user)
    assert len(res) == 1
    assert res[0]["count"] == 62641 # count matches

@pytest.mark.asyncio
async def test_sql_suppression_distinct_query_passes(db_mocks):
    db_mock, conn_mock = db_mocks
    
    # DISTINCT query returning 3 rows (< 5) should NOT raise HTTPException since it is classified as is_distinct
    conn_mock.fetch.side_effect = [
        [{"table_name": "citizens"}],
        [{"column_name": "name"}, {"column_name": "age"}, {"column_name": "state"}],
        [("Seq Scan on citizens",)],
        [{"state": "KA"}, {"state": "AP"}, {"state": "TS"}] # 3 rows returned
    ]
    conn_mock.fetchval.return_value = 0
    
    req = MockRequest(db_mock)
    payload = SQLExecuteRequest(survey="ASI", dataset="asi_2020", table="citizens", sql="SELECT DISTINCT state FROM citizens;")
    current_user = TokenData(username="test@guest.com", role="3")
    
    res = await execute_sql_query(req, payload, current_user)
    assert len(res) == 3
    assert res[0]["state"] == "KA"

@pytest.mark.asyncio
async def test_sql_suppression_group_by_suppresses_small_groups(db_mocks):
    db_mock, conn_mock = db_mocks
    
    # GROUP BY query returns counts per group:
    # State AP count is 2 (below 5 -> suppressed)
    # State KA count is 2000 (above 5 -> kept)
    conn_mock.fetch.side_effect = [
        [{"table_name": "citizens"}],
        [{"column_name": "name"}, {"column_name": "age"}, {"column_name": "state"}],
        [("Seq Scan on citizens",)],
        # Result of executing the GROUP BY query:
        [
            {"state": "AP", "group_cnt": 2},
            {"state": "KA", "group_cnt": 2000}
        ],
        # Result of executing the underlying counts query in Step 3:
        [
            {"state": "AP", "group_cnt": 2},
            {"state": "KA", "group_cnt": 2000}
        ]
    ]
    conn_mock.fetchval.return_value = 0
    
    req = MockRequest(db_mock)
    payload = SQLExecuteRequest(
        survey="ASI", 
        dataset="asi_2020", 
        table="citizens", 
        sql="SELECT state, COUNT(*) as group_cnt FROM citizens GROUP BY state;"
    )
    current_user = TokenData(username="test@guest.com", role="3")
    
    res = await execute_sql_query(req, payload, current_user)
    assert len(res) == 2
    
    # State AP (group_cnt = 2) should be suppressed
    assert res[0]["state"] == "AP"
    assert res[0]["group_cnt"] == "Suppressed"
    
    # State KA (group_cnt = 2000) should be kept
    assert res[1]["state"] == "KA"
    assert res[1]["group_cnt"] == 2000
