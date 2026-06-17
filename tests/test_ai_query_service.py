import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock
from ai_query.ai_query_service import map_ai_plan_to_parsed, interpret_query_with_ai, fetch_table_metadata

def test_map_ai_plan_to_parsed():
    # Test 1: simple aggregate query
    plan = {
        "intent": "aggregate",
        "columns": ["gender", "age", "state"],
        "filters": {
            "gender": "female",
            "age": "18-25",
            "state": "Telangana"
        },
        "metric": "count",
        "confidence": 0.95,
        "explanation": "Counting unemployed females aged 18-25 in Telangana."
    }
    parsed = map_ai_plan_to_parsed(plan, "How many unemployed females aged 18-25 in Telangana?")
    
    assert parsed["intent"] == "aggregate"
    assert parsed["confidence"] == 0.95
    assert parsed["explanation"] == "Counting unemployed females aged 18-25 in Telangana."
    
    # Check filters mapping
    filters = parsed["filters"]
    assert len(filters) == 3
    gender_filter = next(f for f in filters if f["column"] == "gender")
    assert gender_filter["operator"] == "="
    assert gender_filter["value"] == "female"
    
    age_filter = next(f for f in filters if f["column"] == "age")
    assert age_filter["operator"] == "BETWEEN"
    assert age_filter["value"] == "18 AND 25"

def test_map_ai_plan_to_parsed_comparisons():
    # Test 2: comparisons
    plan = {
        "intent": "statistical",
        "columns": ["income", "age"],
        "filters": {
            "age": ">=60",
            "income": "<=50000"
        },
        "metric": "avg"
    }
    parsed = map_ai_plan_to_parsed(plan, "Average income of workers older than 60 with income under 50000")
    
    filters = parsed["filters"]
    age_filter = next(f for f in filters if f["column"] == "age")
    assert age_filter["operator"] == ">="
    assert age_filter["value"] == "60"
    
    income_filter = next(f for f in filters if f["column"] == "income")
    assert income_filter["operator"] == "<="
    assert income_filter["value"] == "50000"
    
    assert len(parsed["aggregations"]) == 1
    assert parsed["aggregations"][0]["function"] == "AVG"
    assert parsed["aggregations"][0]["column"] == "income"

@pytest.mark.asyncio
async def test_fetch_table_metadata():
    mock_conn = AsyncMock()
    mock_conn.fetchval.return_value = True # Has variables table
    mock_conn.fetch.side_effect = [
        # Columns db types query
        [
            {"column_name": "gender", "data_type": "character"},
            {"column_name": "age", "data_type": "numeric"}
        ],
        # Variables query
        [
            {"variable_name": "gender", "label": "Gender description", "ddi_type": "character", "question_text": "What is your gender?"},
            {"variable_name": "age", "label": "Age description", "ddi_type": "numeric", "question_text": "What is your age?"}
        ],
        # Categories query
        [
            {"variable_name": "gender", "value": "1", "label": "Male"},
            {"variable_name": "gender", "value": "2", "label": "Female"}
        ]
    ]
    
    metadata = await fetch_table_metadata(mock_conn, "public", "person", ["gender", "age", "income"])
    assert "gender" in metadata
    assert metadata["gender"]["label"] == "Gender description"
    assert metadata["gender"]["categories"] == ["1 = Male", "2 = Female"]
    assert "age" in metadata
    assert metadata["age"]["type"] == "numeric"
    assert metadata["income"]["label"] == "" # Not in variables database

@pytest.mark.asyncio
async def test_interpret_query_with_ai_success():
    mock_conn = AsyncMock()
    
    # Mock table exists check to return True, and variables table check to return False
    def mock_fetchval_se(query, *args):
        if "table_name = 'variables'" in query or "table_name = 'variable_dictionary'" in query:
            return False
        return True
    mock_conn.fetchval.side_effect = mock_fetchval_se
    
    mock_conn.fetch.side_effect = [
        [], # columns db types query
        [], # No variables rows
        []  # No categories rows
    ]
    
    mock_get_response = MagicMock()
    mock_get_response.status_code = 200
    mock_get_response.json.return_value = {
        "data": [{"id": "meta-llama-3.1-8b-instruct"}]
    }
    
    mock_post_response = MagicMock()
    mock_post_response.status_code = 200
    mock_post_response.json.return_value = {
        "choices": [
            {
                "message": {
                    "content": json.dumps({
                        "success": True,
                        "intent": "aggregate",
                        "columns": ["gender", "age"],
                        "filters": [
                            {"column": "gender", "value": "female"},
                            {"column": "age", "value": "18-25"}
                        ],
                        "metric": "count",
                        "confidence": 0.9,
                        "explanation": "Counting females aged 18-25"
                    })
                }
            }
        ]
    }
    
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_get_response
        with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_post_response
            
            plan = await interpret_query_with_ai(
                mock_conn, 
                "How many females 18-25?", 
                "public", 
                "person", 
                ["gender", "age", "income"]
            )
            
            assert plan["success"] is True
            assert plan["intent"] == "aggregate"
            assert plan["columns"] == ["gender", "age"]
            assert plan["filters"] == {"gender": "female", "age": "18-25"}
            assert plan["confidence"] == 0.9

@pytest.mark.asyncio
async def test_interpret_query_with_ai_hallucinations_filtering():
    mock_conn = AsyncMock()
    
    def mock_fetchval_se(query, *args):
        if "table_name = 'variables'" in query or "table_name = 'variable_dictionary'" in query:
            return False
        return True
    mock_conn.fetchval.side_effect = mock_fetchval_se
    
    mock_conn.fetch.side_effect = [[], [], []]
    
    mock_get_response = MagicMock()
    mock_get_response.status_code = 200
    mock_get_response.json.return_value = {"data": [{"id": "meta-llama-3.1-8b-instruct"}]}
    
    mock_post_response = MagicMock()
    mock_post_response.status_code = 200
    # Llama returns a hallucinated column "nationality"
    mock_post_response.json.return_value = {
        "choices": [
            {
                "message": {
                    "content": json.dumps({
                        "success": True,
                        "intent": "aggregate",
                        "columns": ["gender", "nationality"],
                        "filters": [
                            {"column": "gender", "value": "female"},
                            {"column": "nationality", "value": "Indian"}
                        ],
                        "metric": "count",
                        "confidence": 0.8
                    })
                }
            }
        ]
    }
    
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_get_response
        with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_post_response
            
            # Table only actually has "gender" and "age"
            plan = await interpret_query_with_ai(
                mock_conn, 
                "How many Indian females?", 
                "public", 
                "person", 
                ["gender", "age"]
            )
            
            assert plan["success"] is False
            assert plan["message"] == "Column not found"
