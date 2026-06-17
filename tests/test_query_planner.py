import pytest
import json
from unittest.mock import AsyncMock, MagicMock, patch
from ai_query.query_planner import (
    collect_metadata,
    generate_query_plan,
    normalize_plan_value,
    PLANNER_SYSTEM_PROMPT
)

@pytest.mark.asyncio
async def test_collect_metadata():
    mock_conn = AsyncMock()
    mock_conn.fetchval.return_value = True # table variables exists
    
    # Mock information_schema.columns query, variables query, and distinct values queries
    mock_conn.fetch.side_effect = [
        [
            {"column_name": "Religion", "data_type": "varchar"},
            {"column_name": "State", "data_type": "varchar"},
            {"column_name": "Income", "data_type": "integer"}
        ],
        # Columns of variables table
        [
            {"column_name": "column_name"},
            {"column_name": "label"}
        ],
        [
            {"column_name": "Religion", "label": "Respondent's Religion"},
            {"column_name": "State", "label": "State of Residence"},
            {"column_name": "Income", "label": "Household Monthly Income"}
        ],
        # Distinct values queries
        [{"Religion": "Hinduism"}, {"Religion": "Islam"}],
        [{"State": "Delhi"}, {"State": "Karnataka"}],
        [{"Income": 5000}, {"Income": 10000}]
    ]

    meta = await collect_metadata(mock_conn, "public", "test_table", ["Religion", "State", "Income"])
    assert meta["table"] == "test_table"
    assert len(meta["columns"]) == 3
    assert meta["columns"][0]["name"] == "Religion"
    assert meta["columns"][0]["type"] == "varchar"
    assert meta["columns"][0]["label"] == "Respondent's Religion"
    assert "Hinduism" in meta["sample_values"]["Religion"]
    assert "Karnataka" in meta["sample_values"]["State"]

def test_normalize_plan_value():
    samples = ["Hinduism", "Islam", "Christianity", "Buddhism"]
    # Exact case match
    assert normalize_plan_value("hinduism", samples) == "Hinduism"
    # Fuzzy match
    assert normalize_plan_value("buddism", samples) == "Buddhism"
    # No match
    assert normalize_plan_value("zoroastrian", samples) == "zoroastrian"

@pytest.mark.asyncio
async def test_generate_query_plan_success():
    mock_conn = AsyncMock()
    mock_conn.fetchval.side_effect = [False, False]
    mock_conn.fetch.side_effect = [
        [
            {"column_name": "Religion", "data_type": "varchar"},
            {"column_name": "State", "data_type": "varchar"}
        ],
        [{"Religion": "Hinduism"}],
        [{"State": "Karnataka"}]
    ]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "choices": [
            {
                "message": {
                    "content": json.dumps({
                        "query_type": "record_lookup",
                        "target_columns": ["*"],
                        "filters": [
                            {"column": "religion", "operator": "=", "value": "hinduism"},
                            {"column": "state", "operator": "=", "value": "karnataka"}
                        ],
                        "group_by": [],
                        "aggregations": [],
                        "sorting": [],
                        "limit": 25
                    })
                }
            }
        ]
    }

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_resp
        plan = await generate_query_plan(
            mock_conn,
            "show 25 hindu households in karnataka",
            "public",
            "test_table",
            ["Religion", "State"]
        )

        assert plan["success"] is True
        assert plan["query_type"] == "record_lookup"
        assert plan["limit"] == 25
        assert plan["filters"][0]["column"] == "Religion"
        assert plan["filters"][0]["value"] == "Hinduism"  # Normalized!
        assert plan["filters"][1]["column"] == "State"
        assert plan["filters"][1]["value"] == "Karnataka"  # Normalized!

@pytest.mark.asyncio
async def test_generate_query_plan_column_not_found():
    mock_conn = AsyncMock()
    mock_conn.fetchval.side_effect = [False, False]
    mock_conn.fetch.side_effect = [
        [
            {"column_name": "Religion", "data_type": "varchar"}
        ],
        [{"Religion": "Hinduism"}]
    ]

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "choices": [
            {
                "message": {
                    "content": json.dumps({
                        "query_type": "record_lookup",
                        "target_columns": ["*"],
                        "filters": [
                            {"column": "income", "operator": ">", "value": 10000}
                        ]
                    })
                }
            }
        ]
    }

    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_resp
        plan = await generate_query_plan(
            mock_conn,
            "show households with income > 10000",
            "public",
            "test_table",
            ["Religion"]
        )

        assert plan["success"] is False
        assert "Column not found" in plan["error"]
