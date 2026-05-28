"""
tests/test_ai_query.py
Unit tests for the AI Query (NLP engine, risk classification, and summary generation).
"""

import pytest
from ai_query.nlp_engine import NLPQueryEngine, classify_query_risk, generate_summary

def test_nlp_query_engine_parsing():
    available_columns = ["age", "gender", "income", "state", "sector", "education"]
    variable_configs = {
        "income": {"is_sensitive": True},
        "gender": {"is_sensitive": False}
    }
    
    engine = NLPQueryEngine(available_columns=available_columns, variable_configs=variable_configs)
    
    # Test 1: Simple Filter
    parsed = engine.parse("Show workers who are male")
    assert any(f["column"] == "gender" and f["value"] == "Male" for f in parsed["filters"])
    
    # Test 2: Aggregation
    parsed = engine.parse("Average income of workers")
    assert any(agg["function"] == "AVG" and agg["column"] == "income" for agg in parsed["aggregations"])
    
    # Test 3: Age Range
    parsed = engine.parse("Workers aged 20 to 30")
    assert any(f["column"] == "age" and f["operator"] == "BETWEEN" for f in parsed["filters"])
    
    # Test 4: Limit
    parsed = engine.parse("Top 10 workers")
    assert parsed["limit"] == 10

def test_risk_classification():
    # Low Risk
    parsed_low = {
        "filters": [{"column": "state", "operator": "=", "value": "Bihar"}],
        "aggregations": [],
        "limit": 100
    }
    risk_low = classify_query_risk(parsed_low)
    assert risk_low["level"] == "low"
    
    # Medium Risk (Multiple identity filters)
    parsed_med = {
        "filters": [
            {"column": "gender", "operator": "=", "value": "Male"},
            {"column": "religion", "operator": "=", "value": "Hindu"},
            {"column": "marital_status", "operator": "=", "value": "Married"}
        ],
        "aggregations": [],
        "limit": 100
    }
    risk_med = classify_query_risk(parsed_med)
    assert risk_med["level"] == "medium"
    
    # High Risk (Excessive filtering + low limit)
    parsed_high = {
        "filters": [
            {"column": "gender", "operator": "=", "value": "Male"},
            {"column": "religion", "operator": "=", "value": "Hindu"},
            {"column": "marital_status", "operator": "=", "value": "Married"},
            {"column": "social_group", "operator": "=", "value": "SC"},
            {"column": "age", "operator": "BETWEEN", "value": "25 AND 27"}
        ],
        "aggregations": [],
        "limit": 3
    }
    risk_high = classify_query_risk(parsed_high)
    assert risk_high["level"] == "high"

def test_generate_summary():
    parsed = {
        "filters": [{"column": "gender", "operator": "=", "value": "Male"}],
        "aggregations": [{"function": "AVG", "column": "income"}]
    }
    results = [
        {"gender": "Male", "income": 50000},
        {"gender": "Male", "income": 60000}
    ]
    columns = ["gender", "income"]
    
    summary = generate_summary(parsed, results, columns)
    assert "Average income" in summary or "average income" in summary
    assert "55,000" in summary
