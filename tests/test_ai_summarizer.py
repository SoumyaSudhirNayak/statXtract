import pytest
import os
import json
from unittest.mock import AsyncMock, patch, MagicMock
from ai_service.summarizer import summarize_text, CACHE_DIR

@pytest.mark.asyncio
async def test_truncation_and_formatting():
    # Test that summarizer truncates documentation > 12000 chars
    long_text = "A" * 15000
    
    mock_post_response = MagicMock()
    mock_post_response.status_code = 200
    mock_post_response.json.return_value = {
        "choices": [
            {
                "message": {
                    "content": "• Point 1\n• Point 2\n• Point 3"
                }
            }
        ]
    }
    
    mock_get_response = MagicMock()
    mock_get_response.status_code = 200
    mock_get_response.json.return_value = {
        "data": [
            {
                "id": "Qwen3-VL-4B-Instruct-GGUF"
            }
        ]
    }
    
    # Clean cache file
    cache_path = os.path.join(CACHE_DIR, "test_dataset.json")
    if os.path.exists(cache_path):
        os.remove(cache_path)
        
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_get_response
        with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
            mock_post.return_value = mock_post_response
            
            points = await summarize_text("test_dataset", long_text)
            
            assert mock_post.called
            sent_payload = mock_post.call_args[1]["json"]
            user_msg = sent_payload["messages"][1]["content"]
            assert len(user_msg) >= 12000 and len(user_msg) < 13000
            assert points == ["Point 1", "Point 2", "Point 3"]
        
    # Clean up cache
    if os.path.exists(cache_path):
        os.remove(cache_path)

@pytest.mark.asyncio
async def test_bullet_parsing_variations():
    mock_post_response = MagicMock()
    mock_post_response.status_code = 200
    
    mock_get_response = MagicMock()
    mock_get_response.status_code = 200
    mock_get_response.json.return_value = {
        "data": [
            {
                "id": "Qwen3-VL-4B-Instruct-GGUF"
            }
        ]
    }
    
    test_cases = [
        ("• point one\n* point two\n- point three", ["point one", "point two", "point three"]),
        ("1. First point\n2. Second point", ["First point", "Second point"]),
        ("1) Alternate numbering\n2) Next point", ["Alternate numbering", "Next point"]),
        ("Plain text line one\nPlain text line two", ["Plain text line one", "Plain text line two"]),
    ]
    
    for idx, (input_text, expected) in enumerate(test_cases):
        mock_post_response.json.return_value = {
            "choices": [
                {
                    "message": {
                        "content": input_text
                    }
                }
            ]
        }
        
        dataset_key = f"test_parse_{idx}"
        cache_path = os.path.join(CACHE_DIR, f"{dataset_key}.json")
        if os.path.exists(cache_path):
            os.remove(cache_path)
            
        with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_get_response
            with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
                mock_post.return_value = mock_post_response
                points = await summarize_text(dataset_key, "Some text")
                assert points == expected
            
        if os.path.exists(cache_path):
            os.remove(cache_path)
