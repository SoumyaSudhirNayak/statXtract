import os
import json
import re
import httpx
try:
    from prompts import PROMPT_TEMPLATE
except ImportError:
    from ai_service.prompts import PROMPT_TEMPLATE

LM_STUDIO_URL = os.getenv("LM_STUDIO_URL", "http://127.0.0.1:1234")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.getenv("CACHE_DIR", os.path.join(BASE_DIR, "cache"))
cached_model_name = None

async def get_model_name() -> str:
    global cached_model_name
    if cached_model_name:
        return cached_model_name
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{LM_STUDIO_URL}/v1/models")
            if resp.status_code == 200:
                data = resp.json()
                models = data.get("data", [])
                if models:
                    # Look for meta-llama-3.1-8b-instruct or similar case-insensitive match
                    for m in models:
                        m_id = m.get("id")
                        if m_id and "llama" in m_id.lower():
                            cached_model_name = m_id
                            return cached_model_name
                    # Otherwise, use the first available model
                    m_id = models[0].get("id")
                    if m_id:
                        cached_model_name = m_id
                        return cached_model_name
    except Exception as e:
        print(f"Error auto-detecting model name from {LM_STUDIO_URL}/v1/models: {e}")
        
    return "meta-llama-3.1-8b-instruct"

async def check_lm_studio_health() -> bool:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{LM_STUDIO_URL}/v1/models")
            return resp.status_code == 200
    except Exception:
        return False

async def summarize_text(dataset_key: str, text: str, temperature: float = 0.4, bypass_cache: bool = False) -> list[str]:
    # Check local cache first
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(CACHE_DIR, f"{dataset_key}.json")
    if not bypass_cache and os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                cached_data = json.load(f)
                if isinstance(cached_data, list):
                    return cached_data
        except Exception as e:
            print(f"Error reading local cache: {e}")

    # Truncate safely to 12000 characters
    truncated_text = text[:12000]
    
    # Format user content
    prompt = PROMPT_TEMPLATE.format(TEXT=truncated_text)
    
    # Detect model name dynamically
    model_name = await get_model_name()
    
    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "system",
                "content": "You are a statistical dataset analyst. You must respond with a valid JSON object."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": temperature,
        "max_tokens": 1000
    }
    
    async with httpx.AsyncClient(timeout=90.0) as client:
        response = await client.post(f"{LM_STUDIO_URL}/v1/chat/completions", json=payload)
        if response.status_code != 200:
            raise Exception(f"LM Studio returned status code {response.status_code}: {response.text}")
        
        result = response.json()
        response_text = result["choices"][0]["message"]["content"]
        
        def extract_json_block(text: str) -> dict:
            cleaned = text.strip()
            if cleaned.startswith("```"):
                newline_idx = cleaned.find("\n")
                if newline_idx != -1:
                    cleaned = cleaned[newline_idx:].strip()
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3].strip()
            
            start_idx = cleaned.find("{")
            end_idx = cleaned.rfind("}")
            if start_idx != -1 and end_idx != -1:
                cleaned = cleaned[start_idx:end_idx+1]
                
            lines = []
            for line in cleaned.split("\n"):
                line_str = line.strip()
                if line_str.startswith("//") or line_str.startswith("#"):
                    continue
                lines.append(line)
            cleaned = "\n".join(lines)
            return json.loads(cleaned)

        bullets = []
        try:
            data = extract_json_block(response_text)
            keys = [
                "Dataset Purpose",
                "Geographic Coverage",
                "Population Coverage",
                "Key Variables",
                "Sample Size / Scope",
                "Important Notes"
            ]
            for k in keys:
                val = data.get(k, "")
                if isinstance(val, list):
                    items = []
                    for item in val:
                        if isinstance(item, dict):
                            if "Variable" in item and "Description" in item:
                                items.append(f"{item['Variable']} ({item['Description']})")
                            elif "variable_name" in item and "label" in item:
                                items.append(f"{item['variable_name']} ({item['label']})")
                            elif "variable" in item and "label" in item:
                                items.append(f"{item['variable']} ({item['label']})")
                            else:
                                dict_str = ", ".join(f"{dk}: {dv}" for dk, dv in item.items())
                                items.append(dict_str)
                        else:
                            items.append(str(item))
                    val_str = ", ".join(items)
                elif isinstance(val, dict):
                    val_str = ", ".join(f"{dk}: {dv}" for dk, dv in val.items())
                else:
                    val_str = str(val).strip()
                    
                if not val_str or val_str.lower() in ("null", "none"):
                    val_str = "Not explicitly specified in the dataset documentation."
                bullets.append(f"{k}: {val_str}")
        except Exception as e:
            print(f"JSON parsing of LLM response failed: {e}. Falling back to line parsing.")
            lines = response_text.strip().split("\n")
            for line in lines:
                line_str = line.strip()
                if not line_str:
                    continue
                if line_str.startswith("#"):
                    continue
                    
                clean_line = line_str
                if clean_line.startswith(("•", "*", "-")):
                    clean_line = clean_line.lstrip("•*- ").strip()
                else:
                    match = re.match(r"^\d+[\.\)]\s*(.*)", clean_line)
                    if match:
                        clean_line = match.group(1).strip()
                
                if clean_line:
                    bullets.append(clean_line)
            
            if not bullets:
                bullets = [l.strip() for l in lines if l.strip()]
            
        # Write to cache
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(bullets, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Error writing local cache: {e}")
            
        return bullets
