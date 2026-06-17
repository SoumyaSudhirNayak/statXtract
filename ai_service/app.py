from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import httpx
import os

try:
    from summarizer import summarize_text, check_lm_studio_health, get_model_name, LM_STUDIO_URL
except ImportError:
    from ai_service.summarizer import summarize_text, check_lm_studio_health, get_model_name, LM_STUDIO_URL

app = FastAPI(title="AI Summarizer Service (LM Studio)")

@app.on_event("startup")
async def startup_event():
    model_name = await get_model_name()
    print("LM Studio Provider: Active", flush=True)
    print(f"Detected Model: {model_name}", flush=True)

class SummarizeRequest(BaseModel):
    dataset_key: str
    text: str
    temperature: float = 0.4
    bypass_cache: bool = False

class SummarizeResponse(BaseModel):
    success: bool
    summary: list[str] = []
    error: str | None = None

@app.post("/summarize", response_model=SummarizeResponse)
async def summarize(req: SummarizeRequest):
    # Task 4 Health Check before generating summaries
    is_reachable = await check_lm_studio_health()
    if not is_reachable:
        return JSONResponse(
            status_code=503,
            content={"success": False, "error": "LM Studio server unavailable", "summary": []}
        )
        
    try:
        if not req.text.strip():
            return SummarizeResponse(success=True, summary=[])
            
        summary_points = await summarize_text(
            req.dataset_key,
            req.text,
            temperature=req.temperature,
            bypass_cache=req.bypass_cache
        )
        return SummarizeResponse(success=True, summary=summary_points)
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": str(e), "summary": []}
        )

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "provider": "LM Studio",
        "url": LM_STUDIO_URL
    }

@app.get("/test-model")
async def test_model():
    is_reachable = await check_lm_studio_health()
    if not is_reachable:
        raise HTTPException(status_code=503, detail="LM Studio server unavailable")
        
    model_name = await get_model_name()
    payload = {
        "model": model_name,
        "messages": [
            {
                "role": "user",
                "content": "Say hello"
            }
        ],
        "temperature": 0.2,
        "max_tokens": 50
    }
    
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.post(f"{LM_STUDIO_URL}/v1/chat/completions", json=payload)
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=f"LM Studio returned error: {resp.text}")
            result = resp.json()
            model_response = result["choices"][0]["message"]["content"]
            return {
                "model": model_name,
                "response": model_response
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
