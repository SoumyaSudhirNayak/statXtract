import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx


class NadaError(Exception):
    pass


@dataclass(frozen=True)
class NadaConfig:
    base_url: str
    timeout_s: float = 60.0
    max_retries: int = 3


def get_nada_config() -> NadaConfig:
    base_url = os.getenv("NADA_BASE_URL", "https://microdata.gov.in/NADA/index.php")
    return NadaConfig(base_url=base_url.rstrip("/"))


def _url_join(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


async def _request_json(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    timeout_s: float = 60.0,
    max_retries: int = 3,
) -> Any:
    last_exc: Exception | None = None
    retry_statuses = {429, 500, 502, 503, 504}
    for attempt in range(max_retries + 1):
        try:
            resp = await client.request(
                method,
                url,
                headers=headers,
                params=params,
                timeout=timeout_s,
            )
            if resp.status_code in retry_statuses and attempt < max_retries:
                await asyncio.sleep(min(2**attempt, 8))
                continue
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            status = e.response.status_code if e.response is not None else None
            if status in retry_statuses and attempt < max_retries:
                await asyncio.sleep(min(2**attempt, 8))
                continue
            raise
        except httpx.RequestError as e:
            last_exc = e
            if attempt < max_retries:
                await asyncio.sleep(min(2**attempt, 8))
                continue
            raise
    raise last_exc or RuntimeError("Unexpected NADA client failure")


async def _request_bytes(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    timeout_s: float = 300.0,
    max_retries: int = 3,
) -> bytes:
    last_exc: Exception | None = None
    retry_statuses = {429, 500, 502, 503, 504}
    for attempt in range(max_retries + 1):
        try:
            resp = await client.request(
                method,
                url,
                headers=headers,
                params=params,
                timeout=timeout_s,
            )
            if resp.status_code in retry_statuses and attempt < max_retries:
                await asyncio.sleep(min(2**attempt, 8))
                continue
            resp.raise_for_status()
            return resp.content
        except httpx.HTTPStatusError as e:
            status = e.response.status_code if e.response is not None else None
            if status in retry_statuses and attempt < max_retries:
                await asyncio.sleep(min(2**attempt, 8))
                continue
            raise
        except httpx.RequestError as e:
            last_exc = e
            if attempt < max_retries:
                await asyncio.sleep(min(2**attempt, 8))
                continue
            raise
    raise last_exc or RuntimeError("Unexpected NADA client failure")


async def nada_listdatasets(
    *,
    limit: int | None = None,
    offset: int | None = None,
    config: NadaConfig | None = None,
) -> Any:
    cfg = config or get_nada_config()
    url = _url_join(cfg.base_url, "api/listdatasets")
    page_size = 15
    req_limit = max(int(limit or page_size), 1)
    req_offset = max(int(offset or 0), 0)

    start_page = (req_offset // page_size) + 1
    end_page = ((req_offset + req_limit - 1) // page_size) + 1
    start_index = req_offset % page_size

    async with httpx.AsyncClient() as client:
        pages: list[dict[str, Any]] = []
        for page in range(start_page, end_page + 1):
            payload = await _request_json(
                client,
                "GET",
                url,
                params={"page": str(page)},
                timeout_s=cfg.timeout_s,
                max_retries=cfg.max_retries,
            )
            if isinstance(payload, dict):
                pages.append(payload)
            else:
                pages.append({"result": {"rows": payload}})

    rows: list[Any] = []
    found = None
    total = None
    for payload in pages:
        if not isinstance(payload, dict):
            continue
        result = payload.get("result") if isinstance(payload.get("result"), dict) else None
        if result:
            if found is None and result.get("found") is not None:
                found = result.get("found")
            if total is None and result.get("total") is not None:
                total = result.get("total")
            page_rows = result.get("rows")
            if isinstance(page_rows, list):
                rows.extend(page_rows)

    sliced = rows[start_index : start_index + req_limit]
    return {
        "result": {
            "found": found,
            "total": total,
            "limit": req_limit,
            "offset": req_offset,
            "rows": sliced,
        }
    }


async def nada_fileslist(
    dataset_id: str,
    *,
    api_key: str,
    config: NadaConfig | None = None,
) -> Any:
    cfg = config or get_nada_config()
    url = _url_join(cfg.base_url, f"api/datasets/{dataset_id}/fileslist")
    headers = {"X-API-KEY": api_key}
    async with httpx.AsyncClient() as client:
        return await _request_json(
            client,
            "GET",
            url,
            headers=headers,
            timeout_s=cfg.timeout_s,
            max_retries=cfg.max_retries,
        )


async def nada_download_file(
    dataset_id: str,
    file_no: str | int,
    *,
    api_key: str,
    dest_path: str | Path,
    config: NadaConfig | None = None,
) -> Path:
    cfg = config or get_nada_config()
    url = _url_join(cfg.base_url, f"api/fileslist/download/{dataset_id}/{file_no}")
    headers = {"X-API-KEY": api_key}
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient() as client:
        content = await _request_bytes(
            client,
            "GET",
            url,
            headers=headers,
            timeout_s=max(cfg.timeout_s, 300.0),
            max_retries=cfg.max_retries,
        )
    dest.write_bytes(content)
    return dest


def extract_files_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        for key in ("result", "data"):
            inner = payload.get(key)
            if isinstance(inner, dict):
                for list_key in ("files", "rows"):
                    maybe_list = inner.get(list_key)
                    if isinstance(maybe_list, list):
                        return [x for x in maybe_list if isinstance(x, dict)]
        for list_key in ("files", "rows"):
            maybe_list = payload.get(list_key)
            if isinstance(maybe_list, list):
                return [x for x in maybe_list if isinstance(x, dict)]
    return []


def guess_file_no(file_item: dict[str, Any]) -> str | None:
    for k in ("FileNo", "file_no", "fileNo", "fileno", "file_number", "id"):
        v = file_item.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def guess_file_name(file_item: dict[str, Any], fallback: str) -> str:
    for k in ("file_name", "filename", "FileName", "name", "title"):
        v = file_item.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return fallback
