import re
import hashlib
from sqlalchemy import create_engine, inspect

MAX_TABLE_LENGTH = 63

def sanitize_name(name: str) -> str:
    name = name.lower().replace(" ", "_").replace("-", "_")
    name = re.sub(r'\W+', '_', name)
    return name[:MAX_TABLE_LENGTH]

def _short_hash(value: str, length: int = 6) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:length]

def get_safe_table_name(base_name: str, db_url: str, dataset_id: str | None = None, salt: str | None = None) -> str:
    base = sanitize_name(base_name)

    if not dataset_id:
        return base

    prefix = sanitize_name(dataset_id)
    raw = base_name if salt is None else f"{base_name}:{salt}"
    suffix = _short_hash(f"{dataset_id}:{raw}", length=6)

    if not prefix:
        prefix = "ds"

    reserved = len(prefix) + 1 + 1 + len(suffix)  # prefix + "_" + "_" + suffix
    available_for_base = MAX_TABLE_LENGTH - reserved
    if available_for_base < 1:
        prefix = prefix[: max(1, MAX_TABLE_LENGTH - (1 + 1 + len(suffix)))]
        reserved = len(prefix) + 1 + 1 + len(suffix)
        available_for_base = max(1, MAX_TABLE_LENGTH - reserved)

    base = base[:available_for_base].strip("_") or "data"
    name = f"{prefix}_{base}_{suffix}"
    return name[:MAX_TABLE_LENGTH]
