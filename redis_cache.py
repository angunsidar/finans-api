"""
Upstash Redis REST API — kalıcı stale cache.
Render restart/deploy sonrası bile son bilinen fiyatlar anında hazır olur.
"""
from __future__ import annotations
import os, json
import httpx

_URL   = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
_TTL   = 86400  # 24 saat

def _ok() -> bool:
    return bool(_URL and _TOKEN)

def _headers() -> dict:
    return {"Authorization": f"Bearer {_TOKEN}", "Content-Type": "application/json"}

def rset(key: str, value: dict | list):
    """Redis'e yaz (TTL 24 saat). Hata olursa sessizce geç."""
    if not _ok():
        return
    try:
        httpx.post(
            _URL,
            headers=_headers(),
            content=json.dumps(["SET", key, json.dumps(value, ensure_ascii=False), "EX", str(_TTL)]),
            timeout=3,
        )
    except Exception:
        pass

def rget(key: str) -> dict | list | None:
    """Redis'ten oku. Yoksa veya hata varsa None döner."""
    if not _ok():
        return None
    try:
        r = httpx.post(
            _URL,
            headers=_headers(),
            content=json.dumps(["GET", key]),
            timeout=3,
        )
        val = r.json().get("result")
        return json.loads(val) if val else None
    except Exception:
        return None

def rget_prefix(prefix: str) -> dict[str, dict | None]:
    """Belirli prefix'e sahip tüm key'leri tara ve değerlerini döndür."""
    if not _ok():
        return {}
    try:
        # SCAN ile key'leri bul
        r = httpx.post(
            _URL,
            headers=_headers(),
            content=json.dumps(["KEYS", f"{prefix}*"]),
            timeout=5,
        )
        keys = r.json().get("result", [])
        if not keys:
            return {}
        return rget_many(keys)
    except Exception:
        return {}


def rget_many(keys: list[str]) -> dict[str, dict | list | None]:
    """Birden fazla key'i tek istekte çek (MGET)."""
    if not _ok() or not keys:
        return {}
    try:
        r = httpx.post(
            _URL,
            headers=_headers(),
            content=json.dumps(["MGET", *keys]),
            timeout=5,
        )
        results = r.json().get("result", [])
        return {
            k: (json.loads(v) if v else None)
            for k, v in zip(keys, results)
        }
    except Exception:
        return {}
