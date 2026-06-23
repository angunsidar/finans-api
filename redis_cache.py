"""
Redis Cloud TCP bağlantısı — kalıcı stale cache.
Render restart/deploy sonrası bile son bilinen fiyatlar anında hazır olur.

Ortam değişkenleri:
  REDIS_HOST  → Redis Cloud public endpoint host
  REDIS_PORT  → Redis Cloud public endpoint port
  REDIS_PASSWORD → Redis Cloud default user password
"""
from __future__ import annotations
import os
import json
import logging

_logger = logging.getLogger("uvicorn.error")

_HOST     = os.getenv("REDIS_HOST", "")
_PORT     = int(os.getenv("REDIS_PORT", "6379"))
_PASSWORD = os.getenv("REDIS_PASSWORD", "")
_TTL      = 86400  # 24 saat

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    if not _HOST:
        return None
    try:
        import redis
        _client = redis.Redis(
            host=_HOST,
            port=_PORT,
            password=_PASSWORD,
            ssl=False,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
            retry_on_timeout=True,
            health_check_interval=30,   # 30sn'de bir bağlantıyı kontrol et
        )
        _client.ping()
        _logger.info(f"Redis Cloud bağlandı: {_HOST}:{_PORT}")
        return _client
    except Exception as e:
        _logger.warning(f"Redis Cloud bağlantı hatası: {e}")
        _client = None
        return None


def _reset_client():
    """Bağlantı hatası sonrası client'ı sıfırla → bir sonraki çağrıda yeniden bağlanır."""
    global _client
    _client = None


def rset(key: str, value: dict | list, ttl: int | None = None):
    """Redis'e yaz. TTL verilmezse 24 saat. Hata olursa sessizce geç."""
    r = _get_client()
    if not r:
        return
    try:
        r.set(key, json.dumps(value, ensure_ascii=False), ex=(ttl or _TTL))
    except Exception as e:
        _logger.debug(f"Redis rset hata ({key}): {e}")
        _reset_client()


def rget(key: str) -> dict | list | None:
    """Redis'ten oku. Yoksa veya hata varsa None döner."""
    r = _get_client()
    if not r:
        return None
    try:
        val = r.get(key)
        return json.loads(val) if val else None
    except Exception as e:
        _logger.debug(f"Redis rget hata ({key}): {e}")
        _reset_client()
        return None


def rget_many(keys: list[str]) -> dict[str, dict | list | None]:
    """Birden fazla key'i tek istekte çek (MGET)."""
    r = _get_client()
    if not r or not keys:
        return {}
    try:
        results = r.mget(keys)
        return {
            k: (json.loads(v) if v else None)
            for k, v in zip(keys, results)
        }
    except Exception as e:
        _logger.debug(f"Redis rget_many hata: {e}")
        _reset_client()
        return {}


def rset_many(data: dict, ttl: int | None = None) -> None:
    """Birden fazla key'i tek pipeline ile yaz — Bigpara/toplu cache için."""
    r = _get_client()
    if not r or not data:
        return
    try:
        t = ttl or _TTL
        pipe = r.pipeline()
        for key, value in data.items():
            pipe.set(key, json.dumps(value, ensure_ascii=False), ex=t)
        pipe.execute()
    except Exception as e:
        _logger.debug(f"Redis rset_many hata: {e}")
        _reset_client()


def rget_prefix(prefix: str) -> dict[str, dict | None]:
    """Belirli prefix'e sahip tüm key'leri tara ve değerlerini döndür."""
    r = _get_client()
    if not r:
        return {}
    try:
        keys = list(r.scan_iter(f"{prefix}*", count=500))
        if not keys:
            return {}
        return rget_many(keys)
    except Exception as e:
        _logger.debug(f"Redis rget_prefix hata ({prefix}): {e}")
        _reset_client()
        return {}
