"""
Türk yatırım fonu endpoint'leri.
Veri kaynağı: TEFAS JSON API (fonFiyatBilgiGetir)
  - Birim pay değeri (NAV), günlük getiri, fon adı
  - Statik Bearer token ile kimlik doğrulama — cookie/session gerektirmez
  - TEFAS her iş günü ~10:30 İstanbul saatinde güncellenir

Çekme zamanı:
  - Saat 10:30 öncesi → TEFAS'a gidilmez, stale/Redis veri döndürülür
  - Saat 10:30 sonrası → bugünün verisi yoksa TEFAS API'den çekilir
  - Bugünün verisi cache'e yazıldıktan sonra ertesi 10:30'a kadar bir daha gidilmez
"""
from __future__ import annotations

import logging
import time
import httpx
from datetime import date, datetime
from zoneinfo import ZoneInfo
from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(prefix="/fon", tags=["fon"])

_logger = logging.getLogger("uvicorn.error")
_TZ = ZoneInfo("Europe/Istanbul")
_TEFAS_SAAT = 630  # 10 * 60 + 30 — TEFAS'ın güncelleme saati


POPULER_FONLAR: dict[str, str] = {
    # Altın fonları
    "YKT": "Yapı Kredi Portföy Altın Fonu",
    "GTA": "Garanti Portföy Altın Fonu",
    "TTA": "İş Portföy Altın Fonu",
    "AFO": "Ak Portföy Altın Fonu",
    "TCA": "Ziraat Portföy Altın Katılım Fonu",
    "HBF": "HSBC Portföy Altın Fonu",
    "GOL": "Garanti Portföy Altın Katılım Fonu",
    "DBA": "Deniz Portföy Altın Fonu",
    # Hisse senedi fonları
    "MAC": "Marmara Capital Portföy Hisse Senedi Fonu",
    "TLY": "Tera Portföy Birinci Serbest Fon",
    "IPB": "İstanbul Portföy Birinci Değişken Fon",
    # BYF (Borsa Yatırım Fonları)
    "FGA": "QNB Portföy Altın Katılım BYF",
    "LTK": "Ak Portföy Altın Katılım BYF",
    "ILK": "İş Portföy Altın Katılım BYF",
    "ZGD": "Ziraat Portföy Altın Katılım BYF",
}

# ── Cache + stale fallback ────────────────────────────────────────────────────
# TTL sadece güvenlik ağı — asıl tazelik kontrolü veri tarihi üzerinden yapılır
_cache: dict[str, tuple[float, dict]] = {}
_stale: dict[str, dict] = {}
_TTL = 86400  # 24 saat


def _cache_get(key: str) -> dict | None:
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < _TTL:
            return val
    return None


def _cache_set(key: str, val: dict):
    _cache[key] = (time.time(), val)
    _stale[key] = val
    from redis_cache import rset
    rset(f"finans:fon:{key}", val)


# ── Saat + tarih yardımcıları ─────────────────────────────────────────────────

def _tefas_hazir() -> bool:
    """Saat 10:30 İstanbul geçti mi? TEFAS yalnızca o saatten sonra güncel veri yayınlar."""
    now = datetime.now(_TZ)
    return now.hour * 60 + now.minute >= _TEFAS_SAAT


def _bugunun_verisi_var_mi(kod: str) -> bool:
    """
    Cache veya stale'deki verinin tarihi bugüne mi ait?
    TEFAS T+1 yayınlar: Pazartesi kapanış → Salı 10:30'da yayınlanır.
    Bu yüzden hafta içi işlem günlerinde verinin tarihi bir gün öncesi olabilir.
    Önemli olan bugün TEFAS'tan alınmış olması — bunu ts (timestamp) ile izliyoruz.
    """
    if kod not in _cache:
        return False
    ts, _ = _cache[kod]
    # Cache yazılma saati bugün 10:30 sonrasıysa taze kabul et
    yazilma = datetime.fromtimestamp(ts, tz=_TZ)
    bugun = datetime.now(_TZ).date()
    return yazilma.date() == bugun and yazilma.hour * 60 + yazilma.minute >= _TEFAS_SAAT


# ── TEFAS JSON API ────────────────────────────────────────────────────────────
# Statik Bearer token — TEFAS JavaScript bundle'ına gömülü, tüm fonlarda aynı

_TEFAS_API_URL = "https://www.tefas.gov.tr/api/funds/fonFiyatBilgiGetir"
_TEFAS_BEARER  = "ST-tefaswebwse3irfmSBj4iRAzGPbAlS94Se"

_TEFAS_HEADERS = {
    "Authorization": f"Bearer {_TEFAS_BEARER}",
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://www.tefas.gov.tr",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}


def _fetch_tefas_api(kod: str) -> dict:
    """
    TEFAS JSON API'sinden birim pay değeri çek.
    POST /api/funds/fonFiyatBilgiGetir — statik Bearer token ile kimlik doğrulama.
    periyod=1 → son 1 aylık veri; son kayıt = güncel fiyat.
    """
    key = kod.upper()
    headers = {
        **_TEFAS_HEADERS,
        "Referer": f"https://www.tefas.gov.tr/tr/fon-detayli-analiz/{key}",
    }
    payload = {"fonKodu": key, "dil": "TR", "periyod": 1}

    with httpx.Client(timeout=20, follow_redirects=True) as client:
        resp = client.post(_TEFAS_API_URL, json=payload, headers=headers)
        resp.raise_for_status()

    data = resp.json()
    result_list = data.get("resultList") or []

    if not result_list:
        raise ValueError(f"TEFAS API boş sonuç: {key}")

    latest = result_list[-1]
    fiyat = float(latest["fiyat"])
    if fiyat == 0:
        raise ValueError(f"TEFAS API sıfır fiyat: {key}")

    # Günlük değişim — son iki kayıt üzerinden hesapla
    degisim = 0.0
    if len(result_list) >= 2:
        prev = float(result_list[-2]["fiyat"])
        if prev > 0:
            degisim = round((fiyat - prev) / prev * 100, 4)

    ad = latest.get("fonUnvan") or POPULER_FONLAR.get(key, key)

    return {
        "kod": key,
        "ad": ad,
        "fiyat": round(fiyat, 6),
        "degisim_yuzde": degisim,
        "para_birimi": "TRY",
        "tarih": latest.get("tarih", str(date.today())),
        "kaynak": "tefas_api",
    }


def _fetch(kod: str) -> dict | None:
    """
    Tek fon için veri döndür.
    - Memory cache tazeyse direkt döner
    - Redis'te varsa yükler; 10:30 öncesiyse veya bugün zaten çekildiyse döner
    - 10:30 sonrası + bugün henüz çekilmemişse TEFAS'a gider
    - Her durumda TEFAS'a ulaşamazsa stale döner
    """
    key = kod.upper()

    # 1. Memory cache — timestamp bazlı tazelik
    if _bugunun_verisi_var_mi(key):
        _, val = _cache[key]
        return val

    # 2. Redis
    from redis_cache import rget
    redis_val = rget(f"finans:fon:{key}")
    if redis_val:
        _cache[key] = (time.time(), redis_val)
        _stale[key] = redis_val
        # 10:30 öncesi veya bugün zaten çekildiyse Redis yeterli
        if not _tefas_hazir():
            return redis_val

    # 3. 10:30 öncesiyse TEFAS'a gitme
    if not _tefas_hazir():
        return _stale.get(key)

    # 4. 10:30 sonrası + cache miss → TEFAS JSON API'den çek
    try:
        result = _fetch_tefas_api(key)
        _cache_set(key, result)
        return result
    except Exception as e:
        _logger.warning(f"TEFAS API hata ({key}): {e}")
        return _stale.get(key)


def warm_up() -> list[str]:
    """
    Startup / background worker çağrısı.

    Saat 10:30 İstanbul öncesi:
      → TEFAS'a gidilmez, stale liste döner (önceki günün verisi yeterli)

    Saat 10:30 sonrası:
      → Bugün henüz çekilmemiş fonlar TEFAS'tan alınır
      → Bugün zaten çekilmişler atlanır (BG worker her 5 dk çağırır, tekrar gitmez)
    """
    if not _tefas_hazir():
        _logger.info("Fon warm_up atlandı — saat 10:30 öncesi")
        return list(_stale.keys())

    basarili: list[str] = []
    for kod in POPULER_FONLAR:
        if _bugunun_verisi_var_mi(kod):
            basarili.append(kod)
            continue
        try:
            result = _fetch_tefas_api(kod)
            _cache_set(kod, result)
            basarili.append(kod)
            _logger.debug(f"Fon warm_up ✓ {kod}")
        except Exception as e:
            _logger.warning(f"Fon warm_up ✗ {kod}: {e}")
    return basarili


# ── Endpoint'ler ──────────────────────────────────────────────────────────────

@router.get("/liste", summary="Popüler Türk yatırım fonları listesi")
def liste():
    return {
        "fonlar": [{"kod": k, "ad": a} for k, a in POPULER_FONLAR.items()],
    }


@router.get("/toplu", summary="Çoklu fon birim pay değeri")
def toplu_fiyat(
    fonlar: str = Query(None, description="Virgülle ayrılmış fon kodları. Örn: YAS,GAF,TKF"),
    kodlar: str = Query(None, description="fonlar ile aynı — eski isim desteği"),
):
    """
    Birden fazla fonu tek sorguda getir.
    Önce memory cache, sonra Redis, gerekirse TEFAS (10:30 sonrası).
    """
    raw = fonlar or kodlar
    if not raw:
        raise HTTPException(400, "En az bir fon kodu giriniz.")
    liste = [k.strip().upper() for k in raw.split(",") if k.strip()]
    if not liste:
        raise HTTPException(400, "En az bir fon kodu giriniz.")
    if len(liste) > 30:
        raise HTTPException(400, "En fazla 30 fon sorgulanabilir.")

    bulunanlar: dict[str, dict] = {}
    eksikler: list[str] = []

    # 1. Memory cache (timestamp bazlı)
    for kod in liste:
        if _bugunun_verisi_var_mi(kod):
            _, val = _cache[kod]
            bulunanlar[kod] = val
        else:
            eksikler.append(kod)

    # 2. Redis MGET
    if eksikler:
        from redis_cache import rget_many
        redis_vals = rget_many([f"finans:fon:{k}" for k in eksikler])
        hala_eksik: list[str] = []
        now = time.time()
        for k in eksikler:
            v = redis_vals.get(f"finans:fon:{k}")
            if v:
                _cache[k] = (now, v)
                _stale[k] = v
                # 10:30 öncesiyse Redis yeterli
                if not _tefas_hazir():
                    bulunanlar[k] = v
                else:
                    hala_eksik.append(k)
            else:
                hala_eksik.append(k)

        # 3. TEFAS (yalnızca 10:30 sonrası ve cache miss)
        for k in hala_eksik:
            result = _fetch(k)
            if result is not None:
                bulunanlar[k] = result

    veriler = []
    for k in liste:
        if k in bulunanlar:
            veriler.append(bulunanlar[k])
        else:
            veriler.append({"kod": k, "fiyat": None, "durum": "bulunamadı"})

    return {"sayı": len(liste), "veriler": veriler}


@router.get("/{kod}", summary="Tek fon birim pay değeri")
def fon_fiyat(kod: str):
    result = _fetch(kod)
    if result is None:
        raise HTTPException(404, f"Fon verisi alınamadı: {kod.upper()}")
    return result


@router.post("/seed", summary="GitHub Actions fon verisi yükle")
def seed_fon(payload: dict, request: Request):
    """
    GitHub Actions daily job'ı tarafından çağrılır (her iş günü 10:35 İstanbul).
    Fintables.com'dan çekilen fon verilerini Redis + bellek cache'e yazar.
    Payload'da yield_1m/3m/6m/ytd/1y/3y/5y alanları da kabul edilir.
    X-API-Key header gereklidir.
    """
    veriler = payload.get("veriler", [])
    if not veriler:
        raise HTTPException(400, "veriler listesi boş")

    basarili = []
    now = time.time()
    for item in veriler:
        kod = str(item.get("kod", "")).strip().upper()
        if not kod:
            continue
        # fiyat 0.0 olabilir (fintables birim fiyat vermiyor) — yine de kaydet
        if item.get("fiyat") is None and item.get("yield_1m") is None:
            continue
        _cache[kod] = (now, item)
        _stale[kod] = item
        from redis_cache import rset
        rset(f"finans:fon:{kod}", item)
        basarili.append(kod)

    _logger.info(f"Fon seed ✓ {basarili}")
    return {"basarili": basarili, "sayı": len(basarili)}
